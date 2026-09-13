"""Unit tests for worker-side lifecycle reporting in ``Runner``.

No real Modal account needed — ``modal.current_function_call_id`` and the
registry are faked. Verifies that the worker reports started (with its own
function call id as executor ref), completed (+ artifacts), suspended, and
failed events when a build id is forwarded via ``STARDAG_BUILD_ID`` — and
stays silent otherwise (no build id, NoOp registry, or opt-out).
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

try:
    import modal  # noqa: F401
except ImportError:
    pytest.skip("Skipping modal tests (import not available)", allow_module_level=True)

from stardag.integration.modal._metadata import (
    MODAL_EXECUTOR_NAME,
    STARDAG_BUILD_ID_ENV,
)
from stardag.integration.modal._runner import Runner, _WorkerLifecycleReporter
from stardag.registry import BuildNotifyResult, NoOpRegistry, registry_provider
from stardag.testing.modal._tasks import SyncDynamicRangeSumTask, make_range

WORKER_CALL_ID = "fc-worker-call-1"


class RecordingSyncRegistry(NoOpRegistry):
    """Records the sync lifecycle calls the worker reporter makes."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, dict]] = []
        self.raise_on: set[str] = set()

    def _record(self, method: str, **extra) -> None:
        if method in self.raise_on:
            raise ConnectionError(f"registry down during {method}")
        self.calls.append((method, extra))

    def task_start(
        self,
        build_id,
        task,
        executor=None,
        executor_ref=None,
        executor_metadata=None,
        claim_ttl_seconds=None,
    ) -> None:
        self._record(
            "task_start",
            build_id=build_id,
            executor=executor,
            executor_ref=executor_ref,
            executor_metadata=executor_metadata,
        )

    def task_complete(self, build_id, task) -> None:
        self._record("task_complete", build_id=build_id)

    def task_suspend(self, build_id, task) -> None:
        self._record("task_suspend", build_id=build_id)

    def task_fail(self, build_id, task, error_message=None) -> None:
        self._record("task_fail", build_id=build_id, error_message=error_message)

    def task_upload_artifacts(self, build_id, task, artifacts) -> None:
        self._record("task_upload_artifacts", artifacts=artifacts)

    def methods(self) -> list[str]:
        return [m for (m, _) in self.calls]


@pytest.fixture
def recording_registry():
    registry = RecordingSyncRegistry()
    with registry_provider.override(registry):
        yield registry


@pytest.fixture
def fake_call_id(monkeypatch):
    monkeypatch.setattr(modal, "current_function_call_id", lambda: WORKER_CALL_ID)
    return WORKER_CALL_ID


def _env(build_id: UUID) -> dict[str, str]:
    return {STARDAG_BUILD_ID_ENV: str(build_id)}


class TestRunnerLifecycleReporting:
    def test_success_reports_start_and_complete(
        self, recording_registry, fake_call_id, default_in_memory_fs_target
    ):
        build_id = uuid4()
        task = make_range(limit=3)

        result = Runner()(task, env_overrides=_env(build_id))

        assert result is None
        assert task.complete()
        assert recording_registry.methods() == ["task_start", "task_complete"]
        start_extra = recording_registry.calls[0][1]
        assert start_extra["build_id"] == build_id
        assert start_extra["executor"] == MODAL_EXECUTOR_NAME
        assert start_extra["executor_ref"] == WORKER_CALL_ID

    def test_failure_reports_fail_and_reraises(
        self, recording_registry, fake_call_id, default_in_memory_fs_target
    ):
        class Boom(Exception):
            pass

        class FailingRunner(Runner):
            def run(self, task):
                raise Boom("task exploded")

        with pytest.raises(Boom):
            FailingRunner()(make_range(limit=2), env_overrides=_env(uuid4()))

        assert recording_registry.methods() == ["task_start", "task_fail"]
        assert "task exploded" in recording_registry.calls[1][1]["error_message"]

    def test_dynamic_deps_yield_reports_suspend(
        self, recording_registry, fake_call_id, default_in_memory_fs_target
    ):
        # First invocation: the yielded range task is incomplete → the
        # runner returns the TaskStruct and the task is suspended.
        task = SyncDynamicRangeSumTask(limit=3)

        result = Runner()(task, env_overrides=_env(uuid4()))

        assert result is not None  # TaskStruct of incomplete deps
        assert recording_registry.methods() == ["task_start", "task_suspend"]

    def test_no_build_id_no_reporting(
        self, recording_registry, fake_call_id, default_in_memory_fs_target
    ):
        result = Runner()(make_range(limit=3))

        assert result is None
        assert recording_registry.calls == []

    def test_opt_out_no_reporting(
        self, recording_registry, fake_call_id, default_in_memory_fs_target
    ):
        result = Runner(report_lifecycle=False)(
            make_range(limit=3), env_overrides=_env(uuid4())
        )

        assert result is None
        assert recording_registry.calls == []

    def test_registry_errors_never_fail_the_task(
        self, recording_registry, fake_call_id, default_in_memory_fs_target
    ):
        recording_registry.raise_on = {"task_start", "task_complete"}
        task = make_range(limit=3)

        result = Runner()(task, env_overrides=_env(uuid4()))

        assert result is None
        assert task.complete()  # the actual work still succeeded


class TestReporterCreate:
    def test_none_without_build_id(self, recording_registry):
        assert _WorkerLifecycleReporter.create(make_range(limit=1), None) is None
        assert _WorkerLifecycleReporter.create(make_range(limit=1), {}) is None

    def test_none_with_noop_registry(self):
        with registry_provider.override(NoOpRegistry()):
            reporter = _WorkerLifecycleReporter.create(
                make_range(limit=1), _env(uuid4())
            )
        assert reporter is None

    def test_none_with_invalid_build_id(self, recording_registry):
        reporter = _WorkerLifecycleReporter.create(
            make_range(limit=1), {STARDAG_BUILD_ID_ENV: "not-a-uuid"}
        )
        assert reporter is None

    def test_build_id_from_process_env(self, recording_registry, monkeypatch):
        """Older deployed apps apply env_overrides as process env vars around
        the run call — the reporter also accepts the env-var form."""
        build_id = uuid4()
        monkeypatch.setenv(STARDAG_BUILD_ID_ENV, str(build_id))
        reporter = _WorkerLifecycleReporter.create(make_range(limit=1), None)
        assert reporter is not None
        assert reporter.build_id == build_id


class TestReportingRunsInsideEnvOverrides:
    def test_reporting_sees_env_overrides(
        self, recording_registry, fake_call_id, default_in_memory_fs_target
    ):
        """Lifecycle reporting runs inside the env-overrides context, so
        overrides carrying environment-sensitive config apply to reporting
        exactly as they do to run()."""
        import os

        seen: dict[str, str | None] = {}
        original_record = recording_registry._record

        def observing_record(method, **extra):
            seen[method] = os.environ.get("MY_TEST_OVERRIDE")
            return original_record(method, **extra)

        recording_registry._record = observing_record
        env = {**_env(uuid4()), "MY_TEST_OVERRIDE": "applied"}

        Runner()(make_range(limit=3), env_overrides=env)

        assert seen == {"task_start": "applied", "task_complete": "applied"}


class TestReporterCreationGuard:
    def test_broken_reporter_creation_never_fails_the_task(
        self, monkeypatch, default_in_memory_fs_target
    ):
        """The best-effort contract covers creation itself: a broken registry
        config in the worker env must not fail a task before it runs."""
        from stardag.integration.modal import _runner as runner_module

        def broken_create(task, env_overrides):
            raise RuntimeError("malformed registry config")

        monkeypatch.setattr(
            runner_module._WorkerLifecycleReporter,
            "create",
            staticmethod(broken_create),
        )
        task = make_range(limit=3)

        result = Runner()(task, env_overrides=_env(uuid4()))

        assert result is None
        assert task.complete()  # the work still ran


class TestReactiveWorkerBehavior:
    """In reactive mode the worker wakes the scheduler after terminal events
    and registers dynamically yielded deps itself (no resident orchestrator)."""

    def _reactive_env(self, build_id: UUID, app_name: str = "wake-app") -> dict:
        from stardag.integration.modal._metadata import (
            STARDAG_MODAL_APP_NAME_ENV,
            STARDAG_REACTIVE_ENV,
        )

        return {
            **_env(build_id),
            STARDAG_REACTIVE_ENV: "1",
            STARDAG_MODAL_APP_NAME_ENV: app_name,
        }

    @pytest.fixture
    def tick_spawn_stub(self, monkeypatch):
        captured: dict = {}

        class _Stub:
            def spawn(self, **kwargs):
                captured["spawn_kwargs"] = kwargs
                return "tick-handle"

        def from_name(**kwargs):
            captured["from_name"] = kwargs
            return _Stub()

        monkeypatch.setattr(modal.Function, "from_name", staticmethod(from_name))
        return captured

    def _notify_returning(
        self,
        registry,
        result,
        notified: list[UUID] | None = None,
    ) -> None:
        """Point the registry's ``build_notify`` at a canned answer."""

        def build_notify(build_id: UUID, *, can_spawn: bool = True):
            if notified is not None:
                notified.append(build_id)
            if isinstance(result, Exception):
                raise result
            return result

        registry.build_notify = build_notify  # type: ignore[method-assign]

    def test_complete_notifies_and_spawns_tick(
        self,
        recording_registry,
        fake_call_id,
        tick_spawn_stub,
        default_in_memory_fs_target,
    ):
        build_id = uuid4()
        notified: list[UUID] = []
        self._notify_returning(
            recording_registry,
            BuildNotifyResult(build_id=build_id, scheduler_live=False),
            notified,
        )

        Runner()(make_range(limit=3), env_overrides=self._reactive_env(build_id))

        assert notified == [build_id]
        assert tick_spawn_stub["from_name"] == {
            "app_name": "wake-app",
            "name": "tick",
        }
        assert tick_spawn_stub["spawn_kwargs"] == {"build_id": str(build_id)}

    def test_a_build_that_wants_no_tick_does_not_get_one(
        self,
        recording_registry,
        fake_call_id,
        tick_spawn_stub,
        default_in_memory_fs_target,
    ):
        """A build that is no longer RUNNING is not flagged by the notify,
        because it cannot act on a wake-up — and this worker spawning for it
        anyway is the cancelled-build loop: such a build's workers keep
        running until a tick stops them, and every one of them reports its
        way out through here."""
        build_id = uuid4()
        notified: list[UUID] = []
        self._notify_returning(
            recording_registry,
            BuildNotifyResult(
                build_id=build_id, needs_tick=False, scheduler_live=False
            ),
            notified,
        )

        Runner()(make_range(limit=3), env_overrides=self._reactive_env(build_id))

        assert notified == [build_id], "the registry is still told"
        assert tick_spawn_stub == {}, "but no tick is spawned for a dead build"

    def test_live_scheduler_sets_the_flag_without_spawning(
        self,
        recording_registry,
        fake_call_id,
        tick_spawn_stub,
        default_in_memory_fs_target,
    ):
        """The saving this exists for: a scheduler already holds the lease,
        so it will see the flag, and a tick spawned here would only pay a
        container start to discover the lease is held.

        Safe because the flag is set *before* the answer is evaluated, and
        the scheduler re-reads it on the way out (the tick's exit
        handshake) — see ``_wake_scheduler``."""
        build_id = uuid4()
        notified: list[UUID] = []
        self._notify_returning(
            recording_registry,
            BuildNotifyResult(build_id=build_id, scheduler_live=True),
            notified,
        )

        Runner()(make_range(limit=3), env_overrides=self._reactive_env(build_id))

        assert notified == [build_id], "the flag must still be set"
        assert tick_spawn_stub == {}, "no tick spawned while a scheduler is live"

    @pytest.mark.parametrize(
        "notify_result",
        [
            # A registry predating the field, which leaves the model
            # default None, and a notify that failed outright. Both mean
            # "unknown", and unknown spawns: the mistakes are not
            # symmetric — a redundant tick costs a container, a skipped one
            # costs the build its progress until the watchdog.
            BuildNotifyResult(),
            ConnectionError("registry down"),
        ],
        ids=["field-absent", "notify-failed"],
    )
    def test_unknown_scheduler_state_spawns_as_before(
        self,
        recording_registry,
        fake_call_id,
        tick_spawn_stub,
        default_in_memory_fs_target,
        notify_result,
    ):
        """Never skip on an answer we did not get: a redundant tick costs a
        container, a skipped one costs the build its progress until the
        watchdog."""
        build_id = uuid4()
        self._notify_returning(recording_registry, notify_result)

        Runner()(make_range(limit=3), env_overrides=self._reactive_env(build_id))

        assert tick_spawn_stub["spawn_kwargs"] == {"build_id": str(build_id)}

    def test_failure_also_wakes_scheduler(
        self,
        recording_registry,
        fake_call_id,
        tick_spawn_stub,
        default_in_memory_fs_target,
    ):
        class Boom(Exception):
            pass

        class FailingRunner(Runner):
            def run(self, task):
                raise Boom("nope")

        with pytest.raises(Boom):
            FailingRunner()(
                make_range(limit=2), env_overrides=self._reactive_env(uuid4())
            )

        assert "spawn_kwargs" in tick_spawn_stub

    def test_suspend_registers_dynamic_deps_and_persists(
        self,
        recording_registry,
        fake_call_id,
        tick_spawn_stub,
        default_in_memory_fs_target,
    ):
        """A dynamic-deps yield in reactive mode: the worker registers the
        yielded deps, persists their pickles for the scheduler, records the
        dynamic edges, suspends, and wakes the scheduler."""
        from stardag.build import BuildTaskStore

        build_id = uuid4()
        registered_bulk: list[str] = []
        added_edges: list[tuple[str, list[str]]] = []

        async def record_bulk(b, tasks, *, limit_keys=None):
            registered_bulk.extend(str(t.id) for t in tasks)
            return None

        def record_edges(b, task, upstream_tasks, is_dynamic=True):
            added_edges.append((str(task.id), [str(u.id) for u in upstream_tasks]))

        recording_registry.task_register_bulk_aio = record_bulk  # type: ignore[method-assign]
        recording_registry.task_add_dependencies = record_edges  # type: ignore[method-assign]

        parent = SyncDynamicRangeSumTask(limit=3)
        result = Runner()(parent, env_overrides=self._reactive_env(build_id))

        assert result is not None  # suspended on incomplete yielded dep
        assert recording_registry.methods()[-2:] == ["task_start", "task_suspend"]
        yielded_dep_id = registered_bulk[0]
        assert len(registered_bulk) == 1
        assert added_edges == [(str(parent.id), [yielded_dep_id])]
        # The scheduler can rehydrate the yielded dep.
        store = BuildTaskStore(build_id)
        assert store.load_task(yielded_dep_id) is not None
        # And was woken up.
        assert tick_spawn_stub["spawn_kwargs"] == {"build_id": str(build_id)}

    def test_non_reactive_does_not_wake(
        self,
        recording_registry,
        fake_call_id,
        tick_spawn_stub,
        default_in_memory_fs_target,
    ):
        Runner()(make_range(limit=3), env_overrides=_env(uuid4()))

        assert tick_spawn_stub == {}  # no tick spawn without reactive flag
