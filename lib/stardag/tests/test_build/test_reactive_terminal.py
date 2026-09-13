"""Terminal detection, external-blocker classification and the skip/cancel
remedies (stardag.build._reactive._terminal)."""

from __future__ import annotations

import asyncio
import typing
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4


import pytest

from stardag.build import (
    DetachedExecutionStatus,
    FailMode,
    TickConfig,
    run_tick_aio,
)
from stardag.build._reactive import (
    TickSummary,
    _skip_blocked,
)
from stardag.exceptions import NotFoundError
from stardag.registry import (
    NoOpRegistry,
)
from stardag.target import InMemoryFileTarget
from stardag.utils.testing.helper_tasks import SyncOnlyTask

from tests.test_build.reactive_fakes import (
    FAST_TICK,
    _chain,
    _setup,
)


class TestTerminalHandling:
    async def test_cancelled_build_cancels_running_refs(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        (root,) = _chain("cancelled-root")
        registry, executor, store = _setup([root], auto_complete=False)
        registry.add_task(
            str(root.id), status="running", executor="fake", executor_ref="fc-run"
        )
        registry.build_status = "cancelled"

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.outcome == "terminal"
        assert summary.terminal_status == "cancelled"
        assert executor.cancelled_refs == ["fc-run"]
        assert executor.spawned == []

    async def test_blocked_build_fails_instead_of_idling(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """CONTINUE mode with a failed upstream: nothing runnable/running →
        the tick fails the build rather than idling forever."""
        dep, root = _chain("blocked-dep", "blocked-root")
        registry, executor, store = _setup([dep, root], auto_complete=False)
        registry.add_task(str(dep.id), status="failed")

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.3,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.outcome == "terminal"
        assert summary.terminal_status == "failed"
        assert registry.build_status == "failed"

    async def test_fail_fast_cancels_running_and_fails(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        dep, root = _chain("ff-dep", "ff-root")
        other = SyncOnlyTask(name="ff-other")
        registry, executor, store = _setup([dep, root], auto_complete=False)
        store.save_task(other)
        registry.add_task(str(dep.id), status="failed")
        registry.add_task(
            str(other.id), status="running", executor="fake", executor_ref="fc-x"
        )
        executor.probe_statuses["fc-x"] = DetachedExecutionStatus.RUNNING

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,  # FAIL_FAST default
        )

        assert summary.outcome == "terminal"
        assert summary.terminal_status == "failed"
        assert executor.cancelled_refs == ["fc-x"]
        assert registry.build_status == "failed"


class TestAddedRootsTerminalDetection:
    async def test_added_roots_gate_completion(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Roots appended mid-build keep the build running until they too
        complete (previously completion of the original roots stranded
        re-triggered subtrees)."""
        (r1,) = _chain("roots-r1")
        r2 = SyncOnlyTask(name="roots-r2")
        registry, executor, store = _setup([r1])
        store.save_task(r2)
        # Original root completed already; new root appended (as the
        # re-trigger path does server-side) but still pending.
        registry.statuses[str(r1.id)] = "completed"
        await registry.build_add_roots_aio(uuid4(), [str(r2.id)])
        registry.add_task(str(r2.id), status="pending")

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )
        # r2 was spawned (auto-completes) and only then the build completed.
        assert executor.spawned == [r2.id]
        assert summary.terminal_status == "completed"


class TestCancelDynamicDepWindow:
    async def test_cancel_reaches_non_actionable_running(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """A RUNNING task inside the dynamic-dep window (incomplete upstream
        → not actionable) is still cancelled on build cancellation."""
        blocker = SyncOnlyTask(name="cxl-blocker")
        runner = SyncOnlyTask(name="cxl-runner")
        registry, executor, store = _setup([blocker, runner], auto_complete=False)
        registry.add_task(
            str(runner.id),
            status="running",
            upstreams={str(blocker.id)},  # dynamic edge, blocker incomplete
            executor="fake",
            executor_ref="fc-window",
        )
        registry.build_status = "cancelled"

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "cancelled"
        assert executor.cancelled_refs == ["fc-window"]


class TestCancelAuthority:
    """Whose executions a dying build may stop.

    Authority to revoke is build-scoped. The frontier cannot express that —
    ``running`` is every RUNNING task in the *plan*, which after plan
    closure includes tasks another build claimed — so the tick asks the
    registry which executions are its own.
    """

    async def test_a_neighbours_execution_is_left_alone(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The reported bug. The shared task is in this build's plan and
        RUNNING, but under another build: killing it would take out a live
        worker and release a claim this build never held."""
        (root,) = _chain("shared-root")
        registry, executor, store = _setup([root], auto_complete=False)
        registry.add_task(
            str(root.id), status="running", executor="fake", executor_ref="fc-theirs"
        )
        registry.status_build_id[str(root.id)] = uuid4()
        registry.build_status = "cancelled"

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "cancelled"
        assert summary.cancelled_refs == 0
        assert executor.cancelled_refs == []
        assert registry.statuses[str(root.id)] == "running"
        assert ("cancel", str(root.id)) not in registry.calls

    async def test_a_cascaded_cancel_still_stops_its_own_containers(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """``builds cancel --cascade`` releases the claims server-side, which
        takes the task out of both ``running`` and ``actionable`` while its
        container keeps going. Nothing reached it before this, so the claim
        was released and the execution was not stopped — which is what let a
        second build run the same task concurrently."""
        (root,) = _chain("cascaded-root")
        registry, executor, store = _setup([root], auto_complete=False)
        registry.add_task(
            str(root.id), status="cancelled", executor="fake", executor_ref="fc-mine"
        )
        registry.build_status = "cancelled"

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "cancelled"
        assert executor.cancelled_refs == ["fc-mine"]
        # Already CANCELLED in the registry: a second event would say
        # nothing the cascade has not already recorded.
        assert ("cancel", str(root.id)) not in registry.calls

    async def test_an_old_server_is_filtered_on_the_frontiers_owner_field(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """No executions route, but the frontier does report who holds each
        task — so the tick filters it itself rather than giving up."""
        mine, theirs = SyncOnlyTask(name="mine"), SyncOnlyTask(name="theirs")
        registry, executor, store = _setup([mine, theirs], auto_complete=False)
        registry.serves_executions = False
        for task, ref in ((mine, "fc-mine"), (theirs, "fc-theirs")):
            registry.add_task(
                str(task.id), status="running", executor="fake", executor_ref=ref
            )
        registry.status_build_id[str(theirs.id)] = uuid4()
        registry.build_status = "cancelled"

        await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert executor.cancelled_refs == ["fc-mine"]
        assert registry.statuses[str(theirs.id)] == "running"

    async def test_a_server_that_cannot_say_who_owns_keeps_the_old_behaviour(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Neither the route nor the owner field. None means "this server
        cannot say", not "not mine" — reading it as the latter would leave a
        build against an old registry unable to stop anything at all, which
        is strictly worse than what it does today."""
        (root,) = _chain("unknowable-root")
        registry, executor, store = _setup([root], auto_complete=False)
        registry.serves_executions = False
        registry.serves_status_build_id = False
        registry.add_task(
            str(root.id), status="running", executor="fake", executor_ref="fc-run"
        )
        registry.build_status = "cancelled"

        await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert executor.cancelled_refs == ["fc-run"]


class TestSkipBlockedOnFailure:
    async def test_fail_fast_skips_blocked_descendants(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """On a failure terminal the tick marks transitively blocked tasks
        skipped — they no longer dangle pending while the build is failed."""
        bad = SyncOnlyTask(name="skip-bad")
        mid = SyncOnlyTask(name="skip-mid", deps=(bad,))
        root = SyncOnlyTask(name="skip-root", deps=(mid,))
        registry, executor, store = _setup([bad, mid, root], auto_complete=False)
        registry.add_task(str(bad.id), status="failed")

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,  # FAIL_FAST default
        )

        assert summary.terminal_status == "failed"
        assert summary.skipped == 2
        assert registry.statuses[str(mid.id)] == "skipped"
        assert registry.statuses[str(root.id)] == "skipped"

    async def test_cancelled_branch_descendants_also_skipped(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """FAIL_FAST with a second, still-running branch: the cancel pass
        records TASK_CANCELLED (a cancelled task must not dangle RUNNING —
        workers killed by the executor's cancel can't self-report), so the
        cancelled branch's descendants land in the skip closure too."""
        bad = SyncOnlyTask(name="cb-bad")
        long_running = SyncOnlyTask(name="cb-running")
        downstream = SyncOnlyTask(name="cb-downstream", deps=(long_running,))
        root = SyncOnlyTask(name="cb-root", deps=(bad, downstream))
        registry, executor, store = _setup(
            [bad, long_running, downstream, root], auto_complete=False
        )
        registry.add_task(str(bad.id), status="failed")
        registry.add_task(
            str(long_running.id),
            status="running",
            executor="fake",
            executor_ref="ref-live",
        )
        store.save_task(long_running)

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,  # FAIL_FAST default
        )

        assert summary.terminal_status == "failed"
        assert executor.cancelled_refs == ["ref-live"]
        assert registry.statuses[str(long_running.id)] == "cancelled"
        assert registry.statuses[str(downstream.id)] == "skipped"
        assert registry.statuses[str(root.id)] == "skipped"

    async def test_blocked_terminal_in_continue_mode_also_skips(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        dep, root = _chain("skip-cont-dep", "skip-cont-root")
        registry, executor, store = _setup([dep, root], auto_complete=False)
        registry.add_task(str(dep.id), status="failed")

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.terminal_status == "failed"
        assert registry.statuses[str(root.id)] == "skipped"


class _SkipBlocked404Registry(NoOpRegistry):
    """Registry whose skip-blocked endpoint 404s with a given detail."""

    def __init__(self, detail: str):
        super().__init__()
        self.detail = detail

    async def build_skip_blocked_aio(self, build_id) -> list[str]:
        raise NotFoundError(
            "Skip blocked tasks: resource not found", detail=self.detail
        )


class TestSkipBlockedErrorHandling:
    async def test_missing_route_tolerated(self):
        """Old server without the endpoint (FastAPI default 404) → skip
        silently omitted, no raise."""
        summary = TickSummary(outcome="noop")
        await _skip_blocked(_SkipBlocked404Registry("Not Found"), uuid4(), summary)
        assert summary.skipped == 0

    async def test_app_level_404_reraised(self):
        """A 404 raised inside the endpoint (e.g. build no longer exists)
        signals a registry inconsistency and must propagate."""
        with pytest.raises(NotFoundError):
            await _skip_blocked(
                _SkipBlocked404Registry("Build not found"),
                uuid4(),
                TickSummary(outcome="noop"),
            )


class TestExternalBlockers:
    """Terminal detection when the blocker lives outside this build (#208 A1).

    Dependency gating is environment-global while ``running`` and
    ``status_counts`` are build-scoped, so a task another build is executing
    gates this build's tasks while appearing in neither count. Read as
    "nothing runnable, nothing running", that shape used to fail the build
    outright.
    """

    BLOCKER_ID = "blocking-task-id"

    def _blocked_build(
        self,
        *,
        blocker_status: str = "running",
        blocker_age_seconds: float | None = 60.0,
        blocker_expires_in_seconds: float | None = None,
        in_build: bool = False,
        blocker_attempts: int = 0,
        owner_build_id: "UUID | None" = None,
        owner_build_status: "str | None" = "running",
        owner_build_known: bool = True,
    ):
        """A build whose only task is gated by an upstream it does not own."""
        (root,) = _chain("ext-blocked-root")
        registry, executor, store = _setup([root], auto_complete=False)
        registry.add_blocking_task(
            self.BLOCKER_ID,
            blocks={str(root.id)},
            status=blocker_status,
            status_at=(
                None
                if blocker_age_seconds is None
                else datetime.now(timezone.utc) - timedelta(seconds=blocker_age_seconds)
            ),
            expires_at=(
                None
                if blocker_expires_in_seconds is None
                else datetime.now(timezone.utc)
                + timedelta(seconds=blocker_expires_in_seconds)
            ),
            in_build=in_build,
            attempt_count=blocker_attempts,
            owner_build_id=owner_build_id,
            owner_build_status=owner_build_status,
            owner_build_known=owner_build_known,
            namespace="pipelines",
            name="Ingest",
        )
        return root, registry, executor, store

    async def test_running_blocker_in_another_build_waits_instead_of_failing(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The exact A1 repro: this build has nothing actionable and nothing
        running because its task waits on an upstream another build is
        executing. That upstream's completion will unblock it, so the tick
        must wait (as it does for a concurrency-limit denial) rather than
        fail the build."""
        _, registry, executor, store = self._blocked_build()

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.outcome == "lingered_out"
        assert registry.build_status == "running"
        assert not any(method == "build_fail" for (method, _) in registry.calls)
        assert executor.spawned == []
        assert (summary.external_blockers, summary.external_blockers_waited) == (1, 1)
        assert summary.external_blockers_fatal == 0

    async def test_running_blocker_with_a_live_claim_waits(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The claim's expiry is in the future: somebody is executing it and
        the server still honours their claim. Wait — no lookup, no timer."""
        _, registry, executor, store = self._blocked_build(
            blocker_expires_in_seconds=3600
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.outcome == "lingered_out"
        assert summary.external_blockers_waited == 1
        assert summary.external_blockers_fatal == 0
        assert registry.build_status == "running"
        # A RUNNING blocker is decided from its claim alone.
        assert registry.build_get_calls == []

    async def test_running_blocker_with_a_lapsed_claim_fails(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The claim lapsed: it is not "presumed" abandoned, it provably is
        — the server will hand it to the next claimant. This build is not
        that claimant, so it still fails, but now with certainty, naming the
        blocking task and the build that owns it."""
        _, registry, executor, store = self._blocked_build(
            blocker_age_seconds=60 * 60 * 24,
            blocker_expires_in_seconds=-60,  # lapsed a minute ago
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "failed"
        assert summary.external_blockers_fatal == 1
        assert summary.external_blockers_waited == 0
        assert registry.build_get_calls == []
        message = registry.build_error_message or ""
        assert "pipelines.Ingest" in message  # the blocking task
        assert self.BLOCKER_ID in message
        assert str(registry.status_build_id[self.BLOCKER_ID]) in message  # its owner
        assert "execution claim lapsed" in message
        # A reset cannot take a claim, lapsed or not, so the remedy is a
        # release — and it is the only extra clause the remedy carries.
        assert "release it first" in message
        # Named in surfaces the reader has, not as a REST route they would have
        # to find a base URL and a token for.
        assert "'Release claim' action" in message
        assert "stardag tasks cancel <owning-build-id> <blocking-task-id>" in message
        assert "/api/v1" not in message
        # And it has to say *whose* build id: any build in the environment is
        # accepted, but addressing it to *this* build makes this build the owner
        # of the cancelled status, at which point the task stops being an
        # external blocker and the reset it would have got never happens.
        assert "the build that owns the blocker (named above), not this one" in message

    async def test_running_blocker_without_an_expiry_waits(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """NULL expiry — an older server, or a start recorded before the
        column. That is the server's own encoding of "never lapses", not
        evidence of death: waiting keeps a live blocker from failing a
        healthy build, and the log line says the wait cannot be shown to
        end. Deliberate; see _classify_external_blockers."""
        _, registry, executor, store = self._blocked_build(
            blocker_age_seconds=60 * 60 * 24 * 365,  # a year, and still waited on
            blocker_expires_in_seconds=None,
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.outcome == "lingered_out"
        assert summary.external_blockers_waited == 1
        assert summary.external_blockers_fatal == 0
        assert registry.build_status == "running"

    @pytest.mark.parametrize("owner_build_status", ["completed", "running"])
    async def test_a_running_blockers_owning_build_is_never_consulted(
        self,
        owner_build_status: str,
        default_in_memory_fs_target: typing.Type[InMemoryFileTarget],
    ):
        """A live owning build proves nothing about one of its claims, and a
        terminal one does not release them — so for a RUNNING blocker the
        owner's status is not consulted in either direction."""
        _, registry, executor, store = self._blocked_build(
            blocker_expires_in_seconds=3600,
            owner_build_status=owner_build_status,
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.outcome == "lingered_out"
        assert registry.build_get_calls == []

    @pytest.mark.parametrize("blocker_status", ["pending", "suspended"])
    async def test_owner_driven_blocker_of_a_terminal_build_fails_immediately(
        self,
        blocker_status: str,
        default_in_memory_fs_target: typing.Type[InMemoryFileTarget],
    ):
        """PENDING and SUSPENDED say "the owning build is going to move this".
        Once that build has gone terminal, nobody is, and waiting would be
        waiting forever. Fail now, naming the task, the build that owns it and
        the one remedy there is."""
        _, registry, executor, store = self._blocked_build(
            blocker_status=blocker_status, owner_build_status="completed"
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "failed"
        assert summary.external_blockers_fatal == 1
        assert summary.external_blockers_waited == 0
        message = registry.build_error_message or ""
        assert "pipelines.Ingest" in message
        assert blocker_status.upper() in message
        assert f"under build {registry.status_build_id[self.BLOCKER_ID]}" in message
        # One remedy, since the blocker is in this build's plan: re-trigger.
        # Only a RUNNING blocker needs the cancel-first hint.
        assert "Re-trigger this build" in message
        assert "release it first" not in message

    @pytest.mark.parametrize("owner_build_status", ["running", "pending"])
    async def test_non_running_blocker_of_a_live_build_waits(
        self,
        owner_build_status: str,
        default_in_memory_fs_target: typing.Type[InMemoryFileTarget],
    ):
        """A PENDING blocker belonging to a build that is still live *will* be
        scheduled by that build — failing here would be a brand-new spurious
        failure, the very class of bug being fixed. A build the server still
        reports as pending counts as live too: it may yet start, and the
        staleness bound is what keeps that from being an unbounded wait."""
        _, registry, executor, store = self._blocked_build(
            blocker_status="pending", owner_build_status=owner_build_status
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.outcome == "lingered_out"
        assert registry.build_status == "running"
        assert summary.external_blockers_waited == 1
        assert summary.external_blockers_fatal == 0

    @pytest.mark.parametrize("blocker_status", ["pending", "suspended"])
    async def test_a_non_running_blocker_carries_no_expiry_so_the_owner_is_asked(
        self,
        blocker_status: str,
        default_in_memory_fs_target: typing.Type[InMemoryFileTarget],
    ):
        """The two-row collapse applies to RUNNING blockers ONLY. A SUSPENDED
        (or PENDING) task holds no execution claim, so the server clears the
        expiry with it and there is nothing to read — the owning-build lookup
        is not kept out of caution, it is the only evidence that exists. The
        abandoned-SUSPENDED wedge is real and still has to be decided."""
        _, registry, executor, store = self._blocked_build(
            blocker_status=blocker_status,
            blocker_age_seconds=60 * 60 * 24 * 30,  # age is not the question
            owner_build_status="running",
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.outcome == "lingered_out"
        assert summary.external_blockers_waited == 1
        assert len(registry.build_get_calls) == 1  # the lookup did happen

    async def test_blocker_with_no_status_owning_build_fails(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """No build owns the blocker's status (a row predating status
        denormalisation), so no status-moving event was ever recorded against
        it — there is no evidence anyone intends to run it, and no build to
        ask. Fail, with the remediation intact."""
        _, registry, executor, store = self._blocked_build(blocker_status="pending")
        registry.status_build_id[self.BLOCKER_ID] = None

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "failed"
        assert summary.external_blockers_fatal == 1
        assert registry.build_get_calls == []  # nothing to look up
        message = registry.build_error_message or ""
        assert "no build owns its status" in message
        # The remedy needs no owning build id to be actionable: the blocker is
        # in *this* build's plan, so re-triggering *this* build resets it.
        assert "Re-trigger this build" in message

    async def test_failed_owner_lookup_fails_without_propagating(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """An unresolvable owner (deleted build, unreachable registry) is not
        evidence of life: the build fails with a precise message rather than
        the lookup error escaping the tick."""
        _, registry, executor, store = self._blocked_build(
            blocker_status="pending", owner_build_known=False
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "failed"
        assert summary.external_blockers_fatal == 1
        assert "status is unknown" in (registry.build_error_message or "")

    async def test_server_not_reporting_build_status_fails(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Same treatment for a server (or custom registry) that doesn't
        report the derived build status: the field defaults to None, unknown
        is not evidence of life, and the message says so."""
        _, registry, executor, store = self._blocked_build(
            blocker_status="pending", owner_build_status=None
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "failed"
        assert "status is unknown" in (registry.build_error_message or "")

    async def test_owner_status_is_resolved_once_per_build(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """A wide DAG stalled behind one build yields one blocker entry per
        blocked edge, all naming the same owner — that must cost one request,
        not one per entry."""
        root, registry, executor, store = self._blocked_build(
            blocker_status="pending", owner_build_id=(owner := uuid4())
        )
        sibling = SyncOnlyTask(name="ext-memo-sibling")
        store.save_task(sibling)
        registry.add_task(str(sibling.id), status="pending")
        registry.add_blocking_task(
            "second-blocker",
            blocks={str(sibling.id)},
            status="pending",
            owner_build_id=owner,  # same owner as the first blocker
            namespace="pipelines",
            name="Transform",
        )
        registry.build_get_calls.clear()

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(linger_seconds=0.0, poll_interval_seconds=0.01),
        )

        assert summary.external_blockers_waited == 2
        assert registry.build_get_calls == [owner]

    async def test_cancelled_in_build_blocker_is_retried_not_failed(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Builds collaborate: a task in *this build's own plan* is this
        build's to run, whatever build last touched it.

        The motivating shape is fail-fast. Build A starts a shared task,
        hits an unrelated failure, and cascade-cancels the tasks it started
        — correctly, since it owns those claims. The cancel releases the
        claim, which is the whole point. But the task is left CANCELLED,
        which is not schedulable, so build B — which shares the dependency
        and has failed at nothing — used to die on it too. One build's
        fail-fast became every overlapping build's failure.

        Nothing owns the task now: no claim, no live execution. It is in B's
        plan, so B resets it and runs it.
        """
        _, registry, executor, store = self._blocked_build(
            blocker_status="cancelled", in_build=True
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        # Reset, not failed; see ``test_a_reset_blocker_runs_in_the_same_tick``
        # for the follow-through.
        assert summary.terminal_status is None
        assert ("retry", self.BLOCKER_ID) in registry.calls
        assert registry.statuses[self.BLOCKER_ID] == "pending"
        assert registry.build_error_message is None

    async def test_a_reset_blocker_runs_in_the_same_tick(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Resetting a blocker must not end the pass.

        There is no later tick to pick it up. The registry's wake-up flag
        deliberately skips the build whose own event caused a change — it is
        the one that already knows — so a tick that reset a blocker and then
        lingered would be waiting for news it had already heard, exit on its
        deadline, and leave the build with nothing running, nothing
        scheduled, and no flag to be handed out on. Until the watchdog, if
        one is even deployed.

        Not a theoretical path. It is what a build does whenever a shared
        task is genuinely left CANCELLED — which only became the common
        outcome once cancelling a build started stopping its containers
        instead of letting them run on and complete the task anyway.

        This fixture uses a *real* blocking task, unlike the classification
        tests above: the point here is that the tick goes on to run it.
        """
        blocker, root = _chain("reset-blocker", "reset-blocked-root")
        registry, executor, store = _setup([blocker, root], auto_complete=False)
        registry.add_blocking_task(
            str(blocker.id),
            blocks={str(root.id)},
            status="cancelled",
            in_build=True,
            attempt_count=0,
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.in_build_blockers_reset == 1
        assert executor.spawned == [blocker.id], (
            "the reset blocker must be spawned by the pass that reset it: "
            f"{executor.spawned}"
        )
        assert summary.iterations >= 2, (
            "the tick must re-read the frontier after resetting a blocker, "
            "not linger on a snapshot it has already invalidated"
        )

    async def test_a_reset_that_fails_does_not_count_as_progress(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The counter is read as "the frontier changed, act again now".

        So a reset that raised must not increment it. If it did, the tick
        would loop straight back, re-read the same blocker, fail the same
        reset and refresh its own linger deadline — spinning for as long as
        the retry keeps failing, which one transient registry error is
        enough to start.
        """
        _, registry, executor, store = self._blocked_build(
            blocker_status="cancelled", in_build=True
        )
        registry.retry_by_id_error = RuntimeError("registry unavailable")

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.in_build_blockers_reset == 0
        assert ("retry-failed", self.BLOCKER_ID) in registry.calls
        # One pass, then the linger — not a spin.
        assert summary.iterations == 1, (
            "a failed reset sent the tick round the loop again: "
            f"{summary.iterations} iterations"
        )

    async def test_a_shared_cancelled_blocker_is_reset_once_per_task(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The frontier reports one entry per (blocked, blocker) *edge*.

        A cancelled upstream shared by several of this build's tasks therefore
        arrives once per dependent — the normal shape for the fan-out this path
        exists to unblock, and a diamond is enough to produce it. Resetting per
        entry would call retry N times on one task: the calls after the first
        hit a row that is already PENDING, so they fail and log, and
        ``in_build_blockers_reset`` would count edges rather than tasks.
        """
        root, registry, executor, store = self._blocked_build(
            blocker_status="cancelled", in_build=True
        )
        # A second task of this build gated by the *same* blocker.
        sibling = SyncOnlyTask(name="shared-blocker-sibling")
        store.save_task(sibling)
        registry.add_task(str(sibling.id), status="pending")
        registry.upstreams.setdefault(str(sibling.id), set()).add(self.BLOCKER_ID)

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.terminal_status is None
        # Two edges reported, one task reset.
        assert summary.external_blockers == 0  # short-circuited by the reset
        retries = [
            call for call in registry.calls if call == ("retry", self.BLOCKER_ID)
        ]
        assert len(retries) == 1, registry.calls
        assert summary.in_build_blockers_reset == 1
        assert registry.statuses[self.BLOCKER_ID] == "pending"

    async def test_in_build_retry_respects_the_attempt_budget(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Resetting an in-plan blocker must not become an infinite loop.

        A task that genuinely fails every time would otherwise be reset,
        rerun and re-failed forever. The budget that already bounds ordinary
        retries bounds this one too, and the build fails once it is spent —
        which is the honest outcome, since nothing will move the task.
        """
        _, registry, executor, store = self._blocked_build(
            blocker_status="cancelled", in_build=True, blocker_attempts=5
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
                max_attempts=2,
            ),
        )

        assert summary.terminal_status == "failed"
        assert ("retry", self.BLOCKER_ID) not in registry.calls
        message = registry.build_error_message or ""
        assert "Blocked by" in message
        assert "attempt budget in this build is spent" in message

    @pytest.mark.parametrize("blocker_status", ["failed", "skipped"])
    async def test_a_result_in_this_builds_plan_is_left_to_fail_mode(
        self,
        blocker_status: str,
        default_in_memory_fs_target: typing.Type[InMemoryFileTarget],
    ):
        """S1, and the line the collaboration rule stops at: a task in this
        build's plan is this build's to *run*, but only a CANCELLED status is
        a revocation of permission to run. FAILED and SKIPPED are results.

        Build A ran the shared task and it failed. Nothing about that failure
        belongs to B: resetting it would rerun a task that just told the
        environment it does not work, on nobody's request, and would override
        the ``fail_mode`` B was triggered with. B fails instead, naming the
        blocker — and a re-trigger, where the user *does* ask, resets it.

        Budget deliberately intact (attempts 0 of 2): the point is the status,
        not an exhausted retry allowance.
        """
        _, registry, executor, store = self._blocked_build(
            blocker_status=blocker_status, in_build=True, blocker_attempts=0
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                # CONTINUE, or FAIL_FAST would fail the build on the status
                # count before terminal detection ever looks at a blocker.
                fail_mode=FailMode.CONTINUE,
                max_attempts=2,
            ),
        )

        assert summary.terminal_status == "failed"
        assert ("retry", self.BLOCKER_ID) not in registry.calls
        assert registry.statuses[self.BLOCKER_ID] == blocker_status
        # A result influences neither half of the wait-or-fail decision: the
        # build's own terminal logic owns the outcome.
        assert summary.external_blockers == 1
        assert summary.external_blockers_waited == 0
        assert summary.external_blockers_fatal == 0
        assert summary.in_build_blockers_reset == 0
        message = registry.build_error_message or ""
        assert "pipelines.Ingest" in message
        assert "a result rather than a revocation" in message
        assert "Re-trigger this build" in message

    def _suspended_shared_task(
        self, *, owner_build_status: str, child_claim_lapsed: bool
    ):
        """S2's shape: a shared task suspended on a dynamic child of its own.

        The child is registered into the *owning* build's plan only, which is
        what happens when this build registered before the owner's worker
        yielded. It also keeps the suspended parent out of ``actionable``: a
        suspended task with every upstream complete is simply schedulable, and
        that is not the state S2 is about.
        """
        _, registry, executor, store = self._blocked_build(
            blocker_status="suspended",
            in_build=True,
            owner_build_id=(owner := uuid4()),
            owner_build_status=owner_build_status,
        )
        registry.add_blocking_task(
            "dynamic-child",
            blocks={self.BLOCKER_ID},
            status="running",
            owner_build_id=owner,
            # Same owner, so the same status — the fake keeps one entry per
            # build and the default would overwrite the parent's.
            owner_build_status=owner_build_status,
            expires_at=datetime.now(timezone.utc)
            + timedelta(minutes=-1 if child_claim_lapsed else 60),
            namespace="pipelines",
            name="Child",
        )
        return registry, executor, store

    async def test_suspended_blocker_in_this_builds_plan_is_waited_on(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """S2: build A is mid-flight through the shared task's dynamic deps.

        A ran the shared task, its worker registered dynamic dependencies,
        yielded, and returned — leaving the task SUSPENDED with the children
        in A's plan. The task is in B's plan too, but resetting it would rerun
        all of its pre-yield work for nothing while A is legitimately making
        progress, and would race A's own scheduling of the children.

        So B waits. Bounded, not open-ended: SUSPENDED persists only while A
        progresses the children, and A stalling on one of them stalls on a
        RUNNING task whose claim expires.
        """
        registry, executor, store = self._suspended_shared_task(
            owner_build_status="running", child_claim_lapsed=False
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.terminal_status is None
        assert registry.build_status == "running"
        assert ("retry", self.BLOCKER_ID) not in registry.calls
        assert registry.statuses[self.BLOCKER_ID] == "suspended"
        assert summary.in_build_blockers_reset == 0
        # Both edges are waited on: the suspended parent (its owner is live)
        # and the running child (its claim is live).
        assert summary.external_blockers_waited == 2
        assert summary.external_blockers_fatal == 0

    async def test_suspended_blocker_of_a_dead_owner_fails(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The other end of S2's bound, and why the wait is not a hang.

        The owning build died while its dynamic child was running, so the
        child's claim lapses and the suspended parent has nobody left to
        progress it. Both are fatal on their own evidence — the claim for the
        child, the owner's status for the parent — and the build fails naming
        them instead of waiting forever. A re-trigger resets the whole chain.
        """
        registry, executor, store = self._suspended_shared_task(
            owner_build_status="failed", child_claim_lapsed=True
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.terminal_status == "failed"
        assert ("retry", self.BLOCKER_ID) not in registry.calls
        assert summary.external_blockers_fatal == 2
        assert summary.external_blockers_waited == 0
        message = registry.build_error_message or ""
        assert "its owning build is failed" in message  # the suspended parent
        assert "execution claim lapsed" in message  # its dynamic child

    async def test_in_build_blocker_that_cannot_be_retried_still_fails(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Not every in-plan blocker is recoverable. One in a status no
        retry moves is named and the build fails, rather than idling."""
        _, registry, executor, store = self._blocked_build(
            blocker_status="unregistered", in_build=True
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(
                linger_seconds=0.2,
                poll_interval_seconds=0.01,
                fail_mode=FailMode.CONTINUE,
            ),
        )

        assert summary.terminal_status == "failed"
        assert summary.external_blockers == 1
        assert summary.external_blockers_waited == 0
        assert summary.external_blockers_fatal == 0
        message = registry.build_error_message or ""
        assert "No runnable or running tasks left" in message
        assert "Blocked by" in message
        assert "pipelines.Ingest" in message

    async def test_a_fatal_blocker_wins_over_a_waitable_one(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """One blocker nothing will ever run means the build cannot complete,
        whatever else it is also waiting on."""
        root, registry, executor, store = self._blocked_build()
        registry.add_blocking_task(
            "dead-blocker",
            blocks={str(root.id)},
            status="suspended",
            owner_build_status="cancelled",
            namespace="pipelines",
            name="Abandoned",
            status_at=datetime.now(timezone.utc),
        )

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "failed"
        assert summary.external_blockers == 2
        assert summary.external_blockers_waited == 1
        assert summary.external_blockers_fatal == 1
        # Only the fatal one is named as the reason to fail.
        message = registry.build_error_message or ""
        assert "pipelines.Abandoned" in message
        assert "pipelines.Ingest" not in message

    async def test_truncated_blocker_list_is_flagged_in_the_message(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The server caps its blocker list; the failure must not read as an
        exhaustive account of what is holding the build back."""
        _, registry, executor, store = self._blocked_build(
            blocker_status="suspended", owner_build_status="failed"
        )
        registry.blocked_by_external_truncated = True

        await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        message = registry.build_error_message or ""
        assert "capped the blocker list" in message

    async def test_older_server_without_the_fields_behaves_exactly_as_before(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """A server predating blocked_by_external leaves the fields at their
        defaults, and terminal detection degrades to the pre-fix failure —
        the bug is unfixable client-side there, but nothing regresses."""
        _, registry, executor, store = self._blocked_build()
        registry.serves_blocked_by_external = False

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "failed"
        assert summary.external_blockers == 0
        assert summary.external_blockers_waited == 0
        assert summary.external_blockers_fatal == 0
        assert "No runnable or running tasks left" in (
            registry.build_error_message or ""
        )

    async def test_healthy_build_never_evaluates_blockers(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """The frontier reports blockers only while a build looks stalled, so
        a build that runs to completion records none."""
        dep, root = _chain("ext-healthy-dep", "ext-healthy-root")
        registry, executor, store = _setup([dep, root])

        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=FAST_TICK,
        )

        assert summary.terminal_status == "completed"
        assert summary.external_blockers == 0

    async def test_blocker_completion_releases_the_waiting_build(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """End of the wait: once the owning build completes the blocker (and
        wakes this scheduler), the same lingering tick schedules the task it
        would previously have failed the build over."""
        root, registry, executor, store = self._blocked_build()
        registry.auto_complete = True

        async def complete_blocker_soon() -> None:
            await asyncio.sleep(0.05)
            registry.statuses[self.BLOCKER_ID] = "completed"
            registry.needs_tick = True

        waiter = asyncio.create_task(complete_blocker_soon())
        summary = await run_tick_aio(
            uuid4(),
            registry=registry,
            task_executor=executor,
            task_store=store,
            config=TickConfig(linger_seconds=1.0, poll_interval_seconds=0.01),
        )
        await waiter

        assert summary.terminal_status == "completed"
        assert executor.spawned == [root.id]
        assert summary.external_blockers_waited >= 1


class TestBlockerAgeFormatting:
    """The blocker message is what a stalled build's owner acts on."""

    def test_age_is_human_readable_not_raw_seconds(self):
        from stardag.build._reactive import _format_age

        # Found in a live run: "RUNNING for 10889s" made the reader do
        # arithmetic before they could judge whether it was alarming.
        assert _format_age(10889) == "3h 1m"
        assert _format_age(5) == "5s"
        assert _format_age(95) == "1m"
        assert _format_age(3600) == "1h"
        assert _format_age(90000) == "1d 1h"

    def test_discovery_is_bounded_lower_than_frontier_actions(self):
        """Discovery is limited by the target backend, not the registry.

        Measured live: a 64-task layer against a Modal volume completed at 16
        in flight, stalled at 32 and failed at 50. Sharing one constant with
        the registry-bound actions conflated two different ceilings.
        """
        from stardag.build._reactive import (
            _DEFAULT_MAX_CONCURRENCY,
            _DEFAULT_MAX_CONCURRENT_DISCOVER,
        )

        assert _DEFAULT_MAX_CONCURRENT_DISCOVER < _DEFAULT_MAX_CONCURRENCY
