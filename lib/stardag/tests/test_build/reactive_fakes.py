"""Shared test doubles for the reactive-scheduling test modules.

`FakeReactiveRegistry` mirrors the API's frontier semantics (dependency
gating on task statuses), driven entirely by the tick's own event calls;
`FakeTickExecutor`'s "workers" complete instantly (simulating worker-side
lifecycle reporting + wake-up). Split out of the former monolithic
test_reactive.py so the per-module test files (test_reactive_tick,
_discovery, _frontier_actions, _terminal, _budgets) share one set of
doubles instead of five drifting copies.
"""

from __future__ import annotations

import typing
from datetime import datetime
from uuid import UUID, uuid4


from stardag import (
    BaseTask,
    auto_namespace,
    flatten_task_struct,
)
from stardag.build import (
    BuildTaskStore,
    DetachedExecutionStatus,
    DetachedHandle,
    TaskExecutorABC,
    TickConfig,
)
from stardag.build._reactive import (
    _RETRYABLE_STATUSES,
)
from stardag.exceptions import NotFoundError
from stardag.registry import (
    SchedulerLeaseResult,
    BuildExecution,
    BuildExecutions,
    BuildFrontier,
    BuildNotifyResult,
    FrontierExternalBlocker,
    FrontierTaskRef,
    NoOpRegistry,
    RegisteredTaskInfo,
)
from stardag.utils.testing.helper_tasks import SyncOnlyTask

auto_namespace(__name__)

FAST_TICK = TickConfig(linger_seconds=0.3, poll_interval_seconds=0.01)


# =============================================================================
# Fakes
# =============================================================================


class FakeReactiveRegistry(NoOpRegistry):
    """In-memory registry with frontier semantics mirroring the API.

    State transitions are driven by the tick's own calls (and by tests
    setting up preconditions). ``auto_complete`` simulates instant workers:
    a task transitions straight to completed when its start is recorded,
    with the wake-up flag set (as a self-reporting worker would).
    """

    def __init__(
        self,
        *,
        root_task_ids: list[str],
        auto_complete: bool = False,
    ) -> None:
        super().__init__()
        self.root_task_ids = root_task_ids
        self.auto_complete = auto_complete
        # The reactive marker/owner, surfaced on the frontier. Presence is
        # the marker (defaults to a reactive build); tests set it to None to
        # simulate a non-reactive build.
        self.reactive_app_name: str | None = "test-app"
        self.reactive_tick_kwargs: dict | None = {}
        self.statuses: dict[str, str] = {}
        self.upstreams: dict[str, set[str]] = {}
        self.refs: dict[str, tuple[str | None, str | None]] = {}
        self.start_metadata: dict[str, dict | None] = {}
        self.needs_tick = False
        # Scheduler lease state (see build_acquire_scheduler_lease_aio).
        self.lease_acquired = True
        self.lease_owner: str | None = None
        # Our own lease expired between renewals, with nobody having taken
        # it: renew refuses, re-acquire succeeds. The SDK heals and carries
        # on.
        self.lease_lapsed = False
        # A successor holds the lease: renew refuses and so does the
        # re-acquire, so the tick must stop. Applies from the *second*
        # acquire onwards, since the tick has to get the lease before it
        # can lose it.
        self.lease_stolen = False
        self._lease_acquires = 0
        self.lease_on_release: typing.Callable[[], None] | None = None
        self.build_status = "running"
        self.build_error_message: str | None = None
        self.calls: list[tuple[str, str | None]] = []
        # Named concurrency limits: key -> cap; holders tracked per task.
        self.limits: dict[str, int] = {}
        self.task_limit_keys: dict[str, set[str]] = {}
        # Limit keys as sent on each claiming start: task_id -> keys.
        self.claim_limit_keys: dict[str, list[str]] = {}
        self.status_at: dict[str, datetime] = {}
        # task_id -> when its RUNNING execution claim lapses. Absent = the
        # server's "never lapses" (NULL), which is also what a server
        # predating claim expiry reports for everything.
        self.expires_at: dict[str, datetime] = {}
        # claim_ttl_seconds as sent on each start: task_id -> list of TTLs
        # (a spawn records two starts — the claim and the post-spawn ref).
        self.sent_claim_ttls: dict[str, list[int | None]] = {}
        # --- attempt counting per build ROUND (TickConfig.max_attempts) ---
        # Rather than keeping a number, keep the lifecycle events and derive
        # the count the way the server does (see ``attempt_count``). Each
        # entry is "start", "interrupt" or "other" — three values rather
        # than two because a start is collapsed by BOTH of the first two,
        # for different reasons: consecutive starts are one execution
        # re-recording itself, while a start after an interrupt continues
        # work the platform took away. Only events after the build's most
        # recent BUILD_RESUMED are counted at all.
        self.task_events: dict[str, list[str]] = {}
        # Where each task's current round begins in its event list — moved
        # to the end of the list by ``build_resume_aio``. Absent = the build
        # has never been resumed, so the round is the whole list.
        self.round_start: dict[str, int] = {}
        # Set False to emulate a server predating attempt_count: the field
        # is absent from the payload, so the model default (None) applies —
        # which is what makes "this registry cannot count" distinguishable
        # from "this task has not been attempted" (0).
        self.serves_attempt_counts = True
        # error_message of every recorded failure, per task — the text an
        # operator actually reads in the UI.
        self.fail_reasons: dict[str, list[str | None]] = {}
        # ...and the same for interruptions.
        self.interrupt_reasons: dict[str, list[str | None]] = {}
        # Set False to emulate a server predating interrupt_count (the
        # field is absent, so the model default None applies). Distinct
        # from serves_attempt_counts: the two shipped separately, and the
        # degradations differ.
        self.serves_interrupt_counts = True
        # task_id -> task_data body, served by task_get_metadata_aio
        # (rehydration fallback); missing key -> KeyError, like a 404.
        self.metadata_bodies: dict[str, dict] = {}
        # --- cross-build scope (mirrors the API's two scopes) ---
        # ``statuses`` is environment-global; these task ids exist in the
        # environment but are NOT in this build's task set, so they gate
        # this build's tasks (dependency edges are global too) while
        # contributing to neither ``actionable`` nor ``status_counts``.
        self.not_in_build: set[str] = set()
        self.blocker_attempts: dict[str, int] = {}
        # task_id -> the build whose event produced the current status. An
        # ABSENT key means "this build's own doing" (the common case), which
        # is what makes a task NOT an external blocker; an explicit None is a
        # row predating status denormalisation — not this build's doing
        # either, and with no build to ask about it.
        self.status_build_id: dict[str, UUID | None] = {}
        # task_id -> (namespace, name), echoed on blocker entries.
        self.task_names: dict[str, tuple[str, str]] = {}
        self.blocked_by_external_truncated = False
        # Derived status of OTHER builds in the environment, served by
        # build_get_aio — how the tick decides whether the build owning a
        # blocker is still going to schedule it. Absent ids 404, and
        # ``build_get_status`` of None emulates a server that doesn't report
        # the field.
        self.other_build_statuses: dict[UUID, str | None] = {}
        self.build_get_calls: list[UUID] = []
        # Set False to emulate a server predating blocked_by_external: the
        # fields stay at their model defaults, as they would deserialising
        # a response that never carried them.
        self.serves_blocked_by_external = True
        # Tick summaries reported by run_tick_aio, and an optional error to
        # raise from the reporting endpoint.
        self.reported_tick_summaries: list[dict] = []
        self.tick_summary_error: Exception | None = None
        # Set to make the frontier fetch blow up, i.e. crash the tick itself.
        self.frontier_error: Exception | None = None
        # Set to make every id-based retry fail — a transient registry
        # error, or a route an older server does not serve.
        self.retry_by_id_error: Exception | None = None
        # Set False to emulate a server predating the executions route: the
        # tick falls back to filtering the frontier itself.
        self.serves_executions = True
        # ...and False to emulate one predating the owner on frontier refs,
        # where the fallback cannot filter by ownership either.
        self.serves_status_build_id = True
        self.executions_calls: list[UUID] = []
        # A transient failure of the executions listing — distinct from
        # ``serves_executions``, which models a server that lacks the route.
        self.executions_error: Exception | None = None
        # Page size of the executions listing, so a test can force the
        # drain loop the real server's cap makes reachable.
        self.executions_page_size = 100

    # --- test setup helpers ---

    def add_task(
        self,
        task_id: str,
        status: str = "pending",
        upstreams: set[str] | None = None,
        executor: str | None = None,
        executor_ref: str | None = None,
        status_at: "datetime | None" = None,
        expires_at: "datetime | None" = None,
        attempt_count: int | None = None,
        interrupt_count: int = 0,
    ) -> None:
        self.statuses[task_id] = status
        self.upstreams.setdefault(task_id, set()).update(upstreams or set())
        if executor or executor_ref:
            self.refs[task_id] = (executor, executor_ref)
        if status_at is not None:
            self.status_at[task_id] = status_at
        if expires_at is not None:
            self.expires_at[task_id] = expires_at
        if attempt_count is not None:
            # Pre-seed the history of a task whose earlier attempts this test
            # does not simulate start-by-start (the common shape: a task
            # fabricated straight into RUNNING or PENDING). Each attempt is
            # a start followed by something else; a RUNNING task's last
            # attempt is still open, so it ends on its start.
            events: list[str] = []
            for _ in range(attempt_count):
                events.extend(["start", "other"])
            if status == "running" and events:
                events.pop()
            self.task_events[task_id] = events
        if interrupt_count:
            # Interruptions are appended as start/interrupt pairs, which is
            # the shape the real stream has and — because a start after an
            # interrupt opens no new attempt — leaves ``attempt_count``
            # alone. That property is exactly what these tests are for, so
            # the fake has to reproduce it rather than assert it.
            for _ in range(interrupt_count):
                self.task_events.setdefault(task_id, []).extend(["start", "interrupt"])

    def _count_event(self, task_id: str, *, kind: str) -> None:
        """Record one lifecycle event: "start", "interrupt" or "other"."""
        self.task_events.setdefault(task_id, []).append(kind)

    def _round_events(self, task_id: str) -> list[str]:
        events = self.task_events.get(task_id, [])
        return events[self.round_start.get(task_id, 0) :]

    def attempt_count(self, task_id: str) -> int:
        """Starts in the current round, collapsing consecutive ones.

        The server's rule, applied to the recorded history rather than
        maintained as a counter — so a round boundary needs no bookkeeping
        beyond noting where it fell.

        An "interrupt" predecessor collapses a start too, exactly like
        another start: the platform took the container away, so the run
        that follows continues work rather than beginning a new attempt.
        """
        count = 0
        previous = None
        for kind in self._round_events(task_id):
            if kind == "start" and previous not in ("start", "interrupt"):
                count += 1
            previous = kind
        return count

    def interrupt_count(self, task_id: str) -> int:
        """Interruptions in the current round — a plain count, no collapsing.

        Nothing emits more than one per execution (the dying worker reports
        it once), so unlike attempts there is nothing to de-duplicate.
        """
        return sum(1 for kind in self._round_events(task_id) if kind == "interrupt")

    def add_blocking_task(
        self,
        task_id: str,
        *,
        blocks: "set[str]",
        status: str = "running",
        owner_build_id: "UUID | None" = None,
        owner_build_status: "str | None" = "running",
        owner_build_known: bool = True,
        name: str = "BlockingTask",
        namespace: str = "",
        status_at: "datetime | None" = None,
        expires_at: "datetime | None" = None,
        in_build: bool = False,
        attempt_count: int = 0,
    ) -> None:
        """Register an upstream whose current status this build did not set.

        Defaults to the #208 A1 shape: RUNNING under some *other* build and
        absent from this build's task set, so it gates ``blocks`` while
        appearing in neither this build's ``running`` nor its
        ``status_counts``. ``owner_build_status`` is what ``build_get`` will
        report for that other build (None = a server that doesn't report it);
        ``owner_build_known=False`` makes the lookup 404 instead.
        """
        self.statuses[task_id] = status
        owner_build_id = owner_build_id or uuid4()
        self.status_build_id[task_id] = owner_build_id
        if owner_build_known:
            self.other_build_statuses[owner_build_id] = owner_build_status
        self.task_names[task_id] = (namespace, name)
        if status_at is not None:
            self.status_at[task_id] = status_at
        if expires_at is not None:
            self.expires_at[task_id] = expires_at
        if not in_build:
            self.not_in_build.add(task_id)
        self.blocker_attempts[task_id] = attempt_count
        for downstream in blocks:
            self.upstreams.setdefault(downstream, set()).add(task_id)

    # --- registry surface used by the tick ---

    async def task_register_bulk_aio(self, build_id, tasks, *, limit_keys=None):
        infos = []
        for task in tasks:
            tid = str(task.id)
            self.statuses.setdefault(tid, "pending")
            self.upstreams.setdefault(tid, set()).update(
                str(dep.id)
                for dep in __import__("stardag").flatten_task_struct(task.requires())
            )
            self.calls.append(("register", tid))
            executor, executor_ref = self.refs.get(tid, (None, None))
            infos.append(
                RegisteredTaskInfo(
                    task_id=tid,
                    latest_status=self.statuses[tid],
                    latest_executor=executor,
                    latest_executor_ref=executor_ref,
                )
            )
        return infos

    async def task_start_aio(
        self,
        build_id,
        task,
        executor=None,
        executor_ref=None,
        executor_metadata=None,
        claim_ttl_seconds=None,
    ):
        tid = str(task.id)
        self.calls.append(("start", tid))
        self._count_event(tid, kind="start")
        self.sent_claim_ttls.setdefault(tid, []).append(claim_ttl_seconds)
        self.statuses[tid] = "running"
        self.refs[tid] = (executor, executor_ref)
        self.start_metadata[tid] = executor_metadata
        if self.auto_complete:
            # Instant worker: completes and wakes the scheduler.
            self.statuses[tid] = "completed"
            self.needs_tick = True

    async def task_start_claim_aio(
        self,
        build_id,
        task,
        executor=None,
        executor_ref=None,
        executor_metadata=None,
        limit_keys=None,
        claim_ttl_seconds=None,
        *,
        claim=True,
    ):
        """Real claim arbitration, mirroring the API's claim-on-start.

        ``claim=False`` acquires the limit slots only — the same orthogonal
        flags the API's one ``/start`` endpoint carries."""
        from stardag.registry import StartClaimResult

        tid = str(task.id)
        self.calls.append(("start_claim", tid))
        self.claim_limit_keys[tid] = list(limit_keys or [])
        if claim and self.statuses.get(tid) == "running":
            executor_name, ref = self.refs.get(tid, (None, None))
            return StartClaimResult(
                started=False,
                denied_reason="already_running",
                executor=executor_name,
                executor_ref=ref,
            )
        if claim and self.statuses.get(tid) == "completed":
            return StartClaimResult(started=False, denied_reason="already_completed")
        started = await self._acquire_limits(
            build_id,
            task,
            executor=executor,
            executor_ref=executor_ref,
            executor_metadata=executor_metadata,
            limit_keys=limit_keys,
            claim_ttl_seconds=claim_ttl_seconds,
        )
        if not started:
            return StartClaimResult(started=False, denied_reason="limit")
        return StartClaimResult(started=True)

    async def _acquire_limits(
        self,
        build_id,
        task,
        executor=None,
        executor_ref=None,
        executor_metadata=None,
        limit_keys=None,
        claim_ttl_seconds=None,
    ):
        # The limits half of a start, mirroring the API's semantics: count
        # running holders per key against configured caps (self.limits);
        # all-or-nothing acquisition. A helper on this double, not a
        # registry method — the API has one start endpoint, and the SDK now
        # has one method reaching it.
        self.calls.append(("acquire_limits", str(task.id)))
        for key in limit_keys or []:
            cap = self.limits.get(key)
            if cap is None:
                continue
            active = sum(
                1
                for tid, keys in self.task_limit_keys.items()
                if key in keys
                and self.statuses.get(tid) == "running"
                and tid != str(task.id)
            )
            if active >= cap:
                return False
        self.task_limit_keys[str(task.id)] = set(limit_keys or [])
        await self.task_start_aio(
            build_id,
            task,
            executor=executor,
            executor_ref=executor_ref,
            executor_metadata=executor_metadata,
            claim_ttl_seconds=claim_ttl_seconds,
        )
        return True

    async def task_complete_aio(self, build_id, task):
        tid = str(task.id)
        self.calls.append(("complete", tid))
        self._count_event(tid, kind="other")
        self.statuses[tid] = "completed"

    async def task_retry_by_id_aio(self, build_id, task_id):
        """Id-based retry — what a tick uses for a blocker it cannot
        reconstruct (it has the id off the frontier, not the object)."""
        if self.retry_by_id_error is not None:
            self.calls.append(("retry-failed", task_id))
            raise self.retry_by_id_error
        self.calls.append(("retry", task_id))
        self._count_event(task_id, kind="other")
        if self.statuses.get(task_id) in _RETRYABLE_STATUSES:
            self.statuses[task_id] = "pending"
            self.refs.pop(task_id, None)

    async def task_retry_aio(self, build_id, task):
        tid = str(task.id)
        self.calls.append(("retry", tid))
        self._count_event(tid, kind="other")
        # Same retryable set the server applies (suspended included: a
        # suspended task has no live execution to orphan).
        if self.statuses.get(tid) in _RETRYABLE_STATUSES:
            self.statuses[tid] = "pending"
            self.refs.pop(tid, None)

    async def build_add_roots_aio(self, build_id, root_task_ids):
        self.calls.append(("add_roots", ",".join(root_task_ids)))
        self.root_task_ids += [t for t in root_task_ids if t not in self.root_task_ids]

    async def task_cancel_aio(self, build_id, task, *, if_executor_ref=None):
        tid = str(task.id)
        if if_executor_ref is not None and (
            self.statuses.get(tid) not in ("running", "interrupted")
            or self.refs.get(tid, (None, None))[1] != if_executor_ref
        ):
            # The server's rule, on the locked row: nothing to revoke under
            # that execution, so nothing is recorded. Modelled because the
            # tick's cleanup pass relies on it -- to not stamp a task another
            # build has since reset, and to not revoke an execution this
            # build started after the listing was read.
            self.calls.append(("cancel-skipped", tid))
            return
        self.calls.append(("cancel", tid))
        self._count_event(tid, kind="other")
        self.statuses[tid] = "cancelled"

    async def task_fail_aio(self, build_id, task, error_message=None):
        tid = str(task.id)
        self.calls.append(("fail", tid))
        self._count_event(tid, kind="other")
        self.fail_reasons.setdefault(tid, []).append(error_message)
        self.statuses[tid] = "failed"

    async def task_interrupt_aio(self, build_id, task, reason=None):
        """What a worker reports when the platform took its container.

        Mirrors the server: the claim goes (so no expiry survives) but the
        executor ref stays, because a backend running its own retries may
        be restarting that very call and a scheduler needs the ref to
        probe for it.
        """
        tid = str(task.id)
        self.calls.append(("interrupt", tid))
        self._count_event(tid, kind="interrupt")
        self.interrupt_reasons.setdefault(tid, []).append(reason)
        self.statuses[tid] = "interrupted"
        self.expires_at.pop(tid, None)

    async def build_complete_aio(self, build_id):
        self.calls.append(("build_complete", None))
        self.build_status = "completed"

    async def build_fail_aio(self, build_id, error_message=None):
        self.calls.append(("build_fail", None))
        self.build_status = "failed"
        self.build_error_message = error_message

    async def task_get_metadata_aio(self, task_id):
        from stardag.registry._base import TaskMetadata

        body = self.metadata_bodies[str(task_id)]
        return TaskMetadata(
            id=task_id,
            body=body,
            name=body.get("__name", ""),
            namespace=body.get("__namespace", ""),
            version=body.get("version", ""),
            output_uri=None,
            status=self.statuses.get(str(task_id), "pending"),
            registered_at=None,
            started_at=None,
            completed_at=None,
            error_message=None,
        )

    async def build_skip_blocked_aio(self, build_id):
        # Mirrors the API: pending/suspended tasks transitively downstream
        # of a failed/cancelled/skipped task become skipped.
        self.calls.append(("skip_blocked", None))
        blocked = {
            tid
            for tid, status in self.statuses.items()
            if status in ("failed", "cancelled", "skipped")
        }
        # Blockage only propagates through nodes that will themselves never
        # complete (mirrors the API's CTE gate): a completed intermediate
        # satisfies its downstream; a running one may still complete.
        propagating = ("failed", "cancelled", "skipped", "pending", "suspended")
        changed = True
        while changed:
            changed = False
            for tid, ups in self.upstreams.items():
                if tid not in blocked and any(
                    up in blocked and self.statuses.get(up) in propagating for up in ups
                ):
                    blocked.add(tid)
                    changed = True
        skipped = []
        for tid in blocked:
            if self.statuses.get(tid) in ("pending", "suspended", "interrupted"):
                self.statuses[tid] = "skipped"
                skipped.append(tid)
        return skipped

    async def build_report_tick_summary_aio(self, build_id, summary):
        # Observability sink. ``tick_summary_error`` lets a test make it
        # fail the way a real registry can (route missing, server down)
        # and assert the tick is unaffected.
        self.reported_tick_summaries.append(summary)
        if self.tick_summary_error is not None:
            raise self.tick_summary_error

    async def build_set_reactive_meta_aio(
        self, build_id, *, app_name, tick_kwargs=None
    ):
        self.calls.append(("set_reactive_meta", app_name))
        self.reactive_app_name = app_name
        if tick_kwargs is not None:
            self.reactive_tick_kwargs = tick_kwargs

    async def build_get_aio(self, build_id):
        from stardag.registry import BuildInfo

        self.build_get_calls.append(build_id)
        if build_id not in self.other_build_statuses:
            raise NotFoundError(f"Build {build_id} not found", detail="Build not found")
        return BuildInfo(id=build_id, status=self.other_build_statuses[build_id])

    async def build_resume_aio(self, build_id, executor_metadata=None):
        """BUILD_RESUMED — what a re-trigger of this build id records.

        It starts a new round, and attempt counts are windowed to the
        round, so every task's count restarts at zero. The tick never calls
        this; tests use it the way ``_trigger_reactive`` does, BEFORE the
        discovery retries that reset failed tasks to pending.
        """
        self.calls.append(("build_resume", None))
        self.build_status = "running"
        for tid, events in self.task_events.items():
            self.round_start[tid] = len(events)

    async def build_notify_aio(self, build_id, *, can_spawn=True) -> BuildNotifyResult:
        self.needs_tick = True
        return BuildNotifyResult(build_id=build_id, scheduler_live=True)

    async def build_clear_notify_aio(self, build_id):
        self.needs_tick = False

    async def build_get_notify_aio(self, build_id) -> BuildNotifyResult:
        # The cheap flag read, overridden rather than inherited: the ABC
        # default delegates to the frontier, and a double that inherits it
        # cannot tell "read the flag" from "read the frontier" — which is
        # exactly what the linger poll's cost depends on.
        return BuildNotifyResult(build_id=build_id, needs_tick=self.needs_tick)

    # --- scheduler lease (STA-17: on the build row, not a lock table) ---

    async def build_acquire_scheduler_lease_aio(
        self, build_id, *, owner_id, ttl_seconds
    ):
        # A lapsed lease is re-acquirable — that takeover *is* the healing
        # mechanism, and it is what the SDK falls back on when a renewal is
        # refused. A *stolen* one is not: somebody else holds it.
        self._lease_acquires += 1
        if self.lease_stolen and self._lease_acquires > 1:
            self.lease_owner = "__successor__"
            return SchedulerLeaseResult(build_id=build_id, held=False)
        self.lease_lapsed = False
        self.lease_owner = owner_id if self.lease_acquired else self.lease_owner
        return SchedulerLeaseResult(build_id=build_id, held=self.lease_acquired)

    async def build_renew_scheduler_lease_aio(self, build_id, *, owner_id, ttl_seconds):
        # The server refuses a renewal for two different reasons, and the
        # SDK responds to them differently, so both are modelled.
        #
        # ``lease_lapsed``: our own lease expired between renewals and
        # nobody took it, so the owner column still says us. The SDK
        # re-acquires and carries on.
        #
        # ``lease_stolen``: somebody else holds it, so the re-acquire fails
        # too and the tick must stop.
        if self.lease_stolen:
            # The theft is visible in the owner column, exactly as on the
            # server: the successor's acquire replaced our id, so every
            # later owner-checked call by us is refused — including the
            # exit release, which must not clear the successor's lease.
            self.lease_owner = "__successor__"
            return SchedulerLeaseResult(build_id=build_id, held=False)
        if self.lease_lapsed:
            return SchedulerLeaseResult(build_id=build_id, held=False)
        return SchedulerLeaseResult(
            build_id=build_id, held=self.lease_owner == owner_id
        )

    async def build_release_scheduler_lease_aio(self, build_id, *, owner_id):
        held = self.lease_owner == owner_id
        if held:
            self.lease_owner = None
        if self.lease_on_release is not None:
            # Fires on the release *attempt*, deliberately not only on a
            # successful release: it models a wake-up landing in the
            # release window, and the notifier is a third party whose
            # timing does not depend on whether this release was still the
            # holder's to make. Gating it on ``held`` would leave the
            # lease-lost exit-handshake test asserting against a flag that
            # can never be set.
            self.lease_on_release()
        return SchedulerLeaseResult(build_id=build_id, held=held)

    async def build_get_executions_aio(
        self, build_id, *, cursor=None
    ) -> BuildExecutions:
        """Mirrors the API: this build's own executions, with a ref.

        Ownership is ``status_build_id`` being absent (this build put the
        task where it is). CANCELLED joins the live statuses only when the
        build itself is cancelled — for a running build a cancelled task is
        an attempt it already abandoned, not a container to chase.
        """
        self.executions_calls.append(build_id)
        if self.executions_error is not None:
            raise self.executions_error
        if not self.serves_executions:
            # FastAPI's own unknown-path 404, which is how the SDK tells
            # "this server is too old" from "no such build".
            raise NotFoundError("Not Found", detail="Not Found")
        statuses = {"running", "interrupted"}
        if self.build_status == "cancelled":
            statuses.add("cancelled")
        executions = []
        for tid, status in self.statuses.items():
            if status not in statuses or tid in self.status_build_id:
                continue
            executor, executor_ref = self.refs.get(tid, (None, None))
            if executor is None or executor_ref is None:
                continue
            executions.append(
                BuildExecution(
                    task_id=tid,
                    latest_status=status,
                    executor=executor,
                    executor_ref=executor_ref,
                    latest_status_at=self.status_at.get(tid),
                )
            )
        # Keyset paging, modelled because the tick has to drain it: the
        # server's answer does not shrink as executions are stopped, so a
        # caller that re-asks without the cursor gets the same page forever.
        executions.sort(key=lambda e: e.task_id)
        start = 0
        if cursor is not None:
            start = next(
                (i + 1 for i, e in enumerate(executions) if e.task_id == cursor),
                len(executions),
            )
        page = executions[start : start + self.executions_page_size]
        truncated = start + len(page) < len(executions)
        return BuildExecutions(
            build_id=build_id,
            build_status=self.build_status,
            executions=page,
            truncated=truncated,
            next_cursor=page[-1].task_id if truncated and page else None,
        )

    async def build_get_frontier_aio(self, build_id) -> BuildFrontier:
        if self.frontier_error is not None:
            raise self.frontier_error

        def ref(tid: str) -> FrontierTaskRef:
            executor, executor_ref = self.refs.get(tid, (None, None))
            return FrontierTaskRef(
                task_id=tid,
                latest_status=self.statuses[tid],
                latest_executor=executor,
                latest_executor_ref=executor_ref,
                latest_status_at=self.status_at.get(tid),
                # Absent from status_build_id = this build's own doing,
                # which is what the API reports for a task this build
                # started. An explicit None is a row whose owning build is
                # gone (or a server predating the field).
                latest_status_build_id=(
                    self.status_build_id.get(tid, build_id)
                    if self.serves_status_build_id
                    else None
                ),
                latest_status_expires_at=self.expires_at.get(tid),
                attempt_count=(
                    self.attempt_count(tid) if self.serves_attempt_counts else None
                ),
                interrupt_count=(
                    self.interrupt_count(tid) if self.serves_interrupt_counts else None
                ),
            )

        # Build-scoped, like the API: actionable/running/status_counts see
        # only this build's task set. Dependency gating below stays global.
        in_build = {
            tid: status
            for tid, status in self.statuses.items()
            if tid not in self.not_in_build
        }
        actionable = [
            ref(tid)
            for tid, status in in_build.items()
            if status in ("pending", "suspended", "running", "interrupted")
            and all(
                self.statuses.get(up) == "completed"
                for up in self.upstreams.get(tid, set())
            )
        ]
        counts: dict[str, int] = {}
        for status in in_build.values():
            counts[status] = counts.get(status, 0) + 1
        running = [ref(tid) for tid, status in in_build.items() if status == "running"]
        # Mirrors the API: computed ONLY when the build has nothing
        # actionable and nothing running, so an empty list means "not
        # blocked externally, OR not stalled".
        blocked_by_external: list[FrontierExternalBlocker] = []
        if self.serves_blocked_by_external and not actionable and not running:
            for tid, status in in_build.items():
                if status not in ("pending", "suspended", "running", "interrupted"):
                    continue
                for up in sorted(self.upstreams.get(tid, set())):
                    if up not in self.statuses:
                        continue
                    if self.statuses[up] == "completed":
                        continue
                    if up not in self.status_build_id:
                        continue  # this build's own doing — not external
                    owner_id = self.status_build_id[up]
                    if owner_id == build_id:
                        continue
                    namespace, name = self.task_names.get(up, ("", up))
                    blocked_by_external.append(
                        FrontierExternalBlocker(
                            task_id=tid,
                            blocking_task_id=up,
                            blocking_task_namespace=namespace,
                            blocking_task_name=name,
                            blocking_status=self.statuses[up],
                            blocking_status_at=self.status_at.get(up),
                            # Mirrors the server: only a RUNNING task holds
                            # a claim, so only a RUNNING blocker can carry
                            # an expiry.
                            blocking_status_expires_at=(
                                self.expires_at.get(up)
                                if self.statuses[up] == "running"
                                else None
                            ),
                            blocking_status_build_id=owner_id,
                            blocking_in_build=up not in self.not_in_build,
                            # Mirrors the server: attempts are per build, so
                            # only a blocker in this build's plan has any.
                            blocking_attempt_count=(
                                self.blocker_attempts.get(up, 0)
                                if up not in self.not_in_build
                                else None
                            ),
                        )
                    )
        return BuildFrontier(
            build_id=build_id,
            build_status=self.build_status,
            needs_tick=self.needs_tick,
            root_task_ids=self.root_task_ids,
            roots=[ref(t) for t in self.root_task_ids if t in self.statuses],
            status_counts=counts,
            actionable=actionable,
            running=running,
            blocked_by_external=blocked_by_external,
            blocked_by_external_truncated=(
                self.blocked_by_external_truncated and bool(blocked_by_external)
            ),
            reactive_app_name=self.reactive_app_name,
            reactive_tick_kwargs=self.reactive_tick_kwargs,
        )


class FakeTickExecutor(TaskExecutorABC):
    """Detached executor for ticks: spawn only (results never awaited)."""

    def __init__(
        self,
        statuses: dict[str, DetachedExecutionStatus] | None = None,
        timeout_seconds: float | None = None,
    ):
        # ref -> probe status
        self.probe_statuses = statuses or {}
        self.spawned: list[UUID] = []
        self.cancelled_refs: list[str] = []
        self._spawn_count = 0
        # The backend's own wall-clock limit, from which the tick derives
        # the claim TTL. None = a backend that enforces none.
        self.timeout_seconds = timeout_seconds

    async def submit(self, task):
        raise AssertionError("ticks must not use blocking submit")

    def execution_timeout_seconds(self, task: BaseTask) -> float | None:
        return self.timeout_seconds

    def supports_detached(self, task: BaseTask) -> bool:
        return True

    async def submit_detached(self, task: BaseTask) -> DetachedHandle:
        self.spawned.append(task.id)
        self._spawn_count += 1

        async def wait():
            raise AssertionError("ticks must not await detached results")

        return DetachedHandle(
            executor="fake", ref=f"ref-{self._spawn_count}", wait=wait
        )

    async def detached_status(self, task, executor, ref):
        return self.probe_statuses.get(ref, DetachedExecutionStatus.UNKNOWN)

    async def cancel_detached(self, task, executor, ref):
        self.cancelled_refs.append(ref)

    async def setup(self):
        pass

    async def teardown(self):
        pass


class InMemoryTaskStore(BuildTaskStore):
    """BuildTaskStore on a dict — no target roots needed in engine tests.

    Pickle-only now: the reactive marker/owner/config live in the registry
    (see ``FakeReactiveRegistry.reactive_meta``), not the store.

    Overrides the ``_write_task`` hooks, not ``save_task`` itself, so the
    base class's ``pickle_free`` guard applies here exactly as it does in
    production. A double that wrote where the real store refuses would make
    the tests for that guard vacuous.
    """

    def __init__(self, build_id: UUID, *, pickle_free: bool = False):
        super().__init__(build_id, pickle_free=pickle_free)
        self._tasks: dict[str, BaseTask] = {}

    def _write_task(self, task: BaseTask) -> None:
        self._tasks[str(task.id)] = task

    def load_task(self, task_id):
        return self._tasks.get(str(task_id))

    async def _write_task_aio(self, task: BaseTask) -> None:
        self._write_task(task)

    async def load_task_aio(self, task_id):
        return self.load_task(task_id)


# =============================================================================
# Helpers
# =============================================================================


def _chain(*names: str) -> list[BaseTask]:
    """Build a linear chain of SyncOnlyTasks: first is leaf, last is root."""
    tasks: list[BaseTask] = []
    prev: tuple = ()
    for name in names:
        task = SyncOnlyTask(name=name, deps=prev)
        tasks.append(task)
        prev = (task,)
    return tasks


def _setup(
    tasks: list[BaseTask],
    *,
    auto_complete: bool = True,
    lease_acquired: bool = True,
    executor: FakeTickExecutor | None = None,
    on_release: typing.Callable[[], None] | None = None,
) -> tuple[
    FakeReactiveRegistry,
    FakeTickExecutor,
    InMemoryTaskStore,
]:
    root = tasks[-1]
    registry = FakeReactiveRegistry(
        root_task_ids=[str(root.id)], auto_complete=auto_complete
    )
    # The scheduler lease is registry state now, not a separate lock
    # manager, so the knobs that used to configure the fake lock manager
    # configure the fake registry.
    registry.lease_acquired = lease_acquired
    registry.lease_on_release = on_release
    for task in tasks:
        registry.add_task(
            str(task.id),
            upstreams={str(d.id) for d in flatten_task_struct(task.requires())},
        )
    store = InMemoryTaskStore(uuid4())
    store.save_tasks(tasks)
    return (
        registry,
        executor or FakeTickExecutor(),
        store,
    )


# =============================================================================
# Tests
# =============================================================================
