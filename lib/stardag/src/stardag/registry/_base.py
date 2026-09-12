"""Base registry classes and utilities."""

import abc
import os
import subprocess
from datetime import datetime
from functools import lru_cache
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from stardag.base_model import StardagBaseModel
from stardag.utils.resource_provider import resource_provider

if TYPE_CHECKING:
    from stardag import BaseTask
    from stardag.artifact import Artifact


class FrontierTaskRef(StardagBaseModel):
    """A task in a build's scheduling frontier (see :class:`BuildFrontier`)."""

    task_id: str
    latest_status: str
    latest_executor: str | None = None
    latest_executor_ref: str | None = None
    # Executor-descriptive metadata recorded with the latest start (e.g.
    # Modal app/workspace/environment). None on servers predating the field.
    latest_executor_metadata: dict[str, Any] | None = None
    # When the current status was recorded (None on servers predating the
    # field).
    latest_status_at: datetime | None = None
    # The build whose event produced the current status. The claim
    # holder, for any status a task can be held in — and the field that
    # separates this build's executions from a neighbour's: ``running``
    # is every RUNNING task in the *plan*, which after plan closure
    # includes tasks another build is executing. Acting destructively
    # on one of those kills somebody else's worker. None on servers
    # predating the field, and on a task whose owning build is gone.
    latest_status_build_id: UUID | None = None
    # When the RUNNING execution claim stops being honoured, if ever — the
    # one piece of *third-party evaluable* liveness evidence a claim
    # carries: past it the claim is re-claimable and stops occupying
    # concurrency slots, so a reader may treat it as abandoned without
    # probing anything. None means "never lapses" (older server, or a start
    # predating the column) and is NOT evidence of death. Only meaningful
    # while ``latest_status == "running"``: every other transition releases
    # the claim, and the server clears this with it.
    latest_status_expires_at: datetime | None = None
    # How many times execution has been *started* for this task in the
    # build's current **round** — the budget a reactive tick's
    # ``TickConfig.max_attempts`` spends (see ``stardag.build._reactive``).
    #
    # A round runs from the build's most recent BUILD_RESUMED event, or
    # from the build's beginning if it has never been resumed. So the count
    # is scoped tighter than the build: re-triggering an existing build id
    # emits BUILD_RESUMED and thereby starts a fresh round, which is what
    # makes "re-trigger it" a real escape from an exhausted budget rather
    # than a no-op. A *bare* retry (the retry route, the UI's Retry,
    # ``stardag tasks retry``) emits no such event and does not reset it.
    #
    # The server collapses *runs of consecutive* TASK_STARTED events into
    # one attempt, so the several starts one execution records — the
    # reactive path's two (the claiming start, then the one carrying the
    # executor ref) and the resident path's three (claim, engine ref,
    # worker self-report) — count once each. A start separated from the
    # previous one by any other event (a failure, a suspension) is a new
    # attempt.
    #
    # ``0`` means "not attempted in this **round**" — an ordinary spawn
    # candidate, never a reason to deny a start. Note *round*, not build:
    # BUILD_RESUMED resets the count, so a task that ran in an earlier
    # round of the same build reads ``0`` again after a re-trigger. That is
    # deliberate — a retry budget is per attempt at making the build
    # progress, and a resume is a new attempt.
    #
    # ``None`` means the *server does not report attempts at all* (it
    # predates the field, so the key is absent from the payload and this
    # default applies). That is not the same statement as ``0`` and must
    # not be collapsed into it: a budget is only enforceable against a
    # counter that exists, so a reader with no counter cannot allow a retry
    # either — it has no way to stop allowing one. Callers therefore read
    # ``None`` as "no retry policy is possible here" and degrade to exactly
    # the behaviour they had before attempts were counted.
    attempt_count: int | None = None
    # How many times an execution of this task was **interrupted** by the
    # platform in the same round — a function timeout, or a reclaimed
    # container. Budgeted separately from ``attempt_count``, against
    # ``TickConfig.max_interruptions``, and deliberately so: a task built to
    # be killed and resumed until it converges would otherwise burn a budget
    # meant for genuine failures and fail the build for the one reason it
    # was designed to survive.
    #
    # ``None`` carries the same "this server does not report it" meaning as
    # above, but a different consequence. An unreportable *attempt* count
    # refuses the retry, because retrying is the thing that could loop
    # unbounded. An unreportable *interruption* count has no such danger to
    # guard against on its own — but it also cannot bound a respawn loop, so
    # it degrades to treating the interruption as an ordinary retryable
    # failure under the attempt budget, which is bounded.
    interrupt_count: int | None = None


class FrontierExternalBlocker(StardagBaseModel):
    """An upstream outside this build that holds one of its tasks back.

    Task rows (and their dependency edges) are per *environment*, not per
    build, so an upstream left non-COMPLETED by some *other* build still
    gates this build's downstream tasks — silently, since such an upstream
    need not be part of this build's task set at all (a dynamic dependency
    registered under an earlier build is the common case). Each entry pairs
    one blocked task of this build with one such blocker.

    "Not this build's doing" is decided server-side by the build whose
    event produced the blocker's current status: if that is not this build,
    this build did not put the blocker in that state and cannot generally
    get it out of it.

    The blocked side carries only ``task_id`` (the caller registered it, and
    every other frontier field is task_id-keyed too); the *blocking* side
    carries name/namespace because it may be entirely unknown to the caller,
    and a diagnostic is useless without something human-readable.
    """

    # Blocked task — always a member of this build's task set.
    task_id: str
    # The blocking upstream. Identity is spelled out because this build may
    # never have seen this task.
    blocking_task_id: str
    blocking_task_namespace: str
    blocking_task_name: str
    # Task status value; never "completed" (a completed upstream blocks
    # nothing).
    blocking_status: str
    # When the blocker entered its current status, and the build whose event
    # put it there ("running under build Z since T"). Both are None only for
    # rows predating status denormalisation server-side.
    blocking_status_at: datetime | None = None
    blocking_status_build_id: UUID | None = None
    # When the blocker's RUNNING execution claim lapses, if ever — the same
    # column as ``FrontierTaskRef.latest_status_expires_at``, surfaced here
    # because it turns the wait-or-fail decision for a RUNNING blocker from
    # an inference into a read (see
    # ``stardag.build._reactive._classify_external_blockers``). None =
    # "never lapses". Always None for a non-RUNNING blocker: it holds no
    # claim, so "will anyone move it?" must be asked of its owning build.
    blocking_status_expires_at: datetime | None = None
    # Whether the blocker is also part of *this* build's task set. True is the
    # normal case: a build's plan holds every dependency that was not complete
    # at discovery, so the blocker is this build's own task and shows up in its
    # ``actionable``/``running`` too.
    #
    # False is still reachable, and not only for builds registered before
    # closure existed. Closure runs once, at registration, so a dependency edge
    # written afterwards is not in the plan — which is what happens whenever a
    # concurrent build's worker yields dynamic dependencies into its own plan.
    # Re-triggering re-runs discovery and brings them in.
    #
    # Reported for diagnostics, not branched on: the scheduler decides from
    # the blocker's *status* (see
    # ``stardag.build._reactive._classify_external_blockers``), and the
    # attempt count below is what keeps it from resetting a task outside the
    # plan.
    blocking_in_build: bool
    # Attempts this blocker has already spent **in this build's round**,
    # when the blocker is in this build's plan (None otherwise, and on
    # servers predating the field). A tick that resets an in-plan blocker
    # needs this to stay inside the same budget an ordinary retry obeys —
    # otherwise a task that fails every time is reset, rerun and re-failed
    # forever.
    blocking_attempt_count: int | None = None


class BuildFrontier(StardagBaseModel):
    """Scheduling state of a build, consumed by reactive scheduler ticks.

    ``actionable``: tasks with global status pending/suspended/interrupted/running whose
    upstream dependencies (static + dynamic) are all completed. The
    scheduler partitions them: pending/suspended → spawn; running → probe
    the detached execution ref. ``status_counts`` covers all tasks in the
    build (terminal detection).

    ``blocked_by_external`` explains the gap between the two scopes this
    payload mixes: dependency gating is environment-global while ``running``
    and ``status_counts`` cover only tasks this build has events for, so a
    build can have nothing actionable and nothing running yet still be
    legitimately waiting. See :class:`FrontierExternalBlocker`.
    """

    build_id: UUID
    build_status: str
    needs_tick: bool
    root_task_ids: list[str]
    roots: list[FrontierTaskRef]
    status_counts: dict[str, int]
    actionable: list[FrontierTaskRef]
    # All RUNNING tasks in the build, including non-actionable ones (e.g.
    # inside the dynamic-dep registration window) — cancellation targets.
    # Defaults to empty for servers predating the field.
    running: list[FrontierTaskRef] = []
    # Non-terminal tasks of this build held back by an upstream this build
    # does not own, capped server-side (hence the truncation flag — the list
    # is a diagnostic, not a work queue; a truncated list still proves
    # "waiting, not stuck").
    #
    # Populated ONLY when the build has nothing actionable and nothing
    # running, i.e. only when it looks stalled — which keeps a per-edge join
    # off the hot path of every healthy build's linger polls. So an EMPTY
    # list means "not externally blocked, OR not stalled"; never read it as
    # proof that no external blocker exists. Also empty on servers predating
    # the fields, where the tick's terminal detection degrades to exactly its
    # pre-fix behaviour.
    blocked_by_external: list[FrontierExternalBlocker] = []
    blocked_by_external_truncated: bool = False
    # Reactive-scheduling marker/owner, moved off the target root into the
    # registry. None means the build is NOT reactively scheduled (a stray
    # tick must no-op on it, so a resident-orchestrator build is never
    # double-scheduled). Non-None is the owning app that drives the tick
    # (ownership guard). Set via ``build_set_reactive_meta``. None also on
    # servers predating the field (the reactive trigger fails loudly against
    # such servers when it PUTs the reactive-meta endpoint, so a tick never
    # observes this).
    reactive_app_name: str | None = None
    # Reactive-scheduler tick configuration (a ``TickConfig`` kwargs dict);
    # None/absent is treated as ``{}``. Read from the frontier only for the
    # backstop marker check — the Modal tick reads it from the lighter
    # ``build_get`` before acquiring the lease.
    reactive_tick_kwargs: dict[str, Any] | None = None


class BuildExecution(StardagBaseModel):
    """A detached execution this build started and has not seen end.

    The answer to "what is mine to stop?", which no view of a task's
    *current* state can give — and the difference is not academic. A
    cascading cancel releases the claims a build held so the next build can
    take those tasks over, and the next build can claim one within seconds,
    before the cancelled build's tick has run. From then on the task row
    names the new execution and the old one, still running, is unreachable
    by status: stopping it that way either misses it or kills somebody
    else's container. Both happened.

    So the registry answers from the event log: the ref this build recorded
    when it started the task, unless a worker has since reported the
    execution over. **A ref is not a claim** — the claim says who may run
    the task next, the ref names one execution, and the build that started
    it owns it however the claim has moved since.
    """

    task_id: str
    latest_status: str
    executor: str
    executor_ref: str
    executor_metadata: dict[str, Any] | None = None
    latest_status_at: datetime | None = None


class BuildExecutions(StardagBaseModel):
    """Result of ``build_get_executions`` — see :class:`BuildExecution`."""

    build_id: UUID
    build_status: str
    executions: list[BuildExecution] = []
    # The server capped the list. Stopping an execution takes it out of the
    # answer, so a caller that acts and asks again makes progress.
    truncated: bool = False


class WakeCandidate(StardagBaseModel):
    """A build the caller should spawn a scheduler tick for.

    The hand-over between the two halves of a cross-build wake-up: the
    registry flags builds whose frontier may have changed (it sees every
    write) and hands them out here, once per window; the caller — a tick,
    or a resident engine with a Modal executor — spawns. Carries the app
    name because that is what reaches the right deployed ``tick`` function;
    the build may belong to a different app than the caller.
    """

    build_id: UUID
    reactive_app_name: str


class BuildNotifyResult(StardagBaseModel):
    """Outcome of ``build_notify`` — the wake-up set, and who can serve it.

    ``scheduler_live`` is what makes a wake-up *conditional*: it reports
    whether the build's scheduler lease was held when the server produced
    this response — which is at some point *after* the flag was set, not
    atomically with it. That ordering is the whole guarantee, and it is
    enough: a "yes" means the lease was still held after the flag was
    already durable, so its holder cannot exit without seeing the flag (it
    re-reads once more after releasing — see
    ``stardag.build._reactive._hand_off_if_needed``). A "no" means nobody
    held it, so the caller must spawn. Answering in the notify response
    rather than from a separate query is what pins the read to that side of
    the write; a separate query invites the opposite ordering.

    ``None`` means the server did not say (it predates the field), and is
    **not** "no scheduler": a caller must fall back to spawning
    unconditionally, which is what every SDK did before this existed.
    """

    build_id: UUID | None = None
    needs_tick: bool = True
    scheduler_live: bool | None = None


class BuildInfo(StardagBaseModel):
    """Slim build record from ``build_get`` (``GET /builds/{id}``).

    Carries the reactive marker/owner/config a scheduler tick's pre-lease
    gate needs, without the cost of a full frontier computation. Extra
    fields on the server response are ignored.
    """

    id: UUID
    # The build's *derived* status (computed server-side from its recorded
    # events, same values as ``BuildFrontier.build_status``): pending /
    # running / completed / failed / cancelled. None on servers or custom
    # registries that don't report it — a consumer asking "is this build
    # still live?" must treat None as unknown rather than as terminal.
    status: str | None = None
    # Reactive-scheduling marker/owner (see ``BuildFrontier``). None = not
    # reactively scheduled.
    reactive_app_name: str | None = None
    # Reactive-scheduler tick configuration; None/absent treated as ``{}``.
    reactive_tick_kwargs: dict[str, Any] | None = None


class SchedulerLeaseResult(StardagBaseModel):
    """Outcome of acquiring, renewing or releasing a build's scheduler lease.

    ``held`` is the whole answer: for an acquire it means "you may drive
    this build", for a renew and a release "you still held it". A renew
    answering False is how a tick whose lease lapsed learns it was taken
    over — the honest response is to stop driving, not to keep going.
    """

    build_id: UUID | None = None
    held: bool = False
    expires_at: datetime | None = None


class StartClaimResult(StardagBaseModel):
    """Outcome of a claiming task start (see ``task_start_claim_aio``).

    ``started=True`` means this caller won: the TASK_STARTED event was
    recorded (and any requested concurrency-limit slots acquired). On a
    denial, ``denied_reason`` says why and — for ``already_running`` — the
    running execution's ``(executor, executor_ref)`` is echoed so the
    caller can re-attach or probe liveness.
    """

    started: bool
    denied_reason: Literal["already_running", "already_completed", "limit"] | None = (
        None
    )
    executor: str | None = None
    executor_ref: str | None = None
    # ISO timestamp at which the winning execution's claim lapses, echoed on
    # ``already_running`` denials when the server provides it. A lapsed claim
    # is re-claimable, so a ref-less loser can act on evidence that the
    # winner is gone rather than guessing from how long it has been running.
    # None = "never lapses" (older server, or a start predating the column),
    # which is not evidence of death — the loser waits, as it always has.
    latest_status_expires_at: str | None = None
    denied_keys: list[str] = []


class RegisteredTaskInfo(StardagBaseModel):
    """Slim per-task info echoed back from a bulk task registration.

    Carries the task's current *global* execution state (across builds) so
    the build engine learns — with zero extra roundtrips — whether a task is
    already RUNNING with a re-attachable detached execution. The executor
    fields are only meaningful when ``latest_status == "running"``.
    """

    task_id: str
    latest_status: str | None = None
    latest_executor: str | None = None
    latest_executor_ref: str | None = None
    latest_executor_metadata: dict[str, Any] | None = None


class BuildSummary(StardagBaseModel):
    """One build row from ``GET /builds`` (and the single-build endpoints).

    A read model for operators, not for the build engine: the CLI's
    ``stardag builds list/show/cancel`` and anything else that needs to
    *look at* builds rather than drive one. Unknown response fields are
    ignored (pydantic's default), so a newer server can add fields without
    breaking an older SDK.

    ``last_active_at`` and ``last_activity_at`` are two different numbers
    and confusing them is the classic way to cancel live work:

    - ``last_active_at`` is the column the list is ordered by, bumped only
      by build-level *lifecycle* transitions (resume, complete, fail,
      cancel, exit-early, roots appended). Task events deliberately do not
      touch it, so a build that has been running tasks for three days
      still shows its last lifecycle change here.
    - ``last_activity_at`` is the activity signal: the newest of the
      build's entire event stream (task events included), its
      ``last_active_at``, and any pending scheduler wake-up. This is what
      staleness must be measured on, and what the server's bulk-cancel
      idle filter measures.

    Both are None on servers predating the fields.
    """

    id: UUID
    name: str
    # Derived server-side from the build's recorded events: pending /
    # running / completed / failed / cancelled.
    status: str | None = None
    # Why a FAILED build failed, as recorded server-side. None for any other
    # status, and on servers predating the field — so a consumer must not read
    # None as "failed for no reason".
    latest_error_message: str | None = None
    description: str | None = None
    commit_hash: str | None = None
    root_task_ids: list[str] = []
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    # True when the most recent build-level event is BUILD_RESUMED.
    is_resumed: bool = False
    executor_metadata: dict[str, Any] | None = None
    # Reactive-scheduling marker/owner; None means the build is not
    # reactively scheduled (see :class:`BuildInfo`).
    reactive_app_name: str | None = None
    reactive_tick_kwargs: dict[str, Any] | None = None
    last_active_at: datetime | None = None
    last_activity_at: datetime | None = None


class BuildListPage(StardagBaseModel):
    """One page of ``GET /builds``.

    ``total`` counts everything matching the filter, not this page — the
    two together are what tells a caller whether it has seen everything.
    """

    builds: list[BuildSummary] = []
    total: int = 0
    page: int = 1
    page_size: int = 0


class BuildCancelResult(BuildSummary):
    """Response of ``POST /builds/{id}/cancel``.

    A superset of :class:`BuildSummary`, mirroring the server. The
    cascade fields are empty/zero unless the call passed ``cascade=True``
    — and on a server predating the cascade they are absent from the
    response and default here, which reads correctly as "nothing was
    cascaded".
    """

    # Tasks moved to CANCELLED alongside the build, releasing their
    # execution claims and any concurrency-limit slots they held.
    cascaded_task_ids: list[str] = []
    cascaded_task_count: int = 0


class BulkCancelBuildRef(StardagBaseModel):
    """One build cancelled — or, in a dry run, *selected* — by bulk cancel."""

    build_id: UUID
    name: str = ""
    # The idleness signal the selection was made on (see
    # :class:`BuildSummary` on why this is not ``last_active_at``).
    last_activity_at: datetime | None = None
    reactive_app_name: str | None = None
    # Tasks cancelled along with the build; empty when cascade is off.
    cascaded_task_ids: list[str] = []


class BulkCancelResult(StardagBaseModel):
    """Result of ``POST /builds/bulk-cancel``.

    In a dry run this reports exactly what a real run would have done and
    nothing is written — which is what makes it safe to make ``dry_run``
    the default of any cleanup UX built on top.
    """

    dry_run: bool = False
    builds: list[BulkCancelBuildRef] = []
    build_count: int = 0
    task_count: int = 0
    # Explicitly-requested build ids that were *not* acted on, keyed by id
    # with a machine-readable reason: "not_found" (unknown, or another
    # environment — deliberately indistinguishable), "not_running",
    # "reactive", "not_idle".
    skipped: dict[str, str] = {}
    # More builds matched the filter than ``limit`` allowed — call again.
    truncated: bool = False


class TaskSummary(StardagBaseModel):
    """One task row from ``GET /tasks``.

    The status fields are *environment-global*, not per build: a task row
    is unique per ``(environment_id, task_id)``, so a task left RUNNING by
    a build whose orchestrator died denies the execution claim to every
    future build that needs it until something moves it.
    ``latest_status_build_id`` is therefore the answer to "who is holding
    this claim", and ``latest_status_at`` to "since when".
    """

    id: UUID
    task_id: str
    task_namespace: str = ""
    task_name: str = ""
    version: str | None = None
    output_uri: str | None = None
    created_at: datetime | None = None
    is_phantom: bool = False
    latest_status: str | None = None
    latest_status_at: datetime | None = None
    latest_status_build_id: UUID | None = None
    latest_executor: str | None = None
    latest_executor_ref: str | None = None
    latest_executor_metadata: dict[str, Any] | None = None


class TaskListPage(StardagBaseModel):
    """One page of ``GET /tasks``."""

    tasks: list[TaskSummary] = []
    total: int = 0
    page: int = 1
    page_size: int = 0


class TickSummaryRecord(StardagBaseModel):
    """A persisted reactive-scheduler tick summary.

    ``summary`` is the dict the SDK reported, verbatim and including
    ``outcome`` — the server stores it as an open blob, so a client may
    encounter keys neither it nor the server knows about. Render it
    generically rather than field by field.
    """

    id: UUID
    build_id: UUID
    outcome: str
    summary: dict[str, Any] = {}
    created_at: datetime | None = None


class TaskMetadata(StardagBaseModel):
    """Metadata for a registered task in the registry."""

    # Core Task fields
    id: UUID
    body: dict[str, Any]
    name: str
    namespace: str
    version: str
    output_uri: str | None  # only if the task has a FileSystemTarget output
    # Registry Metadata fields
    status: str
    registered_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None


class RegistryABC(metaclass=abc.ABCMeta):
    """Abstract base class for task registries.

    A registry tracks task execution within builds. Implementations must
    provide at least the `task_register` method. All other methods have default
    no-op implementations for backwards compatibility.

    The registry is stateless with respect to build_id - the build_id is passed
    explicitly to all methods that need it. This allows a single registry instance
    to be reused across multiple builds.

    Method naming convention:
    - Build methods: build_<action> (e.g., build_start, build_complete)
    - Task methods: task_<action> (e.g., task_register, task_start)
    - Async versions: <method>_aio suffix (e.g., build_start_aio, task_register_aio)
    """

    # -------------------------------------------------------------------------
    # Build lifecycle methods
    # -------------------------------------------------------------------------

    def build_start(
        self,
        root_tasks: list["BaseTask"] | None = None,
        description: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
    ) -> UUID:
        """Start a new build session.

        Called at the beginning of a build. Returns a build ID.

        Args:
            root_tasks: The root tasks being built
            description: Optional description of the build
            executor_metadata: Optional metadata describing where/how the
                build is executed (e.g. the Modal app/workspace/environment
                for a triggered build). Backends that don't track it may
                ignore it.

        Returns:
            Build ID (UUID) for the new build session.
        """
        return UUID("00000000-0000-0000-0000-000000000000")

    def build_resume(
        self,
        build_id: UUID,
        executor_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Mark an existing build as resumed.

        Called when ``sd.build(resume_build_id=...)`` reuses an existing
        build (potentially in a terminal state) instead of starting a new
        one. The registry should record a BUILD_RESUMED event so the
        build flips back to RUNNING and the UI can surface a
        "running (resumed)" affordance.

        Default implementation is a no-op so older registry backends
        keep working unchanged.

        Args:
            build_id: The build UUID being resumed.
            executor_metadata: Optional metadata describing where/how the
                resumed build is executed (see :meth:`build_start`).
        """
        pass

    def build_complete(self, build_id: UUID) -> None:
        """Mark a build as completed successfully.

        Args:
            build_id: The build UUID returned by build_start.
        """
        pass

    def build_fail(self, build_id: UUID, error_message: str | None = None) -> None:
        """Mark a build as failed.

        Args:
            build_id: The build UUID returned by build_start.
            error_message: Optional error message describing the failure.
        """
        pass

    def build_cancel(
        self, build_id: UUID, *, cascade: bool = False
    ) -> "BuildCancelResult | None":
        """Cancel a build, optionally releasing the claims its tasks hold.

        Cancelling a build records a build-level event and nothing else,
        which is why it has never actually cleaned anything up: task rows
        are per *environment* with a denormalised global status, so a task
        the build left RUNNING keeps denying its execution claim — and
        occupying its concurrency-limit slots — long after the build is
        gone. ``cascade=True`` cancels those tasks too.

        Default False: cascading is a behaviour change for existing
        callers, and the build engine's own fail-fast path already cancels
        its running tasks itself.

        Returns the cancelled build plus what the cascade released, or
        None for backends that don't report it (the default). The return
        value exists for operator tooling; lifecycle callers ignore it.
        Same optional-return convention as ``task_register_bulk``.
        """
        return None

    def build_exit_early(self, build_id: UUID, reason: str | None = None) -> None:
        """Mark a build as exited early.

        Called when all remaining tasks are running in other builds
        and this build should stop waiting.

        Args:
            build_id: The build UUID returned by build_start.
            reason: Optional reason for exiting early.
        """
        pass

    # -------------------------------------------------------------------------
    # Task lifecycle methods
    # -------------------------------------------------------------------------

    @abc.abstractmethod
    def task_register(self, build_id: UUID, task: "BaseTask") -> None:
        """Register a task as pending/scheduled.

        This is called when a task is about to be executed.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task to register.
        """
        pass

    def task_register_bulk(
        self,
        build_id: UUID,
        tasks: Sequence["BaseTask"],
        *,
        limit_keys: Mapping[UUID, Sequence[str]] | None = None,
    ) -> list[RegisteredTaskInfo] | None:
        """Register many tasks to a build in a single call.

        Default implementation falls back to ``task_register`` per task —
        backends that can batch (e.g. the API registry's bulk endpoint)
        should override this to make one HTTP call instead of N.

        Order of ``tasks`` is significant: the SDK's post-order discover
        walk emits deps before parents so that ``dependency_task_ids``
        lookups inside the registry resolve to existing rows (no phantom
        creation). Backends that process the batch as one transaction
        should preserve array order.

        Args:
            build_id: The build UUID returned by build_start.
            tasks: Tasks to register, in registration order.

        Returns:
            Per-task :class:`RegisteredTaskInfo` (used by the build engine
            to re-attach to detached executions that are still running), or
            None when the backend doesn't provide it.

        ``limit_keys`` maps task ids to the named concurrency-limit keys the
        task runs under, for backends that record them at plan time (the
        API registry does — a slot release wakes the builds queued on a key
        only if the registry knows which pending tasks want it). The
        default ignores it.
        """
        for task in tasks:
            self.task_register(build_id, task)
        return None

    def build_list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: str | None = None,
        reactive_app_name: str | None = None,
        idle_for_seconds: int | None = None,
    ) -> BuildListPage:
        """List builds in the environment, most recently active first.

        The general listing behind ``build_list_running`` and the CLI's
        ``stardag builds list``. Filters are applied *server-side*:

            status: derived build status (e.g. ``"running"``).
            reactive_app_name: only builds driven by this reactive app.
            idle_for_seconds: only builds with no activity of any kind for
                at least this long (minimum 60). Measured on
                ``BuildSummary.last_activity_at`` — see that class for why
                that is not ``last_active_at``.

        Default: not supported (backends that cannot enumerate builds).
        """
        raise NotImplementedError(f"{type(self).__name__} does not support build_list")

    def build_list_running(
        self, limit: int = 100, reactive_app_name: str | None = None
    ) -> list[UUID]:
        """List ids of builds currently in RUNNING status (most recent first).

        Used by the reactive scheduler watchdog to sweep for builds that may
        need a tick. ``reactive_app_name`` narrows the listing to builds
        reactively scheduled by that app — the watchdog's actual question, and
        what keeps ``limit`` from being consumed by builds no tick of this app
        can advance (resident builds, and builds left RUNNING by an
        orchestrator that died without emitting a terminal event).

        Default: empty (no reactive-scheduling support).
        """
        return []

    async def build_list_running_aio(
        self, limit: int = 100, reactive_app_name: str | None = None
    ) -> list[UUID]:
        """Async version of build_list_running."""
        return self.build_list_running(limit, reactive_app_name)

    def build_bulk_cancel(
        self,
        *,
        build_ids: Sequence[UUID | str] | None = None,
        idle_for_seconds: int | None = None,
        reactive_app_name: str | None = None,
        include_reactive: bool = False,
        cascade: bool = True,
        dry_run: bool = False,
        limit: int = 100,
        reason: str | None = None,
    ) -> BulkCancelResult:
        """Cancel RUNNING builds matching a filter (bulk cleanup / reaper).

        Nothing terminates abandoned builds on its own: build status is
        derived from build-level events, so a build whose orchestrator
        died without emitting one stays RUNNING forever, holding whatever
        execution claims and concurrency-limit slots its tasks had when it
        vanished. This is the cleanup.

        At least one of ``build_ids`` / ``idle_for_seconds`` is required —
        an unqualified "cancel everything running" is not a cleanup
        operation. Only RUNNING builds are ever eligible, which makes the
        call idempotent. Reactive builds are excluded unless
        ``include_reactive`` (or ``reactive_app_name``) says otherwise:
        they are quiet between ticks *by design*, so quiet does not mean
        abandoned. ``cascade`` defaults True here (unlike the single-build
        cancel) because releasing leaked claims is the entire point.

        ``dry_run=True`` reports the exact same selection — builds, the
        tasks a real run would cancel, and the per-build ``skipped``
        reasons — and writes nothing. Prefer it over reimplementing
        selection client-side: the server's answer is the one that will
        actually be acted on.

        Default: not supported.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support build_bulk_cancel"
        )

    def build_report_tick_summary(
        self, build_id: UUID, summary: dict[str, Any]
    ) -> None:
        """Record one reactive scheduler tick's summary against a build.

        Pure observability, on a hot path: callers report and move on, and
        must never fail a tick because this failed. ``summary`` is stored
        verbatim server-side apart from a required ``outcome`` key, so new
        summary fields need no server release. Default: no-op — a backend
        without the endpoint simply keeps the trail in its logs.
        """
        pass

    async def build_report_tick_summary_aio(
        self, build_id: UUID, summary: dict[str, Any]
    ) -> None:
        """Async version of build_report_tick_summary."""
        self.build_report_tick_summary(build_id, summary)

    def build_list_tick_summaries(
        self, build_id: UUID, limit: int = 20
    ) -> list[TickSummaryRecord]:
        """List a build's retained tick summaries, newest first.

        The read side of ``build_report_tick_summary``: "why is this build
        not progressing?" answered from the scheduler's own account of
        each tick, instead of from logs scattered across short-lived tick
        containers. Retention is finite server-side. Default: empty.
        """
        return []

    def task_list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: Sequence[str] | None = None,
        status_older_than: datetime | None = None,
        task_name: str | None = None,
        task_namespace: str | None = None,
    ) -> TaskListPage:
        """List tasks in the environment.

        ``status`` is the *environment-global* status and may name several
        values (``["running", "suspended"]`` matches either) — which makes
        this the way to ask "which tasks are holding an execution claim?".
        ``status_older_than`` is an absolute cutoff (``latest_status_at <
        status_older_than``), not a duration, so a paged scan cannot drift
        while it pages; tasks with no recorded timestamp never match.

        With either filter applied, results come back oldest-claim-first
        — the triage order.

        Default: not supported.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support task_list")

    def task_cancel_by_id(self, build_id: UUID, task_id: str) -> None:
        """Cancel a task addressed by id rather than by task object.

        Same event as ``task_cancel``; separate because operator tooling
        (and the server) only ever has the id — rehydrating a task object
        just to cancel it would fail for exactly the abandoned tasks that
        most need cancelling. Default: no-op.
        """
        pass

    async def task_cancel_by_id_aio(self, build_id: UUID, task_id: str) -> None:
        """Async version of task_cancel_by_id."""
        self.task_cancel_by_id(build_id, task_id)

    def task_retry_by_id(self, build_id: UUID, task_id: str) -> None:
        """Reset a retryable task to pending, addressed by id.

        See ``task_cancel_by_id`` for why the id-addressed variant exists.
        Default: no-op.
        """
        pass

    async def task_retry_by_id_aio(self, build_id: UUID, task_id: str) -> None:
        """Async version of task_retry_by_id."""
        self.task_retry_by_id(build_id, task_id)

    def build_add_roots(self, build_id: UUID, root_task_ids: list[str]) -> None:
        """Append root task ids to a build (reactive re-trigger with new roots).

        Default: no-op.
        """
        pass

    async def build_add_roots_aio(
        self, build_id: UUID, root_task_ids: list[str]
    ) -> None:
        """Async version of build_add_roots."""
        self.build_add_roots(build_id, root_task_ids)

    def task_retry(self, build_id: UUID, task: "BaseTask") -> None:
        """Reset a failed/cancelled/skipped task to pending (retry).

        Backends flip only terminal-but-retryable statuses; completed and
        running tasks are unaffected. Default: no-op.
        """
        pass

    async def task_retry_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version of task_retry."""
        self.task_retry(build_id, task)

    def build_skip_blocked(self, build_id: UUID) -> list[str]:
        """Mark tasks transitively blocked by failures as skipped.

        Returns the skipped task ids. Default: no-op (empty).
        """
        return []

    async def build_skip_blocked_aio(self, build_id: UUID) -> list[str]:
        """Async version of build_skip_blocked."""
        return self.build_skip_blocked(build_id)

    def build_notify(
        self, build_id: UUID, *, can_spawn: bool = True
    ) -> "BuildNotifyResult":
        """Set the build's scheduler wake-up flag (reactive scheduling).

        Returns what the server knew *after* the set — in particular
        whether a scheduler is live (see :class:`BuildNotifyResult`), which
        is what lets a caller skip spawning a tick that would only find the
        scheduler lease held and exit.

        Default: no-op, reporting an unknown scheduler state so callers
        keep spawning unconditionally (backends without reactive-scheduling
        support).
        """
        return BuildNotifyResult(build_id=build_id)

    async def build_notify_aio(
        self, build_id: UUID, *, can_spawn: bool = True
    ) -> "BuildNotifyResult":
        """Async version of build_notify."""
        return self.build_notify(build_id, can_spawn=can_spawn)

    def build_get_notify(self, build_id: UUID) -> BuildNotifyResult:
        """Read the build's scheduler wake-up flag without setting it.

        The linger poll's question, and the cheapest possible answer to it:
        one boolean, one row. A lingering tick asks it every few seconds and
        used to ask it by fetching the whole frontier, of which it read this
        field alone.

        ``scheduler_live`` is not reported — the caller holds the lease, so
        the answer is always itself.

        Default: read the flag off the frontier, which is where this poll
        got it before and which every backend that supports reactive
        scheduling can already answer. Deliberately *not* a constant:
        "always needs a tick" turns the linger loop into a hot loop, and
        "never" stalls the build until the watchdog — there is no safe
        direction to default to, only the real answer.

        **Override both this and the async version** to make the poll
        cheap. Unlike every other ``_aio`` default on this class, which
        delegates to its sync twin, ``build_get_notify_aio`` goes sideways
        to ``build_get_frontier_aio`` — delegating to a blocking call on
        the hot path would stall the event loop. The cost of that choice is
        that overriding only the sync method here silently has no effect on
        the path the tick actually takes.
        """
        frontier = self.build_get_frontier(build_id)
        return BuildNotifyResult(build_id=build_id, needs_tick=frontier.needs_tick)

    async def build_get_notify_aio(self, build_id: UUID) -> BuildNotifyResult:
        """Async version of build_get_notify."""
        frontier = await self.build_get_frontier_aio(build_id)
        return BuildNotifyResult(build_id=build_id, needs_tick=frontier.needs_tick)

    async def build_acquire_scheduler_lease_aio(
        self, build_id: UUID, *, owner_id: str, ttl_seconds: int
    ) -> SchedulerLeaseResult:
        """Take the build's scheduler single-flight lease.

        At most one tick drives a build at a time. ``held=False`` means
        somebody else is, and this tick should no-op — safe because the
        wake-up that spawned it was flagged *before* the spawn, so the
        holder's own re-checks (its linger poll, then the exit handshake)
        cover it.

        Default: grant unconditionally. A backend with no notion of a lease
        cannot arbitrate one, and the honest failure there is duplicate
        ticks — wasteful, but idempotent, and task starts are arbitrated
        separately by the execution claim — rather than a build that
        nothing drives.
        """
        return SchedulerLeaseResult(build_id=build_id, held=True)

    async def build_renew_scheduler_lease_aio(
        self, build_id: UUID, *, owner_id: str, ttl_seconds: int
    ) -> SchedulerLeaseResult:
        """Extend the lease while a tick keeps driving the build.

        ``held=False`` means the lease lapsed and was taken over, so this
        tick no longer owns the build. Default: grant, per
        ``build_acquire_scheduler_lease_aio``.
        """
        return SchedulerLeaseResult(build_id=build_id, held=True)

    async def build_release_scheduler_lease_aio(
        self, build_id: UUID, *, owner_id: str
    ) -> SchedulerLeaseResult:
        """Drop the lease, if this caller still holds it. Default: no-op."""
        return SchedulerLeaseResult(build_id=build_id, held=True)

    def build_clear_notify(self, build_id: UUID) -> None:
        """Clear the build's scheduler wake-up flag. Default: no-op."""
        pass

    def build_wake_candidates(self, limit: int = 20) -> list[WakeCandidate]:
        """Hand out the reactive builds that need a tick and have no scheduler.

        The spawn half of a cross-build wake-up (``POST
        /builds/wake-candidates``): every build returned is flagged, holds
        no live scheduler lease, and has not been handed out within the
        server's window — and is marked handed out by this call, so
        concurrent callers get disjoint answers. The caller spawns one tick
        per entry. Default: nothing, which is correct for a backend with no
        notion of cross-build wake-ups.
        """
        return []

    async def build_wake_candidates_aio(self, limit: int = 20) -> list[WakeCandidate]:
        """Async version of build_wake_candidates."""
        return self.build_wake_candidates(limit)

    async def build_clear_notify_aio(self, build_id: UUID) -> None:
        """Async version of build_clear_notify."""
        self.build_clear_notify(build_id)

    def build_get_frontier(self, build_id: UUID) -> BuildFrontier:
        """Return the build's scheduling frontier (reactive scheduling).

        Default: not supported — reactive scheduling requires a registry
        backend that can compute the frontier (e.g. the API registry).
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support reactive scheduling "
            "(build_get_frontier)"
        )

    async def build_get_frontier_aio(self, build_id: UUID) -> BuildFrontier:
        """Async version of build_get_frontier."""
        return self.build_get_frontier(build_id)

    def build_get_executions(self, build_id: UUID) -> BuildExecutions:
        """Detached executions this build must stop (``GET .../executions``).

        Authority to revoke is build-scoped, so an engine tearing a build
        down has to know which executions are its own. Default: not
        supported — same shape as the frontier, and for the same reason.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support build_get_executions"
        )

    async def build_get_executions_aio(self, build_id: UUID) -> BuildExecutions:
        """Async version of build_get_executions."""
        return self.build_get_executions(build_id)

    def build_get(self, build_id: UUID) -> BuildInfo:
        """Return a slim build record (``GET /builds/{id}``).

        Lighter than ``build_get_frontier`` (no frontier computation): used
        by the reactive tick's pre-lease marker/ownership gate, which only
        needs ``reactive_app_name``/``reactive_tick_kwargs``. Default: not
        supported.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support build_get")

    async def build_get_aio(self, build_id: UUID) -> BuildInfo:
        """Async version of build_get."""
        return self.build_get(build_id)

    def build_get_summary(self, build_id: UUID) -> BuildSummary:
        """Return the full build record (``GET /builds/{id}``).

        Same endpoint as ``build_get``, deliberately a different read
        model. ``BuildInfo`` is the contract a *custom* registry backend
        must satisfy for reactive scheduling — four fields, all of which
        such a backend necessarily has. ``BuildSummary`` is the operator
        view of an API-registry build: names, timestamps, liveness. Fusing
        them would make every reactive-capable backend responsible for
        fields it has no notion of.

        Default: not supported.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support build_get_summary"
        )

    def build_set_reactive_meta(
        self,
        build_id: UUID,
        *,
        app_name: str,
        tick_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Mark a build reactively scheduled and store its tick config.

        Upsert (idempotent). ``app_name`` (the marker/owner) is always set
        and surfaces as ``reactive_app_name`` on the build/frontier. When
        ``tick_kwargs`` is None (a bare re-trigger) the stored config is left
        untouched — so a re-trigger with no explicit tick_kwargs preserves
        the existing ones; passing tick_kwargs updates them. Default: no-op
        (backends without reactive support).
        """
        pass

    async def build_set_reactive_meta_aio(
        self,
        build_id: UUID,
        *,
        app_name: str,
        tick_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Async version of build_set_reactive_meta."""
        self.build_set_reactive_meta(
            build_id, app_name=app_name, tick_kwargs=tick_kwargs
        )

    def task_start(
        self,
        build_id: UUID,
        task: "BaseTask",
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
        claim_ttl_seconds: int | None = None,
    ) -> None:
        """Mark a task as started/running.

        Called immediately before a task begins execution. The caller is
        responsible for having already registered the task in the build —
        ``task_start`` only emits the started event.

        ``executor`` / ``executor_ref`` identify a detached execution (e.g.
        executor="modal" with a Modal function call id) so a later resumed
        build can re-attach instead of re-executing.
        ``executor_metadata`` optionally describes the execution backend in
        more detail (e.g. Modal app/workspace/environment/function) for
        surfacing in the UI. Backends that don't track them may ignore all
        three.

        ``claim_ttl_seconds`` is how long the execution claim this start
        records stays honoured (see
        ``FrontierTaskRef.latest_status_expires_at``). None leaves it to the
        backend's own default. Callers that know the wall-clock limit the
        execution runs under should pass it rather than accept that default
        — see ``stardag.build._reactive.claim_ttl_seconds``.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task that is starting.
        """
        pass

    def task_complete(self, build_id: UUID, task: "BaseTask") -> None:
        """Mark a task as completed successfully.

        Called after a task finishes execution without errors.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task that completed.
        """
        pass

    def task_fail(
        self, build_id: UUID, task: "BaseTask", error_message: str | None = None
    ) -> None:
        """Mark a task as failed.

        Called when a task raises an exception during execution.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task that failed.
            error_message: Optional error message describing the failure.
        """
        pass

    def task_interrupt(
        self, build_id: UUID, task: "BaseTask", reason: str | None = None
    ) -> None:
        """Record that a task's execution was interrupted by the platform.

        Not a failure: the execution ended for a reason unrelated to the
        task's correctness (the backend hit its function timeout, or
        reclaimed the container), so the task is the scheduler's to start
        again. Called by the worker itself, in the grace window the
        platform gives it before the kill, which is what releases the
        execution claim and any concurrency-limit slots straight away.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task whose execution was interrupted.
            reason: Optional description of what interrupted it.
        """
        pass

    def task_suspend(self, build_id: UUID, task: "BaseTask") -> None:
        """Mark a task as suspended waiting for dynamic dependencies.

        Called when a task yields dynamic deps that are not yet complete.
        The task will remain suspended until its dynamic deps are built.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task that is suspended.
        """
        pass

    def task_add_dependencies(
        self,
        build_id: UUID,
        task: "BaseTask",
        upstream_tasks: Sequence["BaseTask"],
        is_dynamic: bool = True,
    ) -> None:
        """Record dependency edges for a task.

        Called by the build system when a task yields dynamic deps — the
        edges aren't known at ``task_register`` time (static ``requires()``
        chain only), so this is how they reach the registry so that the
        full DAG renders correctly in the UI.

        Registries that can't write to a graph (the in-memory cases) may
        treat this as a no-op. HTTP-backed implementations should tolerate
        404 from older API versions that don't support the endpoint.

        Args:
            build_id: The build UUID returned by build_start.
            task: The downstream task whose deps are being added.
            upstream_tasks: The yielded deps to record as edges.
            is_dynamic: Marks the edges as dynamic (True by default —
                static ``requires()`` are recorded during task_register).
        """
        pass

    def task_resume(self, build_id: UUID, task: "BaseTask") -> None:
        """Mark a task as resumed after dynamic dependencies completed.

        Called when a task's dynamic dependencies are complete and
        the task is ready to continue execution (either by resuming
        a suspended generator or by re-executing the task).

        Args:
            build_id: The build UUID returned by build_start.
            task: The task that is resuming.
        """
        pass

    def task_cancel(self, build_id: UUID, task: "BaseTask") -> None:
        """Cancel a task.

        Called when a task is cancelled — by the user, or by the build
        engine when terminating in-flight siblings on a fail-fast failure.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task to cancel.
        """
        pass

    def task_skip(self, build_id: UUID, task: "BaseTask") -> None:
        """Mark a task as skipped.

        Called when a task will not run because a dependency failed or
        was cancelled. Distinct from ``task_cancel``: skipped tasks
        never started executing.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task to skip.
        """
        pass

    def task_waiting_for_lock(
        self, build_id: UUID, task: "BaseTask", lock_owner: str | None = None
    ) -> None:
        """Record that a task is waiting for a global lock.

        Called when a task cannot acquire its lock because another
        build is holding it.

        Args:
            build_id: The build UUID returned by build_start.
            task: The task waiting for the lock.
            lock_owner: Optional identifier of who holds the lock.
        """
        pass

    def task_upload_artifacts(
        self, build_id: UUID, task: "BaseTask", artifacts: Sequence["Artifact"]
    ) -> None:
        """Upload artifacts for a completed task.

        Called after a task completes successfully if it has artifacts.

        Args:
            build_id: The build UUID returned by build_start.
            task: The completed task.
            artifacts: List of artifacts to upload.
        """
        pass

    @abc.abstractmethod
    def task_get_metadata(self, task_id: UUID) -> TaskMetadata:
        """Get metadata for a registered task.

        Args:
            task_id: The ID of the task to get metadata for.
        Returns:
            A TaskMetadata object containing task metadata.
        """
        pass

    # -------------------------------------------------------------------------
    # Async versions - default implementations delegate to sync methods
    # -------------------------------------------------------------------------

    async def build_start_aio(
        self,
        root_tasks: list["BaseTask"] | None = None,
        description: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
    ) -> UUID:
        """Async version of build_start."""
        return self.build_start(
            root_tasks, description, executor_metadata=executor_metadata
        )

    async def build_resume_aio(
        self,
        build_id: UUID,
        executor_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Async version of build_resume."""
        self.build_resume(build_id, executor_metadata=executor_metadata)

    async def build_complete_aio(self, build_id: UUID) -> None:
        """Async version of build_complete."""
        self.build_complete(build_id)

    async def build_fail_aio(
        self, build_id: UUID, error_message: str | None = None
    ) -> None:
        """Async version of build_fail."""
        self.build_fail(build_id, error_message)

    async def build_cancel_aio(
        self, build_id: UUID, *, cascade: bool = False
    ) -> "BuildCancelResult | None":
        """Async version of build_cancel."""
        return self.build_cancel(build_id, cascade=cascade)

    async def build_exit_early_aio(
        self, build_id: UUID, reason: str | None = None
    ) -> None:
        """Async version of build_exit_early."""
        self.build_exit_early(build_id, reason)

    async def task_register_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version of task_register."""
        self.task_register(build_id, task)

    async def task_register_bulk_aio(
        self,
        build_id: UUID,
        tasks: Sequence["BaseTask"],
        *,
        limit_keys: Mapping[UUID, Sequence[str]] | None = None,
    ) -> list[RegisteredTaskInfo] | None:
        """Async version of task_register_bulk.

        Default implementation falls back to ``task_register_aio`` per
        task. Override for backends that can batch (the API registry
        does so with the ``/tasks/bulk`` endpoint).
        """
        for task in tasks:
            await self.task_register_aio(build_id, task)
        return None

    async def task_start_claim_aio(
        self,
        build_id: UUID,
        task: "BaseTask",
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
        limit_keys: Sequence[str] | None = None,
        claim_ttl_seconds: int | None = None,
        *,
        claim: bool = True,
    ) -> StartClaimResult:
        """Mark a task started, arbitrating an execution claim and limit slots.

        The claim guarantees at most one concurrent RUNNING execution per
        task (environment-wide, across builds): a start racing an existing
        RUNNING task is denied — with the running execution's ref echoed —
        instead of recorded. COMPLETED tasks deny with
        ``already_completed`` (callers treat this like the lock's
        ALREADY_COMPLETED: verify the target with eventual-consistency
        retries). ``limit_keys`` compose atomically (a denied claim
        consumes no slots).

        ``claim`` is **keyword-only**, and that is what makes it safe: no
        positional argument can reach it, at any position. Had it been
        ordinary and placed before ``claim_ttl_seconds``, a positional TTL
        would have bound to it — any truthy int reads as "claim" — and the
        TTL would silently become None, which is exactly the unexpiring
        claim this API works to prevent. Its position at the end is then
        only tidiness; the ``*`` is the guarantee.

        ``claim=False`` acquires the limit slots without arbitrating: the
        start is recorded even against a live claim, and the only denial
        left is ``limit``. That is not a weaker claim, it is the absence of
        one, and exactly one caller wants it — a limiter acquiring slots
        for a task its *own* build has already claimed (see
        ``stardag.build._registry_limiter``), which a claiming start would
        deny as ``already_running``. Everything that arbitrates leaves it
        alone.

        ``claim_ttl_seconds`` bounds how long the granted claim is honoured
        (surfaced to every reader as
        ``FrontierTaskRef.latest_status_expires_at``). Past it the claim is
        re-claimable and stops occupying concurrency slots, which is what
        lets a build in *another* scheduler decide a claim is abandoned
        without probing an executor it cannot reach. None leaves it to the
        backend's default; derive it from the execution's own wall-clock
        limit where one is known (see
        ``stardag.build._reactive.claim_ttl_seconds``).

        **This is the extension seam for custom arbitration backends**: a
        custom ``RegistryABC`` implementation can arbitrate however it
        likes (Redis, DynamoDB, ...), keeping claim, status and completion
        consistent in one backend. There is deliberately no default
        implementation: a backend that answers "you won" without
        arbitrating is indistinguishable from one that arbitrates
        correctly, and the build engines rely on this method for
        exactly-once execution. Implement it, or subclass
        :class:`NoOpRegistry` to opt out explicitly.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement task_start_claim_aio. "
            "Implement it to arbitrate per-task execution claims (see "
            "APIRegistry), or subclass NoOpRegistry to opt out of "
            "arbitration."
        )

    async def task_start_aio(
        self,
        build_id: UUID,
        task: "BaseTask",
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
        claim_ttl_seconds: int | None = None,
    ) -> None:
        """Async version of task_start."""
        self.task_start(
            build_id,
            task,
            executor=executor,
            executor_ref=executor_ref,
            executor_metadata=executor_metadata,
            claim_ttl_seconds=claim_ttl_seconds,
        )

    async def task_complete_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version of task_complete."""
        self.task_complete(build_id, task)

    async def task_fail_aio(
        self, build_id: UUID, task: "BaseTask", error_message: str | None = None
    ) -> None:
        """Async version of task_fail."""
        self.task_fail(build_id, task, error_message)

    async def task_interrupt_aio(
        self, build_id: UUID, task: "BaseTask", reason: str | None = None
    ) -> None:
        """Async version of task_interrupt."""
        self.task_interrupt(build_id, task, reason)

    async def task_suspend_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version of task_suspend."""
        self.task_suspend(build_id, task)

    async def task_add_dependencies_aio(
        self,
        build_id: UUID,
        task: "BaseTask",
        upstream_tasks: Sequence["BaseTask"],
        is_dynamic: bool = True,
    ) -> None:
        """Async version of task_add_dependencies."""
        self.task_add_dependencies(build_id, task, upstream_tasks, is_dynamic)

    async def task_resume_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version of task_resume."""
        self.task_resume(build_id, task)

    async def task_cancel_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version of task_cancel."""
        self.task_cancel(build_id, task)

    async def task_skip_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version of task_skip."""
        self.task_skip(build_id, task)

    async def task_waiting_for_lock_aio(
        self, build_id: UUID, task: "BaseTask", lock_owner: str | None = None
    ) -> None:
        """Async version of task_waiting_for_lock."""
        self.task_waiting_for_lock(build_id, task, lock_owner)

    async def task_upload_artifacts_aio(
        self, build_id: UUID, task: "BaseTask", artifacts: Sequence["Artifact"]
    ) -> None:
        """Async version of task_upload_artifacts."""
        self.task_upload_artifacts(build_id, task, artifacts)

    async def task_get_metadata_aio(self, task_id: UUID) -> TaskMetadata:
        """Async version of task_get_metadata."""
        return self.task_get_metadata(task_id)


class NoOpRegistry(RegistryABC):
    """A registry that does nothing.

    Used as a default when no registry is configured.
    """

    def build_start(
        self,
        root_tasks: list["BaseTask"] | None = None,
        description: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
    ) -> UUID:
        """Return a placeholder build ID."""
        return UUID("00000000-0000-0000-0000-000000000000")

    def task_register(self, build_id: UUID, task: "BaseTask") -> None:
        pass

    def task_get_metadata(self, task_id: UUID) -> TaskMetadata:
        raise NotImplementedError("NoOpRegistry does not support task_get_metadata.")

    async def task_start_claim_aio(
        self,
        build_id: UUID,
        task: "BaseTask",
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
        limit_keys: Sequence[str] | None = None,
        claim_ttl_seconds: int | None = None,
        *,
        claim: bool = True,
    ) -> StartClaimResult:
        """Always grant: there is nothing to arbitrate against.

        This is the registry-*less* path — no shared state records that a
        task is RUNNING, so no other execution can be observed and no
        cross-build exactly-once guarantee is on offer in the first place.
        Granting unconditionally is the honest answer here, unlike on
        :class:`RegistryABC` (where it would silently defeat arbitration a
        real backend was expected to provide).
        """
        return StartClaimResult(started=True)


def init_registry() -> RegistryABC:
    """Initialize the default registry based on configuration.

    Returns APIRegistry if registry is configured, otherwise NoOpRegistry.
    """
    from stardag.config import config_provider
    from stardag.registry._api_registry import APIRegistry

    config = config_provider.get()

    if config.registry is not None:
        return APIRegistry()

    return NoOpRegistry()


registry_provider = resource_provider(RegistryABC, init_registry)


@lru_cache
def get_git_commit_hash() -> str:
    """Get the short SHA of the current Git commit."""

    supported_env_vars = ["SHORT_SHA", "COMMIT_HASH"]

    for env_var in supported_env_vars:
        short_sha = os.environ.get(env_var)
        if short_sha:
            return short_sha

    try:
        short_sha = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
            )
            .strip()
            .decode("utf-8")
        )
        # Check if there are uncommitted changes
        dirty_flag = subprocess.check_output(
            ["git", "status", "--porcelain"], stderr=subprocess.DEVNULL
        ).strip()

        if dirty_flag:
            short_sha += "-dirty"

        return short_sha

    except subprocess.CalledProcessError:
        raise RuntimeError(
            "Unable to get Git commit short SHA, you need to either run in an "
            "environment where git is available or set one of the env vars SHORT_SHA "
            "or COMMIT_HASH."
        )
