from __future__ import annotations

import asyncio
import logging
import typing
from dataclasses import asdict, dataclass
from typing import Callable, Sequence
from uuid import UUID, uuid4

from stardag import (
    BaseTask,
)
from stardag.build._base import (
    FailMode,
    TaskExecutorABC,
    current_build_id_var,
)
from stardag.build._task_store import BuildTaskStore
from stardag.build._wakeups import SpawnTick, drain_wake_candidates
from stardag.exceptions import NotFoundError, is_missing_route_error
from stardag.registry import (
    RegistryABC,
)

from stardag.build._reactive._frontier_actions import (
    _act_on_frontier,
    _any_ref_settled,
)
from stardag.build._reactive._terminal import _handle_terminal

if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# Default in-flight bound for the frontier actions a tick performs per task
# (load / probe / claim / spawn / record). These are registry HTTP calls, and
# 50 is the resident engine's long-standing bound against the same registry.
_DEFAULT_MAX_CONCURRENCY = 50


# The scheduler lease's TTL, and how often it is renewed while a tick
# lingers.
#
# The TTL is the **dead-tick recovery window**, and that is what sizes it:
# nothing releases the lease of a container that vanished, so until it
# lapses the build is invisible to drainers *and* ``notify`` answers
# ``scheduler_live=True``, so workers skip their spawn too. A preempted tick
# therefore stalls its build for up to one TTL, with the watchdog off by
# default. 60 s is what the lock-table lease this replaces used, and there
# was no reason to lengthen it.
#
# Renewing at a third rather than the old half means two consecutive
# renewals can fail before the lease is at risk — and a third failure is
# survivable too, because a refused renewal re-acquires (see
# ``SchedulerLease._renew_once``).
_LEASE_TTL_SECONDS = 60
_LEASE_RENEW_INTERVAL_SECONDS = _LEASE_TTL_SECONDS / 3


class SchedulerLease:
    """The build's single-flight lease, held for the life of one tick.

    Replaces the lease that used to ride on the global concurrency lock —
    the one live use of a deprecated subsystem, which meant every reader
    assembled a lock name from a build id and queried a second table to
    answer "does this build have a scheduler?".

    Renews itself in the background while the tick lingers, and stops
    driving if it can no longer show that it holds the lease: continuing
    past that point is the double-scheduling the lease exists to prevent.

    "Can no longer show" is deliberately broader than "the server said no".
    A renewal that *raises* proves nothing, and a lease has an expiry
    whether or not anyone is reachable to confirm it — so the deadline is
    tracked client-side and consulted directly. Without that, a registry
    outage spanning the whole TTL would leave a tick driving a build whose
    lease had lapsed server-side and could already have been taken over,
    with nothing having returned an answer to notice.
    """

    def __init__(self, registry: RegistryABC, build_id: UUID) -> None:
        self._registry = registry
        self._build_id = build_id
        # Per-tick identity, not per-process: two ticks for one build in one
        # container must not be able to renew or release each other's lease.
        self._owner_id = uuid4().hex
        self.acquired = False
        self._renewal: asyncio.Task | None = None
        self._lost = False
        # Monotonic, because this is a duration from a local acquire, not a
        # point in the server's calendar — a clock skew between the two
        # must not be able to extend it.
        self._expires_at: float | None = None

    @property
    def lost(self) -> bool:
        """Whether this tick can still show that it holds the lease."""
        if self._lost:
            return True
        if self._expires_at is None:
            return False
        return asyncio.get_event_loop().time() >= self._expires_at

    def _arm(self, granted_after: float) -> None:
        # Anchored to a timestamp taken BEFORE the granting request was
        # sent, not to when its answer arrived: the server starts the TTL
        # when it writes the row, which is inside that window, so anchoring
        # at the reply would leave the client believing in the lease for up
        # to one network round-trip after the server had let it lapse. The
        # error must land on the safe side — expiring early is a spurious
        # re-acquire; expiring late is the double-drive the deadline exists
        # to rule out.
        self._expires_at = granted_after + _LEASE_TTL_SECONDS

    async def __aenter__(self) -> "SchedulerLease":
        asked_at = asyncio.get_event_loop().time()
        result = await self._registry.build_acquire_scheduler_lease_aio(
            self._build_id, owner_id=self._owner_id, ttl_seconds=_LEASE_TTL_SECONDS
        )
        self.acquired = result.held
        if self.acquired:
            self._arm(asked_at)
            self._renewal = asyncio.create_task(self._renew_forever())
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._renewal is not None:
            renewal, self._renewal = self._renewal, None
            renewal.cancel()
            # ``gather(..., return_exceptions=True)`` reaps the child's
            # ``CancelledError`` as a *result* rather than raising it, while
            # still propagating a cancellation of this task. Catching
            # ``CancelledError`` here cannot tell the two apart — we
            # cancelled the child ourselves, so ``renewal.cancelled()`` is
            # true whichever one arrived — and swallowing it would strand
            # the caller's cancellation. ``run_tick_aio`` is public, so a
            # caller wrapping it in ``wait_for`` or a TaskGroup hits this.
            await asyncio.gather(renewal, return_exceptions=True)
        if not self.acquired:
            return
        try:
            await self._registry.build_release_scheduler_lease_aio(
                self._build_id, owner_id=self._owner_id
            )
        except Exception as e:
            # Best-effort: an unreleased lease lapses on its own, so the
            # cost is one TTL of delay before another tick may drive the
            # build — never a lost wake-up.
            logger.warning(
                "Could not release the scheduler lease for build %s "
                "(ignored; it expires on its own): %s",
                self._build_id,
                e,
            )

    async def _renew_once(self) -> bool:
        """Extend the lease, re-acquiring if it lapsed. False = really lost.

        A refused renewal has two causes and they are not the same event.
        Usually nothing else is competing for the build, and the refusal
        just means our own lease expired while we were between renewals —
        healed by taking it again, which is the same re-acquire the server
        performs for any lapsed lease. Only if that *also* fails is somebody
        else driving the build, and only then must this tick stop.
        """
        asked_at = asyncio.get_event_loop().time()
        result = await self._registry.build_renew_scheduler_lease_aio(
            self._build_id, owner_id=self._owner_id, ttl_seconds=_LEASE_TTL_SECONDS
        )
        if result.held:
            self._arm(asked_at)
            return True
        asked_at = asyncio.get_event_loop().time()
        retaken = await self._registry.build_acquire_scheduler_lease_aio(
            self._build_id, owner_id=self._owner_id, ttl_seconds=_LEASE_TTL_SECONDS
        )
        if retaken.held:
            self._arm(asked_at)
            logger.info(
                "Scheduler lease for build %s had lapsed and was re-taken; "
                "nothing else was driving it.",
                self._build_id,
            )
            return True
        return False

    async def _renew_forever(self) -> None:
        while True:
            await asyncio.sleep(_LEASE_RENEW_INTERVAL_SECONDS)
            try:
                held = await self._renew_once()
            except Exception as e:
                # A blip is not a loss — but it is not proof of the
                # opposite either, so nothing is re-armed here. If the
                # registry stays unreachable past the TTL, ``lost`` turns
                # true on the clock alone and the tick stops.
                logger.warning(
                    "Could not renew the scheduler lease for build %s "
                    "(will retry; the lease expires on its own if this "
                    "keeps failing): %s",
                    self._build_id,
                    e,
                )
                continue
            if not held:
                logger.warning(
                    "Lost the scheduler lease for build %s to another tick. "
                    "Stopping rather than driving a build a successor is "
                    "already driving.",
                    self._build_id,
                )
                self._lost = True
                return


# =============================================================================
# The tick
# =============================================================================


@dataclass
class TickConfig:
    """Configuration for reactive scheduler ticks.

    ``linger_seconds`` spans the many-small-tasks ↔ few-long-tasks
    spectrum: while the DAG is churning the tick stays resident (each
    action resets the linger deadline) and behaves like a tight scheduling
    loop; when only long-running work remains in flight, the tick exits and
    nothing runs until a worker (or the watchdog) wakes the scheduler.
    """

    linger_seconds: float = 120.0
    poll_interval_seconds: float = 3.0
    fail_mode: FailMode = FailMode.FAIL_FAST
    # Budget, per task per build **round**, on how many executions a tick
    # will start for one task — counted server-side and read off the
    # frontier as ``FrontierTaskRef.attempt_count``, since a tick is
    # short-lived and remembers nothing. A round runs from the build's most
    # recent BUILD_RESUMED event (so re-triggering an existing build id
    # starts a fresh one and resets every task's count) or from the build's
    # beginning. A bare retry emits no such event and does not reset it.
    #
    # ``1`` disables retrying entirely: a failure is recorded and never
    # respawned, exactly as before this existed — and, being a budget of
    # one start, a task reset to PENDING after its one attempt in the
    # round is refused rather than started again.
    #
    # **Why the default is 2, not 1.** 1 would be the conservative choice
    # if the failures on this path were a random sample of failures — you
    # do not want a deterministic bug run three times. They are not. A tick
    # only ever sees the failures no backend can retry for it: a spawn that
    # failed before a container existed, and an execution the backend
    # killed or lost. An exception inside the container is self-reported by
    # the worker and leaves the frontier without a tick ever touching it,
    # so a task that is simply broken cannot spend this budget — which
    # removes the usual reason to default a retry policy to "off". What is
    # left is the status quo it replaces: under FAIL_FAST, one transient
    # spawn failure kills an entire build, and the only recovery is a human
    # noticing and re-triggering.
    #
    # **Why 2 and not more.** One retry covers the shape these failures
    # actually have — a spot preemption, an OOM on one worker, a backend
    # that refused a spawn for a moment. A task that fails the same way
    # twice is telling you something a third attempt will not fix, and
    # every extra attempt is paid in full on the pathological case. 2 is
    # also the smallest step that closes the gap, which is what a default
    # that changes existing behaviour should be.
    #
    # Note for DAGs with **dynamic dependencies**: resuming a SUSPENDED
    # task records a fresh start, so such a task accumulates attempts
    # simply by yielding. Resumption is never budget-gated (see
    # ``_act_on_frontier``) — a gate there would wedge dynamic DAGs
    # outright — but a suspend-heavy task does reach its retry budget
    # sooner than a plain one, so raise this if you retry those.
    max_attempts: int = 2
    # Budget, per task per build round, on how many times a task may ask
    # to be resumed before the scheduler stops obliging — the companion to
    # ``max_attempts``, and reached only by a task that raised
    # ``ResumableInterruption``.
    #
    # **Why a second budget rather than one.** An interruption is the
    # platform taking the container away: a function timeout, or a
    # reclaimed instance. The task did nothing wrong, and for the workload
    # this whole path exists for — a trainer that checkpoints and is
    # *supposed* to be killed and resumed until it converges — being
    # interrupted is not an error condition, it is the operating mode.
    # Charging those to ``max_attempts`` would exhaust a budget meant for
    # genuine failures and fail the build for the one reason it was
    # designed to survive. The precedent is resuming a SUSPENDED task,
    # which is never budget-gated for the same reason (see
    # ``_act_on_frontier``).
    #
    # **Why bounded at all, then.** "Not charged to the retry budget"
    # cannot mean "free": a task that times out every time would otherwise
    # loop forever, paying for a full container each round. So the bound
    # exists, and is set generously rather than tightly — 20 resumes of a
    # long training run is a plausible afternoon, 20 identical timeouts of
    # a hung task is a clear signal and a bounded bill.
    max_interruptions: int = 20
    # How many of a pass's per-task actions may be in flight at once. Each
    # actionable task costs a task-store read, an acquiring start, an
    # executor spawn and a ref-recording start; doing that serially makes a
    # wide layer a queue of thousands of round-trips in one container.
    #
    # Bounded rather than unbounded because the failure mode of "spawn
    # everything at once" is not better than the failure mode of "spawn
    # them one at a time" — it just moves it from the tick's own clock to
    # the registry's connection pool. The default is the resident engine's
    # (see ``_DEFAULT_MAX_CONCURRENCY``).
    max_concurrent_actions: int = _DEFAULT_MAX_CONCURRENCY
    # Hard cap on how many tasks ONE pass may spawn, i.e. how much work a
    # single container commits to. ``None`` (the default) derives it from a
    # wall-clock limit — see :func:`_spawn_cap` for the ladder.
    #
    # Truncation is never silent, and never a stall: the pass acted, so the
    # tick re-evaluates immediately on a fresh frontier (see
    # ``_run_tick_body_aio``'s ``if acted:`` branch) and picks up the rest.
    max_spawns_per_tick: int | None = None
    # The wall-clock limit of the container running THIS tick, when the
    # caller knows it (e.g. the Modal integration knows the ``timeout`` its
    # ``tick`` function was registered with). This is the quantity the
    # spawn cap actually wants: how long this process may live, not how
    # long the executions it spawns may run.
    #
    # Deployment infrastructure, not per-build configuration — deliberately
    # NOT in the Modal ``_TICK_KWARGS_ALLOWED`` allowlist, because a value
    # persisted in a build's stored tick config at trigger time would go
    # stale the moment the app is redeployed with a different timeout. The
    # one legitimate override is a caller that runs several ticks inside
    # one container and is passing on a *share* of it (the Modal watchdog
    # sweep does exactly this).
    tick_timeout_seconds: float | None = None
    # Maps a task to the named concurrency-limit keys it runs under (see
    # the registry's environment concurrency limits). Acquisition happens
    # atomically at task start, before the spawn: a denied task simply
    # stays in the frontier — a slot-holder's completion wakes the
    # scheduler (cross-build slot releases are covered by the watchdog).
    limit_key_selector: "Callable[[BaseTask], Sequence[str]] | None" = None
    # How to spawn a scheduler tick for a build on a deployed app. Used for
    # two things that mean the same — "somebody has to look at this build,
    # and it is not me": the post-release half of the exit handshake (see
    # ``_hand_off_if_needed``), and the cross-build drain that spawns ticks
    # for flagged builds nobody is serving (see ``_drain_wake_candidates``).
    # Deployed-app configuration for the same reason ``limit_key_selector``
    # is: it is a callable, and "how do I start a tick" is knowledge only
    # the executor integration has — ``run_tick_aio`` is deliberately
    # executor-agnostic.
    #
    # ``None`` disables both. That is correct for a caller whose wake-ups
    # spawn a tick unconditionally and that has no neighbours to wake — a
    # test harness, say. The two halves of the conditional wake-up go
    # together: an integration that makes its wake-ups conditional on
    # ``scheduler_live`` MUST provide this, or a wake-up that lands in the
    # release window is served by nobody until the watchdog.
    spawn_tick: "SpawnTick | None" = None
    # Report each tick's :class:`TickSummary` to the registry, so "why is
    # this build not progressing?" is answerable without reading logs
    # across many short-lived tick containers. Strictly best-effort: a
    # reporting failure never fails a tick or changes its outcome.
    #
    # Deployed-app configuration, NOT a per-trigger tick_kwarg (see
    # ``_TICK_KWARGS_ALLOWED``): the reasons to turn this off — a registry
    # without the endpoint, or a deployment that keeps its observability
    # elsewhere — are properties of the whole deployment, never of one
    # build. Widening the allowlist later is additive; narrowing it is not.
    report_tick_summaries: bool = True


@dataclass
class TickSummary:
    """Outcome of one scheduler tick, for logging/observability.

    Flat and JSON-friendly on purpose: the whole dataclass is serialized
    with ``dataclasses.asdict`` and reported to the registry.
    """

    # "not_reactive" | "lease_held" | "lease_lost" | "terminal" |
    # "lingered_out" | "error"
    outcome: str
    terminal_status: str | None = None
    # Set only for outcome == "error": the exception that ended the tick.
    # A crashed tick is the single most informative thing a "why did this
    # build stall?" query can find, so it is recorded like any other
    # outcome — but the exception itself is never masked or replaced (see
    # ``run_tick_aio``). The message is bounded so a pathological
    # traceback-in-a-message cannot push the summary past the server's
    # size cap and turn a recorded failure into an unrecorded one.
    error_type: str | None = None
    error_message: str | None = None
    spawned: int = 0
    self_healed: int = 0
    failed_recorded: int = 0
    cancelled_refs: int = 0
    iterations: int = 0
    limit_denied: int = 0
    claim_denied: int = 0
    skipped: int = 0
    # --- attempt budget (TickConfig.max_attempts) ---
    # Failures this tick recorded and then reset to PENDING because the
    # task's attempt budget for this build round still had room.
    retried: int = 0
    # Failures this tick recorded and deliberately left failed because the
    # budget was spent. Under FAIL_FAST every one of these fails the build
    # on the next pass, so a non-zero value here is the direct answer to
    # "why did this build fail when the failure looked transient?".
    retry_exhausted: int = 0
    # Starts this tick refused to make because the task arrived PENDING
    # with its budget already spent — i.e. a *bare* retry (the API's retry
    # route, the UI's Retry, ``stardag tasks retry``) put a budget-
    # exhausted task back in the frontier without starting a new round.
    # Counted separately from ``retry_exhausted`` because it is a different
    # event with a different remedy: not "a failure we won't retry" but "a
    # retry that cannot do anything". See ``_act_on_frontier``'s budget
    # gate.
    budget_denied: int = 0
    # --- interruptions (TickConfig.max_interruptions) ---
    # Interrupted tasks this tick resumed. A steadily climbing count on a
    # long-running task is the healthy checkpoint-and-resume signal, not a
    # problem.
    interruptions_restarted: int = 0
    # Interrupted tasks this tick refused to respawn because the
    # interruption budget for the round was spent, and failed instead.
    # Under FAIL_FAST each of these fails the build, so a non-zero value is
    # the direct answer to "why did my long-running task's build die?".
    interruptions_exhausted: int = 0
    # Interrupted tasks this tick converted to a retryable failure because
    # the registry cannot report interruption counts, so no budget could
    # bound resuming them. The only route to this counter.
    interruptions_failed: int = 0
    # Interrupted tasks left alone because their executor ref still probes
    # as live: the execution backend is retrying the input itself, so
    # spawning here would duplicate the execution. Not an error — it is the
    # guard working.
    interruptions_backend_retrying: int = 0
    # Cross-build blocking, summed over the terminal evaluations of this
    # tick (like limit_denied, these are counts of observations, not of
    # distinct tasks — one blocker seen on three linger passes counts
    # three times).
    #
    # ``external_blockers`` is every entry the frontier reported while the
    # build looked stalled; ``waited`` and ``fatal`` cover only the entries
    # that drove the wait-or-fail decision, so the three do not add up —
    # a blocker whose status is a result influences neither (see
    # ``_ExternalBlockers.inert``). A tick with waited > 0 and fatal == 0 is
    # the healthy "waiting on another build" state; fatal > 0 always
    # accompanies a failed build.
    external_blockers: int = 0
    external_blockers_waited: int = 0
    external_blockers_fatal: int = 0
    # Blockers this build reset so it could run them itself (the
    # collaboration path: a shared task another build cancelled is still this
    # build's to run). Only ever a task in this build's plan — the attempt
    # budget the reset is bounded by does not exist for anything else.
    in_build_blockers_reset: int = 0
    # --- exit handshake (see ``_hand_off_if_needed``) ---
    # Times the linger deadline expired with the wake-up flag set, so the
    # tick kept the lease and re-acted instead of exiting. The fast half of
    # the handshake: no successor container was needed.
    linger_extended: int = 0
    # Successor ticks spawned because the flag was set in the window
    # between this tick's last look and its release of the lease. Rare by
    # construction; a non-zero value is the handshake doing exactly the job
    # it exists for, not a problem.
    successor_spawned: int = 0
    # --- cross-build wake-ups (see ``_drain_wake_candidates``) ---
    # Ticks this tick spawned for *other* builds: flagged by the registry
    # (a task they hold changed status, or a slot they were queued on
    # freed) with no scheduler of their own live. Each is one neighbour
    # this tick's pass, or its exit, unblocked.
    neighbour_ticks_spawned: int = 0


# Outcomes worth persisting. ``not_reactive`` is excluded by definition:
# the build is not reactively scheduled, so the tick is a stray that did
# nothing and learnt nothing — pure noise in a finite retention window.
# Everything else says something: ``lease_held`` is contention (many of
# them means ticks are piling up on one build), ``lingered_out`` is a tick
# that found nothing to do, ``terminal`` carries the outcome and the
# counters explaining it, and ``error`` is a tick that crashed — the most
# informative of the lot.
_UNREPORTED_TICK_OUTCOMES = frozenset({"not_reactive"})

# Bounds on the recorded exception identity/text. The server caps a summary
# at 8 KiB of compact JSON and rejects anything larger, so an unbounded
# message — a chained traceback, a repr of a huge payload — would turn "this
# tick crashed" into no record at all, losing exactly the outcome most worth
# keeping. Generous enough that a real error message survives intact.
_MAX_ERROR_TYPE_CHARS = 128
_MAX_ERROR_MESSAGE_CHARS = 1024
_TRUNCATION_MARKER = "… [truncated]"


def _bounded(text: str, limit: int) -> str:
    """Clip ``text`` to ``limit`` characters, marking that it was clipped."""
    if len(text) <= limit:
        return text
    return text[: limit - len(_TRUNCATION_MARKER)] + _TRUNCATION_MARKER


# Outcomes that end a tick with nothing left for *this* tick to hand off,
# so the exit handshake's post-release re-read is skipped (see
# :func:`_hand_off_if_needed`). ``terminal`` means the build is finished —
# a wake-up arriving after it changes nothing a tick could act on;
# ``not_reactive`` means this build is not tick-driven at all; and
# ``lease_lost`` means a successor is already driving it, so spawning one
# would only add a container that finds the lease held and exits. (The
# drain is not skipped for ``lease_lost``: it is about *other* builds, and
# our own is filtered out of the candidates by the successor's live lease.)
#
# Every other outcome that got as far as holding the lease
# (``lingered_out``, and the crash path, which is still ``lingered_out``
# here because ``run_tick_aio`` relabels it afterwards) may have left a
# wake-up unserved.
_NO_HANDOFF_OUTCOMES = frozenset({"terminal", "not_reactive", "lease_lost"})


async def _hand_off_if_needed(
    build_id: UUID,
    *,
    registry: RegistryABC,
    config: TickConfig,
    summary: TickSummary,
) -> None:
    """Post-release half of the exit handshake — see ``_run_tick_body_aio``.

    Called once the scheduler lease is **released**, on every exit that may
    have left a wake-up unserved. Re-reads the wake-up flag and, if it is
    set, spawns a successor tick.

    A no-op without ``TickConfig.spawn_tick``, and deliberately
    so: an integration whose wake-ups always spawn a tick has no window to
    close, and paying a frontier fetch at the end of every tick to discover
    that would be pure cost. Skipping the read entirely (rather than
    reading and then finding nothing to do with the answer) is the point.

    Best-effort, like ``_report_tick_summary``: this runs in a ``finally``
    that may be unwinding an exception, so anything it raises would replace
    the error the caller is about to see with an unrelated one. A failed
    hand-off degrades to today's behaviour — the flag stays set, and the
    next completion or the watchdog picks it up.
    """
    spawn = config.spawn_tick
    if spawn is None:
        return
    try:
        flag = await registry.build_get_notify_aio(build_id)
        if not flag.needs_tick:
            return
        # The frontier read stays here, but only past the flag check: the
        # common case is an unset flag, which now costs one row instead of
        # a frontier. Deliberately not the slim ``build_get`` — this path
        # asks nothing of a backend that the pre-STA-18 code did not.
        #
        # The trade is honest rather than free. When the flag *is* set —
        # the case this exists for — it is two round trips where it used to
        # be one; and against a backend on the ABC default, or a server old
        # enough to have latched the fallback, the flag read *is* a
        # frontier fetch, so it is two frontier reads. This runs once per
        # tick, on the way out, so the poll's saving dominates either way.
        info = await registry.build_get_frontier_aio(build_id)
        if info.reactive_app_name is None:
            # Cannot happen for a build this tick just drove — the marker
            # never flips back — but the spawner needs an app to reach, and
            # guessing one would be worse than leaving the flag for the
            # next completion or the watchdog.
            return
        # Blocking spawner in async code, so off the loop: spawning is what
        # every executor integration offers, and requiring an async spawner
        # would exclude the ones that only have a blocking call. It used to
        # run inline, justified by "the last thing the tick does — lease
        # released, renewal cancelled, nothing else in flight". True of the
        # tick; false of its *container*, once ticks share one (see the
        # Modal integration's ``_TICK_CONCURRENCY``), where an inline
        # backend RPC stalls every co-resident tick's polling.
        # ``drain_wake_candidates`` already spawns this way.
        await asyncio.to_thread(spawn, build_id, info.reactive_app_name)
        summary.successor_spawned += 1
        logger.info(
            "Build %s was notified while this tick released the scheduler "
            "lease; handed off to a successor tick.",
            build_id,
        )
    except Exception as e:
        logger.warning(
            "Failed to hand off the scheduler for build %s (ignored — the "
            "wake-up flag stays set for the next completion or the "
            "watchdog): %s",
            build_id,
            e,
        )


async def _drain_wake_candidates(
    build_id: UUID,
    *,
    registry: RegistryABC,
    config: TickConfig,
    summary: TickSummary,
) -> list[UUID]:
    """Spawn ticks for the other builds the registry says need one.

    The cross-build half of a wake-up. The registry flags every reactive
    build whose frontier a status change may have touched — a task it holds
    changed status, a concurrency slot it was queued on freed, it was
    cancelled from the UI — but it has no executor. This tick has one, and
    is already here, so it asks for the flagged builds nobody is serving
    and spawns one tick each. The server hands each build out once per
    window, so N ticks draining at once still cost one container per
    flagged build: the storm that made every-writer-spawns unworkable
    cannot happen here by construction.

    Called after every pass that acted (the pass is what may have flagged a
    neighbour) and on every exit path (a finishing build's last act is
    often what unblocks its neighbours, and the exit is the last chance to
    say so). Not called when the lease was held — the tick that holds it
    drains on its own passes. Best-effort, see :mod:`stardag.build._wakeups`.
    """
    if config.spawn_tick is None:
        return []
    spawned = await drain_wake_candidates(
        registry, config.spawn_tick, build_id=build_id
    )
    # The tick's own build can be handed out on the exit path (flagged while
    # this tick held the lease, lease now released): that is a successor,
    # not a neighbour, and is counted as the hand-off it replaces.
    own = build_id in spawned
    summary.neighbour_ticks_spawned += len(spawned) - (1 if own else 0)
    if own:
        summary.successor_spawned += 1
    return spawned


# Set once per process the first time a tick takes the scheduler lease
# without a way to hand off (see :func:`_warn_if_no_successor_spawner`).
# Process-global for the same reason ``_tick_summary_route_missing`` is: the
# condition is a property of how this process was configured, not of one
# build, so saying it once is saying it.
_warned_missing_successor_spawner = False


def _warn_if_no_successor_spawner(config: TickConfig) -> None:
    """Say something when this tick holds the lease and cannot hand off.

    The two halves of the conditional wake-up are enforced in different
    places, and nothing connects them: a worker skips its tick spawn
    because the *registry* reports a scheduler live, while the ability to
    hand off at the end belongs to whoever *holds the lease*. A tick
    running without ``TickConfig.spawn_tick`` therefore disables
    the post-release read entirely — for every worker of that build, on the
    strength of a decision it never sees.

    In-tree that cannot happen: the Modal integration is the only thing
    that runs ticks and it always supplies the spawner. What this catches
    is the public entry point being driven by hand — ``run_tick_aio`` with
    a default ``TickConfig``, to poke at a build — which holds a real lease
    against real workers.

    Once per process, and not an error: an integration whose wake-ups
    always spawn unconditionally is correct without a spawner, and has
    nothing to be told.
    """
    global _warned_missing_successor_spawner
    if config.spawn_tick is not None or _warned_missing_successor_spawner:
        return
    _warned_missing_successor_spawner = True
    logger.warning(
        "Scheduler tick running without TickConfig.spawn_tick, so it cannot "
        "hand off on the way out or wake other builds. If this deployment's workers "
        "skip their tick spawn when the registry reports a live scheduler "
        "(BuildNotifyResult.scheduler_live), a wake-up arriving as this tick "
        "releases the lease is served by nobody until the next completion or "
        "the watchdog. Harmless if the wake-ups always spawn a tick "
        "regardless. Reported once per process."
    )


# Set once per process when the registry answers the tick-summary route
# with a missing-route 404, so an SDK pointed at an older server stops
# paying for a doomed request on every tick. Process-global rather than
# per-registry because a process talks to one registry, and the cost of
# being wrong is one extra request in the (nonexistent) other case.
_tick_summary_route_missing = False


async def _report_tick_summary(
    build_id: UUID,
    registry: RegistryABC,
    config: TickConfig,
    summary: TickSummary,
) -> None:
    """Report a finished tick's summary to the registry. Best-effort.

    "Best-effort" is a contract, not a hope: this runs at the end of every
    tick — a hot path — and recording observability must never fail a tick
    or change its outcome. Hence a bare ``except Exception``, which is the
    correct breadth here precisely because *nothing* this call can raise
    is worth propagating to a scheduler.
    """
    global _tick_summary_route_missing
    if not config.report_tick_summaries:
        return
    if summary.outcome in _UNREPORTED_TICK_OUTCOMES:
        return
    if _tick_summary_route_missing:
        return
    try:
        await registry.build_report_tick_summary_aio(build_id, asdict(summary))
    except NotFoundError as e:
        # Same tolerance as ``_skip_blocked``: a server predating the
        # endpoint answers with FastAPI's generic missing-route 404, which
        # is version skew rather than an error. A resource-level 404 (the
        # build is gone) is also not worth escalating from here — the tick
        # already ran — but it is not a reason to disable reporting for
        # every other build in the process.
        if is_missing_route_error(e):
            _tick_summary_route_missing = True
            logger.debug(
                "Registry API does not support tick summaries; reporting "
                "disabled for this process. Upgrade stardag-api to see "
                "per-tick scheduler reasoning in the registry."
            )
        else:
            logger.debug("Tick summary for build %s not recorded: %s", build_id, e)
    except Exception as e:
        logger.warning(
            "Failed to report tick summary for build %s (ignored): %s",
            build_id,
            e,
        )


async def run_tick_aio(
    build_id: UUID,
    *,
    registry: RegistryABC,
    task_executor: TaskExecutorABC,
    task_store: BuildTaskStore | None = None,
    config: TickConfig | None = None,
) -> TickSummary:
    """Run one reactive scheduler tick for ``build_id`` (see module docs).

    Idempotent and safe to invoke at any time from anywhere (worker
    wake-ups, periodic watchdog, manual): single-flighted per build via the
    scheduler lease, and a no-op for builds whose registry frontier carries
    no ``reactive_app_name`` (i.e. not reactively scheduled).

    The tick's summary is reported to the registry on the way out. The
    scheduling itself lives in ``_run_tick_body_aio``, which mutates the
    summary in place and has several early returns — wrapping it here is
    what keeps the report to exactly one call site instead of one per
    return, which is how a return added later would silently stop being
    observable.

    **If your workers can skip spawning a tick**, pass
    ``TickConfig.spawn_tick``. The two halves of that
    optimisation live apart: a worker skips because the registry reports a
    scheduler live, while handing off at the end is the lease holder's job.
    A tick without a spawner disables the hand-off for every worker of that
    build, so driving this by hand against a live reactive build — the
    "manual" case above — is the one place the pairing can silently come
    apart. It warns once per process when it does.

    A tick that *raises* is reported too, as ``outcome="error"`` carrying
    the exception's type and (bounded) message: a crashed tick is the most
    informative answer a "why did this build stall?" query can get, and
    the summary is stored as an open blob so the extra fields need no
    server change. Reporting it never changes what the caller sees — the
    original exception is re-raised unconditionally, and a failure to
    record the failure is logged and swallowed, exactly like the
    success-path report.
    """
    config = config or TickConfig()
    task_store = task_store or BuildTaskStore(build_id)
    summary = TickSummary(outcome="lingered_out")
    try:
        await _run_tick_body_aio(
            build_id,
            registry=registry,
            task_executor=task_executor,
            task_store=task_store,
            config=config,
            summary=summary,
        )
    except Exception as e:
        summary.outcome = "error"
        summary.error_type = _bounded(type(e).__name__, _MAX_ERROR_TYPE_CHARS)
        summary.error_message = _bounded(str(e), _MAX_ERROR_MESSAGE_CHARS)
        # Best-effort, and cannot mask: _report_tick_summary swallows
        # everything it can raise. The bare `raise` re-raises the original
        # with its traceback intact.
        await _report_tick_summary(build_id, registry, config, summary)
        raise
    await _report_tick_summary(build_id, registry, config, summary)
    return summary


async def _run_tick_body_aio(
    build_id: UUID,
    *,
    registry: RegistryABC,
    task_executor: TaskExecutorABC,
    task_store: BuildTaskStore,
    config: TickConfig,
    summary: TickSummary,
) -> None:
    """The tick proper — see :func:`run_tick_aio`. Mutates ``summary``.

    **The exit handshake.** A wake-up is two steps — set the build's
    wake-up flag, then make sure somebody looks at it — and a caller may
    skip the second step when the registry reports a scheduler already
    live (``BuildNotifyResult.scheduler_live``). That skip is sound only
    if a live scheduler is *guaranteed* to observe a flag set before it
    releases the lease, and "poll until the deadline, then unwind" does not
    guarantee it: nothing re-reads the flag between the final poll and the
    release, so a flag set in that window would be served by nobody.

    The window is closed from both sides:

    * at deadline expiry, **before** giving up the lease, the flag is
      re-read; if it is set the tick keeps the lease and re-acts
      (``summary.linger_extended``);
    * **after** the lease is released, the flag is re-read once more and a
      successor tick spawned if it is set (``summary.successor_spawned``,
      see :func:`_hand_off_if_needed`).

    Releasing *before* the second read is what makes it airtight, and it
    needs no new server state. A wake-up landing while this tick unwinds
    either finds the lease already released — and spawns its own tick — or
    finds it held, in which case this tick has not yet done its
    post-release read and will find the flag. Both ticks happening is
    possible and harmless: one wins the lease, the other no-ops.

    **What it does not do.** This closes the release window; it is not
    crash recovery. A tick that clears the flag and then dies leaves the
    flag false, so the hand-off has nothing to see and the wake-up that
    tick had taken responsibility for waits for the next completion or the
    watchdog — exactly as it did before any of this existed. The
    ``finally`` is there so that an *exception* cannot skip the
    release-window check, not to resurrect the crashed tick's own wake-up.

    The pre-release re-read is the optimisation (one fewer cold start);
    the post-release hand-off is the correctness guarantee. Only the
    second is load-bearing, which is why the first may be skipped when
    extending the linger would not make sense (see the linger loop).
    """
    # Scheduler lease: renews itself while the tick lingers and releases on
    # exit. A held lease means immediate no-op — the wake-up that spawned
    # this tick was flagged before the spawn, so the holder's re-checks (the
    # linger poll, then the exit handshake above) cover it.
    lease = SchedulerLease(registry, build_id)
    acquired = False
    # Whether this tick ever cleared the wake-up flag — i.e. whether it took
    # responsibility for a wake-up at all. Gates the hand-off; see the
    # ``finally`` below for why that matters.
    cleared_a_wakeup = False
    try:
        async with lease:
            if not lease.acquired:
                logger.info(f"Scheduler lease for build {build_id} held; tick no-op.")
                summary.outcome = "lease_held"
                return

            acquired = True
            _warn_if_no_successor_spawner(config)

            # Expose the ambient build id (the executor forwards it to
            # self-reporting workers, exactly like the resident engine does).
            build_id_token = current_build_id_var.set(build_id)
            try:
                loop = asyncio.get_event_loop()
                deadline = loop.time() + config.linger_seconds
                while True:
                    if lease.lost:
                        summary.outcome = "lease_lost"
                        return
                    summary.iterations += 1
                    try:
                        await registry.build_clear_notify_aio(build_id)
                        cleared_a_wakeup = True
                        frontier = await registry.build_get_frontier_aio(build_id)
                    except NotFoundError as e:
                        # Only a genuine missing-route 404 (server predating the
                        # frontier/notify endpoints) becomes the clear "server
                        # too old" error; a resource-level 404 (e.g. the build
                        # was deleted) is a real not-found and must propagate.
                        if not is_missing_route_error(e):
                            raise
                        raise RuntimeError(
                            "The registry server does not support reactive "
                            "scheduling (frontier/notify endpoints missing). "
                            "Upgrade stardag-api to a version matching this SDK."
                        ) from e

                    if frontier.reactive_app_name is None:
                        # Not a reactively-scheduled build (e.g. a resident-
                        # orchestrator build, or the metadata was never set) —
                        # never schedule on top of it. The Modal tick wrapper
                        # short-circuits this before acquiring the lease; the
                        # check here is the backstop for direct callers. The
                        # marker never flips back to None mid-build, so it is
                        # safe to re-evaluate each iteration.
                        summary.outcome = "not_reactive"
                        return

                    acted, denied_this_round, awaiting_backend = await _act_on_frontier(
                        frontier,
                        build_id=build_id,
                        registry=registry,
                        task_executor=task_executor,
                        task_store=task_store,
                        config=config,
                        summary=summary,
                    )
                    blockers_reset_before = summary.in_build_blockers_reset
                    terminal = await _handle_terminal(
                        frontier,
                        build_id=build_id,
                        registry=registry,
                        task_executor=task_executor,
                        task_store=task_store,
                        config=config,
                        summary=summary,
                        denied_this_round=denied_this_round,
                    )
                    if terminal is not None:
                        summary.outcome = "terminal"
                        summary.terminal_status = terminal
                        return
                    if summary.in_build_blockers_reset > blockers_reset_before:
                        # Resetting a cancelled blocker made a task runnable,
                        # and this tick is the only thing that knows. The
                        # registry's wake-up flag deliberately skips the build
                        # whose own event caused the change — it is the one
                        # that already knows — so lingering here waits for
                        # news that cannot arrive, and the build stalls until
                        # the watchdog with nothing running and nothing to
                        # report. Counts as having acted, because it is:
                        # terminal handling is the only phase that changes
                        # the frontier without going through the action pass.
                        acted = True
                    if acted:
                        # The tick's own actions (spawns recorded as started,
                        # self-healed completions, recorded failures) changed the
                        # scheduling state — re-evaluate immediately on a fresh
                        # frontier instead of waiting for an external wake-up
                        # (terminal detection above ran on the pre-action
                        # snapshot).
                        #
                        # This is also what makes the per-tick spawn cap a
                        # throttle rather than a stall: a pass that truncated at
                        # the cap necessarily spawned (or failed to spawn, or
                        # was denied) every task it did attempt, so either it
                        # acted — and lands here, taking the next batch off a
                        # fresh frontier without waiting for anything — or every
                        # attempt was denied by a concurrency limit, in which
                        # case lingering for a slot to free up is precisely the
                        # right thing to do and re-acting immediately would be a
                        # hot loop against the registry.
                        deadline = loop.time() + config.linger_seconds
                        # This pass may have flagged a neighbour (a shared
                        # task changed status, a slot freed); wake it now
                        # rather than at exit, which may be minutes away.
                        await _drain_wake_candidates(
                            build_id, registry=registry, config=config, summary=summary
                        )
                        continue

                    # Linger: poll the wake-up flag until deadline.
                    #
                    # Two ways out. ``needs_tick`` is the ordinary one: some
                    # worker reported something.
                    #
                    # The other is an interrupted task whose execution still
                    # probes as live. That one is NOT waiting for an event —
                    # nothing will ever emit one, because the worker that would
                    # have reported is dead and an interrupted task produces
                    # nothing further. Waiting on the flag for it stalls the
                    # build until the watchdog, which is off by default.
                    #
                    # So those refs are re-probed directly, and only they: this
                    # deliberately does not re-enter the pass. Re-acting on
                    # every poll would re-attempt the claim of every
                    # limit-denied task at the poll interval, which is exactly
                    # the hot loop the ``acted`` branch above declines to
                    # create. The pass resumes only once a ref stops being
                    # live, which is the single fact being waited on.
                    #
                    # Residual, and honest: a ref that stays live past
                    # ``linger_seconds`` still lingers out with nothing
                    # scheduled to follow up. For a genuine backend retry that
                    # is fine — the restarted worker's own events re-tick the
                    # build. It is the unwinding race this closes.
                    while True:
                        if loop.time() >= deadline:
                            # Exit handshake, pre-release half (see the
                            # docstring): the flag may have been set since
                            # the last poll, and a worker that saw the
                            # lease held will not have spawned for it.
                            #
                            # Skipped when lingering is disabled
                            # (``linger_seconds <= 0``): extending a
                            # zero-length linger re-arms an already-expired
                            # deadline, so a build being notified steadily
                            # would spin here without ever sleeping.
                            #
                            # The watchdog sweep is still the caller that
                            # makes this load-bearing, for a changed reason:
                            # it used to run one pass per build across many
                            # builds in one container, so a spinning build
                            # starved the rest of the sweep. It spawns now,
                            # and asks each tick for one pass — so the spin
                            # would cost that build's container instead of
                            # the sweep, which is better and still wrong.
                            # Nothing is lost either way: the post-release
                            # hand-off is the guarantee, and handing the
                            # wake-up to a dedicated tick is what should
                            # happen anyway.
                            if config.linger_seconds <= 0:
                                return
                            flag = await registry.build_get_notify_aio(build_id)
                            if not flag.needs_tick:
                                return
                            summary.linger_extended += 1
                            # Re-arm before re-acting: a pass that finds
                            # nothing actionable does not reset the deadline
                            # (only ``acted`` does), so without this the
                            # tick would come straight back to an expired
                            # deadline and re-read the flag with no sleep in
                            # between.
                            deadline = loop.time() + config.linger_seconds
                            break  # outer loop clears the flag and re-acts
                        await asyncio.sleep(config.poll_interval_seconds)
                        if lease.lost:
                            # A successor already holds this build. Acting
                            # now is the double-scheduling the lease exists
                            # to prevent, and the successor has the flag.
                            summary.outcome = "lease_lost"
                            return
                        if awaiting_backend and await _any_ref_settled(
                            awaiting_backend, task_executor
                        ):
                            break  # a ref resolved — re-act and pick it up
                        flag = await registry.build_get_notify_aio(build_id)
                        if flag.needs_tick:
                            break  # outer loop clears the flag and re-acts
            finally:
                current_build_id_var.reset(build_id_token)
    finally:
        # Post-release half of the exit handshake (see this function's
        # docstring). In a ``finally`` so that an exception cannot bypass
        # it: a wake-up landing while a *crashing* tick unwinds is in the
        # same release window as one landing while a healthy tick exits,
        # and the worker that set it skipped its spawn either way.
        #
        # It is NOT crash recovery, and does not pretend to be. A tick that
        # cleared the flag and then died leaves it false, so nothing is
        # spawned and the wake-up it had taken responsibility for waits for
        # the next completion or the watchdog. That is unchanged from before
        # this handshake existed.
        #
        # ``cleared_a_wakeup`` is what stops the hand-off from turning a
        # repeatable failure into an unbounded chain of containers. The
        # first thing each pass does is clear the flag; if that call itself
        # keeps failing — a rate-limited or 5xx ``DELETE /notify`` while the
        # frontier ``GET`` stays healthy — then every tick would raise with
        # the flag still set, hand off, and be replaced by a successor that
        # fails identically, one cold container after another, forever. The
        # earlier reasoning here ("the successor clears the flag before it
        # can do the same") assumed the clear succeeds, which is exactly
        # what is broken in that state.
        #
        # So a tick that never cleared anything hands nothing on: it took no
        # responsibility, and its successor could only repeat its failure.
        # The cost is one narrow case — a worker skipped its spawn and this
        # tick died before its first clear — where the wake-up now waits for
        # the next completion or the watchdog. In that state the registry is
        # refusing writes and the build is not progressing regardless, which
        # makes it much the cheaper of the two failures.
        # Cross-build drain on the way out, whatever the outcome — terminal
        # included, since a finishing build's last completion is often
        # exactly what unblocked a neighbour. A tick that never held the
        # lease skips it: the holder drains on its own passes.
        #
        # BEFORE the hand-off, on purpose. With the lease released, this
        # tick's own build is a candidate if a wake-up landed while it held
        # the lease (the notifier saw a live scheduler and did not spawn), so
        # the drain may hand it out and spawn its successor — stamped
        # server-side, so no other drainer doubles it. The hand-off below
        # then only covers what the drain could not: a registry without the
        # route, or a hand-out refused because the build was stamped within
        # the window by a notifier that will spawn anyway.
        drained: list[UUID] = []
        if acquired and summary.outcome != "not_reactive":
            drained = await _drain_wake_candidates(
                build_id, registry=registry, config=config, summary=summary
            )
        if (
            acquired
            and cleared_a_wakeup
            and build_id not in drained
            and summary.outcome not in _NO_HANDOFF_OUTCOMES
        ):
            await _hand_off_if_needed(
                build_id, registry=registry, config=config, summary=summary
            )
    return  # unreachable: the loop above always returns
