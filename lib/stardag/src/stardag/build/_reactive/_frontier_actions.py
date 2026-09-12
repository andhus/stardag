from __future__ import annotations

import asyncio
import logging
import typing
from datetime import datetime, timezone
from functools import partial
from typing import Sequence
from uuid import UUID

from stardag import (
    BaseTask,
    task_from_registry_data,
)
from stardag.build._base import (
    DetachedExecutionStatus,
    TaskExecutorABC,
)
from stardag.build._task_modules import import_failure_note
from stardag.build._task_store import BuildTaskStore
from stardag.registry import (
    BuildFrontier,
    FrontierTaskRef,
    RegistryABC,
)

from stardag.build._reactive._budgets import (
    _attempts_phrase,
    _record_task_failure,
    _start_denied_by_budget,
)
from stardag.build._reactive._discovery import (
    _run_bounded,
)

if typing.TYPE_CHECKING:
    from stardag.build._reactive._tick import TickConfig, TickSummary

logger = logging.getLogger(__name__)


class _MissingTaskRef(typing.NamedTuple):
    """Stand-in passed to lifecycle registry calls for a task whose pickle
    is missing from the build task store. Registry backends only use
    ``task.id`` to address lifecycle endpoints."""

    id: UUID


# Statuses considered "in flight" for terminal detection.
_RUNNING_STATUSES = ("running",)
_TERMINAL_BUILD_STATUSES = ("completed", "failed", "cancelled")

# The platform ended an execution for a reason unrelated to the task (a
# function timeout, a reclaimed container) and the worker said so before
# it died. Deliberately NOT in ``_RUNNING_STATUSES``: an interrupted task
# holds no claim and occupies no concurrency slot, which is the point of
# reporting it. It arrives in the frontier's actionable set like a pending
# task, and ``_act_on_interrupted`` decides what happens to it.
_INTERRUPTED_STATUS = "interrupted"

# A revoked task. Terminal, so it never appears in the frontier's
# actionable or running lists — which is exactly why a build that cascaded
# its own cancel through the server needs the executions route to find the
# containers it is still responsible for stopping.
_CANCELLED_STATUS = "cancelled"


# Slack added to an executor's own timeout when deriving a claim TTL. It
# covers the ways the claim's clock and the execution's clock differ: the
# claim is recorded at *acquire* time, BEFORE the spawn, so it absorbs
# queueing and cold-start latency the timeout clock has not started
# counting yet; a backend does not kill an execution the instant its
# timeout elapses; and client and server clocks are not identical.
#
# Deliberately generous, because the two errors are not symmetric: too
# short and a live execution's claim becomes stealable (a duplicate
# execution, which is what claims exist to prevent); too long and an
# abandoned claim merely heals later than it could have.
_CLAIM_TTL_GRACE_SECONDS = 900.0


# --- Per-tick spawn cap ------------------------------------------------
#
# A tick runs in a container with a finite life, so "spawn everything that
# is actionable" is a bound on nothing: a wide enough layer outlives the
# container and the tick is killed mid-fan-out. These constants turn the
# cap into a duration budget instead of a magic count — see
# :func:`_spawn_cap`, which also documents the ladder of inputs the budget
# is derived from.

# Fraction of the container's wall-clock limit that one fan-out pass may
# spend. Well under half, because the pass is not all a tick does: the
# frontier fetch, terminal evaluation, the summary report and (usually) a
# second pass on a fresh frontier all have to fit in the same container.
_SPAWN_BUDGET_FRACTION = 0.25

# Wall-clock cost of putting ONE actionable task on a worker: a task-store
# read, the acquiring start, the executor spawn, and the ref-recording
# start — three network round-trips and a local read. Deliberately
# pessimistic (a p99 round-trip, not a median), because underestimating it
# inflates the cap, and an inflated cap is the failure this exists to
# prevent.
_SECONDS_PER_SPAWN = 2.0

# Used when NO wall-clock limit is known at all — neither the tick's own
# nor the executor's. There is nothing to derive from, so this is the one
# place a plain number is unavoidable. Chosen so that the pass stays short
# on any plausible container: 500 spawns x 2 s / 50 in flight is ~20 s of
# round-trips.
_DEFAULT_MAX_SPAWNS_PER_TICK = 500

# Floor and ceiling on the derived cap. The floor keeps a tight timeout
# from producing a cap so small that a build advances by dribs; the ceiling
# is where the fan-out stops being the limiting factor anyway (a frontier
# carrying 10k actionable refs is itself the expensive part of the tick).
#
# Note what the ceiling is NOT: a substitute for reading the right timeout.
# It bounds the absurd, not the merely wrong — 10k spawns is still far more
# than a five-minute container can do, so a cap derived from the wrong
# duration is not rescued by clamping it.
_MIN_SPAWN_CAP = 50
_MAX_SPAWN_CAP = 10_000

# The server's accepted range for claim_ttl_seconds (outside it: 422).
# Clamped rather than raised on — a 30-second task and a 60-day task are
# both legitimate, and each should get the closest expiry the server can
# express rather than a failed start.
_MIN_CLAIM_TTL_SECONDS = 60
_MAX_CLAIM_TTL_SECONDS = 2592000  # 30 days


def claim_ttl_seconds(task: BaseTask, task_executor: TaskExecutorABC) -> int | None:
    """Claim TTL for ``task``, derived from the executor's own timeout.

    Returns None when the executor exposes no timeout for the task, which
    leaves the expiry to the registry's default.

    **Why derive it rather than take the default.** Putting an expiry on
    *every* start is what maximises healing: an execution claim with no
    expiry records no liveness evidence a third party can evaluate, which
    is precisely the gap that makes an abandoned RUNNING task wedge every
    other build downstream of it. But an expiry also makes a task that
    outlives its TTL *stealable while it is still alive* — a duplicate
    execution, i.e. the failure mode claims exist to prevent.

    Deriving the TTL from the backend's own wall-clock limit is what keeps
    that from being a real risk: the backend will have killed the execution
    before its claim lapses, so the executions whose claims can lapse are
    the ones that are already dead. A generic server-side default cannot
    make that promise for a task whose timeout it does not know — which is
    why "the server has a default" is not a reason to omit this.

    The executor is asked, never guessed at: an executor that enforces no
    limit says so by returning None, and a resolution failure is swallowed
    (this runs on the spawn path of every task and must not fail a start
    over a diagnostic).
    """
    try:
        timeout = task_executor.execution_timeout_seconds(task)
    except Exception:
        logger.debug(
            f"Execution timeout resolution failed for task {task.id}; "
            "claiming with the registry's default TTL.",
            exc_info=True,
        )
        return None
    if timeout is None:
        return None
    ttl = int(timeout + _CLAIM_TTL_GRACE_SECONDS)
    return max(_MIN_CLAIM_TTL_SECONDS, min(_MAX_CLAIM_TTL_SECONDS, ttl))


async def _load_task(
    task_id: str,
    registry: RegistryABC,
    task_store: BuildTaskStore,
    *,
    quiet: bool = False,
) -> BaseTask | None:
    """Load a task object: store pickle first, registry rehydration second.

    The pickle-free fallback reconstructs the task from the registry's
    stored ``task_data`` (see ``stardag.task_from_registry_data``) — which
    also survives cases the pickle store can't (e.g. an app redeploy with
    compatible task definitions invalidating stored pickles).

    Successful rehydrations are written back to the store, so a build that
    fell back once does not keep paying for it. Two qualifications, in
    this order:

    * **Not on a pickle-free store.** A build whose app declared
      ``require_pickle_free`` gets a store that refuses every write, so
      this call is a no-op there — the cache would otherwise leave pickles
      on a target root the build promised never to write to, silently and
      on a build where rehydration is the *designed* path rather than a
      fallback. The guard lives in ``BuildTaskStore.save_task_aio``, which
      is why no argument is threaded down here.
    * **Best-effort when it does write.** The task object is already in
      hand, so a store error must not abort the caller.

    With ``quiet=True`` a rehydration failure logs a single warning without
    the stack trace — for callers where a missing object is tolerated (a
    RUNNING task resolves via its worker's self-reporting), the repeated
    per-tick ``logger.exception`` would be noise.

    A rehydration failure is annotated with any declared task modules that
    failed to import in this process (see
    ``stardag.build._task_modules``): "no task class registered for X" and
    "the module defining X blew up on import" are the same incident seen
    from two ends, and only the annotation connects them. The annotation is
    read from the task-module registry rather than plumbed through
    ``rehydrate.py``, which stays a pure reconstruction primitive with no
    notion of how its classes got imported.
    """
    task = await task_store.load_task_aio(task_id)
    if task is not None:
        return task
    try:
        metadata = await registry.task_get_metadata_aio(UUID(task_id))
        task = task_from_registry_data(metadata.body, expected_task_id=task_id)
    except Exception as e:
        message = (
            f"Task {task_id} is missing from the task store and could not "
            f"be rehydrated from registry data"
        )
        note = import_failure_note()
        if quiet:
            logger.warning(f"{message}: {e}{note}")
        else:
            logger.exception(f"{message}.{note}")
        return None
    # DEBUG on a pickle-free build, INFO otherwise: there, a store miss is
    # the expected path for every task on every tick, and reporting it at
    # INFO trains readers to skim the scheduler's log lines on the very
    # configuration the feature recommends. Elsewhere a miss means a pickle
    # was expected and was not there, which is worth a line.
    if task_store.pickle_free:
        logger.debug(f"Rehydrated task {task_id} from registry data.")
    else:
        logger.info(f"Rehydrated task {task_id} from registry data.")
    try:
        await task_store.save_task_aio(task)
    except Exception as e:
        logger.warning(f"Failed to write rehydrated task {task_id} back: {e}")
    return task


class _SpawnCap(typing.NamedTuple):
    """A per-pass spawn cap and a plain-English account of where it came from.

    The source is carried, not re-derived at logging time, because the one
    question an operator has when a tick truncates is "which number did it
    read?" — and the ladder in :func:`_spawn_cap` has four rungs, three of
    which produce plausible-looking caps from very different inputs.
    """

    limit: int
    source: str


def _derived_spawn_cap(budget_source_seconds: float, config: TickConfig) -> int:
    """Cap from a wall-clock limit: how many spawns fit in a fraction of it.

    Putting one task on a worker costs a bounded amount of round-trips
    (``_SECONDS_PER_SPAWN``) and ``max_concurrent_actions`` of them are in
    flight at once, so the largest batch a pass can finish inside its
    budget is ``budget * concurrency / cost``, clamped.
    """
    budget_seconds = _SPAWN_BUDGET_FRACTION * budget_source_seconds
    derived = int(
        budget_seconds * max(1, config.max_concurrent_actions) / _SECONDS_PER_SPAWN
    )
    return max(_MIN_SPAWN_CAP, min(_MAX_SPAWN_CAP, derived))


def _spawn_cap(
    candidates: Sequence[BaseTask],
    task_executor: TaskExecutorABC,
    config: TickConfig,
) -> _SpawnCap:
    """How many tasks this pass may spawn, and why.

    The number that matters is not a count at all — it is a duration. A
    tick lives in a container with a wall-clock limit, and the cap exists
    to stop it starting more work than it can live long enough to finish.
    So the whole question is *which* duration to read, resolved down this
    ladder, most specific first:

    1. **``TickConfig.max_spawns_per_tick``** — an explicit answer, taken
       as given. The override always wins.
    2. **``TickConfig.tick_timeout_seconds``** — the wall-clock limit of
       *this* container, when the caller knows it (the Modal integration
       reads the ``timeout`` its ``tick`` function was registered with).
       This is the quantity the cap is actually about, so it is preferred
       over anything below it.
    3. **The executor's ``execution_timeout_seconds``** — a *proxy*, used
       only when rung 2 is unavailable. It measures how long the spawned
       executions may run, which is a different quantity and can differ by
       orders of magnitude (a 24-hour worker under a 5-minute tick would
       derive a cap the tick cannot possibly work through). It is still
       better than a bare constant for the common case of a tick sized
       like the work it schedules, and the log line says which rung
       produced the cap so a truncating tick is diagnosable. The
       **smallest** timeout among the candidates is used: with
       heterogeneous routing the tightest backend limit is the one that
       bounds the pass.
    4. **``_DEFAULT_MAX_SPAWNS_PER_TICK``** — no wall clock is known
       anywhere. Never unbounded: "however many are actionable" is exactly
       what this replaces.
    """
    if config.max_spawns_per_tick is not None:
        return _SpawnCap(
            max(1, config.max_spawns_per_tick),
            "TickConfig.max_spawns_per_tick (set explicitly)",
        )
    if config.tick_timeout_seconds is not None:
        return _SpawnCap(
            _derived_spawn_cap(config.tick_timeout_seconds, config),
            f"this tick container's own timeout ({config.tick_timeout_seconds:.0f}s)",
        )
    timeouts: list[float] = []
    for task in candidates:
        try:
            timeout = task_executor.execution_timeout_seconds(task)
        except Exception:
            # The ABC says this must not raise, but a spawn cap is not
            # worth failing a tick over — an executor that misbehaves here
            # simply contributes no timeout.
            logger.debug(
                f"Execution timeout resolution failed for task {task.id} "
                "while sizing the spawn cap; ignoring it.",
                exc_info=True,
            )
            continue
        if timeout is not None:
            timeouts.append(timeout)
    if not timeouts:
        return _SpawnCap(
            _DEFAULT_MAX_SPAWNS_PER_TICK,
            "the default (no wall-clock limit is known for this tick or its executor)",
        )
    return _SpawnCap(
        _derived_spawn_cap(min(timeouts), config),
        f"the executor's tightest execution timeout ({min(timeouts):.0f}s) "
        "as a proxy — this tick does not know its own container's timeout; "
        "set TickConfig.tick_timeout_seconds (or max_spawns_per_tick) if "
        "the tick is sized differently from its workers",
    )


async def _act_on_frontier(
    frontier: BuildFrontier,
    *,
    build_id: UUID,
    registry: RegistryABC,
    task_executor: TaskExecutorABC,
    task_store: BuildTaskStore,
    config: TickConfig,
    summary: TickSummary,
) -> tuple[bool, int, "list[tuple[FrontierTaskRef, BaseTask]]"]:
    """Spawn/probe/heal the actionable tasks, with bounded concurrency.

    Returns ``(acted, denied_this_round, awaiting_backend)``: whether
    anything acted, how
    many tasks were denied by concurrency limits in THIS pass (used by
    terminal detection — a cumulative count would keep suppressing the
    stuck-build check long after the denied tasks have run).

    **Three phases, each bounded by ``max_concurrent_actions``.** Resolve
    every actionable task's object; probe the ones already RUNNING; spawn
    the rest. Phases exist because the spawn cap has to be sized against
    the tasks that are actually spawn candidates, which is not knowable
    until the objects are loaded. Within a phase the work is independent
    per task; between phases nothing is.

    **What concurrency does NOT change.** Each spawn coroutine still runs
    its three steps in order — acquiring start, spawn, ref-recording start
    — so a task denied by a concurrency limit or by a competing claim never
    reaches ``submit_detached`` and never occupies a worker, and no
    executor ref is ever recorded for an execution that does not exist.
    Ordering *between* tasks was never guaranteed and is not relied upon:
    the frontier's actionable set is by definition a set of tasks whose
    dependencies are all satisfied.

    **Nor does it change the claim.** Every task here is claimed at most
    once per pass — the frontier lists a task once, and phases do not
    overlap — so these coroutines are not racing each other for the same
    claim. Nor is this tick racing another tick of the same build: it holds
    the build's scheduler lease. The claim is still what arbitrates against
    *other builds* (and against a tick of this build that the lease
    manager's own failure modes let through), which is exactly the job it
    had before, unchanged.

    **Which failures are retried.** Three failure paths run through here,
    and they are not the same kind of thing:

    - a **spawn** that raised before any container existed → retryable.
      This is the case no execution backend can cover: its function-level
      retries start counting once there is a function call to retry, and
      there is not one.
    - an execution the backend reports **failed**, or one whose claim
      lapsed with no ref to probe → retryable. OOM kills, preemptions and
      workers that died mid-write all land here, and every one of them is
      transient by nature.
    - a task whose **object cannot be resolved** (no stored pickle and no
      rehydratable registry data) → *not* retryable. The inputs to that
      failure are the task store and the imported task classes; neither
      changes between two passes of the same tick, so a retry re-reads the
      same absence and fails identically, having spent the budget that a
      genuinely transient failure elsewhere in the build might have needed.

    Note what is absent: an exception *inside* a task never appears here.
    The worker self-reports TASK_FAILED, which takes the task out of the
    frontier entirely, so the deterministic failures — the ones where
    "retry it" is the wrong answer — are structurally out of reach of this
    budget rather than excluded by a judgement call.

    **The budget gate.** A task arriving PENDING with its budget already
    spent is refused a start and failed again, with a message saying why.
    Exactly one thing produces that shape: a **bare retry** of a
    budget-exhausted task — the UI's Retry button, ``stardag tasks retry``,
    the API's retry route. Those flip the status without recording
    BUILD_RESUMED, so the round the count is measured against is unchanged
    and the task comes back pending with nothing left to spend. The retry
    *succeeds* server-side, so without the gate saying something the
    operator sees a task go PENDING and then quietly do nothing at all.

    A **re-trigger** is not this case and never lands here: it records
    BUILD_RESUMED *before* its discovery retries the failed tasks, so the
    round boundary is ahead of them and they arrive at zero. That is why
    the message points at a re-trigger rather than at a retry.

    Resumption of a **SUSPENDED** task is never gated either: a
    dynamic-dependency yield records a fresh start, so gating there would
    refuse to resume any task that yielded more times than the budget
    allows — turning a retry policy into a cap on dynamic dependencies.

    **Counters.** ``summary``'s fields and the two returned values are
    mutated from several coroutines. That is safe without a lock because
    every mutation is a bare ``+=`` on the same event loop with no
    ``await`` between the read and the write — asyncio switches tasks only
    at suspension points. Please keep it that way: a counter update that
    grows an ``await`` in the middle stops being atomic.
    """
    if frontier.build_status in _TERMINAL_BUILD_STATUSES:
        return False, 0, []  # terminal handling deals with it
    acted = False
    denied_this_round = 0
    # Interrupted tasks left alone because their execution still probes as
    # live, as ``(ref, task)``. Returned so the caller can keep re-probing
    # *just these*: unlike everything else a pass can be waiting for,
    # NOTHING will emit an event when this resolves — the worker that would
    # have is already dead.
    awaiting_backend: list[tuple[FrontierTaskRef, BaseTask]] = []
    # Task ids appended to ``spawn_candidates`` because they asked to be
    # resumed, so the spawn phase can count only the ones it actually
    # spawned (see ``summary.interruptions_restarted``).
    resumption_requests: set[UUID] = set()
    semaphore = asyncio.Semaphore(max(1, config.max_concurrent_actions))

    # --- phase 1: resolve task objects --------------------------------
    # Results are written by index, not appended, so the partition below
    # follows the frontier's own order regardless of which loads finished
    # first — a tick's logs stay readable in the order the registry
    # reported.
    resolved: list[BaseTask | None] = [None] * len(frontier.actionable)

    async def resolve(index: int, item: FrontierTaskRef) -> None:
        nonlocal acted
        task = await _load_task(
            item.task_id,
            registry,
            task_store,
            quiet=item.latest_status in _RUNNING_STATUSES,
        )
        if task is not None:
            resolved[index] = task
            return
        if item.latest_status in _RUNNING_STATUSES:
            # Can't probe without the object, but the worker reports its
            # own terminal events — leave it to resolve itself.
            return
        # A pending/suspended task with no stored object AND no
        # rehydratable registry data can never be scheduled: fail it
        # (rather than leaving it in the frontier forever, where it
        # would block terminal detection and stall the build across
        # endless watchdog ticks).
        logger.error(
            f"Task {item.task_id} of build {build_id} has no stored "
            "task object and could not be rehydrated; failing it."
        )
        try:
            await _record_task_failure(
                typing.cast(BaseTask, _MissingTaskRef(id=UUID(item.task_id))),
                "Task object missing from the build task store",
                build_id=build_id,
                registry=registry,
                config=config,
                summary=summary,
                # Deterministic: neither the task store nor this process's
                # imported task classes change between passes, so a retry
                # buys a second identical failure at the cost of an
                # attempt. Fail it once and let the build say so.
                retryable=False,
            )
            acted = True
        except Exception as e:
            logger.error(
                f"Failed to record store-missing failure for task {item.task_id}: {e}"
            )

    await _run_bounded(
        [
            partial(resolve, index, item)
            for index, item in enumerate(frontier.actionable)
        ],
        semaphore,
    )

    running_items: list[tuple[FrontierTaskRef, BaseTask]] = []
    spawn_candidates: list[BaseTask] = []
    budget_denied: list[tuple[FrontierTaskRef, BaseTask]] = []
    interrupted_items: list[tuple[FrontierTaskRef, BaseTask]] = []
    for item, task in zip(frontier.actionable, resolved):
        if task is None:
            continue
        if item.latest_status in _RUNNING_STATUSES:
            running_items.append((item, task))
        elif item.latest_status == _INTERRUPTED_STATUS:
            # Its own phase, not a spawn candidate: what happens to an
            # interrupted task depends on a policy, on a separate budget,
            # and — when the execution backend runs its own retries — on
            # whether the backend is already restarting the very same
            # input. See ``_act_on_interrupted``.
            interrupted_items.append((item, task))
        elif item.latest_status == "pending" and _start_denied_by_budget(
            item.attempt_count, config.max_attempts
        ):
            # PENDING with the budget already spent — see the docstring's
            # "budget gate". Only a BARE retry gets a task into this shape
            # (a re-trigger records BUILD_RESUMED first, so its retried
            # tasks arrive at zero), and the gate exists so that retry is
            # not silently inert.
            #
            # SUSPENDED is excluded by the status check above, on purpose:
            # resuming a task that yielded dynamic dependencies records a
            # fresh start, so a suspend-heavy task is "over budget" while
            # being entirely healthy, and gating it would refuse to resume
            # it — a wedged DAG, not a declined retry.
            budget_denied.append((item, task))
        else:
            spawn_candidates.append(task)

    # Attempt counts by task id, so the spawn coroutine can size the budget
    # for a spawn failure. It takes the task *object* (that is what the cap
    # and the executor need), and the count lives on the frontier ref.
    attempts_by_task_id = {
        item.task_id: item.attempt_count for item in frontier.actionable
    }

    # --- phase 2: probe the RUNNING ones ------------------------------
    async def probe(item: FrontierTaskRef, task: BaseTask) -> None:
        nonlocal acted
        resolution = await _resolve_running(item, task, task_executor)
        if resolution == "complete":
            await registry.task_complete_aio(build_id, task)
            summary.self_healed += 1
            acted = True
        elif resolution == "failed":
            # Retryable: the backend killed or lost this execution (OOM,
            # preemption, a worker that vanished and let its claim lapse),
            # and none of those are things the backend's own function-level
            # retries cover. The attempt being closed here is the one the
            # frontier already counted — the claim-expiry path arrives via
            # exactly this branch, so the two mechanisms record one attempt
            # between them, not two.
            await _record_task_failure(
                task,
                "Detached execution failed (observed by tick)",
                build_id=build_id,
                registry=registry,
                config=config,
                summary=summary,
                retryable=True,
                attempts_spent=item.attempt_count,
            )
            acted = True
        # "leave": still running (or unprobeable) — nothing to do.

    await _run_bounded(
        [partial(probe, item, task) for item, task in running_items],
        semaphore,
    )

    # --- phase 2b: refuse starts for tasks already at their budget -----
    async def deny_budget(item: FrontierTaskRef, task: BaseTask) -> None:
        nonlocal acted
        # Non-None by construction: ``_start_denied_by_budget`` denies only
        # on a positive, server-reported count.
        spent = item.attempt_count or 0
        logger.error(
            f"Task {task.id} of build {build_id} is PENDING again with its "
            "attempt budget for this build round already spent "
            f"({_attempts_phrase(spent, config.max_attempts)}), so a BARE "
            "RETRY put it back — the UI's Retry button, `stardag tasks "
            "retry`, or the retry API route. That retry SUCCEEDED: the task "
            "really is pending again. But a bare retry does not start a new "
            "build round, so the attempt count it is measured against is "
            "unchanged, and the scheduler will not start it. Failing it "
            "again rather than leaving it PENDING and inert, so this is "
            "visible instead of looking like nothing happened. What you "
            "wanted is a RE-TRIGGER of this build — "
            f"build_trigger(..., build_id={build_id}, reactive=True) — which "
            "records BUILD_RESUMED, starts a new round, resets every task's "
            "attempt count to zero and re-runs exactly this task. Add "
            'tick_kwargs={"max_attempts": 4} to the re-trigger if it needs '
            "more attempts per round."
        )
        await _record_task_failure(
            task,
            (
                f"Attempt budget spent ({spent} of {config.max_attempts} "
                "allowed attempt(s) in this build round). A bare retry does "
                "not reset it — re-trigger this build "
                f"(build_id={build_id}) to start a new round, optionally "
                'with tick_kwargs={"max_attempts": N}.'
            ),
            build_id=build_id,
            registry=registry,
            config=config,
            summary=summary,
            # Already over budget by construction — going through the
            # retry branch would only re-derive that and log it twice.
            retryable=False,
        )
        summary.budget_denied += 1
        # Deliberately NOT counted in ``denied_this_round``: that number
        # suppresses terminal detection because a limit-denied task is
        # waiting for a slot and will run. This one will not run, and the
        # failure just recorded is what should fail the build.
        acted = True

    await _run_bounded(
        [partial(deny_budget, item, task) for item, task in budget_denied],
        semaphore,
    )

    # --- phase 2c: decide what an interruption meant ------------------
    async def act_on_interrupted(item: FrontierTaskRef, task: BaseTask) -> None:
        nonlocal acted
        # The backend may be retrying the input itself. Modal, for one,
        # restarts a timed-out input when the worker function declares
        # ``retries``, and it does so under the SAME executor ref — so the
        # ref probing as live is proof that a restart is in flight and that
        # spawning here would run the task twice. Probed rather than
        # assumed, because the tick cannot see the worker's retry config.
        #
        # Cost is one non-blocking probe per interrupted task, on a path
        # that only runs when something was actually interrupted.
        if item.latest_executor_ref and item.latest_executor:
            status = await _probe_detached(item, task, task_executor)
            if status == DetachedExecutionStatus.RUNNING:
                awaiting_backend.append((item, task))
                summary.interruptions_backend_retrying += 1
                logger.info(
                    f"Task {task.id} of build {build_id} was interrupted but "
                    f"its execution {item.latest_executor_ref} still probes "
                    "as live — either the backend is retrying the input "
                    "itself, or the call has not finished unwinding yet. "
                    "Leaving it alone this pass and re-probing shortly, "
                    "rather than spawning a duplicate."
                )
                return

        # Every INTERRUPTED task is here because a worker asked to be
        # resumed — that status is only ever written for a task that raised
        # ``ResumableInterruption``. An interruption the task did NOT catch
        # never reaches this branch: the worker reports nothing, the
        # execution dies, and a later pass records it as an ordinary
        # retryable failure. So there is no policy to consult here, and no
        # per-task configuration deciding whether a timeout was "expected" —
        # the task said so by raising, or it did not.
        spent = item.interrupt_count
        if spent is None:
            # No counter, so nothing can bound a resume loop. Degrade to the
            # thing that IS bounded (``max_attempts``) rather than to an
            # unbounded one. See ``FrontierTaskRef.interrupt_count``.
            logger.warning(
                f"Task {task.id} of build {build_id} asked to be resumed, "
                "but this registry does not report per-round interruption "
                "counts, so TickConfig.max_interruptions cannot be enforced "
                "and resuming would be unbounded. Recording a retryable "
                "failure instead. Upgrade stardag-api to enable resumption."
            )
            summary.interruptions_failed += 1
            await _record_task_failure(
                task,
                "Execution interrupted; this registry cannot bound resumption",
                build_id=build_id,
                registry=registry,
                config=config,
                summary=summary,
                retryable=True,
                attempts_spent=item.attempt_count,
            )
            acted = True
            return

        if spent < config.max_interruptions:
            # Counted at spawn time, not here: this phase runs before the
            # per-pass spawn cap, and interrupted tasks are appended after
            # the pending ones, so they are the first to be truncated.
            # Incrementing here would report resumes that did not happen.
            resumption_requests.add(task.id)
            spawn_candidates.append(task)
            logger.info(
                f"Task {task.id} of build {build_id} checkpointed and asked "
                f"to be resumed; starting it again ({spent} of "
                f"{config.max_interruptions} allowed interruption(s) "
                "absorbed this build round)."
            )
            return

        summary.interruptions_exhausted += 1
        logger.error(
            f"Task {task.id} of build {build_id} has been interrupted "
            f"{spent} time(s) this build round, which is its whole budget "
            f"(TickConfig.max_interruptions={config.max_interruptions}). "
            "Failing it rather than resuming it again. If this task "
            "legitimately needs more resumes — a long training run that "
            "checkpoints, say — re-trigger this build with "
            'tick_kwargs={"max_interruptions": N}, which also starts a new '
            "round and resets the count."
        )
        await _record_task_failure(
            task,
            (
                f"Interruption budget spent ({spent} of "
                f"{config.max_interruptions} allowed interruption(s) in this "
                f"build round). Re-trigger this build (build_id={build_id}) "
                "to start a new round, optionally with "
                'tick_kwargs={"max_interruptions": N}.'
            ),
            build_id=build_id,
            registry=registry,
            config=config,
            summary=summary,
            # Already over its budget; the retry branch would only re-derive
            # that against a different budget and log it twice.
            retryable=False,
        )
        acted = True

    await _run_bounded(
        [partial(act_on_interrupted, item, task) for item, task in interrupted_items],
        semaphore,
    )

    # --- phase 3: spawn, up to this pass's cap ------------------------
    cap = _spawn_cap(spawn_candidates, task_executor, config)
    if summary.iterations <= 1:
        # Once per tick, at INFO: the cap and — more importantly — which of
        # :func:`_spawn_cap`'s four rungs produced it. Three of them yield
        # plausible-looking numbers from very different inputs, so a
        # truncating tick is only diagnosable if the log says which was
        # read. Subsequent passes of the same tick re-derive the same
        # answer and would only repeat themselves.
        logger.info(
            f"Tick for build {build_id} will spawn at most {cap.limit} "
            f"task(s) per pass, from {cap.source}."
        )
    if len(spawn_candidates) > cap.limit:
        # Loud, always: a build that spawns in batches is a build whose
        # logs must say so, or the next reader concludes the frontier is
        # shrinking for some other reason. Not a stall — see the
        # ``if acted:`` branch in ``_run_tick_body_aio``.
        logger.info(
            f"Build {build_id} has {len(spawn_candidates)} spawnable tasks "
            f"this pass, more than the per-tick cap of {cap.limit} (from "
            f"{cap.source}); spawning the first {cap.limit} and "
            "re-evaluating on a fresh frontier immediately. Set "
            "TickConfig.max_spawns_per_tick to change the cap."
        )
        spawn_candidates = spawn_candidates[: cap.limit]

    async def spawn(task: BaseTask) -> None:
        nonlocal acted, denied_this_round
        limit_keys: list[str] = (
            list(config.limit_key_selector(task))
            if config.limit_key_selector is not None
            else []
        )
        # Atomic claiming start BEFORE spawning — the execution claim
        # (exactly-once arbitration) and any concurrency-limit slots in
        # one transaction, so a denied task never occupies a worker.
        # The acquiring TASK_STARTED carries no executor ref yet (there
        # is nothing to reference), but it does carry the executor
        # metadata when the executor can resolve it pre-spawn —
        # otherwise a UI read in the acquire→spawn window shows a
        # RUNNING task with blank executor info. The post-spawn start
        # below re-records with the ref (duplicate starts are
        # tolerated, and slots are counted per task, not per start).
        acquire_metadata: "dict[str, typing.Any] | None" = None
        try:
            acquire_metadata = await task_executor.get_executor_metadata(task)
        except Exception:
            logger.debug(
                f"Executor metadata resolution failed for task "
                f"{task.id}; acquiring without it.",
                exc_info=True,
            )
        # Both starts below carry the same derived TTL. The post-spawn one
        # needs it as much as the claiming one: the TTL applies to every
        # start, so omitting it there would hand the claim straight back to
        # the registry's generic default and undo the derivation.
        ttl_seconds = claim_ttl_seconds(task, task_executor)
        claim_result = await registry.task_start_claim_aio(
            build_id,
            task,
            executor_metadata=acquire_metadata,
            limit_keys=limit_keys or None,
            claim_ttl_seconds=ttl_seconds,
        )
        if not claim_result.started:
            if claim_result.denied_reason == "limit":
                logger.info(
                    f"Task {task.id} denied by concurrency limits "
                    f"{limit_keys}; leaving in frontier."
                )
                summary.limit_denied += 1
            else:
                # already_running / already_completed: another scheduler
                # won the race (or the frontier snapshot is stale) — the
                # next frontier fetch reflects the true status and the
                # RUNNING-probe partition takes over.
                logger.info(
                    f"Claim for task {task.id} denied "
                    f"({claim_result.denied_reason}); leaving in frontier."
                )
                summary.claim_denied += 1
            denied_this_round += 1
            return
        try:
            handle = await task_executor.submit_detached(task)
        except Exception as e:
            logger.error(f"Failed to spawn task {task.id}: {e}")
            # The one failure no execution backend can retry for us: there
            # is no function call to retry. The claiming start above went
            # through, so the attempt the server has counted by now is one
            # more than the frontier reported when this pass began.
            spent = attempts_by_task_id.get(str(task.id))
            await _record_task_failure(
                task,
                f"Spawn failed: {e}",
                build_id=build_id,
                registry=registry,
                config=config,
                summary=summary,
                retryable=True,
                attempts_spent=None if spent is None else spent + 1,
            )
            acted = True
            return
        await registry.task_start_aio(
            build_id,
            task,
            executor=handle.executor,
            executor_ref=handle.ref,
            executor_metadata=handle.executor_metadata,
            claim_ttl_seconds=ttl_seconds,
        )
        summary.spawned += 1
        if task.id in resumption_requests:
            # Counted here rather than where the request was read, so a
            # pass truncated by the spawn cap does not report resumes it
            # never made.
            summary.interruptions_restarted += 1
        acted = True

    await _run_bounded([partial(spawn, task) for task in spawn_candidates], semaphore)
    return acted, denied_this_round, awaiting_backend


def _claim_has_lapsed(expires_at: datetime | None, now: datetime) -> bool:
    """Whether an execution claim is past its own expiry.

    None — "never lapses", i.e. an older server, a start recorded before
    the column existed, or a caller that asked for no expiry — is False.
    Absence of evidence is not evidence of death, and every caller here is
    deciding whether to kill something.

    Naive timestamps (a custom registry that drops the offset) are read as
    UTC rather than raising: this runs on paths that decide a task's or a
    build's fate, so a formatting quirk must not become a tick crash.
    """
    if expires_at is None:
        return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return now > expires_at


async def _resolve_running(
    item: "FrontierTaskRef",
    task: BaseTask,
    task_executor: TaskExecutorABC,
) -> str:
    """Decide what to do with a RUNNING task: leave/complete/failed.

    Self-heal precedence: the target is the ground truth — if it exists the
    task is complete regardless of what happened to the execution (e.g. the
    worker wrote the output, then died before reporting). Then the executor
    is asked. A claim expiry is only a *floor* on liveness; the backend's
    own answer about the execution is the truth, so probing keeps
    precedence over anything the expiry says wherever a ref exists.
    """
    executor_name, ref = item.latest_executor, item.latest_executor_ref
    if await task.complete_aio():
        return "complete"
    if executor_name is None or ref is None:
        # RUNNING with no ref: nothing to probe, and no worker to report it.
        # The window is still reachable — the claiming start is recorded
        # BEFORE the spawn, so a tick that dies in between leaves exactly
        # this shape — and it does still need handling: while RUNNING the
        # task holds any concurrency-limit slots it acquired, starving those
        # keys environment-wide.
        #
        # What it no longer needs is a locally configured guess at how long
        # is too long. The claim carries its own expiry; past it the claim
        # is not honoured by anyone, which is the fact the old bound was
        # approximating. Lapsed → fail. Otherwise leave it: the
        # spawn-in-progress window of a perfectly healthy tick looks
        # identical from here, and failing that would kill a task that is
        # about to start.
        if _claim_has_lapsed(item.latest_status_expires_at, datetime.now(timezone.utc)):
            logger.error(
                f"Task {task.id} is RUNNING without an executor ref and its "
                f"execution claim lapsed at {item.latest_status_expires_at}; "
                "failing it (nothing can probe it and no worker will report "
                "it — most likely a scheduler crash between the claiming "
                "start and the spawn)."
            )
            return "failed"
        logger.warning(
            f"Task {task.id} is RUNNING without an executor ref; leaving it"
            + (
                " (its claim has not lapsed)."
                if item.latest_status_expires_at is not None
                else " (its claim carries no expiry, so it cannot be shown "
                "abandoned from here — cancel the task to release it)."
            )
        )
        return "leave"
    status = await task_executor.detached_status(task, executor_name, ref)
    if status == DetachedExecutionStatus.RUNNING:
        return "leave"
    if status == DetachedExecutionStatus.SUCCEEDED:
        # Finished successfully but target check above said incomplete —
        # eventual consistency; treat as complete (worker wrote it).
        return "complete"
    if status == DetachedExecutionStatus.FAILED:
        return "failed"
    # UNKNOWN: possibly still running somewhere we can't see — leave rather
    # than risk a duplicate execution. The watchdog re-probes periodically.
    logger.warning(
        f"Detached execution {ref!r} for task {task.id} has unknown status; "
        "leaving it (watchdog will re-check)."
    )
    return "leave"


async def _any_ref_settled(
    awaiting: "list[tuple[FrontierTaskRef, BaseTask]]",
    task_executor: TaskExecutorABC,
) -> bool:
    """Whether any awaited execution has stopped probing as live.

    The linger loop's cheap poll: one non-blocking probe per interrupted
    task the pass left behind, and nothing else. Returning True sends the
    tick back through a full pass, which will re-probe and act.
    """
    for item, task in awaiting:
        status = await _probe_detached(item, task, task_executor)
        if status != DetachedExecutionStatus.RUNNING:
            return True
    return False


async def _probe_detached(
    item: "FrontierTaskRef",
    task: BaseTask,
    task_executor: TaskExecutorABC,
) -> DetachedExecutionStatus:
    """Ask the executor about ``item``'s recorded execution, never raising.

    Thin because the interesting judgement is at the call site: only
    :data:`DetachedExecutionStatus.RUNNING` is treated as "hands off". In
    particular ``UNKNOWN`` is NOT — and that is a deliberate departure from
    :func:`_resolve_running`, where UNKNOWN means "leave it".

    The difference is what "leave it" costs. A RUNNING task that is left
    keeps a live claim and gets re-probed by the next tick, so waiting is
    free and duplicate-safe. An *interrupted* task holds no claim and is
    nobody else's to run, so leaving it on UNKNOWN risks stalling the build
    outright — and UNKNOWN is not rare here: an executor that does not
    recognise the recorded ref's backend (a resident build reading a ref a
    Modal tick wrote, say) answers UNKNOWN for every task, forever.

    So this guard covers exactly the case it was built for — a backend
    retrying the same input under the same ref, which probes as RUNNING —
    and does not pretend to cover an unreachable backend.
    """
    executor_name, ref = item.latest_executor, item.latest_executor_ref
    if executor_name is None or ref is None:
        return DetachedExecutionStatus.UNKNOWN
    try:
        return await task_executor.detached_status(task, executor_name, ref)
    except Exception:
        logger.warning(
            f"Probing detached execution {ref!r} for task {task.id} raised; "
            "treating its status as unknown.",
            exc_info=True,
        )
        return DetachedExecutionStatus.UNKNOWN
