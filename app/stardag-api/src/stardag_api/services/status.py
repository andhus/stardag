"""Status derivation from events."""

from collections.abc import Sequence
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from stardag_api.models import Build, BuildStatus, Event, EventType, Task, TaskStatus
from stardag_api.services.claims import claim_expires_at
from stardag_api.services.dependencies import retract_dynamic_edges_if_new_attempt
from stardag_api.services.wakeups import flag_after_task_transition

# Statuses TASK_RETRIED resets to PENDING. Shared by the denormalised path
# (_apply_event_to_task) and the two per-build event replays below, which
# must agree — they answer the same question for different readers.
#
# SUSPENDED is in the set because it is a dead end otherwise: a task
# suspended for dynamic dependencies whose orchestrator then died stays
# suspended forever, and no supported operation makes it schedulable again
# (the only escape was to cancel it first, purely to reach a status that
# *was* retryable). Nothing is running at that point — the suspension
# itself means the execution yielded and returned — so resetting it cannot
# orphan an execution.
#
# RUNNING is deliberately NOT retryable: it holds a live execution claim,
# and releasing that is cancellation, not retry. Flipping it to PENDING
# would let a scheduler spawn a second execution of a task that is still
# running. COMPLETED is excluded by stickiness.
# INTERRUPTED is in the set for exactly the reason SUSPENDED is: the
# execution is over (the platform ended it), nothing is running, and the
# task is otherwise a dead end. Resetting it cannot orphan an execution.
_RETRYABLE_STATUSES = (
    TaskStatus.FAILED,
    TaskStatus.CANCELLED,
    TaskStatus.SKIPPED,
    TaskStatus.SUSPENDED,
    TaskStatus.INTERRUPTED,
)


# --- Execution attempts -------------------------------------------------
#
# "How many times has execution been started for this task in the build's
# current round?" — the number a scheduler needs to decide whether a
# failure is worth another go. It is NOT the number of TASK_STARTED
# events, and the gap is not an edge case: engines routinely emit several
# starts per attempt.
#
#   reactive (_act_on_frontier):  an *acquiring* start (the atomic claim /
#       limit-slot acquisition, before the spawn, with no executor ref),
#       a second start carrying the ref once the spawn returned, and the
#       worker's own self-reported start with its own call id — which
#       lands seconds later under cold start. Three for one execution.
#   resident (_concurrent):       the same three, for the same reasons.
#   sequential / unclaimed:       exactly one start.
#
# (Observed live, one execution: 3-field start at t+0, 5-field at t+2.4s,
# 5-field at t+7.7s. This comment previously credited reactive with two
# and resident with three; the worker self-report happens in both.)
#
# Counting events would therefore make the same task look like it had
# 1, 2 or 3 attempts depending on which engine and which executor ran it,
# and would exhaust any retry budget on the first try.
#
# The rule instead counts *transitions into RUNNING*: a start begins a new
# attempt unless the previous status-affecting event was itself a start.
# Consecutive starts are one execution re-recording itself, whatever the
# engine's reason for re-recording. That is engine-agnostic by
# construction — it needs no knowledge of who emits how many starts, only
# that nothing else happened to the task in between.
#
# Status-neutral events are excluded from "previous" so one landing inside
# an attempt's acquire→spawn window cannot split it in two. They can't
# change status, so they can't end an attempt either.
_ATTEMPT_ORDERING_EVENT_TYPES = (
    EventType.TASK_STARTED,
    EventType.TASK_RESUMED,
    EventType.TASK_SUSPENDED,
    EventType.TASK_RETRIED,
    EventType.TASK_COMPLETED,
    EventType.TASK_FAILED,
    EventType.TASK_INTERRUPTED,
    EventType.TASK_SKIPPED,
    EventType.TASK_CANCELLED,
)

# Predecessors that do NOT open a new attempt.
#
# TASK_STARTED is the engine-agnostic dedupe described above: consecutive
# starts are one execution re-recording itself.
#
# TASK_INTERRUPTED is here for a different reason, and it is a policy
# choice rather than a de-duplication: an interruption is the platform
# taking the container away, so the run that follows it is a continuation
# of work the task itself did nothing wrong in. A task designed to be
# killed and resumed until it converges — the checkpointing trainer this
# status exists for — would otherwise exhaust a budget meant for genuine
# failures and fail the build for the one reason it was built to survive.
# Interruptions are bounded separately, by their own count (see
# ``get_interrupt_counts_in_build``) against
# ``TickConfig.max_interruptions``, so "not counted here" does not mean
# "unbounded".
#
# Worth knowing when changing this: two consecutive TASK_STARTEDs are also
# what makes the execution backend's *own* restart of an input free — a
# preempted container that comes back records a second start and spends no
# attempt. Inserting any new event type between those two starts silently
# starts charging for it.
_ATTEMPT_CONTINUING_EVENT_TYPES = (
    EventType.TASK_STARTED,
    EventType.TASK_INTERRUPTED,
)


def starts_new_attempt(event_type: str | None, prev_event_type: str | None) -> bool:
    """Whether ``event_type`` begins a new execution attempt.

    ``prev_event_type`` is the preceding *status-affecting* event for the
    same (build, task) **within the current round** — ``None`` for the
    first one, which is why the first start after a resume always counts
    even if the last event before the resume was also a start. The single
    Python definition of the rule described above;
    ``get_attempt_counts_in_build`` expresses the identical predicate in
    SQL, and the two must agree.
    """
    return (
        event_type == EventType.TASK_STARTED
        and prev_event_type not in _ATTEMPT_CONTINUING_EVENT_TYPES
    )


def _as_utc(value: datetime) -> datetime:
    """Normalise a timestamp to aware UTC so two of them can be compared.

    One event stream can hold both shapes at once: Postgres returns aware
    values and SQLite returns naive ones, and on either backend an object
    flushed earlier in *this* session still carries the aware value
    ``utc_now()`` produced, never having round-tripped through the
    database. Every one of them is UTC, so the only real work is putting
    the tzinfo back on the ones that lost it — without which the round
    comparison below raises on a mixed stream.
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def last_build_resumed_at(build_id: UUID) -> Select[tuple[datetime]]:
    """When this build was last resumed — NULL if it never was.

    (The annotation is SQLAlchemy's column type; ``max`` over no rows
    still yields NULL, so executing this returns ``datetime | None``.)

    The start of the attempt-counting window (see
    :func:`get_attempt_counts_in_build`). Factored out because the two
    consumers need it in different forms — ``.scalar_subquery()`` inside
    the grouped statement, executed directly beside the per-task replay,
    which must still see the whole stream — and they must not drift.

    Served by ``ix_events_build_task_type`` as a seek: build-level events
    carry ``task_id IS NULL``, so ``(build_id, NULL, 'build_resumed')`` is
    a point lookup rather than a scan of the build's events.
    """
    return select(func.max(Event.created_at)).where(
        Event.build_id == build_id,
        # Explicit, even though only build-level events carry this type: it
        # is what makes the index a seek on (build_id, NULL, 'build_resumed')
        # rather than a scan over every event the build ever recorded, and
        # the comment above claims exactly that.
        Event.task_id.is_(None),
        Event.event_type == EventType.BUILD_RESUMED.value,
    )


async def get_attempt_counts_in_build(
    db: AsyncSession,
    build_id: UUID,
    task_db_ids: Sequence[UUID] | None = None,
) -> dict[UUID, int]:
    """Execution attempts per task in a build's **current round**, by task pk.

    A "round" starts at the build's most recent ``BUILD_RESUMED`` event, or
    at the beginning of the build when it has never been resumed.

    **Why the resume, and not simply the whole build.** A retry budget has
    to have an escape hatch the user can reach, and re-triggering an
    existing build id — ``build_trigger(..., build_id=<existing>,
    reactive=True)`` — is the recommended way to pick a failed reactive
    build back up. That path does NOT mint a new build: it resumes this
    one and calls ``task_retry`` on the failed tasks *in it*. Counting
    over the build's whole history would therefore leave the budget
    already spent the moment the user asked for another round, so the one
    action anybody reaches for after a failure would do nothing unless
    they also raised ``max_attempts``.

    ``BUILD_RESUMED`` is exactly the durable marker of "another round was
    asked for": it already exists, it is already emitted on that path, and
    the server skips it for a build with no activity beyond
    ``BUILD_STARTED`` — so a *first* trigger is not a resume and the
    window is simply the whole build. No new event type, column or marker.
    Repeated re-triggers do hand out fresh budgets, but that is a user
    deciding to try again, not a scheduler looping.

    **Per build, deliberately** — unlike the denormalised ``Task.latest_*``
    columns beside it, which are environment-global because a completed
    task is completed for everyone. Attempts are not like that: the
    retry-relevant question is "how many times has *this* build tried in
    this round", and a task that burned two attempts in an earlier build
    must not arrive in a new one with its budget already spent. That is
    also why this cannot live on the task row at all — there is no
    per-(build, task) row to denormalise onto, and adding one would buy a
    fold on every start path plus a backfill to replace a single grouped
    query over an index that already exists
    (``ix_events_build_task_type``).

    Note what a resume does NOT reset: ``TASK_RETRIED`` on its own never
    resets the count. A scheduler retries a failed task *through* that
    endpoint, so a counter cleared by it would be cleared by every
    enforcement of the budget it defines. The distinction is the whole
    point — a bare retry spends budget, a resume grants a new round.

    Tasks with no attempt in this round are simply absent from the result;
    callers default them to 0.

    ``task_db_ids`` bounds the scan to the tasks the caller will actually
    report, so the cost tracks the size of the response rather than the
    size of the build. Omit it for every task in the build.

    One grouped query, never one per task: the frontier that consumes this
    is re-read on every linger poll of every active build. The round
    cutoff rides along as a correlated scalar subquery, so windowing costs
    no extra round trip.
    """
    if task_db_ids is not None and not task_db_ids:
        return {}

    # LAG gives each event its predecessor within the task's own stream, so
    # the attempt-boundary rule becomes a WHERE clause. Restricting the rows
    # to the status-affecting types is safe *and* is what makes the rule
    # right (see _ATTEMPT_ORDERING_EVENT_TYPES); restricting them to
    # ``task_db_ids`` is safe because the window partitions by task, so a
    # task's predecessor is never in another task's rows.
    #
    # The round cutoff is applied HERE, in the inner query, not to the
    # outer count — so LAG never sees a pre-resume event and the first
    # start of a new round has no predecessor and always counts. Filtering
    # afterwards would instead let the last start of the *previous* round
    # collapse the first start of this one, and a resumed build would
    # report zero attempts however many times it tried.
    #
    # ``>=``, and the tie it admits is unreachable in practice: it would
    # take a task event committed in the same microsecond as the resume,
    # by a different request, to land on the wrong side. Counting such an
    # event is also the conservative direction — it can only spend budget,
    # never grant an unbounded loop.
    resumed_at = last_build_resumed_at(build_id).scalar_subquery()
    ordered = (
        select(
            Event.task_id.label("task_id"),
            Event.event_type.label("event_type"),
            func.lag(Event.event_type)
            .over(
                partition_by=Event.task_id,
                order_by=(Event.created_at, Event.id),
            )
            .label("prev_event_type"),
        )
        .where(
            Event.build_id == build_id,
            Event.task_id.is_not(None),
            Event.event_type.in_([e.value for e in _ATTEMPT_ORDERING_EVENT_TYPES]),
            # Never resumed → no cutoff → the window is the whole build.
            or_(resumed_at.is_(None), Event.created_at >= resumed_at),
        )
        .where(*([Event.task_id.in_(task_db_ids)] if task_db_ids is not None else []))
        .subquery()
    )
    rows = (
        await db.execute(
            select(ordered.c.task_id, func.count())
            .where(
                ordered.c.event_type == EventType.TASK_STARTED.value,
                # The SQL twin of ``starts_new_attempt`` — the two must
                # agree, and a change to one is a change to both.
                #
                # Null-safe: the first event in a stream has no predecessor
                # and must still count as an attempt. ``NOT IN`` would
                # answer NULL there and drop the row, so the predicate is
                # spelled as an AND of IS DISTINCT FROMs (which renders IS
                # DISTINCT FROM on Postgres and IS NOT on SQLite).
                *[
                    ordered.c.prev_event_type.is_distinct_from(e.value)
                    for e in _ATTEMPT_CONTINUING_EVENT_TYPES
                ],
            )
            .group_by(ordered.c.task_id)
        )
    ).all()
    return {task_id: count for task_id, count in rows}


async def get_interrupt_counts_in_build(
    db: AsyncSession,
    build_id: UUID,
    task_db_ids: Sequence[UUID] | None = None,
) -> dict[UUID, int]:
    """Interruptions per task in a build's **current round**, by task pk.

    The companion budget to :func:`get_attempt_counts_in_build`, and
    deliberately a much simpler rule: a plain count of TASK_INTERRUPTED
    events, with no de-duplication.

    **Why no dedupe here, when attempts need one.** The attempt rule exists
    because several engines emit two or three TASK_STARTEDs per execution
    (acquire, ref-record, worker self-report) — counting events would make
    the same task look different depending on who ran it. Nothing emits
    more than one interruption per execution: it is reported once, by the
    dying worker, in its grace window. One event, one interruption.

    Same round window as attempts, for the same reason: re-triggering a
    build is how a user says "try again", and a budget the user cannot
    reset is a budget that eventually wedges the build.

    Tasks with no interruption in this round are absent; callers default
    them to 0.
    """
    if task_db_ids is not None and not task_db_ids:
        return {}

    resumed_at = last_build_resumed_at(build_id).scalar_subquery()
    rows = (
        await db.execute(
            select(Event.task_id, func.count())
            .where(
                Event.build_id == build_id,
                Event.task_id.is_not(None),
                Event.event_type == EventType.TASK_INTERRUPTED.value,
                or_(resumed_at.is_(None), Event.created_at >= resumed_at),
            )
            .where(
                *([Event.task_id.in_(task_db_ids)] if task_db_ids is not None else [])
            )
            .group_by(Event.task_id)
        )
    ).all()
    return {task_id: count for task_id, count in rows}


async def transition_task(
    db: AsyncSession,
    task: Task,
    event: Event,
) -> None:
    """Record ``event`` and let it move ``task``'s denormalised status.

    **The one way a task's ``latest_status`` changes.** Every path that
    records a task event used to do this by hand — the event routes,
    skip-blocked, cascade cancel, the lock's completion release, and both
    registration paths — each pairing :func:`_apply_event_to_task` with the
    post-transition hooks itself. Two were missed when the cross-build
    wake-up hook was added, so skip-blocked and the lock release flagged
    nobody; the fix was to go and find them all again. This function exists
    so there is only one to find, however many callers it grows.

    Runs, in order: the event is registered, flushed if it needs to be, the
    previous status is captured, the event is applied, and every
    post-transition hook runs. The hooks are part of the caller's
    transaction by construction, which is what makes a flag impossible to
    set for a change that then rolls back.

    ``event.build_id`` is the build whose event this is — the one build a
    same-transaction wake-up flag must *not* be set on, since it is the one
    that already knows.
    """
    db.add(event)
    if event.id is None or event.created_at is None:
        # The apply reads both, and they are Python-side column defaults,
        # so an unstamped event needs a flush to have them. Derived rather
        # than passed: this used to be a ``flush: bool`` argument, and a
        # caller that got it wrong would have written a status alongside a
        # NULL ``latest_status_at`` and event id — nullable columns, so it
        # would commit silently and leave the denormalised row disagreeing
        # with the event stream. That is the exact class of quiet bug this
        # function exists to end, so it is not reintroduced as a keyword.
        #
        # Bulk registration stamps both itself, to keep a 500-task plan to
        # one round trip; it therefore skips the flush without having to
        # say so.
        await db.flush()
    previous_status = task.latest_status
    previous_status_build_id = task.latest_status_build_id
    _apply_event_to_task(task, event)
    # Cross-build wake-up (see services.wakeups): a status change is news
    # for every *other* live reactive build holding this task, which has
    # nobody else to tell it. Transition-gated inside the hook, so an event
    # landing on an already-completed task — or a registration event, which
    # is status-neutral by design — flags nobody and costs no query.
    await flag_after_task_transition(
        db, task, previous_status=previous_status, build_id=event.build_id
    )
    # Retraction (see services.dependencies): a transition that begins a
    # *new attempt* abandons whatever dynamic dependencies the previous one
    # yielded. Both hooks read the pre-apply status, which is why they live
    # here rather than in their callers — and why this one also needs the
    # previous owner, which the apply has already overwritten by now.
    await retract_dynamic_edges_if_new_attempt(
        db,
        task,
        previous_status=previous_status,
        previous_status_build_id=previous_status_build_id,
        event=event,
    )


def _apply_event_to_task(task: Task, event: Event) -> None:
    """Mutate a Task's denormalised latest_* columns to reflect a new event.

    This implements the same priority logic as the historical
    get_all_task_global_statuses event scan, applied incrementally:

      - TASK_COMPLETED is sticky — once completed, no other event downgrades.
      - Otherwise TASK_RUNNING / TASK_FAILED / TASK_CANCELLED overwrite
        PENDING (and each other, in event-arrival order).
      - TASK_RESUMED is treated like TASK_STARTED (re-asserts RUNNING).
      - TASK_WAITING_FOR_LOCK only sets the flag if the task is still PENDING.
      - TASK_REFERENCED and TASK_PENDING are status-neutral on existing rows
        (they don't downgrade COMPLETED → PENDING).

    It also maintains ``latest_status_expires_at``, the expiry of the
    execution claim that RUNNING *is* (see ``services.claims``): every event
    that asserts RUNNING sets it from the event's own
    ``claim_ttl_seconds`` metadata (or the server default), and every event
    that moves the task out of RUNNING clears it. Deriving it here rather
    than at the route means the value is a function of the event stream —
    the TTL is recorded in the event metadata, so a replay reproduces the
    expiry it produced the first time — and means no start path can grant an
    unexpiring claim by forgetting a keyword argument.

    Note what "every event that asserts RUNNING" does and does not buy. A
    re-start (recording an executor ref) or a resume extends the claim for
    free, on traffic that already exists. But a task that emits nothing
    between start and finish — the long-running batch job this whole system
    exists for — gets exactly one expiry, at start. That is why the TTL
    wants to come from the caller's executor timeout rather than being a
    short lease topped up by liveness traffic there is none of.

    The caller is responsible for persisting the Task — this function only
    mutates the in-memory ORM instance.
    """
    event_commit = (
        event.event_metadata.get("commit_hash") if event.event_metadata else None
    )
    et = event.event_type

    # Generic bookkeeping that always reflects the most-recently-applied event.
    # latest_status_event_id moves only when status itself moves; see below.

    if et == EventType.TASK_COMPLETED:
        task.latest_status = TaskStatus.COMPLETED
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        task.latest_completed_at = event.created_at
        task.latest_status_expires_at = None
        task.latest_waiting_for_lock = False
        if event_commit is not None:
            task.latest_commit_hash = event_commit
        return

    # All branches below are no-ops once the task is COMPLETED.
    if task.latest_status == TaskStatus.COMPLETED:
        return

    if et == EventType.TASK_STARTED:
        task.latest_status = TaskStatus.RUNNING
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        if task.latest_started_at is None:
            task.latest_started_at = event.created_at
        task.latest_waiting_for_lock = False
        if event_commit is not None:
            task.latest_commit_hash = event_commit
        # Executor ref of this start (detached execution re-attach). Set or
        # *cleared* on every start so a non-detached run can't leave a stale
        # ref from an earlier detached one behind. The descriptive
        # executor_metadata follows the exact same set/clear semantics.
        metadata = event.event_metadata or {}
        task.latest_executor = metadata.get("executor")
        task.latest_executor_ref = metadata.get("executor_ref")
        task.latest_executor_metadata = metadata.get("executor_metadata")
        # Grant (or re-grant) the claim's expiry alongside the executor
        # fields, from the same event. Doing both here is what makes a
        # re-claim of an expired claim coherent: the new holder's ref, its
        # build and its expiry replace the dead holder's together, so no
        # reader can pair a fresh expiry with a stale ref.
        task.latest_status_expires_at = claim_expires_at(
            event.created_at, metadata.get("claim_ttl_seconds")
        )
    elif et == EventType.TASK_RETRIED:
        # Reset a retryable status to PENDING (see _RETRYABLE_STATUSES);
        # sticky-COMPLETED is already handled by the early return, and
        # RUNNING is never downgraded by a retry.
        if task.latest_status in _RETRYABLE_STATUSES:
            task.latest_status = TaskStatus.PENDING
            task.latest_status_at = event.created_at
            task.latest_status_event_id = event.id
            task.latest_status_build_id = event.build_id
            task.latest_completed_at = None
            task.latest_error_message = None
            task.latest_status_expires_at = None
            # The executor fields describe the run that reached the
            # retryable status — including a suspended one, which keeps the
            # ref of the execution that yielded. A retry re-runs from
            # scratch, so clearing them is what stops a scheduler from
            # re-attaching to an execution that will never resume.
            task.latest_executor = None
            task.latest_executor_ref = None
            task.latest_executor_metadata = None
    elif et == EventType.TASK_RESUMED:
        task.latest_status = TaskStatus.RUNNING
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        task.latest_waiting_for_lock = False
        # A resume re-asserts the claim, so it re-grants the expiry —
        # renewal on lifecycle traffic that already exists.
        task.latest_status_expires_at = claim_expires_at(
            event.created_at,
            (event.event_metadata or {}).get("claim_ttl_seconds"),
        )
        if event_commit is not None:
            task.latest_commit_hash = event_commit
    elif et == EventType.TASK_FAILED:
        task.latest_status = TaskStatus.FAILED
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        task.latest_completed_at = event.created_at
        task.latest_error_message = event.error_message
        task.latest_status_expires_at = None
        if event_commit is not None:
            task.latest_commit_hash = event_commit
    elif et == EventType.TASK_CANCELLED:
        task.latest_status = TaskStatus.CANCELLED
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        task.latest_completed_at = event.created_at
        task.latest_status_expires_at = None
        if event_commit is not None:
            task.latest_commit_hash = event_commit
    elif et == EventType.TASK_SKIPPED:
        task.latest_status = TaskStatus.SKIPPED
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        task.latest_completed_at = event.created_at
        task.latest_status_expires_at = None
    elif et == EventType.TASK_SUSPENDED:
        task.latest_status = TaskStatus.SUSPENDED
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        # A suspension is an execution that yielded and returned: nothing is
        # running, so the claim is over and its expiry is meaningless.
        task.latest_status_expires_at = None
    elif et == EventType.TASK_INTERRUPTED:
        task.latest_status = TaskStatus.INTERRUPTED
        task.latest_status_at = event.created_at
        task.latest_status_event_id = event.id
        task.latest_status_build_id = event.build_id
        # The platform ended this execution, so the claim is over — same as
        # a suspension, and the reason the interruption is worth reporting
        # at all: it frees the claim and any concurrency-limit slots now,
        # instead of leaving them held until something notices the corpse.
        task.latest_status_expires_at = None
        # Recorded like a failure's, because "why was it interrupted?" is
        # the same question a reader asks of a failure — but deliberately
        # NOT written to latest_completed_at: an interruption is not an
        # ending, it is a pause before the next attempt.
        #
        # Assigned unconditionally, exactly like TASK_FAILED: a conditional
        # write would leave a *previous* failure's text on a task that has
        # since been retried and then interrupted, so the UI would explain
        # this interruption with an unrelated stack trace.
        task.latest_error_message = event.error_message
        # The executor ref is left alone on purpose. An interruption does
        # not always mean the execution is gone: a backend configured with
        # its own retries may be restarting the very same call, and a
        # scheduler needs the ref to probe for exactly that before it
        # spawns a duplicate. A TASK_STARTED will replace it; a
        # TASK_RETRIED clears it.
        if event_commit is not None:
            task.latest_commit_hash = event_commit
    elif et == EventType.TASK_WAITING_FOR_LOCK:
        if task.latest_status == TaskStatus.PENDING:
            task.latest_waiting_for_lock = True
    elif et == EventType.TASK_PENDING:
        # Initial PENDING for a brand-new task: keep PENDING but record the
        # event so the latest_status_at field reflects the most recent
        # contributing event. Don't downgrade an already-non-PENDING state.
        if task.latest_status == TaskStatus.PENDING:
            task.latest_status_at = event.created_at
            task.latest_status_event_id = event.id
            task.latest_status_build_id = event.build_id
    # TASK_REFERENCED is informational and never affects latest_* state
    # (the global semantics treat it as a no-op).


# The build-level statuses a terminal event produces, keyed by event type.
# BUILD_STARTED / BUILD_RESUMED are handled separately: they are the only
# two that move a build *back* into RUNNING, and they differ from each other
# (and from the terminals) in which fields they touch.
_TERMINAL_BUILD_STATUS = {
    EventType.BUILD_COMPLETED: BuildStatus.COMPLETED,
    EventType.BUILD_FAILED: BuildStatus.FAILED,
    EventType.BUILD_CANCELLED: BuildStatus.CANCELLED,
    EventType.BUILD_EXIT_EARLY: BuildStatus.EXIT_EARLY,
}

# Terminals that can carry a "this was a manual override from the UI"
# attribution. BUILD_EXIT_EARLY is excluded because it is emitted by the
# SDK itself ("everything left is running in another build") and is never
# user-triggered — it has no ``triggered_by_user_id`` to read.
_USER_TRIGGERABLE_BUILD_EVENTS = (
    EventType.BUILD_COMPLETED,
    EventType.BUILD_FAILED,
    EventType.BUILD_CANCELLED,
)


def apply_event_to_build(build: Build, event: Event) -> None:
    """Mutate a Build's denormalised ``latest_*`` columns for a new event.

    The build-level counterpart of :func:`_apply_event_to_task`, and the sole
    definition of what a build's status is. It implements the same rules the
    historical ``get_build_status`` event replay did, applied incrementally:

      - BUILD_STARTED / BUILD_RESUMED assert RUNNING. A start records
        ``latest_started_at``; a resume deliberately does not, because the
        build started when it first started, and instead clears
        ``latest_completed_at`` so the UI stops showing a stale "completed
        at" from the terminal it superseded.

        Note this differs from ``_apply_event_to_task``, where a start only
        fills ``latest_started_at`` if it is unset. A build emits exactly one
        BUILD_STARTED (at creation), so the two rules can only diverge on a
        hand-inserted event stream — and there, matching what the replay this
        replaces would have said is the right default for a refactor.
      - The four terminals (COMPLETED / FAILED / CANCELLED / EXIT_EARLY) set
        the status and ``latest_completed_at``, and record the triggering
        user when the event carries one.
      - ``latest_is_resumed`` is true only immediately after a BUILD_RESUMED;
        anything else — including a later BUILD_STARTED — clears it.
      - Task-level events are no-ops.

    **There is no stickiness here, and that is the difference from tasks.**
    A completed task cannot un-complete: COMPLETED means the target exists.
    A build genuinely can go COMPLETED → RUNNING, because
    ``sd.build(resume_build_id=...)`` picking a finished build back up is a
    supported operation, so the fold is plain last-event-wins.

    **Tie-break.** "Last" means *arrival* order — the order the server
    committed the events — not ``created_at`` order. That is not a choice so
    much as a consequence of folding at write time: the event insert and this
    mutation share a transaction, so two events are strictly ordered by their
    commits even when their timestamps collide. It also resolves a
    disagreement the two previous implementations had: the replay ordered by
    ``created_at`` and broke ties arbitrarily (whatever the index returned),
    while the reaper's SQL predicate read a tie as *not* running. Both were
    guessing, because equal timestamps mean the proxy lost the information
    the arrival order still has.

    The reaper is unaffected in practice: it can only act on a build whose
    column says RUNNING *and* which has then been silent for at least
    ``idle_for_seconds`` (floor: 60 s). A build whose last committed
    lifecycle event was a start or resume is running by every available
    reading, and a build that is running and has since gone quiet for the
    idle window is exactly what the reaper exists to find.

    The caller is responsible for persisting the Build — this function only
    mutates the in-memory ORM instance — and for holding the row lock that
    makes the read-modify-write safe (see ``_get_build_for_update``).
    """
    et = event.event_type
    metadata = event.event_metadata or {}

    if et == EventType.BUILD_STARTED:
        build.latest_status = BuildStatus.RUNNING
        build.latest_started_at = event.created_at
        build.latest_status_triggered_by_user_id = None  # never user-triggered
        # Clear the flag rather than leave it: the SDK never emits
        # BUILD_STARTED after a BUILD_RESUMED, but the fold shouldn't depend
        # on that — a manual/admin event insert could produce any sequence,
        # and "resumed" must keep meaning "the event that put this build in
        # its current status was a resume".
        build.latest_is_resumed = False
    elif et == EventType.BUILD_RESUMED:
        build.latest_status = BuildStatus.RUNNING
        build.latest_completed_at = None
        build.latest_status_triggered_by_user_id = None
        build.latest_is_resumed = True
    elif et in _TERMINAL_BUILD_STATUS:
        build.latest_status = _TERMINAL_BUILD_STATUS[et]
        build.latest_completed_at = event.created_at
        build.latest_is_resumed = False
        build.latest_status_triggered_by_user_id = (
            metadata.get("triggered_by_user_id")
            if et in _USER_TRIGGERABLE_BUILD_EVENTS
            else None
        )
    # Task-level events never affect build status.


async def get_task_status_in_build(
    db: AsyncSession, build_id: UUID, task_db_id: UUID
) -> tuple[TaskStatus, datetime | None, datetime | None, str | None, int]:
    """Get derived task status from events for a specific build.

    Returns:
        Tuple of (status, started_at, completed_at, error_message,
        attempt_count)

    ``attempt_count`` rides along on the replay this already does, so every
    task-event response can carry the authoritative post-event count
    without a second round-trip for a caller deciding whether to retry. It
    applies the same rule as :func:`get_attempt_counts_in_build`, via the
    same :func:`starts_new_attempt` predicate and the same round window.

    The window cannot be pushed into the query the way it is for the
    grouped path: the *status* fold has to see the whole stream (a task
    completed before a resume is still completed), so only the attempt
    tally is windowed. That costs one small indexed lookup for the round
    cutoff.
    """
    # The build's current round (see get_attempt_counts_in_build). Fetched
    # rather than filtered on, because the status fold below needs every
    # event regardless of round.
    resumed_at = await db.scalar(last_build_resumed_at(build_id))
    round_start = _as_utc(resumed_at) if resumed_at is not None else None

    result = await db.execute(
        select(Event)
        .where(Event.build_id == build_id)
        .where(Event.task_id == task_db_id)
        # id (UUID7) breaks created_at ties, matching the SQL attempt count.
        # Without it, two same-timestamp events replay in whatever order the
        # index returned, which the attempt rule is sensitive to.
        .order_by(Event.created_at.desc(), Event.id.desc())
    )
    events = result.scalars().all()

    status = TaskStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    attempt_count = 0
    prev_ordering_type: str | None = None

    # Process events from oldest to newest to build final state
    for event in reversed(events):
        # Pre-resume events are skipped for the tally *without* updating
        # prev_ordering_type, so the first start of a new round still sees
        # no predecessor and counts — mirroring the SQL path, where the
        # cutoff is applied before LAG rather than after it.
        if event.event_type in _ATTEMPT_ORDERING_EVENT_TYPES and (
            round_start is None or _as_utc(event.created_at) >= round_start
        ):
            if starts_new_attempt(event.event_type, prev_ordering_type):
                attempt_count += 1
            prev_ordering_type = event.event_type
        if event.event_type == EventType.TASK_PENDING:
            status = TaskStatus.PENDING
        elif event.event_type == EventType.TASK_REFERENCED:
            # Informational: task already existed, stays PENDING
            pass
        elif event.event_type == EventType.TASK_STARTED:
            status = TaskStatus.RUNNING
            started_at = event.created_at
        elif event.event_type == EventType.TASK_SUSPENDED:
            status = TaskStatus.SUSPENDED
        elif event.event_type == EventType.TASK_RESUMED:
            status = TaskStatus.RUNNING
        elif event.event_type == EventType.TASK_RETRIED:
            # Retry: reset a retryable status back to PENDING so the task is
            # schedulable again (a re-trigger of a failed build, a new build
            # referencing a previously-failed task, or an abandoned
            # suspension). No-op for completed/running — a retry never
            # downgrades those. See _RETRYABLE_STATUSES.
            if status in _RETRYABLE_STATUSES:
                status = TaskStatus.PENDING
                completed_at = None
                error_message = None
        elif event.event_type == EventType.TASK_WAITING_FOR_LOCK:
            # Informational: blocked by global lock, stays PENDING
            pass
        elif event.event_type == EventType.TASK_COMPLETED:
            status = TaskStatus.COMPLETED
            completed_at = event.created_at
        elif event.event_type == EventType.TASK_FAILED:
            status = TaskStatus.FAILED
            completed_at = event.created_at
            error_message = event.error_message
        elif event.event_type == EventType.TASK_INTERRUPTED:
            # Not an ending, so completed_at is deliberately untouched —
            # mirrors _apply_event_to_task, including the unconditional
            # error_message write (a stale one would explain this
            # interruption with an earlier failure's text).
            status = TaskStatus.INTERRUPTED
            error_message = event.error_message
        elif event.event_type == EventType.TASK_SKIPPED:
            status = TaskStatus.SKIPPED
            completed_at = event.created_at
        elif event.event_type == EventType.TASK_CANCELLED:
            status = TaskStatus.CANCELLED
            completed_at = event.created_at

    return status, started_at, completed_at, error_message, attempt_count


async def get_all_task_statuses_in_build(
    db: AsyncSession, build_id: UUID
) -> dict[UUID, tuple[TaskStatus, datetime | None, datetime | None, str | None]]:
    """Get derived status for all tasks in a build.

    Returns:
        Dict mapping task_db_id to (status, started_at, completed_at, error_message)
    """
    result = await db.execute(
        select(Event)
        .where(Event.build_id == build_id)
        .where(Event.task_id.isnot(None))
        .order_by(Event.created_at.asc())
    )
    events = result.scalars().all()

    # Build status for each task
    statuses: dict[
        UUID, tuple[TaskStatus, datetime | None, datetime | None, str | None]
    ] = {}

    for event in events:
        if event.task_id is None:
            continue

        task_id = event.task_id
        current = statuses.get(task_id, (TaskStatus.PENDING, None, None, None))
        status, started_at, completed_at, error_message = current

        if event.event_type == EventType.TASK_PENDING:
            status = TaskStatus.PENDING
        elif event.event_type == EventType.TASK_REFERENCED:
            # Informational: task already existed, stays PENDING
            pass
        elif event.event_type == EventType.TASK_STARTED:
            status = TaskStatus.RUNNING
            started_at = event.created_at
        elif event.event_type == EventType.TASK_SUSPENDED:
            status = TaskStatus.SUSPENDED
        elif event.event_type == EventType.TASK_RESUMED:
            status = TaskStatus.RUNNING
        elif event.event_type == EventType.TASK_RETRIED:
            # Retry: reset a retryable status back to PENDING so the task is
            # schedulable again (a re-trigger of a failed build, a new build
            # referencing a previously-failed task, or an abandoned
            # suspension). No-op for completed/running — a retry never
            # downgrades those. See _RETRYABLE_STATUSES.
            if status in _RETRYABLE_STATUSES:
                status = TaskStatus.PENDING
                completed_at = None
                error_message = None
        elif event.event_type == EventType.TASK_WAITING_FOR_LOCK:
            # Informational: blocked by global lock, stays PENDING
            pass
        elif event.event_type == EventType.TASK_COMPLETED:
            status = TaskStatus.COMPLETED
            completed_at = event.created_at
        elif event.event_type == EventType.TASK_FAILED:
            status = TaskStatus.FAILED
            completed_at = event.created_at
            error_message = event.error_message
        elif event.event_type == EventType.TASK_INTERRUPTED:
            # Not an ending, so completed_at is deliberately untouched —
            # mirrors _apply_event_to_task, including the unconditional
            # error_message write (a stale one would explain this
            # interruption with an earlier failure's text).
            status = TaskStatus.INTERRUPTED
            error_message = event.error_message
        elif event.event_type == EventType.TASK_SKIPPED:
            status = TaskStatus.SKIPPED
            completed_at = event.created_at
        elif event.event_type == EventType.TASK_CANCELLED:
            status = TaskStatus.CANCELLED
            completed_at = event.created_at

        statuses[task_id] = (status, started_at, completed_at, error_message)

    return statuses


async def get_task_global_status(
    db: AsyncSession, task_db_id: UUID
) -> tuple[TaskStatus, datetime | None, datetime | None, str | None, UUID | None]:
    """Get task status considering events from ALL builds.

    Reads the denormalised ``latest_*`` columns on tasks (maintained
    in-transaction by ``transition_task`` whenever a task event is
    created). Falls back to PENDING for tasks that don't exist.

    Returns:
        Tuple of (status, started_at, completed_at, error_message, completed_in_build_id)
    """
    task = await db.get(Task, task_db_id)
    if task is None:
        return TaskStatus.PENDING, None, None, None, None

    # The "completed_in_build_id" semantic was: the build where TASK_COMPLETED
    # fired. With denormalised columns we use latest_status_build_id when the
    # status is a terminal one (matches existing UI use, since the field is
    # only consulted for non-pending statuses).
    completed_in_build_id = (
        task.latest_status_build_id
        if task.latest_status
        in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.SKIPPED,
        )
        else None
    )

    return (
        task.latest_status,
        task.latest_started_at,
        task.latest_completed_at,
        task.latest_error_message,
        completed_in_build_id,
    )


async def get_all_task_global_statuses(
    db: AsyncSession, task_db_ids: list[UUID]
) -> dict[
    UUID,
    tuple[
        TaskStatus,
        datetime | None,
        datetime | None,
        str | None,
        UUID | None,
        bool,
        str | None,
    ],
]:
    """Get global status for multiple tasks considering events from ALL builds.

    Reads the denormalised ``latest_*`` columns on tasks. Tasks not present
    in the DB default to PENDING with no metadata.

    Returns:
        Dict mapping task_db_id to:
        (status, started_at, completed_at, error_message, status_build_id,
         waiting_for_lock, commit_hash)
    """
    if not task_db_ids:
        return {}

    result = await db.execute(
        select(
            Task.id,
            Task.latest_status,
            Task.latest_started_at,
            Task.latest_completed_at,
            Task.latest_error_message,
            Task.latest_status_build_id,
            Task.latest_waiting_for_lock,
            Task.latest_commit_hash,
        ).where(Task.id.in_(task_db_ids))
    )

    found: dict[
        UUID,
        tuple[
            TaskStatus,
            datetime | None,
            datetime | None,
            str | None,
            UUID | None,
            bool,
            str | None,
        ],
    ] = {}
    for (
        task_id,
        latest_status,
        latest_started_at,
        latest_completed_at,
        latest_error_message,
        latest_status_build_id,
        latest_waiting_for_lock,
        latest_commit_hash,
    ) in result.all():
        # latest_status comes back as the string value because the column is
        # String(32). Coerce back to the enum so callers see a TaskStatus.
        status = (
            latest_status
            if isinstance(latest_status, TaskStatus)
            else TaskStatus(latest_status)
        )
        found[task_id] = (
            status,
            latest_started_at,
            latest_completed_at,
            latest_error_message,
            latest_status_build_id,
            bool(latest_waiting_for_lock),
            latest_commit_hash,
        )

    # Preserve the contract of the prior implementation: every requested
    # task_db_id appears in the result dict, with a default for missing rows.
    return {
        task_id: found.get(
            task_id, (TaskStatus.PENDING, None, None, None, None, False, None)
        )
        for task_id in task_db_ids
    }
