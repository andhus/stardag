"""Build management routes - primary interface for SDK."""

import json
import logging
from datetime import datetime, timedelta
from typing import Annotated, Any, Mapping, Sequence, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete, func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from stardag_api.auth import (
    SdkAuth,
    require_sdk_auth,
)
from stardag_api.config import (
    MAX_CLAIM_TTL_SECONDS,
    MIN_CLAIM_TTL_SECONDS,
    limits_settings,
)
from stardag_api.db import get_db
from stardag_api.limits import (
    ErrorCode,
    LimitExceededError,
    check_entity_creation_limit,
    check_payload_size,
    check_rate_limit,
    check_structural_limit,
    record_entity_created,
)
from stardag_api.models import (
    Build,
    BuildStatus,
    EnvironmentConcurrencyLimit,
    Event,
    EventType,
    Task,
    TaskDependency,
    TaskArtifact,
    TaskLimitKey,
    TaskStatus,
    User,
    WorkspaceRole,
)
from stardag_api.models.base import generate_uuid7, utc_now
from stardag_api.routes.workspaces import require_workspace_access
from stardag_api.schemas import (
    AddBuildRootsRequest,
    SkipBlockedResponse,
    AddDependenciesRequest,
    AddDependenciesResponse,
    BuildCancelResponse,
    BuildCreate,
    BuildExecutionRef,
    BuildExecutionsResponse,
    BuildFrontierResponse,
    BuildListResponse,
    BuildNotifyResponse,
    SchedulerLeaseResponse,
    BuildResponse,
    BulkCancelBuildsRequest,
    BulkCancelBuildsResponse,
    CancelledBuildRef,
    FrontierExternalBlocker,
    FrontierTaskRef,
    EventResponse,
    SetReactiveMetaRequest,
    StatusTriggeredByUser,
    BulkTaskIdRef,
    TaskBulkCreate,
    TaskBulkIdOnlyResponse,
    TaskBulkResponse,
    TaskCreate,
    TaskEventResponse,
    TaskGraphExtendedResponse,
    TaskArtifactCreate,
    TaskArtifactListResponse,
    TaskArtifactResponse,
    TaskResponse,
    TaskWithStatusResponse,
    WakeCandidate,
    WakeCandidatesResponse,
)
from stardag_api.services import generate_build_slug
from stardag_api.services.build_cleanup import (
    CASCADE_CANCEL_STATUSES,
    STALEST_FIRST_ORDER,
    cancel_builds,
    cascade_cancel_build_tasks,
    idle_filters,
    last_activity_at,
    select_cancellable_builds,
)
from stardag_api.services.claims import (
    claim_is_live,
    live_claim_filter,
    may_revoke,
)
from stardag_api.services.wakeups import (
    MAX_WAKE_CANDIDATES,
    MAX_LEASE_TTL_SECONDS,
    MIN_LEASE_TTL_SECONDS,
    acquire_scheduler_lease,
    flag_build,
    lease_is_live,
    mark_tick_requested,
    release_scheduler_lease,
    renew_scheduler_lease,
    select_wake_candidates,
)
from stardag_api.services.status import (
    apply_event_to_build,
    transition_task,
    get_all_task_global_statuses,
    get_attempt_counts_in_build,
    get_interrupt_counts_in_build,
    get_task_status_in_build,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/builds", tags=["builds"])


def _raise_if_limit_exceeded(error: LimitExceededError | None) -> None:
    """Raise HTTP 429 if a limit check returned an error."""
    if error is None:
        return
    headers = {}
    if error.retry_after is not None:
        headers["Retry-After"] = str(error.retry_after)
    raise HTTPException(
        status_code=429,
        detail=error.model_dump(exclude_none=True),
        headers=headers or None,
    )


# --- Helpers ---


# Size cap on the executor_metadata dict (compact-JSON byte size). It holds
# executor identity fields only, and the blob is echoed on every task
# list/search/frontier row — a small cap keeps abuse/mistakes from bloating
# every read. Enforced consistently on the query-param paths (task start,
# build resume) and the build-create body path.
_MAX_EXECUTOR_METADATA_BYTES = 2048

# Task statuses the build frontier considers still in play: neither
# COMPLETED nor a failure terminal, so the build either can act on them or
# is waiting for someone who can.
_FRONTIER_NON_TERMINAL_STATUSES = (
    TaskStatus.PENDING,
    TaskStatus.SUSPENDED,
    TaskStatus.RUNNING,
    # An interrupted task is the scheduler's to start again — the platform
    # ended the execution, the task did nothing wrong, and nothing else
    # will pick it up. Listed here for the same reason SUSPENDED is: leave
    # it out and the build looks finished while a task still needs running.
    TaskStatus.INTERRUPTED,
)

# Cap on BuildFrontierResponse.blocked_by_external. A wide DAG stalled
# behind another build can produce one entry per blocked edge; the list is
# a diagnostic ("you are waiting, and here is on what"), so a bounded
# sample plus the truncation flag carries the same signal at a fixed
# payload size. Deliberately not silent — see blocked_by_external_truncated.
_MAX_FRONTIER_EXTERNAL_BLOCKERS = 50


def _validate_executor_metadata_size(metadata: dict) -> None:
    """Raise 422 when the metadata exceeds ``_MAX_EXECUTOR_METADATA_BYTES``."""
    encoded = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_EXECUTOR_METADATA_BYTES:
        raise HTTPException(
            status_code=422,
            detail=(
                f"executor_metadata must be at most "
                f"{_MAX_EXECUTOR_METADATA_BYTES} bytes as compact JSON "
                f"(got {len(encoded)})"
            ),
        )


def _parse_executor_metadata_param(raw: str | None) -> dict | None:
    """Parse the JSON-encoded ``executor_metadata`` query param.

    The task-start and build-resume endpoints have no request body, so the
    metadata dict rides as a JSON string query param (small — executor
    identity fields only; size-capped).
    """
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=422, detail="executor_metadata must be valid JSON"
        )
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=422, detail="executor_metadata must be a JSON object"
        )
    _validate_executor_metadata_size(parsed)
    return parsed


async def _touch_build_last_active(db: AsyncSession, build_id: UUID) -> None:
    """Bump ``Build.last_active_at`` to now in the current transaction.

    Called only from the five build-level lifecycle endpoints
    (``/resume``, ``/complete``, ``/fail``, ``/cancel``, ``/exit-early``)
    — task events deliberately do NOT call this. Touching on every task
    event would issue an ``UPDATE builds`` against the same row from
    every concurrent task worker, serialising on a row-level exclusive
    lock and bloating ``builds`` with MVCC versions. Confining the touch
    to lifecycle events caps it at ≤ 5 UPDATEs per build lifetime, with
    no contention.

    The caller is expected to commit the surrounding transaction; we
    don't commit here.
    """
    await db.execute(
        update(Build).where(Build.id == build_id).values(last_active_at=utc_now())
    )


async def _last_event_at(db: AsyncSession, build_id: UUID) -> datetime | None:
    """``max(events.created_at)`` for one build — the activity half of
    :func:`stardag_api.services.build_cleanup.last_activity_at`.

    One row off ``ix_events_build_created`` (backward index scan). Batched
    by :func:`_last_event_at_map` when assembling a list of builds.
    """
    return (
        await db.execute(
            select(func.max(Event.created_at)).where(Event.build_id == build_id)
        )
    ).scalar_one_or_none()


async def _last_event_at_map(
    db: AsyncSession, build_ids: list[UUID]
) -> dict[UUID, datetime]:
    """Batched :func:`_last_event_at` — one grouped query for a whole page."""
    if not build_ids:
        return {}
    rows = (
        await db.execute(
            select(Event.build_id, func.max(Event.created_at))
            .where(Event.build_id.in_(build_ids))
            .group_by(Event.build_id)
        )
    ).all()
    return {build_id: ts for build_id, ts in rows}


# Build-level event types that carry a failure reason worth reporting on the
# build itself. BUILD_FAILED is the one that matters; listing it rather than
# "any event with an error_message" keeps a task-level error from being
# promoted to the build, which would attribute one task's failure to the whole
# build.
_BUILD_ERROR_EVENT_TYPES = (EventType.BUILD_FAILED,)


def _latest_build_error_query(build_ids: list[UUID]):
    """Newest error-carrying build-level event per build, as a subquery-free join.

    Deliberately *not* a denormalised ``Build.latest_error_message`` column,
    even though ``Task`` has one and the status columns next to it were
    denormalised precisely to stop replaying events. Two reasons: a column
    needs a migration and a backfill, and this read is not on a hot path —
    ``GET /builds`` is a human-facing listing, not the frontier a scheduler
    polls every few seconds. If it ever shows up in a profile, the column is
    the answer and this helper is where to delete.
    """
    ranked = (
        select(
            Event.build_id.label("build_id"),
            Event.error_message.label("error_message"),
            func.row_number()
            .over(
                partition_by=Event.build_id,
                # `id` breaks the tie: two BUILD_FAILED events can share a
                # timestamp (same transaction, or coarse clock resolution), and
                # "latest" has to be a single well-defined row rather than
                # whichever the planner happens to emit first.
                order_by=(Event.created_at.desc(), Event.id.desc()),
            )
            .label("rn"),
        )
        .where(
            Event.build_id.in_(build_ids),
            Event.task_id.is_(None),
            Event.event_type.in_(_BUILD_ERROR_EVENT_TYPES),
            Event.error_message.is_not(None),
            # A blank reason is not a reason. Excluded here rather than
            # filtered by each consumer, so "no reason recorded" has exactly
            # one representation (None) everywhere downstream — otherwise a
            # CLI or UI that renders on truthiness and one that renders on
            # `is not None` disagree about the same build.
            Event.error_message != "",
        )
        .subquery()
    )
    return select(ranked.c.build_id, ranked.c.error_message).where(ranked.c.rn == 1)


async def _latest_build_error_map(
    db: AsyncSession, build_ids: list[UUID]
) -> dict[UUID, str]:
    """One grouped query for a whole page's failure reasons."""
    if not build_ids:
        return {}
    rows = (await db.execute(_latest_build_error_query(build_ids))).all()
    return {build_id: message for build_id, message in rows}


async def _build_to_response(
    db: AsyncSession,
    build: Build,
    last_event_at: datetime | None = None,
    latest_error_messages: Mapping[UUID, str | None] | None = None,
) -> BuildResponse:
    """Assemble a BuildResponse from a build row.

    Status and its timestamps come straight off the denormalised
    ``latest_*`` columns, maintained in-transaction by
    :func:`~stardag_api.services.status.apply_event_to_build`. This used to
    replay the build's whole build-level event stream on every response.

    ``last_event_at`` short-circuits the per-build activity lookup when the
    caller already fetched it in bulk. Passing None for a build that has
    events simply costs one extra index lookup, never a wrong answer.

    ``latest_error_messages`` is a *mapping* rather than a value for one
    build, because the two states a value cannot distinguish are exactly the
    ones that matter: "the caller has not looked this up" and "the caller
    looked and there is none". With a scalar, a FAILED build whose reason is
    absent — which happens, `POST /fail` takes no message — looked identical to
    an unfetched one and re-queried per build, reintroducing the N+1 the batch
    exists to avoid. A supplied mapping means "already resolved, do not ask
    again", even for ids it does not contain.

    Absent the mapping, the lookup happens here, gated on the build actually
    being FAILED — so the ten single-build callers need no changes and pay
    nothing for a build that cannot have a reason.

    The gate is also what the field *means*: the reason is reported while the
    build is failed, and not afterwards. A build cancelled after failing reads
    as cancelled, and pairing a current status with a previous status's reason
    would be worse than reporting none.
    """
    triggered_by_user = await _get_triggered_by_user(
        db, build.latest_status_triggered_by_user_id
    )
    if last_event_at is None:
        last_event_at = await _last_event_at(db, build.id)
    resolved_errors: Mapping[UUID, str | None] = (
        latest_error_messages
        if latest_error_messages is not None
        else (
            await _latest_build_error_map(db, [build.id])
            if build.latest_status == BuildStatus.FAILED
            else {}
        )
    )
    return BuildResponse(
        id=build.id,
        environment_id=build.environment_id,
        user_id=build.user_id,
        name=build.name,
        description=build.description,
        commit_hash=build.commit_hash,
        root_task_ids=build.root_task_ids,
        created_at=build.created_at,
        executor_metadata=build.executor_metadata,
        reactive_app_name=build.reactive_app_name,
        reactive_tick_kwargs=build.reactive_tick_kwargs,
        status=build.latest_status,
        started_at=build.latest_started_at,
        completed_at=build.latest_completed_at,
        status_triggered_by_user=triggered_by_user,
        is_resumed=build.latest_is_resumed,
        last_active_at=build.last_active_at,
        last_activity_at=last_activity_at(build, last_event_at),
        latest_error_message=resolved_errors.get(build.id),
    )


async def _get_triggered_by_user(
    db: AsyncSession, external_id: str | None
) -> StatusTriggeredByUser | None:
    """Look up user by external_id and return StatusTriggeredByUser or None."""
    if not external_id:
        return None

    result = await db.execute(select(User).where(User.external_id == external_id))
    user = result.scalar_one_or_none()
    if not user:
        return None

    return StatusTriggeredByUser(
        id=user.external_id,
        email=user.email or "",
        display_name=user.display_name,
    )


async def _get_build_for_update(
    build_id: UUID, db: AsyncSession, auth: SdkAuth
) -> Build:
    """Fetch a build for a lifecycle transition, with its row locked.

    ``SELECT ... FOR UPDATE``, for the same reason ``_get_build_and_task``
    takes it on the task row: writing the denormalised status columns is a
    read-modify-write (see ``services.status.apply_event_to_build``), and
    without the lock two concurrent lifecycle calls on the same build can
    each read the pre-state, apply different events, and leave a row that is
    a mixture of the two — e.g. a COMPLETED build still carrying the
    ``is_resumed`` flag and the NULL ``completed_at`` of a racing resume.
    Every path that folds an event must go through here; the lock releases on
    commit, so request duration governs hold time. On SQLite the clause is
    silently dropped — fine, the test suite runs single-connection.

    A build is always locked *before* any task rows it cascades to, so this
    and ``_create_task_event`` acquire in one consistent order.

    The scheduler-lease routes take it for the same reason: acquire, renew
    and release are each a read-modify-write on the lease columns, and two
    ticks racing to take one build's lease have to serialize somewhere.

    Read-only paths (``GET /builds``, ``/{id}``, the frontier) deliberately
    don't take it — they just read the columns.
    """
    build = (
        await db.execute(select(Build).where(Build.id == build_id).with_for_update())
    ).scalar_one_or_none()
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )
    return build


async def _record_build_event(db: AsyncSession, build: Build, event: Event) -> None:
    """Add a build-level event and fold it into the build's status columns.

    The two halves are one operation and must stay in one transaction — an
    event without its fold silently desynchronises the row, and every read
    (and the reaper's selection) trusts the row. The flush is what populates
    ``event.created_at`` before the fold reads it.
    """
    db.add(event)
    await db.flush()
    apply_event_to_build(build, event)


async def _get_build_and_task(
    build_id: UUID,
    task_id: str,
    db: AsyncSession,
    auth: SdkAuth,
    *,
    for_update: bool = False,
) -> tuple[Build, Task]:
    """Get build and task, verifying ownership. Raises HTTPException on errors.

    When ``for_update=True`` issues ``SELECT ... FOR UPDATE`` on the task row
    so concurrent event-creating handlers serialise on the same task. Required
    by anything that does a read-modify-write on the denormalised
    ``Task.latest_*`` columns — without it two concurrent writers can each
    read PENDING, apply different events, and the last-committer wins
    regardless of the priority logic in ``_apply_event_to_task`` (e.g. a
    concurrent TASK_STARTED could clobber a TASK_COMPLETED). The lock is
    released on transaction commit, so request duration governs hold time.
    On SQLite the FOR UPDATE clause is silently dropped — fine, since the
    test suite runs single-connection.
    """
    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    stmt = (
        select(Task)
        .where(Task.environment_id == build.environment_id)
        .where(Task.task_id == task_id)
    )
    if for_update:
        stmt = stmt.with_for_update()
    result = await db.execute(stmt)
    db_task = result.scalar_one_or_none()
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found")

    return build, db_task


async def _replace_limit_keys(
    db: AsyncSession, keys_by_task_pk: Mapping[UUID, Sequence[str]]
) -> None:
    """Make each task's ``TaskLimitKey`` rows exactly the keys given.

    Shared by the start path (keys the task is started under) and plan-time
    registration (keys a pending task will want). ON CONFLICT DO NOTHING:
    two concurrent starts for the same task can both pass the delete and
    race the inserts (only reachable when the scheduler lease is bypassed
    or via manual API use) — a duplicate key is then a benign no-op instead
    of a 500.
    """
    if not keys_by_task_pk:
        return
    await db.execute(
        delete(TaskLimitKey).where(TaskLimitKey.task_pk.in_(list(keys_by_task_pk)))
    )
    rows = [
        {"id": generate_uuid7(), "task_pk": task_pk, "key": key}
        for task_pk, keys in keys_by_task_pk.items()
        for key in dict.fromkeys(keys)
    ]
    if not rows:
        return
    insert_stmt = (
        sqlite_insert(TaskLimitKey)
        if db.bind is not None and db.bind.dialect.name == "sqlite"
        else pg_insert(TaskLimitKey)
    )
    await db.execute(insert_stmt.values(rows).on_conflict_do_nothing())


async def _create_task_event(
    build_id: UUID,
    task_id: str,
    event_type: EventType,
    db: AsyncSession,
    auth: SdkAuth,
    error_message: str | None = None,
    commit_hash: str | None = None,
    extra_metadata: dict | None = None,
    limit_keys: list[str] | None = None,
    claim: bool = False,
    only_if_held: bool = False,
) -> TaskEventResponse:
    """Create a task event and return slim response.

    ``limit_keys`` (TASK_STARTED only): replaces the task's recorded
    concurrency-limit keys in the same transaction — a RUNNING task with a
    key row occupies one slot of that key's limit.

    ``claim`` (TASK_STARTED only): deny with 409 when the task holds a
    *live* claim or is already COMPLETED (see ``start_task``); evaluated on
    the FOR-UPDATE-locked task row, and the raised HTTPException rolls back
    the whole transaction (no event, no limit-key rows, and any
    limit-row locks taken by the enforce_limits pre-check are released).

    ``only_if_held`` (TASK_CANCELLED only): record nothing unless this build
    still holds the task in a status that has an execution to revoke. See
    :func:`cancel_task`.
    """
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    # Lock the task row so the transition can safely do a
    # read-modify-write on the denormalised latest_* columns. Without the
    # lock, two concurrent event-creators racing on the same task could
    # both observe PENDING, apply different events (e.g. STARTED in one,
    # COMPLETED in the other), and the last committer wins regardless of
    # COMPLETED-stickiness.
    _, db_task = await _get_build_and_task(build_id, task_id, db, auth, for_update=True)

    if claim and event_type == EventType.TASK_STARTED:
        # Atomic execution claim: at most one concurrent claiming start can
        # win. The row is locked FOR UPDATE, so a racing claimant blocks
        # here and re-reads the committed RUNNING status once we commit.
        #
        # RUNNING alone is not the test — the claim also has to still be
        # believable. A claim past its expiry is not a claim, so it does not
        # deny anything: the start below overwrites the dead holder's
        # status, build, executor fields and expiry in one go. That
        # re-claim IS the healing mechanism (see services.claims); nothing
        # has to release the old claim first, and there is nothing to
        # release it *with* across builds.
        if claim_is_live(db_task):
            raise HTTPException(
                status_code=409,
                detail={
                    "error_code": "task_already_running",
                    "executor": db_task.latest_executor,
                    "executor_ref": db_task.latest_executor_ref,
                    "latest_status_at": (
                        db_task.latest_status_at.isoformat()
                        if db_task.latest_status_at
                        else None
                    ),
                    # When the denial stops applying without anyone doing
                    # anything. Null = never: a claim nothing can date, so
                    # only an operator releases it (the claims already
                    # RUNNING when the column shipped were backfilled).
                    "latest_status_expires_at": (
                        db_task.latest_status_expires_at.isoformat()
                        if db_task.latest_status_expires_at
                        else None
                    ),
                },
            )
        if db_task.latest_status == TaskStatus.COMPLETED:
            raise HTTPException(
                status_code=409,
                detail={"error_code": "task_already_completed"},
            )

    if event_type == EventType.TASK_CANCELLED and only_if_held:
        # Evaluated on the FOR-UPDATE-locked row, which is the whole point:
        # a cleanup pass decides what to cancel from a listing it read a
        # moment ago, and in that moment another build can have reset the
        # task to PENDING and be about to run it. Writing CANCELLED then
        # would take no claim — PENDING holds none — but it would still
        # stamp a neighbour's freshly scheduled task dead and send it round
        # the reset loop again, which is the class of damage the caller is
        # cleaning up after.
        #
        # A no-op rather than a 409: the caller is doing best-effort
        # cleanup over a list, and "somebody else moved it on" is a normal
        # outcome, not an error to log per task.
        held = (
            db_task.latest_status in (TaskStatus.RUNNING, TaskStatus.INTERRUPTED)
            and db_task.latest_status_build_id == build_id
        )
        if not held:
            status, _, _, _, attempt_count = await get_task_status_in_build(
                db, build_id, db_task.id
            )
            return TaskEventResponse(
                task_id=db_task.task_id,
                status=status,
                latest_status=db_task.latest_status,
                attempt_count=attempt_count,
            )

    if event_type == EventType.TASK_CANCELLED and not may_revoke(db_task, build_id):
        # Authority to revoke is build-scoped. Evaluated on the same
        # FOR-UPDATE-locked row as the claim above, so the answer cannot go
        # stale between the check and the event: a caller that held the
        # claim a moment ago and lost it to an expiry takeover is refused
        # here rather than silently releasing the new holder's claim.
        raise HTTPException(
            status_code=409,
            detail={
                "error_code": "not_claim_holder",
                "latest_status": db_task.latest_status,
                "latest_status_build_id": (
                    str(db_task.latest_status_build_id)
                    if db_task.latest_status_build_id
                    else None
                ),
            },
        )

    # Build event_metadata from commit_hash and any extra metadata
    event_metadata: dict | None = None
    if commit_hash or extra_metadata:
        event_metadata = {}
        if commit_hash:
            event_metadata["commit_hash"] = commit_hash
        if extra_metadata:
            event_metadata.update(extra_metadata)

    event = Event(
        build_id=build_id,
        task_id=db_task.id,
        event_type=event_type,
        error_message=error_message,
        event_metadata=event_metadata,
    )
    await transition_task(db, db_task, event)
    if limit_keys is not None:
        # Replace the task's limit-key rows (only when explicitly provided —
        # a later ref-recording re-start without keys must not clear them).
        await _replace_limit_keys(db, {db_task.id: limit_keys})
    await db.commit()

    record_entity_created(auth.workspace_id, "events")

    # The attempt count comes out of the replay this call already performs,
    # so every task-event response carries it at no extra query cost — see
    # TaskEventResponse.attempt_count for why it is worth carrying.
    status, _, _, _, attempt_count = await get_task_status_in_build(
        db, build_id, db_task.id
    )

    return TaskEventResponse(
        task_id=db_task.task_id,
        status=status,
        latest_status=db_task.latest_status,
        attempt_count=attempt_count,
    )


# --- Build CRUD ---


@router.post("", response_model=BuildResponse, status_code=201)
async def create_build(
    build: BuildCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Create a new build.

    This is the entry point for SDK - creates a new build and returns its ID.
    Requires API key authentication (recommended) or JWT token with environment_id.
    The environment is determined from the authentication context.
    """
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "builds", limits_settings
        )
    )
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    if build.executor_metadata is not None:
        # Same cap as the query-param paths (task start / build resume).
        _validate_executor_metadata_size(build.executor_metadata)

    # Generate memorable slug
    name = generate_build_slug()

    # Use environment from auth context (API key determines environment)
    db_build = Build(
        environment_id=auth.environment_id,
        user_id=auth.user.id if auth.user else None,
        name=name,
        description=build.description,
        commit_hash=build.commit_hash,
        root_task_ids=build.root_task_ids,
        executor_metadata=build.executor_metadata,
    )
    db.add(db_build)
    await db.flush()  # Get the build ID

    # Create BUILD_STARTED event, and fold it — this is what puts the fresh
    # build in RUNNING with a started_at. No row lock: the build row is not
    # visible to any other transaction until this one commits.
    start_event = Event(
        build_id=db_build.id,
        task_id=None,
        event_type=EventType.BUILD_STARTED,
        event_metadata={"executor_metadata": build.executor_metadata}
        if build.executor_metadata is not None
        else None,
    )
    await _record_build_event(db, db_build, start_event)

    await db.commit()
    await db.refresh(db_build)

    record_entity_created(auth.workspace_id, "builds")
    record_entity_created(auth.workspace_id, "events")

    # Build response with derived status
    return await _build_to_response(db, db_build)


@router.get("", response_model=BuildListResponse)
async def list_builds(
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    reactive_app_name: Annotated[str | None, Query()] = None,
    status: Annotated[BuildStatus | None, Query()] = None,
    idle_for_seconds: Annotated[
        int | None,
        Query(
            ge=60,
            description=(
                "Only builds that are still RUNNING and have had no "
                "activity of any kind for at least this many seconds, "
                "measured on `last_activity_at`. Same definition and same "
                "floor as POST /builds/bulk-cancel, so the two cannot "
                "disagree about what is idle. Ordering switches to "
                "stalest-first."
            ),
        ),
    ] = None,
):
    """List builds in an environment.

    Requires authentication via API key or JWT token with environment_id.
    The environment is determined from the authentication context.

    Optional filters:

    - ``reactive_app_name``: only builds reactively scheduled by the named
      app (``reactive_app_name`` column). A server-side filter — the
      watchdog's real question is "RUNNING reactive builds owned by app X".
    - ``status``: only builds with the given status. A plain predicate on the
      denormalised ``builds.latest_status`` column, for **every** status, so
      ``total`` is an exact ``COUNT(*)`` and pagination is server-side and
      unbounded. (It used to derive status in Python over the 500
      most-recently-active candidates and report the matches within that
      window as ``total``.)
    - ``idle_for_seconds``: only builds that are **still running** and have
      been idle for at least that long. Also a real SQL predicate, on the
      same terms.

    Idleness implies RUNNING because that is the only state in which it
    means anything. A finished build has no activity by definition, so
    without the RUNNING predicate a staleness query returns every build
    that ever completed, ordered by how long ago — which is a history
    listing wearing a cleanup query's clothes. The word this filter exists
    to express is *abandoned*, and only a running build can be abandoned.
    Now that every status is a real predicate, that is a deliberate
    semantic choice rather than a limitation of what SQL could express.

    Idleness is the same definition the stale-build reaper uses
    (:func:`~stardag_api.services.build_cleanup.idle_filters`), deliberately
    shared rather than reimplemented: this endpoint is how an operator
    previews what ``POST /builds/bulk-cancel`` would do, and a preview that
    disagrees with the action is worse than no preview. In particular it is
    measured on ``last_activity_at`` — the newest of the build's whole event
    stream, its ``last_active_at`` and any pending scheduler wake-up — not on
    the ``last_active_at`` column alone, which task events never touch.

    **Ordering** is ``last_active_at`` *descending* (most recently active
    first, so a resumed build jumps to the top) — except with
    ``idle_for_seconds``, where it flips to stalest-first, since a capped
    page of a staleness query should contain the builds an operator actually
    wants to act on. See
    :data:`~stardag_api.services.build_cleanup.STALEST_FIRST_ORDER` for why
    the sort key is a proxy for the full signal.

    **Combining ``status`` with ``idle_for_seconds``:** only
    ``status=running`` is accepted, and it is redundant — the idle filter
    already implies it. Any other status is a contradiction rather than a
    narrower query, so it is rejected with **422** instead of being served
    the empty result the predicates would produce.
    """
    environment_id = auth.environment_id
    if idle_for_seconds is not None and status not in (None, BuildStatus.RUNNING):
        raise HTTPException(
            status_code=422,
            detail=(
                f"status={status.value!r} cannot be combined with "
                "idle_for_seconds: an idle filter already means 'still "
                "running, but nothing is happening'. A build that reached "
                f"{status.value!r} is finished, not idle. Drop "
                "idle_for_seconds to list builds by status, or drop status "
                "to find abandoned builds."
            ),
        )

    filters = [Build.environment_id == environment_id]
    if reactive_app_name is not None:
        filters.append(Build.reactive_app_name == reactive_app_name)
    if status is not None:
        filters.append(Build.latest_status == status)
    if idle_for_seconds is not None:
        filters.extend(idle_filters(utc_now() - timedelta(seconds=idle_for_seconds)))
        # Unconditionally, not just when status=running was asked for: this
        # endpoint is the preview for POST /builds/bulk-cancel, which only
        # ever acts on running builds (`select_cancellable_builds` opens with
        # this same predicate). A preview that lists rows the action will not
        # touch is worse than no preview.
        filters.append(Build.latest_status == BuildStatus.RUNNING)

    # Sort by last_active_at so a resumed build (BUILD_RESUMED touches this
    # column) jumps back to the top of the list. ``Build.id`` is a UUID7
    # (time-sortable) so it's a stable tiebreaker for builds that share a
    # timestamp — without it, paginating across a tie can yield duplicates
    # or skips. A staleness query wants the opposite end of the same
    # ordering.
    ordered = (
        select(Build)
        .where(*filters)
        .order_by(
            *(
                STALEST_FIRST_ORDER
                if idle_for_seconds is not None
                else (Build.last_active_at.desc(), Build.id.desc())
            )
        )
    )

    count_query = select(func.count()).select_from(Build).where(*filters)
    total = (await db.execute(count_query)).scalar() or 0
    result = await db.execute(ordered.offset((page - 1) * page_size).limit(page_size))
    page_builds = list(result.scalars().all())
    # One grouped query for the page's activity timestamps rather than one
    # per build.
    last_events = await _last_event_at_map(db, [b.id for b in page_builds])
    # Only the failed ones can have a reason, so only they are asked about —
    # and the map is passed whole, so a failed build the query returned nothing
    # for is not mistaken for one nobody has asked about yet.
    last_errors = await _latest_build_error_map(
        db, [b.id for b in page_builds if b.latest_status == BuildStatus.FAILED]
    )
    build_responses = [
        await _build_to_response(db, build, last_events.get(build.id), last_errors)
        for build in page_builds
    ]

    return BuildListResponse(
        builds=build_responses,
        total=total,
        page=page,
        page_size=page_size,
    )


# Declared before the ``/{build_id}`` routes: a literal path segment must be
# matched ahead of the path-parameter ones.
@router.post("/bulk-cancel", response_model=BulkCancelBuildsResponse)
async def bulk_cancel_builds(
    payload: BulkCancelBuildsRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Cancel RUNNING builds matching a filter — bulk cleanup, and the reaper.

    Nothing terminates abandoned builds today. Build status is derived from
    build-level events, so a build whose orchestrator died without emitting
    one stays RUNNING forever: interrupted local runs, crashed CI jobs and
    failed triggers accumulate permanently, each holding whatever execution
    claims and concurrency-limit slots its tasks had at the moment it
    vanished. This endpoint is the cleanup, and — driven on a timer with
    ``idle_for_seconds`` — the thing that stops the problem recurring.

    Bulk cancel and "reap idle builds" are one operation with two filters,
    so they are one endpoint: ``build_ids`` for an explicit set,
    ``idle_for_seconds`` for staleness, or both. See
    :class:`BulkCancelBuildsRequest` for every parameter; the two decisions
    worth reading before you use it:

    **Idleness is measured on activity, not on ``last_active_at``.** That
    column is bumped by build-level lifecycle transitions only — task events
    deliberately skip it so worker traffic doesn't contend on the build row
    — so a build that has been running tasks for three days still shows its
    BUILD_STARTED timestamp there. Reaping on it would cancel live work. The
    signal used instead is ``last_activity_at``: the newest of the build's
    entire event stream (task events included), its ``last_active_at``, and
    any pending scheduler wake-up. It is returned on every build response so
    a UI can show operators the same number this endpoint acts on.

    **Reactive builds are excluded unless you ask for them.** A reactive
    build is quiet between ticks by design and its ticks emit no events when
    there is nothing to do, so "no events for a day" does not mean abandoned
    — and it already has a watchdog for the case where it wedges. Pass
    ``include_reactive`` (or ``reactive_app_name``) when you know the owning
    app is gone.

    Beyond that: only builds whose status is RUNNING are ever touched, so
    the call is idempotent — safe to retry, and safe for two replicas to run
    concurrently (duplicated work, not double cancellation). ``dry_run``
    reports the exact same selection and writes nothing.

    **Cost.** The RUNNING test is a predicate on ``builds.latest_status``,
    served by ``ix_builds_environment_status`` — the same column, and the
    same answer, as ``GET /builds?status=running``, so the preview an
    operator runs cannot disagree with what this cancels. The idle filter's
    correlated aggregates then run only for builds already known to be
    RUNNING. Writes are capped by ``limit``; the scan is not.

    Auth: destructive, so the JWT/UI path requires the workspace ADMIN role
    (API keys, being environment-scoped machine credentials, are
    unrestricted) — the same gate as concurrency-limit eviction.
    """
    if payload.build_ids is None and payload.idle_for_seconds is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "Provide build_ids and/or idle_for_seconds. Cancelling every "
                "running build in an environment unconditionally is not a "
                "cleanup operation."
            ),
        )
    await _require_admin_for_user_auth(db, auth)
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))

    idle_before = (
        utc_now() - timedelta(seconds=payload.idle_for_seconds)
        if payload.idle_for_seconds is not None
        else None
    )
    rows, truncated = await select_cancellable_builds(
        db,
        environment_id=auth.environment_id,
        build_ids=payload.build_ids,
        idle_before=idle_before,
        reactive_app_name=payload.reactive_app_name,
        include_reactive=payload.include_reactive,
        limit=payload.limit,
    )

    skipped = await _explain_skipped_build_ids(
        db,
        payload,
        auth,
        selected={build.id for build, _ in rows},
        truncated=truncated,
    )

    if payload.dry_run:
        # Report the selection — including the tasks a real run would cancel
        # — without writing anything. Resolved with the same query the
        # cascade uses, minus the events.
        preview: list[CancelledBuildRef] = []
        task_count = 0
        for build, last_event_at in rows:
            task_ids = (
                await _preview_cascade_task_ids(db, build.id) if payload.cascade else []
            )
            task_count += len(task_ids)
            preview.append(
                CancelledBuildRef(
                    build_id=build.id,
                    name=build.name,
                    last_activity_at=last_activity_at(build, last_event_at),
                    reactive_app_name=build.reactive_app_name,
                    cascaded_task_ids=task_ids,
                )
            )
        return BulkCancelBuildsResponse(
            dry_run=True,
            builds=preview,
            build_count=len(preview),
            task_count=task_count,
            skipped=skipped,
            truncated=truncated,
        )

    if rows:
        # Pre-check covers the BUILD_CANCELLED events only: the cascade count
        # isn't known until the task rows are locked, and re-selecting them
        # up front would double the query cost of every sweep. The cascaded
        # events *are* recorded against the quota afterwards, so a workspace
        # near its ceiling is stopped on the next call rather than this one —
        # acceptable for an admin cleanup path whose write set is already
        # capped by `limit`.
        _raise_if_limit_exceeded(
            await check_entity_creation_limit(
                db, auth.workspace_id, "events", limits_settings, amount=len(rows)
            )
        )

    cancelled = await cancel_builds(
        db,
        rows,
        cascade=payload.cascade,
        reason=payload.reason,
        triggered_by_user_id=auth.user.external_id if auth.user else None,
    )
    task_count = sum(len(c.cascaded_task_ids) for c in cancelled)
    for _ in range(len(cancelled) + task_count):
        record_entity_created(auth.workspace_id, "events")
    if cancelled:
        logger.info(
            "bulk-cancel: cancelled %d build(s) and released %d task claim(s) "
            "in environment %s",
            len(cancelled),
            task_count,
            auth.environment_id,
        )

    return BulkCancelBuildsResponse(
        dry_run=False,
        builds=[
            CancelledBuildRef(
                build_id=c.build.id,
                name=c.build.name,
                last_activity_at=c.last_activity_at,
                reactive_app_name=c.build.reactive_app_name,
                cascaded_task_ids=c.cascaded_task_ids,
            )
            for c in cancelled
        ],
        build_count=len(cancelled),
        task_count=task_count,
        skipped=skipped,
        truncated=truncated,
    )


async def _preview_cascade_task_ids(db: AsyncSession, build_id: UUID) -> list[str]:
    """Task ids a cascade would cancel for ``build_id`` (dry run only)."""
    build_task_pks = (
        select(Event.task_id)
        .where(Event.build_id == build_id, Event.task_id.is_not(None))
        .distinct()
        .scalar_subquery()
    )
    rows = (
        await db.execute(
            select(Task.task_id)
            .where(
                Task.id.in_(build_task_pks),
                Task.latest_status.in_(CASCADE_CANCEL_STATUSES),
                Task.latest_status_build_id == build_id,
            )
            .order_by(Task.task_id.asc())
        )
    ).all()
    return [task_id for (task_id,) in rows]


async def _explain_skipped_build_ids(
    db: AsyncSession,
    payload: BulkCancelBuildsRequest,
    auth: SdkAuth,
    *,
    selected: set[UUID],
    truncated: bool,
) -> dict[str, str]:
    """Say why each explicitly-requested build id was not acted on.

    Only for ``build_ids`` — a filter-driven sweep has no "expected" set to
    diff against. A build in another environment is reported as
    ``not_found``, identical to an unknown id, so the endpoint can't be used
    to probe which build ids exist elsewhere.
    """
    if not payload.build_ids:
        return {}
    requested = [b for b in dict.fromkeys(payload.build_ids) if b not in selected]
    if not requested:
        return {}
    rows = (
        (
            await db.execute(
                select(Build).where(
                    Build.id.in_(requested),
                    Build.environment_id == auth.environment_id,
                )
            )
        )
        .scalars()
        .all()
    )
    visible = {b.id: b for b in rows}
    running_ids: set[UUID] = set()
    eligible_ids: set[UUID] = set()
    if visible:
        running_rows, _ = await select_cancellable_builds(
            db,
            environment_id=auth.environment_id,
            build_ids=list(visible),
            include_reactive=True,
            limit=len(visible),
        )
        running_ids = {build.id for build, _ in running_rows}
        # A second pass under the request's *own* filters, so "would this
        # have been cancelled if the batch had room?" can be answered
        # separately from "was it running at all?". Without it, a build
        # skipped for not being idle is reported as `limit_reached` the
        # moment any truncation happens — telling the caller to retry for a
        # build no retry will ever select.
        if payload.idle_for_seconds is None:
            eligible_ids = running_ids
        else:
            eligible_rows, _ = await select_cancellable_builds(
                db,
                environment_id=auth.environment_id,
                build_ids=list(visible),
                idle_before=utc_now() - timedelta(seconds=payload.idle_for_seconds),
                include_reactive=True,
                limit=len(visible),
            )
            eligible_ids = {build.id for build, _ in eligible_rows}
    reasons: dict[str, str] = {}
    for build_id in requested:
        build = visible.get(build_id)
        if build is None:
            reason = "not_found"
        elif build_id not in running_ids:
            reason = "not_running"
        elif build.reactive_app_name is not None and not (
            payload.include_reactive or payload.reactive_app_name is not None
        ):
            reason = "reactive"
        elif build_id not in eligible_ids:
            # Running, and not excluded for being reactive, but it failed
            # the idle cut. Checked before truncation: this build would not
            # be selected however many times the caller retries.
            reason = "not_idle"
        elif truncated:
            # Eligible, but the batch hit `limit`. Call again.
            reason = "limit_reached"
        else:
            reason = "not_idle"
        reasons[str(build_id)] = reason
    return reasons


async def _require_admin_for_user_auth(db: AsyncSession, auth: SdkAuth) -> None:
    """Gate destructive bulk operations to workspace admins on the JWT path.

    Same rule as the concurrency-limit admin surface: ``auth.user`` acts on
    behalf of a workspace member, so cancelling other people's builds
    wholesale requires the ADMIN role. API-key auth (``auth.user is None``)
    is an environment-scoped machine credential and stays full-access — it
    is how a CLI cleanup or a scheduled sweep authenticates.
    """
    if auth.user is None:
        return
    await require_workspace_access(
        db, auth.user.id, auth.workspace_id, min_role=WorkspaceRole.ADMIN
    )


@router.post("/wake-candidates", response_model=WakeCandidatesResponse)
async def wake_candidates(
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    limit: Annotated[int, Query(ge=1, le=MAX_WAKE_CANDIDATES)] = MAX_WAKE_CANDIDATES,
):
    """Hand out the reactive builds that need a scheduler tick and have none.

    The spawn half of a cross-build wake-up. The server flags builds whose
    frontier may have changed (see ``services.wakeups``) but cannot spawn;
    a caller that can — a scheduler tick, a resident engine with a Modal
    executor — asks here and spawns one tick per returned build, on that
    build's own ``reactive_app_name``.

    Each build is handed out at most once per
    ``services.wakeups.WAKE_HANDOUT_WINDOW``: the rows returned are
    stamped ``tick_requested_at`` in the same transaction, so concurrent
    callers get disjoint answers and a flagged build costs one container,
    however many schedulers are running in the environment. A build whose
    handed-out spawn never happened is offered again once the window has
    passed.

    Empty is the normal answer. It is also the answer on a registry that
    predates this route (a missing-route 404 on the SDK side), where the
    watchdog remains the only carrier of cross-build wake-ups.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    chosen = await select_wake_candidates(
        db, environment_id=auth.environment_id, limit=limit
    )
    await db.commit()
    return WakeCandidatesResponse(
        builds=[
            WakeCandidate(build_id=b.id, reactive_app_name=app)
            for b in chosen
            # Non-empty by the query's filter; the walrus tells the type
            # checker so and keeps "listed" and "spawnable" the same set.
            if (app := b.reactive_app_name)
        ]
    )


@router.get("/{build_id}", response_model=BuildResponse)
async def get_build(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Get a build by ID with derived status.

    Requires authentication via API key or JWT token with environment_id.
    """
    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    # Verify build belongs to authenticated environment
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    return await _build_to_response(db, build)


def _build_event_metadata(
    commit_hash: str | None = None,
    triggered_by_user_id: str | None = None,
) -> dict | None:
    """Build event_metadata dict from optional fields."""
    metadata: dict = {}
    if commit_hash:
        metadata["commit_hash"] = commit_hash
    if triggered_by_user_id:
        metadata["triggered_by_user_id"] = triggered_by_user_id
    return metadata or None


@router.post("/{build_id}/complete", response_model=BuildResponse)
async def complete_build(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    triggered_by_user_id: str | None = None,
    commit_hash: str | None = None,
):
    """Mark a build as completed.

    Args:
        triggered_by_user_id: Optional user ID if this is a manual override from UI.
        commit_hash: Optional git commit hash of the code that ran this build.
    """
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    build = await _get_build_for_update(build_id, db, auth)

    event = Event(
        build_id=build_id,
        task_id=None,
        event_type=EventType.BUILD_COMPLETED,
        event_metadata=_build_event_metadata(commit_hash, triggered_by_user_id),
    )
    await _record_build_event(db, build, event)
    await _touch_build_last_active(db, build_id)
    await db.commit()

    record_entity_created(auth.workspace_id, "events")

    return await _build_to_response(db, build)


@router.post("/{build_id}/fail", response_model=BuildResponse)
async def fail_build(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    error_message: str | None = None,
    triggered_by_user_id: str | None = None,
    commit_hash: str | None = None,
):
    """Mark a build as failed.

    Args:
        error_message: Optional error message.
        triggered_by_user_id: Optional user ID if this is a manual override from UI.
        commit_hash: Optional git commit hash of the code that ran this build.
    """
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    build = await _get_build_for_update(build_id, db, auth)

    event = Event(
        build_id=build_id,
        task_id=None,
        event_type=EventType.BUILD_FAILED,
        error_message=error_message,
        event_metadata=_build_event_metadata(commit_hash, triggered_by_user_id),
    )
    await _record_build_event(db, build, event)
    await _touch_build_last_active(db, build_id)
    await db.commit()

    record_entity_created(auth.workspace_id, "events")

    return await _build_to_response(db, build)


@router.post("/{build_id}/cancel", response_model=BuildCancelResponse)
async def cancel_build(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    triggered_by_user_id: str | None = None,
    commit_hash: str | None = None,
    cascade: Annotated[
        bool,
        Query(
            description=(
                "Also cancel the claims this build holds: emit TASK_CANCELLED "
                "for its RUNNING/SUSPENDED tasks, freeing their execution "
                "claims and concurrency-limit slots. Off by default."
            ),
        ),
    ] = False,
):
    """Cancel a build, optionally cascading to the claims its tasks hold.

    Without ``cascade`` this writes a single build-level BUILD_CANCELLED
    event and nothing else — which is what it has always done, and why
    cancelling a build has never actually cleaned anything up. Task rows are
    per *environment* with a denormalised global ``latest_status``, so a task
    the build left RUNNING keeps denying its execution claim to every future
    build that needs it, and keeps occupying its concurrency-limit slots,
    long after the build itself is gone.

    ``cascade=true`` releases those: TASK_CANCELLED for every task of this
    build that is RUNNING or SUSPENDED **and whose current status this build
    produced**. Both restrictions matter —

    - PENDING tasks are left alone. They hold no claim, and cancelling one
      would reach into other builds: a task this build registered may be
      referenced by a live build elsewhere. (``skip-blocked`` is the
      operation for pending work whose upstreams failed.)
    - Tasks another build put into RUNNING are left alone. Releasing those
      is that build's cancel, not this one's.

    Default off because it is a behaviour change for existing callers — the
    SDK's own fail-fast path cancels its running tasks itself.

    **The server cannot stop anything.** Like every other status write, this
    rewrites the registry's view; a worker whose task is cancelled here keeps
    running until it notices (a reactive tick cancels the detached execution;
    a resident engine polls). If the task then completes, COMPLETED is
    sticky and wins — coherent with "targets are ground truth", but worth
    knowing before cancelling a build you are not sure is dead.

    Args:
        triggered_by_user_id: Optional user ID if this is a manual override from UI.
        commit_hash: Optional git commit hash of the code that ran this build.
        cascade: See above.
    """
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    # Locked before the cascade below takes its task locks — build then
    # tasks, the order every path uses.
    build = await _get_build_for_update(build_id, db, auth)

    metadata = _build_event_metadata(commit_hash, triggered_by_user_id)
    cascaded_task_ids: list[str] = []
    if cascade:
        cascaded = await cascade_cancel_build_tasks(
            db,
            build_id,
            event_metadata=(metadata or {}) | {"cancelled_by": "build_cancel_cascade"},
        )
        cascaded_task_ids = [t.task_id for t in cascaded]
        if cascaded_task_ids:
            _raise_if_limit_exceeded(
                await check_entity_creation_limit(
                    db,
                    auth.workspace_id,
                    "events",
                    limits_settings,
                    # +1 for the BUILD_CANCELLED event written below.
                    # `check_entity_creation_limit` reserves nothing, so the
                    # earlier single-event check has not held any capacity —
                    # counting only the cascade lets `1 + len(cascade)` cross
                    # the limit.
                    amount=len(cascaded_task_ids) + 1,
                )
            )

    event = Event(
        build_id=build_id,
        task_id=None,
        event_type=EventType.BUILD_CANCELLED,
        event_metadata=metadata,
    )
    await _record_build_event(db, build, event)
    await _touch_build_last_active(db, build_id)
    # A cancelled reactive build still has executions only a tick can stop.
    # Flag it, so the next scheduler pass anywhere in the environment picks
    # it up instead of leaving it to the watchdog.
    await flag_build(db, build)
    # One transaction: the build and the claims it held go terminal together,
    # so a failure here cannot leave a cancelled build still holding claims.
    await db.commit()

    for _ in range(len(cascaded_task_ids) + 1):
        record_entity_created(auth.workspace_id, "events")

    base = await _build_to_response(db, build)
    return BuildCancelResponse(
        **base.model_dump(),
        cascaded_task_ids=cascaded_task_ids,
        cascaded_task_count=len(cascaded_task_ids),
    )


@router.post("/{build_id}/exit-early", response_model=BuildResponse)
async def exit_early(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    reason: str | None = None,
    commit_hash: str | None = None,
):
    """Mark a build as exited early (all remaining tasks running in other builds)."""
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    build = await _get_build_for_update(build_id, db, auth)

    event = Event(
        build_id=build_id,
        task_id=None,
        event_type=EventType.BUILD_EXIT_EARLY,
        error_message=reason,  # Reuse error_message field for the reason
        event_metadata=_build_event_metadata(commit_hash),
    )
    await _record_build_event(db, build, event)
    await _touch_build_last_active(db, build_id)
    await db.commit()

    record_entity_created(auth.workspace_id, "events")

    return await _build_to_response(db, build)


@router.post("/{build_id}/resume", response_model=BuildResponse)
async def resume_build(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
    executor_metadata: str | None = None,
):
    """Mark an existing build as resumed.

    Called by the SDK when ``sd.build(resume_build_id=...)`` reuses an
    existing build that may have already terminated. Emits a
    ``BUILD_RESUMED`` event, which flips the build back to RUNNING with
    ``is_resumed`` set so the UI shows a "running (resumed)" affordance.

    A build with no recorded activity beyond its ``BUILD_STARTED`` event
    is "fresh" — attaching to it is not a resume (e.g. a build id minted
    at the trigger point handed to the first orchestrator invocation).
    In that case no ``BUILD_RESUMED`` event is recorded, so the build's
    first run doesn't show as resumed.

    Args:
        commit_hash: Optional git commit hash of the resuming run.
        executor_metadata: Optional JSON-encoded dict describing the
            resuming trigger's executor (e.g. Modal app/workspace). When
            provided it replaces ``builds.executor_metadata``; when absent
            the stored value is kept — a resume from inside a Modal build
            container doesn't know its trigger metadata, and clearing
            would lose it.
    """
    parsed_executor_metadata = _parse_executor_metadata_param(executor_metadata)

    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    build = await _get_build_for_update(build_id, db, auth)

    has_activity = (
        await db.execute(
            select(Event.id)
            .where(Event.build_id == build_id)
            .where(Event.event_type != EventType.BUILD_STARTED)
            .limit(1)
        )
    ).first() is not None

    needs_commit = False
    if parsed_executor_metadata is not None:
        # Replace the stored trigger metadata even on the no-activity path
        # below, where no BUILD_RESUMED event is recorded (a fresh build
        # attaching at its trigger-minted id isn't a "resume"). The column
        # update is then invisible in the event log — accepted: the column
        # is a descriptive denormalisation of "how is this build driven",
        # not audited state, and the metadata does appear in the event log
        # once real activity produces a BUILD_RESUMED.
        build.executor_metadata = parsed_executor_metadata
        needs_commit = True

    if has_activity:
        event_metadata = _build_event_metadata(commit_hash) or {}
        if parsed_executor_metadata is not None:
            event_metadata["executor_metadata"] = parsed_executor_metadata
        event = Event(
            build_id=build_id,
            task_id=None,
            event_type=EventType.BUILD_RESUMED,
            event_metadata=event_metadata or None,
        )
        await _record_build_event(db, build, event)
        await _touch_build_last_active(db, build_id)
        needs_commit = True

    if needs_commit:
        await db.commit()
    if has_activity:
        record_entity_created(auth.workspace_id, "events")

    return await _build_to_response(db, build)


async def _get_build_checked(build_id: UUID, db: AsyncSession, auth: SdkAuth) -> Build:
    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )
    return build


_LeaseOwner = Annotated[
    str,
    Query(
        # ``min_length`` is not cosmetic: the whole "a lapsed tick cannot
        # clear its successor's lease" property rests on this string, and
        # two callers both sending "" would hold each other's lease.
        min_length=1,
        max_length=64,
        description=(
            "Identity of the scheduler asking. Renew and release are "
            "owner-checked, so a tick whose lease lapsed and was taken over "
            "cannot extend or clear its successor's."
        ),
    ),
]
_LeaseTtl = Annotated[
    int,
    Query(
        ge=MIN_LEASE_TTL_SECONDS,
        le=MAX_LEASE_TTL_SECONDS,
        description=(
            "How long the lease stays believable, in seconds, from now. "
            "Nothing renews it on the server's side: a tick that wants to "
            "keep driving a build renews it itself while it lingers."
        ),
    ),
]


@router.post("/{build_id}/scheduler-lease", response_model=SchedulerLeaseResponse)
async def acquire_build_scheduler_lease(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    owner_id: _LeaseOwner,
    ttl_seconds: _LeaseTtl = 60,
):
    """Take the build's scheduler lease. At most one tick drives a build.

    ``held=False`` means somebody else is driving it and this tick should
    no-op — which is safe because the wake-up that spawned it was flagged
    *before* the spawn, so the holder's own re-checks (its linger poll and
    the exit handshake) cover it.

    A lapsed lease denies nothing: this acquire takes it over, replacing
    the dead holder's owner and expiry together. That takeover is the
    healing mechanism — a tick whose container vanished releases nothing,
    and across containers there is nothing to release it with.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_for_update(build_id, db, auth)
    acquired, expires_at = acquire_scheduler_lease(
        build, owner_id=owner_id, ttl_seconds=ttl_seconds
    )
    await db.commit()
    return SchedulerLeaseResponse(
        build_id=build_id, held=acquired, expires_at=expires_at
    )


@router.put("/{build_id}/scheduler-lease", response_model=SchedulerLeaseResponse)
async def renew_build_scheduler_lease(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    owner_id: _LeaseOwner,
    ttl_seconds: _LeaseTtl = 60,
):
    """Extend the lease, for its holder only.

    ``held=False`` means this tick no longer owns the build: its lease
    lapsed and somebody took it over. Being refused here is how it finds
    out, and the honest answer is to stop driving.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_for_update(build_id, db, auth)
    expires_at = renew_scheduler_lease(
        build, owner_id=owner_id, ttl_seconds=ttl_seconds
    )
    await db.commit()
    return SchedulerLeaseResponse(
        build_id=build_id, held=expires_at is not None, expires_at=expires_at
    )


@router.delete("/{build_id}/scheduler-lease", response_model=SchedulerLeaseResponse)
async def release_build_scheduler_lease(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    owner_id: _LeaseOwner,
):
    """Drop the lease, for its holder only.

    ``held`` reports whether this caller was still the holder — a release
    by a tick that had already lost the build is a no-op rather than a way
    to clear its successor's lease.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_for_update(build_id, db, auth)
    released = release_scheduler_lease(build, owner_id=owner_id)
    await db.commit()
    return SchedulerLeaseResponse(build_id=build_id, held=released)


@router.post("/{build_id}/notify", response_model=BuildNotifyResponse)
async def notify_build(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    can_spawn: Annotated[
        bool,
        Query(
            description=(
                "Whether the caller can spawn a scheduler tick itself. The "
                "default assumes it can, and marks the build as handed out so "
                "no concurrent wake-candidates call spawns a second tick. A "
                "caller that cannot (no deployed app to reach) says so here, "
                "so the build stays available to drainers that can."
            ),
        ),
    ] = True,
):
    """Set the build's scheduler wake-up flag (``needs_tick_at``).

    Called by workers when they finish a task (and by anything else that
    changes the build's scheduling state). A reactive scheduler tick clears
    the flag before computing the frontier and re-checks it while lingering,
    so a notify landing mid-tick is never lost.

    The response reports whether a scheduler holds the build's lease
    (``scheduler_live``), which lets the caller skip spawning a tick that
    would only find the lease held. **The read happens after the commit**,
    on purpose — and after, rather than atomically with, is the entire
    requirement: a ``True`` then means the lease was still held once the
    flag was already durable, so its holder cannot exit without seeing it.
    Reading before the write would invert that and let a scheduler exit
    between the two with the caller having been told not to spawn. The
    SDK's tick closes the other end of the same window by re-reading the
    flag once more after it releases the lease.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_checked(build_id, db, auth)
    now = utc_now()
    # Only a RUNNING build can act on a wake-up, so only a RUNNING build is
    # flagged here. This is the same restriction ``_flag_builds`` applies to
    # the transition hook, and its absence here was a live loop: a cancelled
    # build's workers keep running until a tick stops them, every one of
    # them notifies on its way out, and each notify re-flagged the build for
    # the next drain to hand out again — forever, for as long as neighbours
    # kept touching its tasks.
    #
    # A cancelled build still gets its one tick: its own cancel sets the
    # flag (``flag_build``), which survives here and is reported below, so
    # the first caller to see it still spawns. Once that tick clears the
    # flag, later notifies report False and nobody spawns again.
    if build.latest_status == BuildStatus.RUNNING:
        build.needs_tick_at = now
    needs_tick = build.needs_tick_at is not None
    # Stamp the hand-out mark in the SAME transaction as the flag, on the
    # assumption that the caller will spawn: a concurrent
    # ``POST /builds/wake-candidates`` must never see this build flagged
    # and unstamped, or it hands it to a second spawner. If the lease read
    # below says a scheduler is live — so the caller will *not* spawn —
    # the stamp is put back, and the build is exactly as it was.
    previous_stamp = build.tick_requested_at
    if can_spawn and needs_tick:
        await mark_tick_requested(db, build, now=now)
    await db.commit()
    # Re-read from the database, not off the ORM instance. The session is
    # created with ``expire_on_commit=False``, so ``build`` still holds the
    # values loaded *before* the commit — and reading the lease from those
    # would invert the one ordering this endpoint's guarantee rests on. The
    # refresh is what makes ``scheduler_live=True`` mean "the lease was
    # still held once the flag was already durable".
    await db.refresh(build, ["scheduler_lease_until"])
    scheduler_live = lease_is_live(build)
    if scheduler_live and can_spawn and needs_tick:
        build.tick_requested_at = previous_stamp
        await db.commit()
    return BuildNotifyResponse(
        build_id=build_id, needs_tick=needs_tick, scheduler_live=scheduler_live
    )


@router.get("/{build_id}/notify", response_model=BuildNotifyResponse)
async def read_build_notify(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Read the build's scheduler wake-up flag. One row, nothing derived.

    A lingering tick asks one question every few seconds — "has anything
    changed?" — and used to ask it by fetching the whole frontier: seven
    statements, one of them a window-function aggregate over the event log,
    of which it read a single boolean. This is that boolean.

    ``scheduler_live`` is deliberately not answered here, and its absence is
    not a version signal. The caller is the tick that *holds* the lease, so
    the answer would only ever be "yourself"; computing it would cost the
    second table this endpoint exists to avoid.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_checked(build_id, db, auth)
    return BuildNotifyResponse(
        build_id=build_id, needs_tick=build.needs_tick_at is not None
    )


@router.delete("/{build_id}/notify", response_model=BuildNotifyResponse)
async def clear_build_notify(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Clear the build's scheduler wake-up flag.

    Called by a scheduler tick right before it computes the frontier.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_checked(build_id, db, auth)
    build.needs_tick_at = None
    await db.commit()
    return BuildNotifyResponse(build_id=build_id, needs_tick=False)


@router.post("/{build_id}/roots", response_model=BuildResponse)
async def add_build_roots(
    build_id: UUID,
    payload: AddBuildRootsRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Append root task ids to a build (deduplicated, order-preserving).

    Adding roots to an active build: terminal detection (all roots
    complete) covers the appended roots from the moment they land here.
    Callers must register the tasks separately (bulk registration).
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_checked(build_id, db, auth)
    existing = list(build.root_task_ids or [])
    merged = existing + [t for t in payload.root_task_ids if t not in existing]
    if merged != existing:
        build.root_task_ids = merged
        await _touch_build_last_active(db, build_id)
        await db.commit()

    return await _build_to_response(db, build)


# Size cap on the reactive tick_kwargs dict (compact-JSON byte size). It
# holds a handful of JSON-scalar TickConfig fields and is echoed on every
# build and frontier read — a small cap keeps it bounded. (app_name is a
# separate typed column, length-capped by the model.)
_MAX_REACTIVE_TICK_KWARGS_BYTES = 4096


@router.put("/{build_id}/reactive-meta", response_model=BuildResponse)
async def set_build_reactive_meta(
    build_id: UUID,
    payload: SetReactiveMetaRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Mark a build reactively scheduled and store its scheduler config.

    Upsert (idempotent): called by the reactive trigger and re-trigger.
    ``app_name`` (the owner/marker) is always set; its presence
    (``reactive_app_name``, surfaced on the build frontier) is the "this
    build is reactively scheduled" marker (a stray tick no-ops on a build
    without it), and the owning app drives the ticks. ``tick_kwargs`` is
    updated only when provided: a bare re-trigger (``tick_kwargs`` omitted)
    preserves the existing config, while a re-trigger that passes it updates
    it — the registry is mutable, unlike a possibly-immutable target root.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    if payload.tick_kwargs is not None:
        encoded = json.dumps(payload.tick_kwargs, separators=(",", ":")).encode("utf-8")
        if len(encoded) > _MAX_REACTIVE_TICK_KWARGS_BYTES:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"tick_kwargs must be at most "
                    f"{_MAX_REACTIVE_TICK_KWARGS_BYTES} bytes as compact JSON "
                    f"(got {len(encoded)})"
                ),
            )
    build = await _get_build_checked(build_id, db, auth)
    build.reactive_app_name = payload.app_name
    # Update tick_kwargs only when explicitly provided; a bare re-trigger
    # (tick_kwargs omitted) preserves the stored config rather than wiping it.
    if payload.tick_kwargs is not None:
        build.reactive_tick_kwargs = payload.tick_kwargs
    await db.commit()

    return await _build_to_response(db, build)


@router.post("/{build_id}/skip-blocked", response_model=SkipBlockedResponse)
async def skip_blocked_tasks(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
):
    """Emit TASK_SKIPPED for tasks transitively blocked by failures.

    Computes (recursive CTE over dependency edges) the pending/suspended
    tasks in the build that are downstream of a failed/cancelled/skipped
    task, and records TASK_SKIPPED for each in one transaction. Called by
    reactive scheduler ticks when a build reaches a failure terminal, so
    blocked tasks show as skipped instead of dangling pending forever —
    mirroring the resident engine's skip emission.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    await _get_build_checked(build_id, db, auth)

    build_task_pks = (
        select(Event.task_id)
        .where(Event.build_id == build_id, Event.task_id.is_not(None))
        .distinct()
        .scalar_subquery()
    )

    # Transitive closure downward from terminal-blocking seeds. Blockage
    # only propagates through nodes that will themselves never complete:
    # the seeds (failed/cancelled/skipped) and pending/suspended nodes
    # (which this call turns skipped). A COMPLETED intermediate satisfies
    # its downstream regardless of its own upstreams (mirroring the
    # resident engine, which only propagates skips through tasks that
    # themselves become skipped); RUNNING intermediates may still complete.
    _propagating_statuses = [
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.SKIPPED,
        TaskStatus.PENDING,
        TaskStatus.SUSPENDED,
        # Grouped with pending/suspended, not with running: an interrupted
        # task has no live execution that might still complete, so a failed
        # upstream blocks it exactly as it blocks a pending one.
        TaskStatus.INTERRUPTED,
    ]
    seeds = (
        select(Task.id)
        .where(
            Task.id.in_(build_task_pks),
            Task.latest_status.in_(
                [TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.SKIPPED]
            ),
        )
        .cte("blocked_closure", recursive=True)
    )
    downstream = (
        select(TaskDependency.downstream_task_id.label("id"))
        .join(seeds, TaskDependency.upstream_task_id == seeds.c.id)
        .join(Task, Task.id == seeds.c.id)
        .where(Task.latest_status.in_(_propagating_statuses))
    )
    closure = seeds.union(downstream)

    blocked_tasks = (
        (
            await db.execute(
                select(Task)
                .where(
                    Task.id.in_(select(closure.c.id)),
                    Task.id.in_(build_task_pks),
                    Task.latest_status.in_(
                        [
                            TaskStatus.PENDING,
                            TaskStatus.SUSPENDED,
                            TaskStatus.INTERRUPTED,
                        ]
                    ),
                )
                # Deterministic lock order (matching bulk-register's
                # task_id ordering) so concurrent skip-blocked calls or
                # skip-blocked vs bulk-register can't deadlock.
                .order_by(Task.task_id.asc())
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )

    if blocked_tasks:
        _raise_if_limit_exceeded(
            await check_entity_creation_limit(
                db,
                auth.workspace_id,
                "events",
                limits_settings,
                amount=len(blocked_tasks),
            )
        )
        metadata = _build_event_metadata(commit_hash)
        for task in blocked_tasks:
            event = Event(
                build_id=build_id,
                task_id=task.id,
                event_type=EventType.TASK_SKIPPED,
                event_metadata=metadata,
            )
            await transition_task(db, task, event)
        await db.commit()
        for _ in blocked_tasks:
            record_entity_created(auth.workspace_id, "events")

    return SkipBlockedResponse(
        build_id=build_id,
        skipped_task_ids=[t.task_id for t in blocked_tasks],
    )


@router.get("/{build_id}/frontier", response_model=BuildFrontierResponse)
async def get_build_frontier(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Return the build's scheduling frontier (for reactive scheduler ticks).

    See :class:`BuildFrontierResponse`. Statuses are the tasks' *global*
    denormalised statuses — a task completed or running in another build
    counts as such here too (which is exactly what a scheduler wants:
    don't re-run what's done, re-attach to what's running).

    ``blocked_by_external`` reconciles the two scopes this endpoint mixes:
    dependency gating is environment-global, while ``running`` and
    ``status_counts`` cover only tasks this build has events for. See
    :class:`FrontierExternalBlocker`.

    ``attempt_count`` on every task ref is the one field here that is
    scoped to **this build**, and to its current round, rather than to the
    environment — deliberately, because "how many times has this build
    tried since it was last resumed" is the retry-relevant number. A task
    that failed twice in an earlier build must not arrive here with its
    budget already spent, and neither must one whose build the user has
    just re-triggered. It also counts *attempts*, not TASK_STARTED events;
    see :class:`FrontierTaskRef`.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_checked(build_id, db, auth)

    # All tasks referenced by this build (registration/lifecycle events
    # carry build_id; ix_events_build_task_type serves this).
    build_task_ids = (
        select(Event.task_id)
        .where(Event.build_id == build_id, Event.task_id.is_not(None))
        .distinct()
        .scalar_subquery()
    )

    counts_rows = (
        await db.execute(
            select(Task.latest_status, func.count())
            .where(Task.id.in_(build_task_ids))
            .group_by(Task.latest_status)
        )
    ).all()
    # Normalize keys to the enum *value* explicitly — str(TaskStatus.X)
    # would silently become "TaskStatus.X" if the column ever turns
    # Enum-typed, breaking SDK terminal detection.
    status_counts = {
        (status.value if isinstance(status, TaskStatus) else str(status)): count
        for status, count in counts_rows
    }

    upstream = aliased(Task)
    has_incomplete_upstream = (
        select(TaskDependency.id)
        .join(upstream, TaskDependency.upstream_task_id == upstream.id)
        .where(
            TaskDependency.downstream_task_id == Task.id,
            upstream.latest_status != TaskStatus.COMPLETED,
        )
        .exists()
    )
    actionable_tasks = (
        (
            await db.execute(
                select(Task)
                .where(
                    Task.id.in_(build_task_ids),
                    Task.latest_status.in_(_FRONTIER_NON_TERMINAL_STATUSES),
                    ~has_incomplete_upstream,
                )
                .order_by(Task.created_at)
            )
        )
        .scalars()
        .all()
    )

    # ALL running tasks in the build (not just actionable ones): a RUNNING
    # task whose freshly-registered dynamic-dep edges are incomplete drops
    # out of `actionable` — but cancellation (fail-fast / externally
    # cancelled build) must still reach it.
    running_tasks = (
        (
            await db.execute(
                select(Task).where(
                    Task.id.in_(build_task_ids),
                    Task.latest_status == TaskStatus.RUNNING,
                )
            )
        )
        .scalars()
        .all()
    )

    # Why a build with nothing actionable and nothing running can still be
    # perfectly healthy: `has_incomplete_upstream` above joins Task
    # *globally* (task rows and dependency edges are per-environment), while
    # `running` / `status_counts` are scoped to this build's task set. An
    # upstream that some other build left RUNNING therefore gates this
    # build's downstream tasks while contributing nothing this build can
    # see — and it need not be in this build's task set at all (a dynamic
    # dependency registered under an earlier build is the usual case).
    # Reported explicitly rather than folded into `running`, which must keep
    # meaning "RUNNING tasks of *this* build" (it is the cancellation
    # target list).
    #
    # Computed ONLY when this build has nothing actionable and nothing
    # running — i.e. exactly when it looks stuck, which is the only state in
    # which either consumer asks the question. That is not an optimisation
    # detail to gloss over: the frontier is re-read on every linger poll
    # (~3 s per active build), and this query sorts over the build's
    # dependency edges, so computing it unconditionally would put a
    # per-edge sort on the hot path of every healthy build. A build that is
    # visibly progressing does not need to be told what it is waiting on.
    #
    # The gate mirrors the SDK's own stuck check (`not actionable and
    # running == 0`) so the two cannot disagree about when the diagnostic
    # is meaningful. Consequence for consumers: an EMPTY list means "not
    # blocked externally, or not stalled" — never read it as proof that no
    # external blocker exists while the build is still making progress.
    #
    # One flat (blocked, blocker) query — resolving blockers per blocked
    # task would be N+1. It mirrors the join `actionable` already performs,
    # so the added cost is ~one more pass over this build's dependency
    # edges (ix_task_dep_downstream), bounded by LIMIT.
    blocked_by_external: list[FrontierExternalBlocker] = []
    blocked_by_external_truncated = False
    blocker_rows: Sequence[Any] = []
    if not actionable_tasks and not running_tasks:
        blocked = aliased(Task)
        blocker = aliased(Task)
        blocker_rows = (
            await db.execute(
                select(
                    blocked.task_id,
                    blocker.task_id,
                    blocker.task_namespace,
                    blocker.task_name,
                    blocker.latest_status,
                    blocker.latest_status_at,
                    blocker.latest_status_expires_at,
                    blocker.latest_status_build_id,
                    # Labelled, because these two are the ones read by name
                    # before the row is destructured below — and a positional
                    # index into a ten-column select is a silent breakage
                    # waiting for someone to reorder it.
                    blocker.id.in_(build_task_ids).label("blocking_in_build"),
                    blocker.id.label("blocker_pk"),
                )
                .select_from(TaskDependency)
                .join(blocked, TaskDependency.downstream_task_id == blocked.id)
                .join(blocker, TaskDependency.upstream_task_id == blocker.id)
                .where(
                    blocked.id.in_(build_task_ids),
                    blocked.latest_status.in_(_FRONTIER_NON_TERMINAL_STATUSES),
                    blocker.latest_status != TaskStatus.COMPLETED,
                    # Null-safe inequality (renders IS DISTINCT FROM on
                    # Postgres, IS NOT on SQLite): a blocker with no recorded
                    # status build — a pre-denormalisation row — is likewise
                    # not something this build put there.
                    blocker.latest_status_build_id.is_distinct_from(build_id),
                )
                # Registration order, matching `actionable`. Deterministic,
                # so a truncated list is stable across the polls of one tick.
                .order_by(blocked.created_at, blocker.created_at)
                .limit(_MAX_FRONTIER_EXTERNAL_BLOCKERS + 1)
            )
        ).all()
        blocked_by_external_truncated = (
            len(blocker_rows) > _MAX_FRONTIER_EXTERNAL_BLOCKERS
        )
        # Response objects are built lower down, once attempt counts are
        # known: an in-plan blocker carries its attempts so a scheduler can
        # apply the same retry budget it applies to anything else.
        blocker_rows = blocker_rows[:_MAX_FRONTIER_EXTERNAL_BLOCKERS]

    root_task_ids: list[str] = list(build.root_task_ids or [])
    roots: list[Task] = []
    if root_task_ids:
        roots = list(
            (
                await db.execute(
                    select(Task).where(
                        Task.environment_id == auth.environment_id,
                        Task.task_id.in_(root_task_ids),
                    )
                )
            )
            .scalars()
            .all()
        )

    # Execution attempts per task in this build's current round, for the
    # scheduler's retry policy (see FrontierTaskRef.attempt_count). Derived
    # rather than denormalised: attempts are per *build* and per *round*,
    # and there is no per-(build, task) row to denormalise onto — inventing
    # one would cost a table, a fold on every start path and a backfill, to
    # replace this.
    #
    # ONE grouped query for every task in the response — the frontier is
    # re-read on every linger poll (~3 s per active build), so a per-task
    # aggregate would be N+1 on the hottest read in the system. Bounded to
    # the tasks actually being reported, so the added scan is proportional
    # to the response, not to the build's whole event history. Frontier
    # query-count delta: +1, or 0 when the frontier has no tasks to report
    # (`get_attempt_counts_in_build` returns without touching the DB).
    attempt_task_pks = {t.id for t in actionable_tasks}
    attempt_task_pks.update(t.id for t in running_tasks)
    attempt_task_pks.update(t.id for t in roots)
    # In-plan blockers too — a scheduler may reset one, and it needs the
    # budget to decide. Out-of-plan blockers are skipped: they have no
    # attempts in this build by definition.
    attempt_task_pks.update(
        row.blocker_pk for row in blocker_rows if row.blocking_in_build
    )
    attempt_counts = await get_attempt_counts_in_build(
        db, build_id, list(attempt_task_pks)
    )
    # A second grouped query over the same bounded id set, for the same
    # reason and at the same cost shape as the first. Kept separate rather
    # than folded in: the attempt query windows over LAG'd event pairs and
    # this one is a plain count, so sharing a statement would mean an outer
    # join between two different aggregations to save one index scan on a
    # query the frontier already pays four of.
    interrupt_counts = await get_interrupt_counts_in_build(
        db, build_id, list(attempt_task_pks)
    )

    blocked_by_external = [
        FrontierExternalBlocker(
            task_id=blocked_task_id,
            blocking_task_id=blocking_task_id,
            blocking_task_namespace=blocking_namespace,
            blocking_task_name=blocking_name,
            blocking_status=blocking_status,
            blocking_status_at=blocking_status_at,
            # The point of the whole entry: a blocker RUNNING under a build
            # that died used to be indistinguishable from one running
            # normally, leaving the consumer to guess from elapsed time.
            # Past this instant it is provably gone.
            blocking_status_expires_at=blocking_status_expires_at,
            blocking_status_build_id=blocking_status_build_id,
            blocking_in_build=bool(blocking_in_build),
            # Only meaningful for a blocker in this build's plan; a task
            # outside it has spent no attempts here.
            blocking_attempt_count=(
                attempt_counts.get(blocker_pk, 0) if blocking_in_build else None
            ),
        )
        for (
            blocked_task_id,
            blocking_task_id,
            blocking_namespace,
            blocking_name,
            blocking_status,
            blocking_status_at,
            blocking_status_expires_at,
            blocking_status_build_id,
            blocking_in_build,
            blocker_pk,
        ) in blocker_rows
    ]

    def _ref(t: Task) -> FrontierTaskRef:
        return FrontierTaskRef(
            task_id=t.task_id,
            latest_status=t.latest_status,
            latest_executor=t.latest_executor,
            latest_executor_ref=t.latest_executor_ref,
            latest_executor_metadata=t.latest_executor_metadata,
            # Schedulers bound staleness with this (e.g. "RUNNING for too
            # long with no executor ref"); omitting it silently disabled
            # those guards, since the field defaults to None.
            latest_status_at=t.latest_status_at,
            # Who holds it. `running` is every RUNNING task in this build's
            # plan, not every task this build started, so without this a
            # scheduler cannot tell its own executions from a neighbour's.
            latest_status_build_id=t.latest_status_build_id,
            # ...and this turns that heuristic into evidence: past the
            # expiry the server itself will hand the task to the next
            # claimant, so a scheduler can stop inferring from elapsed time.
            latest_status_expires_at=t.latest_status_expires_at,
            # Absent from the map = no attempt recorded in this build. A
            # root cached from an earlier build is the normal case.
            attempt_count=attempt_counts.get(t.id, 0),
            interrupt_count=interrupt_counts.get(t.id, 0),
        )

    return BuildFrontierResponse(
        build_id=build_id,
        build_status=build.latest_status,
        needs_tick=build.needs_tick_at is not None,
        root_task_ids=root_task_ids,
        roots=[_ref(t) for t in roots],
        status_counts=status_counts,
        actionable=[_ref(t) for t in actionable_tasks],
        running=[_ref(t) for t in running_tasks],
        blocked_by_external=blocked_by_external,
        blocked_by_external_truncated=blocked_by_external_truncated,
        reactive_app_name=build.reactive_app_name,
        reactive_tick_kwargs=build.reactive_tick_kwargs,
    )


# Cap on GET /builds/{id}/executions. Generous, because stopping them is
# the whole point and a truncated answer costs another round-trip.
_MAX_BUILD_EXECUTIONS = 500

# Events by which a build learns that an execution it started has ended.
# A *worker* reported one of these, so there is no container left to stop.
#
# TASK_CANCELLED is deliberately absent, and that absence is the whole
# point of this endpoint: a cancel is a request to stop, not evidence that
# anything stopped. The server cannot stop an execution — it can only
# record that the claim is gone — so a task this build cancelled is
# precisely a task whose container it still has to go and kill.
#
# TASK_INTERRUPTED is absent for a different reason: the platform ended one
# attempt and the backend may be retrying under the same call, so the ref
# can still be live. That is the premise the tick's backend-retry guard
# already rests on.
_EXECUTION_ENDED_EVENTS = (
    EventType.TASK_COMPLETED,
    EventType.TASK_FAILED,
    EventType.TASK_SUSPENDED,
)


@router.get("/{build_id}/executions", response_model=BuildExecutionsResponse)
async def get_build_executions(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """The detached executions this build started and never saw end.

    **The server cannot stop anything** — it can only say what is left to
    stop. Only the engine that spawned an execution can cancel it, and it
    needs three things: which executions are this build's to revoke, which
    backend ran them, and the ref to cancel.

    The frontier cannot answer that, and neither can the task rows. Both
    describe the task's *current* state, and the question here is about the
    past: what did this build start? Those differ in exactly the case that
    matters. A cascading build cancel releases the claims this build held —
    which is the point, it is what lets the next build take those tasks
    over — and the next build can claim one within seconds, long before the
    cancelled build's tick gets to run. From that moment the task row names
    the *new* execution, and the old one, still running, is unreachable:
    stopping it by task status would either miss it or kill the new one.
    Both happened.

    So this reads the event log instead, which is where the past is kept.
    For each task, this build's most recent TASK_STARTED carrying an
    executor ref — unless this build has since recorded one of
    :data:`_EXECUTION_ENDED_EVENTS` for it, which means a worker reported
    the execution over and there is nothing left to kill.

    **An execution ref is not a claim.** The claim says who may run the
    task next; the ref names one execution, and the build that started it
    owns it however the claim has moved since. Cancelling that ref cannot
    touch anybody else's container, which is what makes answering from the
    past safe rather than reckless.

    Newest first, capped (see ``_MAX_BUILD_EXECUTIONS``). Cancelling is
    idempotent at every backend stardag supports, so a ref stopped twice —
    or one whose execution already ended without this build hearing about
    it — costs nothing.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    build = await _get_build_checked(build_id, db, auth)

    ref_column = Event.event_metadata["executor_ref"].as_string()
    executor_column = Event.event_metadata["executor"].as_string()
    ranked = (
        select(
            Event.task_id.label("task_pk"),
            Event.created_at.label("started_at"),
            ref_column.label("executor_ref"),
            executor_column.label("executor"),
            Event.id.label("event_id"),
            # id (UUID7) breaks created_at ties, the same way the status
            # replay does — without it two same-timestamp starts order by
            # whatever the index returned, and "the latest ref" becomes a
            # coin toss between two executions.
            func.row_number()
            .over(
                partition_by=Event.task_id,
                order_by=(Event.created_at.desc(), Event.id.desc()),
            )
            .label("rank"),
        )
        .where(
            Event.build_id == build_id,
            Event.event_type == EventType.TASK_STARTED,
            Event.task_id.is_not(None),
            # A claiming start records no ref — the spawn has not happened
            # yet — and there is nothing to stop until the one that does.
            ref_column.is_not(None),
        )
        .subquery()
    )
    latest = select(ranked).where(ranked.c.rank == 1).subquery()
    ended = (
        select(Event.id)
        .where(
            Event.build_id == build_id,
            Event.task_id == latest.c.task_pk,
            Event.event_type.in_(_EXECUTION_ENDED_EVENTS),
            # Same tie-break, for the same reason: an end recorded in the
            # same microsecond as the start it ends would otherwise be
            # missed, and the execution reported as still to stop.
            tuple_(Event.created_at, Event.id)
            > tuple_(latest.c.started_at, latest.c.event_id),
        )
        .exists()
    )
    rows = (
        await db.execute(
            select(
                Task,
                latest.c.executor,
                latest.c.executor_ref,
                latest.c.started_at,
            )
            .join(latest, latest.c.task_pk == Task.id)
            .where(~ended)
            .order_by(latest.c.started_at.desc())
            .limit(_MAX_BUILD_EXECUTIONS + 1)
        )
    ).all()

    truncated = len(rows) > _MAX_BUILD_EXECUTIONS
    return BuildExecutionsResponse(
        build_id=build_id,
        build_status=build.latest_status,
        executions=[
            BuildExecutionRef(
                task_id=task.task_id,
                latest_status=task.latest_status,
                # Narrowed to str by the query's IS NOT NULL filter; an
                # older event that recorded a ref without a backend name
                # falls back to the task row's.
                executor=cast(str, executor or task.latest_executor),
                executor_ref=cast(str, executor_ref),
                executor_metadata=task.latest_executor_metadata,
                latest_status_at=started_at,
            )
            for task, executor, executor_ref, started_at in rows[:_MAX_BUILD_EXECUTIONS]
        ],
        truncated=truncated,
    )


# --- Tasks within Builds ---


async def _close_plan_over_dependencies(
    db: AsyncSession,
    *,
    build_id: UUID,
    task_pks: Sequence[UUID],
) -> int:
    """Admit incomplete upstreams of ``task_pks`` into this build's plan.

    **A build's plan is every dependency of its roots that was not complete
    at discovery time**, pruned at complete tasks — whose own upstreams are
    assumed complete with them. That is not a new rule: it is exactly what
    discovery does when it walks ``task.requires()``.

    The gap is that discovery walks *static* edges while gating consults
    every recorded edge, dynamic ones included. A dynamic edge is written by
    whichever build first ran the task and then outlives it, environment
    -global and permanent. A later build that statically discovers the same
    task therefore inherits the dependency without inheriting the task, and
    is gated on an upstream it never registered — which no build containing
    it can schedule, because the only thing that would produce it is the
    very task being gated. A permanent deadlock.

    Admitting an upstream is a status-neutral TASK_REFERENCED: nothing about
    the upstream's own state changes, it simply becomes part of this build's
    plan, which is what makes it schedulable here.

    Over-approximating is safe, under-approximating is not. If this run
    would in fact yield different dynamic dependencies, the build completes
    an upstream it did not need — wasted work, correct outcome — whereas
    missing one deadlocks. So no attempt is made to decide whether a
    recorded edge is still current.

    **RUNNING upstreams are admitted like any other.** Excluding them was
    tempting — another build is executing it, so nothing is deadlocked
    *right now* — but closure runs once, at registration, while RUNNING is
    transient. The moment the task stops running the exclusion becomes a
    permanent hole, and the likeliest way for it to stop running is an
    operator releasing a stale claim: the documented remedy would strand
    every build that inherited the dependency while it was running.

    A task this build did not start therefore appears in its own
    ``running``, and its liveness heuristics may act on it. That is correct:
    the destructive action is gated on the claim's expiry, and past that
    expiry the server no longer honours the claim and will hand the task to
    the next claimant whoever asks. Which build started it was never what
    made recovery safe — the claim is.
    """
    if not task_pks:
        return 0

    admitted = 0
    frontier_pks = list(task_pks)
    seen: set[UUID] = set(task_pks)
    while frontier_pks:
        in_plan = (
            select(Event.task_id)
            .where(Event.build_id == build_id, Event.task_id.is_not(None))
            .distinct()
            .scalar_subquery()
        )
        rows = (
            (
                await db.execute(
                    select(Task)
                    .join(TaskDependency, TaskDependency.upstream_task_id == Task.id)
                    .where(
                        TaskDependency.downstream_task_id.in_(frontier_pks),
                        Task.latest_status != TaskStatus.COMPLETED,
                        Task.id.not_in(in_plan),
                    )
                    .distinct()
                )
            )
            .scalars()
            .all()
        )

        frontier_pks = []
        for upstream in rows:
            if upstream.id in seen:
                continue
            seen.add(upstream.id)
            # The one task event not recorded through ``transition_task``,
            # and the exemption is worth stating rather than leaving to be
            # rediscovered: TASK_REFERENCED is purely informational — it
            # moves no ``latest_*`` — so there is no transition here for a
            # post-transition hook to run on. Admitting an upstream into a
            # plan says nothing about its status.
            #
            # What the guard test enforces is therefore the narrower
            # invariant "nothing outside services/status.py applies an
            # event", not "every task event goes through transition_task".
            # If this ever emits a status-bearing event, it must move.
            db.add(
                Event(
                    build_id=build_id,
                    task_id=upstream.id,
                    event_type=EventType.TASK_REFERENCED,
                )
            )
            admitted += 1
            frontier_pks.append(upstream.id)
        if frontier_pks:
            # Flush so the next level's `in_plan` subquery sees these.
            await db.flush()

    return admitted


async def _reconcile_dependency_edges(
    *,
    db: AsyncSession,
    environment_id: UUID,
    downstream_task_pk: UUID,
    upstream_task_ids: list[str],
    is_dynamic: bool,
) -> int:
    """Create any missing upstream tasks (as phantoms) and dependency edges.

    **Phantom-creation is a safety hatch, not the happy path.** With the
    SDK's post-order discover walk (every dep registers before its
    parent) and within-batch ordering of the bulk-register endpoint, the
    upstream ``task_id`` lookup at step 1 always finds existing rows in
    normal operation, so steps 2 + 3 are skipped. Phantoms only appear
    when:
      - A build crashes between registering a parent and registering
        its deps (orphan rows from the failed build).
      - An out-of-band caller registers edges via the
        ``/dependencies`` endpoint pointing at not-yet-registered task
        ids.
      - A future caller registers in pre-order again.
    The rest of the system (UI list, DAG view) treats phantoms as
    placeholder rows and the next register-with-real-data call upgrades
    them in place.

    Issues two statements when every upstream task already exists, or four
    when missing upstream ids must be created as phantoms — independent of
    N otherwise:
      1. SELECT existing tasks WHERE task_id IN (...).
      2. (only if any upstream ids were missing) INSERT ... VALUES (...)
         ON CONFLICT DO NOTHING — bulk phantom insert.
      3. (only if any upstream ids were missing) SELECT to re-fetch PKs.
         Required because ON CONFLICT DO NOTHING + RETURNING only returns
         our own inserted rows; a concurrent caller may have created the
         row first and we still need its PK.
      4. INSERT ... VALUES (...) ON CONFLICT DO NOTHING — bulk edge insert.

    Idempotent: ``ON CONFLICT DO NOTHING`` handles concurrent registrations.
    An edge's ``is_dynamic`` value is set from the *first* successful insert;
    a later call with a different ``is_dynamic`` value does not overwrite the
    existing row. That's intentional — if a dep is both static and yielded
    dynamically (unusual) we record the first observation as authoritative.

    Returns the number of edges inserted by this call. On Postgres the
    asyncpg cursor reports an accurate rowcount; on dialects that don't
    expose rowcount we conservatively report 0 so callers like
    ``AddDependenciesResponse.added`` don't over-claim.
    """
    if not upstream_task_ids:
        return 0

    # Deduplicate upstream ids so we don't propose the same row twice.
    requested_ids = list(dict.fromkeys(upstream_task_ids))
    # Single timestamp for every row this call writes — phantoms and edges
    # share it, which keeps the audit log self-consistent. UUID7 PKs encode
    # time, so per-row order is still preserved within the batch.
    now = utc_now()

    # 1. Find existing tasks for these task_ids in one round-trip.
    existing_result = await db.execute(
        select(Task.id, Task.task_id)
        .where(Task.environment_id == environment_id)
        .where(Task.task_id.in_(requested_ids))
    )
    task_pk_by_task_id: dict[str, UUID] = {
        task_id: pk for pk, task_id in existing_result.all()
    }

    # 2. For any missing ids, batch-insert phantoms. ON CONFLICT DO NOTHING
    # keeps us idempotent under concurrent registrations of the same id.
    missing_ids = [tid for tid in requested_ids if tid not in task_pk_by_task_id]
    if missing_ids:
        phantom_rows = [
            {
                "id": generate_uuid7(),
                "task_id": tid,
                "environment_id": environment_id,
                "task_namespace": "",
                "task_name": tid[:12],
                "task_data": {},
                "is_phantom": True,
                "created_at": now,
                # Match the historical "task with no events shows as PENDING"
                # semantic so phantoms appear consistently in the UI; the
                # is_phantom column distinguishes them for consumers that
                # care.
                "latest_status": TaskStatus.PENDING,
                "latest_waiting_for_lock": False,
            }
            for tid in missing_ids
        ]
        await db.execute(
            pg_insert(Task)
            .values(phantom_rows)
            .on_conflict_do_nothing(constraint="uq_task_environment_taskid")
        )
        # Re-fetch all missing rows in one go (catches both rows we inserted
        # and rows a concurrent writer beat us to). This is required because
        # ``RETURNING`` would only give us our own inserted rows, not the
        # ones we lost the race on.
        refetch_result = await db.execute(
            select(Task.id, Task.task_id)
            .where(Task.environment_id == environment_id)
            .where(Task.task_id.in_(missing_ids))
        )
        for pk, task_id in refetch_result.all():
            task_pk_by_task_id[task_id] = pk

    # Every requested id must now resolve to a PK. The KeyError below is
    # essentially unreachable in practice — under READ COMMITTED the
    # ON CONFLICT DO NOTHING + re-fetch resolves to a row in every
    # realistic ordering. The realistic concurrent-delete failure mode is
    # an FK violation on the edge insert below, not this lookup. The lookup
    # exists as a defence-in-depth tripwire so a future regression would
    # surface a clear error rather than silently dropping a dep.
    edge_rows = []
    for tid in requested_ids:
        upstream_pk = task_pk_by_task_id[tid]
        edge_rows.append(
            {
                "id": generate_uuid7(),
                "upstream_task_id": upstream_pk,
                "downstream_task_id": downstream_task_pk,
                "is_dynamic": is_dynamic,
                "created_at": now,
            }
        )

    # 3. Bulk insert the edge rows.
    edge_stmt = (
        pg_insert(TaskDependency)
        .values(edge_rows)
        .on_conflict_do_nothing(constraint="uq_task_dependency_edge")
    )
    result = await db.execute(edge_stmt)
    # CursorResult.rowcount totals across all VALUES rows on Postgres
    # (with asyncpg, this is reliable even for ON CONFLICT DO NOTHING —
    # only actually-inserted rows are counted). On dialects that don't
    # expose rowcount we report 0 rather than len(edge_rows), since
    # len(edge_rows) would over-claim whenever any conflict occurred.
    inserted = getattr(result, "rowcount", None)
    if inserted is None or inserted < 0:
        inserted = 0
    return inserted


@router.post("/{build_id}/tasks", response_model=TaskResponse, status_code=201)
async def register_task(
    build_id: UUID,
    task: TaskCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Register a task to a build.

    If the task already exists in the environment, it will be reused and a
    TASK_REFERENCED event is created. Otherwise creates the task and a
    TASK_PENDING event.
    """
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        check_payload_size(
            task.task_data,
            limits_settings.max_task_data_bytes,
            ErrorCode.TASK_DATA_SIZE_LIMIT,
            "task_data",
        )
    )
    _raise_if_limit_exceeded(
        check_structural_limit(
            len(task.dependency_task_ids),
            limits_settings.max_dependency_ids_per_task,
            ErrorCode.DEPENDENCY_COUNT_LIMIT,
            "dependency_task_ids",
        )
    )
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "events", limits_settings
        )
    )

    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    # Verify build belongs to authenticated environment
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    # Check if task already exists in environment. Lock for update so the
    # phantom-upgrade path (mutating an existing row) and the transition
    # call below don't race with a concurrent event-creator on the same task.
    # New rows go through pg_insert ON CONFLICT in the dependency reconcile
    # loop and are race-safe via the unique constraint, so the lock is only
    # needed when an existing row is found.
    result = await db.execute(
        select(Task)
        .where(Task.environment_id == build.environment_id)
        .where(Task.task_id == task.task_id)
        .with_for_update()
    )
    db_task = result.scalar_one_or_none()
    task_already_existed = db_task is not None

    if not db_task:
        # Check task creation limit only for new tasks
        _raise_if_limit_exceeded(
            await check_entity_creation_limit(
                db, auth.workspace_id, "tasks", limits_settings
            )
        )
        # Create new task
        db_task = Task(
            task_id=task.task_id,
            environment_id=build.environment_id,
            task_namespace=task.task_namespace,
            task_name=task.task_name,
            task_data=task.task_data,
            version=task.version,
            output_uri=task.output_uri,
        )
        db.add(db_task)
        await db.flush()  # Get the id

    elif db_task.is_phantom:
        # Upgrade phantom to real task
        db_task.task_namespace = task.task_namespace
        db_task.task_name = task.task_name
        db_task.task_data = task.task_data
        db_task.version = task.version
        db_task.output_uri = task.output_uri
        db_task.is_phantom = False

    # Reconcile static dependency edges (is_dynamic=False).
    await _reconcile_dependency_edges(
        db=db,
        environment_id=build.environment_id,
        downstream_task_pk=db_task.id,
        upstream_task_ids=task.dependency_task_ids,
        is_dynamic=False,
    )

    # Create appropriate event for this build:
    # - TASK_PENDING if this build first registered the task
    # - TASK_REFERENCED if the task already existed from another build
    event = Event(
        build_id=build_id,
        task_id=db_task.id,
        event_type=EventType.TASK_REFERENCED
        if task_already_existed
        else EventType.TASK_PENDING,
    )
    await transition_task(db, db_task, event)

    await _close_plan_over_dependencies(db, build_id=build_id, task_pks=[db_task.id])

    await db.commit()
    await db.refresh(db_task)

    record_entity_created(auth.workspace_id, "events")
    if not task_already_existed:
        record_entity_created(auth.workspace_id, "tasks")

    return TaskResponse(
        id=db_task.id,
        task_id=db_task.task_id,
        environment_id=db_task.environment_id,
        task_namespace=db_task.task_namespace,
        task_name=db_task.task_name,
        task_data=db_task.task_data,
        version=db_task.version,
        output_uri=db_task.output_uri,
        created_at=db_task.created_at,
        is_phantom=db_task.is_phantom,
        latest_executor=db_task.latest_executor,
        latest_executor_ref=db_task.latest_executor_ref,
        latest_executor_metadata=db_task.latest_executor_metadata,
        latest_status=db_task.latest_status,
        latest_status_at=db_task.latest_status_at,
        latest_status_build_id=db_task.latest_status_build_id,
    )


# Cap on the number of tasks per bulk-register call. Bounds memory/transaction
# size on the API side; the SDK's build engine should chunk if it ever exceeds.
_MAX_BULK_REGISTER_TASKS = 1000


@router.post(
    "/{build_id}/tasks/bulk",
    response_model=TaskBulkResponse | TaskBulkIdOnlyResponse,
    status_code=201,
)
async def register_tasks_bulk(
    build_id: UUID,
    payload: TaskBulkCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    id_only: Annotated[
        bool,
        Query(
            description=(
                "If true, return only ``{id, task_id}`` pairs in the response "
                "instead of the full TaskResponse list. Cuts response size by "
                "~10× for batches with rich task_data; the SDK's build engine "
                "passes this since it doesn't read the response."
            ),
        ),
    ] = False,
):
    """Register multiple tasks to a build in a single transaction.

    Tasks are processed in array order. With the SDK's post-order discover
    walk, deps appear earlier in the array than their parents — so when a
    parent's ``dependency_task_ids`` resolves the API finds existing rows
    (no phantom-creation in ``_reconcile_dependency_edges``). Within the
    same transaction ``db.flush()`` makes earlier tasks visible to later
    SELECTs, so the in-batch ordering carries through correctly.

    Sibling-of single-task registration: same TASK_PENDING /
    TASK_REFERENCED event semantics, same phantom-upgrade behaviour for
    rows left over from prior failed builds.
    """
    raw_tasks = payload.tasks

    if not raw_tasks:
        return TaskBulkResponse(tasks=[])

    if len(raw_tasks) > _MAX_BULK_REGISTER_TASKS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Bulk register limited to {_MAX_BULK_REGISTER_TASKS} tasks per "
                f"call (got {len(raw_tasks)})"
            ),
        )

    # Deduplicate by task_id, keeping the first occurrence. The schema
    # contract documents this so callers can rely on "first wins" semantics
    # for accidental duplicates in a single batch (rather than getting
    # multiple events / repeated dep reconciliation per task).
    seen_ids: set[str] = set()
    tasks_in: list[TaskCreate] = []
    for t in raw_tasks:
        if t.task_id in seen_ids:
            continue
        seen_ids.add(t.task_id)
        tasks_in.append(t)

    # Rate limit (1 per call). The bulk endpoint deliberately doesn't count
    # as N requests — entity-creation limits below cap actual writes.
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))

    # Per-task structural limits.
    for t in tasks_in:
        _raise_if_limit_exceeded(
            check_payload_size(
                t.task_data,
                limits_settings.max_task_data_bytes,
                ErrorCode.TASK_DATA_SIZE_LIMIT,
                "task_data",
            )
        )
        _raise_if_limit_exceeded(
            check_structural_limit(
                len(t.dependency_task_ids),
                limits_settings.max_dependency_ids_per_task,
                ErrorCode.DEPENDENCY_COUNT_LIMIT,
                "dependency_task_ids",
            )
        )

    # Auth/build check first — a probe with a bogus build_id mustn't
    # consume rate-limit budget or fingerprint the workspace's 24h
    # creation limits via timing or error type.
    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    # Each (post-dedup) task produces exactly one event in this build.
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db,
            auth.workspace_id,
            "events",
            limits_settings,
            amount=len(tasks_in),
        )
    )

    # Pre-query which task_ids already exist in this environment so the
    # 24h tasks limit only counts brand-new task creations.
    #
    # Note: this estimate is computed without ``FOR UPDATE`` and may
    # diverge from the actual new-task count if a concurrent transaction
    # inserts the same ``task_id`` between this SELECT and the bulk
    # INSERT below. In that race we'd skip the limit check for what
    # turns out to be 0 new rows — strictly an under-counting of the
    # actual writes, never an over-counting, so the guard rail stays
    # safe.
    existing_tasks_result = await db.execute(
        select(Task)
        .where(Task.environment_id == build.environment_id)
        .where(Task.task_id.in_([t.task_id for t in tasks_in]))
    )
    existing_tasks: dict[str, Task] = {
        t.task_id: t for t in existing_tasks_result.scalars().all()
    }
    new_task_count_estimate = sum(
        1 for t in tasks_in if t.task_id not in existing_tasks
    )
    if new_task_count_estimate:
        _raise_if_limit_exceeded(
            await check_entity_creation_limit(
                db,
                auth.workspace_id,
                "tasks",
                limits_settings,
                amount=new_task_count_estimate,
            )
        )

    # Lock existing rows in **sorted task_id order** so that two
    # concurrent bulk calls hitting overlapping cached tasks acquire
    # locks in the same order and can't deadlock on each other. We only
    # need FOR UPDATE on rows we'll mutate (phantom upgrades) or on rows
    # whose denormalised ``latest_*`` columns the transition
    # touches — i.e., existing rows. Brand-new rows are inserted
    # without a competing writer, no lock needed.
    if existing_tasks:
        sorted_existing_ids = sorted(existing_tasks.keys())
        await db.execute(
            select(Task)
            .where(Task.environment_id == build.environment_id)
            .where(Task.task_id.in_(sorted_existing_ids))
            .order_by(Task.task_id.asc())
            .with_for_update()
        )

    # Phase 1: insert new tasks + upgrade phantoms in-memory.
    # Maintain ``pk_by_task_id`` so we can resolve dependency_task_ids
    # in Python without per-task SELECTs, and keep ORM instances in
    # ``db_task_by_task_id`` so we can apply events to them after
    # flushing.
    now = utc_now()
    pk_by_task_id: dict[str, UUID] = {t_id: t.id for t_id, t in existing_tasks.items()}
    db_task_by_task_id: dict[str, Task] = dict(existing_tasks)

    new_task_count = 0
    for t in tasks_in:
        if t.task_id in existing_tasks:
            db_task = existing_tasks[t.task_id]
            if db_task.is_phantom:
                # Phantom upgrade: real task data overrides the
                # ``tid[:12]`` placeholder. ``latest_status_at`` and
                # ``latest_status_event_id`` are deliberately left alone,
                # and they stay NULL: a phantom row is in
                # ``existing_tasks``, so the event below is
                # TASK_REFERENCED, which ``_apply_event_to_task`` treats as
                # purely informational and which writes no ``latest_*`` at
                # all.
                #
                # (This comment used to claim a TASK_PENDING apply
                # refreshed them. It does not — the event is never
                # TASK_PENDING for a row that already exists — so nothing
                # was refreshing anything. Recorded because the wrong
                # version reads as a reason *not* to add an explicit reset,
                # and the right version says only that nothing needs one
                # today.)
                db_task.task_namespace = t.task_namespace
                db_task.task_name = t.task_name
                db_task.task_data = t.task_data
                db_task.version = t.version
                db_task.output_uri = t.output_uri
                db_task.is_phantom = False
        else:
            # Pre-generate the UUID7 PK so we can resolve dep edges
            # below without an extra round-trip after flush.
            new_pk = generate_uuid7()
            db_task = Task(
                id=new_pk,
                task_id=t.task_id,
                environment_id=build.environment_id,
                task_namespace=t.task_namespace,
                task_name=t.task_name,
                task_data=t.task_data,
                version=t.version,
                output_uri=t.output_uri,
            )
            db.add(db_task)
            pk_by_task_id[t.task_id] = new_pk
            db_task_by_task_id[t.task_id] = db_task
            new_task_count += 1

    # Single flush: SQLAlchemy with asyncpg batches INSERTs of the same
    # entity type into one round-trip when their primary keys are
    # client-generated (which our UUID7 PKs are). For a 1000-task batch
    # this is the difference between 1000 sequential round-trips and
    # one ``executemany``.
    if new_task_count:
        await db.flush()

    # Phase 2: bulk-reconcile dependency edges.
    # Collect all upstream task_ids referenced anywhere in the batch.
    all_upstream_ids: set[str] = set()
    for t in tasks_in:
        all_upstream_ids.update(t.dependency_task_ids)
    # Subtract task_ids already in our map (in-batch deps + pre-existing).
    unknown_upstream_ids = all_upstream_ids - set(pk_by_task_id.keys())
    if unknown_upstream_ids:
        # Look up unknown upstreams in DB — they may be tasks that
        # exist in the env but weren't part of this batch.
        unknown_lookup_result = await db.execute(
            select(Task.id, Task.task_id)
            .where(Task.environment_id == build.environment_id)
            .where(Task.task_id.in_(unknown_upstream_ids))
        )
        for pk, t_id in unknown_lookup_result.all():
            pk_by_task_id[t_id] = pk
        still_unknown = unknown_upstream_ids - set(pk_by_task_id.keys())
        if still_unknown:
            # Safety hatch: edges referencing a task not in the batch
            # *and* not in the DB. Phantom-create. With the SDK's
            # post-order discover walk this should not happen in normal
            # operation; documented in ``_reconcile_dependency_edges``.
            phantom_rows = [
                {
                    "id": generate_uuid7(),
                    "task_id": tid,
                    "environment_id": build.environment_id,
                    "task_namespace": "",
                    "task_name": tid[:12],
                    "task_data": {},
                    "is_phantom": True,
                    "created_at": now,
                    "latest_status": TaskStatus.PENDING,
                    "latest_waiting_for_lock": False,
                }
                for tid in still_unknown
            ]
            await db.execute(
                pg_insert(Task)
                .values(phantom_rows)
                .on_conflict_do_nothing(constraint="uq_task_environment_taskid")
            )
            # Re-fetch (ON CONFLICT DO NOTHING + RETURNING only returns
            # rows we inserted; a concurrent writer may have created
            # the row first and we still need its PK).
            refetch_result = await db.execute(
                select(Task.id, Task.task_id)
                .where(Task.environment_id == build.environment_id)
                .where(Task.task_id.in_(still_unknown))
            )
            for pk, t_id in refetch_result.all():
                pk_by_task_id[t_id] = pk

    # Build edge rows for the whole batch and bulk-insert in one shot.
    edge_rows: list[dict[str, object]] = []
    for t in tasks_in:
        if not t.dependency_task_ids:
            continue
        downstream_pk = pk_by_task_id[t.task_id]
        # Deduplicate within a single task's dep list (the schema
        # constraint allows it, but emitting duplicates is wasteful).
        seen_in_task: set[str] = set()
        for upstream_id in t.dependency_task_ids:
            if upstream_id in seen_in_task:
                continue
            seen_in_task.add(upstream_id)
            edge_rows.append(
                {
                    "id": generate_uuid7(),
                    "upstream_task_id": pk_by_task_id[upstream_id],
                    "downstream_task_id": downstream_pk,
                    "is_dynamic": False,
                    "created_at": now,
                }
            )
    if edge_rows:
        await db.execute(
            pg_insert(TaskDependency)
            .values(edge_rows)
            .on_conflict_do_nothing(constraint="uq_task_dependency_edge")
        )

    # Phase 3: bulk-insert events with explicit per-event timestamps so
    # that ``list_tasks_in_build`` can order tasks by per-build first
    # event in array order. (The endpoint joins against
    # ``min(events.created_at)`` — if every event in this batch shared
    # one timestamp, ordering would fall to ``Task.id``, which is
    # unstable for re-referenced cached tasks whose UUID7 came from an
    # earlier build.)
    events: list[Event] = []
    for i, t in enumerate(tasks_in):
        already_existed = t.task_id in existing_tasks
        events.append(
            Event(
                id=generate_uuid7(),
                build_id=build_id,
                task_id=pk_by_task_id[t.task_id],
                event_type=EventType.TASK_REFERENCED
                if already_existed
                else EventType.TASK_PENDING,
                created_at=now + timedelta(microseconds=i),
            )
        )
    # Plan-time concurrency-limit keys (STA-14). Recorded here so the
    # server knows which *pending* tasks want a key — the relation a slot
    # release needs to wake the builds queued on it, and one it can learn
    # nowhere else (keys come from a deployed-app callable). Rows for a
    # task without a live claim are inert for occupancy: every reader
    # joins them to ``live_claim_filter()``. Replace semantics, per task,
    # only when the caller supplied keys; a RUNNING task keeps the keys it
    # was started under, since those are what it currently occupies.
    await _replace_limit_keys(
        db,
        {
            pk_by_task_id[t.task_id]: t.limit_keys or []
            for t in tasks_in
            if t.limit_keys is not None
            and db_task_by_task_id[t.task_id].latest_status != TaskStatus.RUNNING
        },
    )

    if events:
        # Every event above carries an explicit ``id`` and ``created_at``,
        # which is all the apply reads, so ``transition_task`` skips the
        # flush of its own accord and a 500-task plan stays one round trip.
        # Registration events are status-neutral, so the transition hooks
        # run and cost nothing — pinned by
        # ``test_bulk_registration_flags_nobody``.
        for t, ev in zip(tasks_in, events):
            await transition_task(db, db_task_by_task_id[t.task_id], ev)

    await _close_plan_over_dependencies(
        db,
        build_id=build_id,
        task_pks=[t.id for t in db_task_by_task_id.values()],
    )

    # One final flush + commit at the end. Earlier flushes (just the
    # task INSERT batch) populated the rows we need to FK against.
    await db.commit()

    # Update entity-count cache for in-process limit tracking. Done
    # before response construction since the slim-response path skips
    # the ORM column reads.
    for _ in range(len(tasks_in)):
        record_entity_created(auth.workspace_id, "events")
    for _ in range(new_task_count):
        record_entity_created(auth.workspace_id, "tasks")

    if id_only:
        # Slim path — echo the (id ↔ task_id) mapping plus the current
        # global status/executor-ref so the SDK's build engine can
        # re-attach to detached executions that are still running (no
        # task_data, namespace or timestamps).
        return TaskBulkIdOnlyResponse(
            tasks=[
                BulkTaskIdRef(
                    id=(db_task := db_task_by_task_id[t.task_id]).id,
                    task_id=t.task_id,
                    latest_status=db_task.latest_status,
                    latest_executor=db_task.latest_executor,
                    latest_executor_ref=db_task.latest_executor_ref,
                    latest_executor_metadata=db_task.latest_executor_metadata,
                )
                for t in tasks_in
            ]
        )

    # Default: full TaskResponse for each task in array order.
    return TaskBulkResponse(
        tasks=[
            TaskResponse(
                id=(db_task := db_task_by_task_id[t.task_id]).id,
                task_id=db_task.task_id,
                environment_id=db_task.environment_id,
                task_namespace=db_task.task_namespace,
                task_name=db_task.task_name,
                task_data=db_task.task_data,
                version=db_task.version,
                output_uri=db_task.output_uri,
                created_at=db_task.created_at,
                is_phantom=db_task.is_phantom,
                latest_executor=db_task.latest_executor,
                latest_executor_ref=db_task.latest_executor_ref,
                latest_executor_metadata=db_task.latest_executor_metadata,
                latest_status=db_task.latest_status,
                latest_status_at=db_task.latest_status_at,
                latest_status_build_id=db_task.latest_status_build_id,
            )
            for t in tasks_in
        ]
    )


@router.post("/{build_id}/tasks/{task_id}/start", response_model=TaskEventResponse)
async def start_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
    executor: str | None = None,
    executor_ref: str | None = None,
    executor_metadata: str | None = None,
    limit_key: Annotated[list[str] | None, Query()] = None,
    enforce_limits: bool = False,
    claim: bool = False,
    claim_ttl_seconds: Annotated[
        int | None,
        Query(
            ge=MIN_CLAIM_TTL_SECONDS,
            le=MAX_CLAIM_TTL_SECONDS,
            description=(
                "How long this execution's claim on the task stays "
                "believable, in seconds. Recorded as "
                "`tasks.latest_status_expires_at`; once past, the task is "
                "claimable again by anyone. Set it to the executor's own "
                "timeout plus a small grace — it is NOT a lease and nothing "
                "renews it mid-execution. Omitted: the server default."
            ),
        ),
    ] = None,
):
    """Mark a task as started within a build.

    Args:
        executor: Name of the execution backend running the task (e.g.
            ``"modal"``) for detached executions.
        executor_ref: Backend-specific reference to the detached execution
            (e.g. a Modal function call id). Recorded in the event metadata
            and denormalised onto the task so a resumed build can re-attach
            to a still-running execution instead of re-executing.
        executor_metadata: Optional JSON-encoded dict describing the
            execution backend (e.g. Modal app/workspace/environment/
            function). Recorded in the event metadata and denormalised to
            ``tasks.latest_executor_metadata`` with the same set/clear-on-
            every-start semantics as ``executor_ref``.
        limit_key: Named concurrency-limit keys this task runs under
            (repeatable). Recorded so the task's RUNNING status occupies one
            slot per key.
        enforce_limits: Atomically check every ``limit_key`` with a
            configured environment limit before starting: if any is at
            capacity the start is rejected with **409** and error code
            ``concurrency_limit_reached`` (no event recorded). The
            environment's limit rows are locked for the duration of the
            check, serializing concurrent acquires.
        claim: Atomic per-task execution claim: reject the start with
            **409** when the task already holds a *live* claim (error code
            ``task_already_running``, echoing the running execution's
            ``executor``/``executor_ref`` so the caller can re-attach, and
            its ``latest_status_expires_at``) or is already COMPLETED
            (``task_already_completed``). The check runs on the
            FOR-UPDATE-locked task row inside the start transaction, so
            concurrent claiming starts serialize — at most one wins. A
            denied claim records nothing (no event, no concurrency-limit
            slots). A claim whose expiry has passed denies nothing: this
            start takes it over, replacing the previous holder's build,
            executor fields and expiry together.
        claim_ttl_seconds: Lifetime of the claim this start grants, from
            the event's timestamp. Written to
            ``tasks.latest_status_expires_at`` and echoed in the event
            metadata; outside [``MIN_CLAIM_TTL_SECONDS``,
            ``MAX_CLAIM_TTL_SECONDS``] the request is rejected with **422**.
            Applies to *every* start, not only claiming ones: RUNNING is
            the claim however it was recorded, and a start that granted no
            expiry would be exactly the wedge this exists to end. Omitted
            → ``ClaimSettings.default_ttl_seconds``.
    """
    parsed_executor_metadata = _parse_executor_metadata_param(executor_metadata)
    limit_keys = list(dict.fromkeys(limit_key)) if limit_key else None
    if enforce_limits and limit_keys:
        denied = await _check_concurrency_limits(db, auth, task_id, limit_keys)
        if denied:
            raise HTTPException(
                status_code=409,
                detail={
                    "error_code": "concurrency_limit_reached",
                    "denied_keys": denied,
                },
            )

    extra_metadata: dict | None = None
    if (
        executor is not None
        or executor_ref is not None
        or parsed_executor_metadata is not None
        or limit_keys
        or claim_ttl_seconds is not None
    ):
        extra_metadata = {}
        if executor is not None:
            extra_metadata["executor"] = executor
        if executor_ref is not None:
            extra_metadata["executor_ref"] = executor_ref
        if parsed_executor_metadata is not None:
            extra_metadata["executor_metadata"] = parsed_executor_metadata
        if limit_keys:
            extra_metadata["limit_keys"] = limit_keys
        if claim_ttl_seconds is not None:
            # Carried on the event, not passed alongside it: the expiry is
            # derived in _apply_event_to_task from the event that granted it,
            # so what the caller asked for stays auditable and a replay of
            # the stream reproduces the same expiry.
            extra_metadata["claim_ttl_seconds"] = claim_ttl_seconds
    return await _create_task_event(
        build_id,
        task_id,
        EventType.TASK_STARTED,
        db,
        auth,
        commit_hash=commit_hash,
        extra_metadata=extra_metadata,
        limit_keys=limit_keys,
        claim=claim,
    )


async def _check_concurrency_limits(
    db: AsyncSession,
    auth: SdkAuth,
    task_id: str,
    limit_keys: list[str],
) -> list[str]:
    """Return the limit keys that are at capacity (empty = all acquirable).

    Locks the environment's limit rows FOR UPDATE so concurrent acquires
    for the same keys serialize against each other; the lock is held until
    the caller's transaction commits (i.e. until the TASK_STARTED event —
    which occupies the slot — is durably recorded). Keys without a
    configured limit are unlimited. The task being started is excluded
    from the count, so re-starting a RUNNING task (e.g. re-recording an
    executor ref) never self-blocks.

    A slot is occupied by a *live* claim, not by the RUNNING string — the
    count uses the same predicate as the claim check
    (:func:`~stardag_api.services.claims.live_claim_filter`). The two have
    to agree: counting expired claims here would mean an abandoned task
    stops blocking its own re-execution while still consuming the cap it
    was admitted under, which is precisely the leak this expiry exists to
    stop, preserved in the one place nobody looks.
    """
    limits = (
        (
            await db.execute(
                select(EnvironmentConcurrencyLimit)
                .where(
                    EnvironmentConcurrencyLimit.environment_id == auth.environment_id,
                    EnvironmentConcurrencyLimit.key.in_(limit_keys),
                )
                # Deterministic lock order: concurrent acquires with
                # overlapping key sets must lock rows in the same order or
                # they can deadlock (same principle as the sorted-key
                # acquisition in the SDK's LocalConcurrencyLimiter).
                .order_by(EnvironmentConcurrencyLimit.key)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )
    denied: list[str] = []
    for limit in limits:
        active = (
            await db.execute(
                select(func.count(func.distinct(TaskLimitKey.task_pk)))
                .select_from(TaskLimitKey)
                .join(Task, TaskLimitKey.task_pk == Task.id)
                .where(
                    TaskLimitKey.key == limit.key,
                    Task.environment_id == auth.environment_id,
                    live_claim_filter(),
                    Task.task_id != task_id,
                )
            )
        ).scalar_one()
        if active >= limit.max_concurrent:
            denied.append(limit.key)
    return denied


@router.post("/{build_id}/tasks/{task_id}/complete", response_model=TaskEventResponse)
async def complete_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
):
    """Mark a task as completed within a build."""
    return await _create_task_event(
        build_id, task_id, EventType.TASK_COMPLETED, db, auth, commit_hash=commit_hash
    )


@router.post("/{build_id}/tasks/{task_id}/fail", response_model=TaskEventResponse)
async def fail_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    error_message: str | None = None,
    commit_hash: str | None = None,
):
    """Mark a task as failed within a build."""
    return await _create_task_event(
        build_id,
        task_id,
        EventType.TASK_FAILED,
        db,
        auth,
        error_message,
        commit_hash=commit_hash,
    )


@router.post("/{build_id}/tasks/{task_id}/interrupt", response_model=TaskEventResponse)
async def interrupt_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    reason: str | None = None,
    commit_hash: str | None = None,
):
    """Record that a task's execution was interrupted by the platform.

    An interruption is **not** a failure: the execution ended for a reason
    unrelated to the task's correctness — the backend hit its function
    timeout, or reclaimed the container — and the task is the scheduler's
    to start again. Reported by the worker itself, inside the grace window
    the platform gives it before the kill, which is what makes the claim
    and any concurrency-limit slots free up immediately instead of when
    something later notices the execution is gone.

    Why this is a separate route from ``/fail`` rather than a flag on it:
    a worker-recorded *failure* would be read by the next scheduler pass as
    a build-killing failure before anything could retry it (a tick avoids
    that only by recording and retrying inside one pass). A status that is
    not a failure cannot lose that race.

    ``reason`` is recorded like ``/fail``'s ``error_message`` — the same
    question gets asked of both — but does not set ``latest_completed_at``:
    an interruption is a pause, not an ending.
    """
    return await _create_task_event(
        build_id,
        task_id,
        EventType.TASK_INTERRUPTED,
        db,
        auth,
        reason,
        commit_hash=commit_hash,
    )


@router.post("/{build_id}/tasks/{task_id}/suspend", response_model=TaskEventResponse)
async def suspend_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
):
    """Mark a task as suspended (waiting for dynamic dependencies)."""
    return await _create_task_event(
        build_id, task_id, EventType.TASK_SUSPENDED, db, auth, commit_hash=commit_hash
    )


@router.post(
    "/{build_id}/tasks/{task_id}/dependencies",
    response_model=AddDependenciesResponse,
)
async def add_task_dependencies(
    build_id: UUID,
    task_id: str,
    request: AddDependenciesRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Register dependency edges for an existing task.

    Used by the SDK to record dynamically-yielded dependencies at runtime —
    deps that weren't known at ``task_register`` time because they come from
    a ``yield`` inside ``run()`` / ``run_aio()``. Static deps declared via
    ``requires()`` are registered in :func:`register_task` and don't use
    this endpoint.

    Creates phantom upstream tasks for unknown ``upstream_task_ids`` and
    inserts edges idempotently (``ON CONFLICT DO NOTHING``). The first write
    of a given edge sets ``is_dynamic``; subsequent writes do not overwrite.

    Returns:
        ``{"added": <new edges>, "total": <upstream_task_ids length>}``.
    """
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    _raise_if_limit_exceeded(
        check_structural_limit(
            len(request.upstream_task_ids),
            limits_settings.max_dependency_ids_per_task,
            ErrorCode.DEPENDENCY_COUNT_LIMIT,
            "upstream_task_ids",
        )
    )

    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    # Locate the downstream task. Scoped by environment, not by build —
    # a task may pre-exist from an earlier build in the same environment
    # and still be a valid target for new dynamic-edge records. Fails with
    # 404 if the task_id is unknown in this environment.
    result = await db.execute(
        select(Task)
        .where(Task.environment_id == build.environment_id)
        .where(Task.task_id == task_id)
    )
    db_task = result.scalar_one_or_none()
    if not db_task:
        raise HTTPException(
            status_code=404,
            detail=f"Task {task_id} not registered in this environment",
        )

    added = await _reconcile_dependency_edges(
        db=db,
        environment_id=build.environment_id,
        downstream_task_pk=db_task.id,
        upstream_task_ids=request.upstream_task_ids,
        is_dynamic=request.is_dynamic,
    )
    await db.commit()

    return AddDependenciesResponse(added=added, total=len(request.upstream_task_ids))


@router.post("/{build_id}/tasks/{task_id}/resume", response_model=TaskEventResponse)
async def resume_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
):
    """Mark a task as resumed (dynamic dependencies completed)."""
    return await _create_task_event(
        build_id, task_id, EventType.TASK_RESUMED, db, auth, commit_hash=commit_hash
    )


@router.post("/{build_id}/tasks/{task_id}/cancel", response_model=TaskEventResponse)
async def cancel_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
    only_if_held: Annotated[
        bool,
        Query(
            description=(
                "Record nothing unless this build still holds the task in "
                "RUNNING or INTERRUPTED — the statuses that have a live "
                "execution to revoke. For an engine cleaning up after "
                "itself from a list it read a moment ago: by the time it "
                "gets here another build may have reset the task and be "
                "about to run it. Answers 200 with the unchanged status "
                "rather than an error, since that is a normal outcome of "
                "the race rather than a fault."
            ),
        ),
    ] = False,
):
    """Cancel a task, releasing its execution claim and limit slots.

    **Only the build that put the task where it is may cancel it.** A task
    that is RUNNING, SUSPENDED or INTERRUPTED belongs to the build whose
    event produced that status, and a cancel from anyone else is refused
    with 409 ``not_claim_holder`` — see
    :func:`stardag_api.services.claims.may_revoke`. Everything else stays
    cancellable by any build in the environment: PENDING and the terminal
    statuses hold no claim.

    Operators reach a stranded claim the same way they always did, by
    passing the build from ``latest_status_build_id`` (``stardag tasks
    list`` prints it, and ``stardag tasks cancel`` has documented that
    argument as the claim holder since it shipped). What the guard removes
    is a build declaring somebody else's live worker dead.
    """
    return await _create_task_event(
        build_id,
        task_id,
        EventType.TASK_CANCELLED,
        db,
        auth,
        commit_hash=commit_hash,
        only_if_held=only_if_held,
    )


@router.post("/{build_id}/tasks/{task_id}/skip", response_model=TaskEventResponse)
async def skip_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
):
    """Skip a task that won't run (e.g. its dependency failed)."""
    return await _create_task_event(
        build_id, task_id, EventType.TASK_SKIPPED, db, auth, commit_hash=commit_hash
    )


@router.post("/{build_id}/tasks/{task_id}/retry", response_model=TaskEventResponse)
async def retry_task(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    commit_hash: str | None = None,
):
    """Reset a failed/cancelled/skipped/suspended task to pending (retry).

    Emits TASK_RETRIED; status derivation flips only retryable statuses
    back to PENDING. Used by reactive triggers so a re-triggered failed
    build (or a new build referencing a previously failed task) becomes
    schedulable again.

    **Suspended tasks are retryable.** A task suspended for dynamic
    dependencies is not executing — the execution yielded and returned —
    so a task whose orchestrator then died has no path forward except
    running again from scratch, which is what a retry means. Without this
    it would be permanently unschedulable.

    **Completed and running tasks are unaffected.** COMPLETED is sticky.
    RUNNING is excluded on purpose: it holds a live execution claim, and
    releasing that claim is cancellation (POST .../cancel), not retry —
    resetting it to PENDING would invite a second, concurrent execution of
    the same task. The event is recorded either way, which is what makes
    concurrent trigger/retry races benign.
    """
    return await _create_task_event(
        build_id, task_id, EventType.TASK_RETRIED, db, auth, commit_hash=commit_hash
    )


@router.post(
    "/{build_id}/tasks/{task_id}/waiting-for-lock", response_model=TaskEventResponse
)
async def task_waiting_for_lock(
    build_id: UUID,
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    lock_owner: str | None = None,
    commit_hash: str | None = None,
):
    """Record that a task is waiting for a global lock held by another build."""
    extra_metadata = {"lock_owner": lock_owner} if lock_owner else None
    return await _create_task_event(
        build_id,
        task_id,
        EventType.TASK_WAITING_FOR_LOCK,
        db,
        auth,
        commit_hash=commit_hash,
        extra_metadata=extra_metadata,
    )


@router.post(
    "/{build_id}/tasks/{task_id}/artifacts",
    response_model=TaskArtifactListResponse,
    status_code=201,
)
async def upload_task_artifacts(
    build_id: UUID,
    task_id: str,
    artifacts: list[TaskArtifactCreate],
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """Upload artifacts for a completed task.

    Artifacts are rich outputs like markdown reports or JSON data that
    can be viewed in the UI.

    Body format:
    - For markdown: {"content": "<markdown string>"}
    - For json: the actual JSON data dict
    """
    # Limit checks
    _raise_if_limit_exceeded(check_rate_limit(auth.workspace_id, limits_settings))
    for artifact in artifacts:
        _raise_if_limit_exceeded(
            check_payload_size(
                artifact.body,
                limits_settings.max_artifact_body_bytes,
                ErrorCode.ARTIFACT_BODY_SIZE_LIMIT,
                "artifact body",
            )
        )
    _raise_if_limit_exceeded(
        await check_entity_creation_limit(
            db, auth.workspace_id, "artifacts", limits_settings, amount=len(artifacts)
        )
    )

    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    # Verify build belongs to authenticated environment
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    # Find task by task_id (hash) in environment
    result = await db.execute(
        select(Task)
        .where(Task.environment_id == build.environment_id)
        .where(Task.task_id == task_id)
    )
    db_task = result.scalar_one_or_none()
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found")

    # Check artifacts-per-task limit (conservative: counts all submitted artifacts as new,
    # even if some may update existing artifacts - acceptable for guardrails)
    if limits_settings.max_artifacts_per_task is not None:
        existing_count_result = await db.execute(
            select(func.count())
            .select_from(TaskArtifact)
            .where(TaskArtifact.task_pk == db_task.id)
        )
        existing_count = existing_count_result.scalar() or 0
        _raise_if_limit_exceeded(
            check_structural_limit(
                existing_count + len(artifacts),
                limits_settings.max_artifacts_per_task,
                ErrorCode.ARTIFACTS_PER_TASK_LIMIT,
                "artifacts per task",
            )
        )

    created_artifacts = []
    new_artifact_count = 0
    for artifact in artifacts:
        # Check if artifact with same type and name already exists
        existing_result = await db.execute(
            select(TaskArtifact)
            .where(TaskArtifact.task_pk == db_task.id)
            .where(TaskArtifact.artifact_type == artifact.type)
            .where(TaskArtifact.name == artifact.name)
        )
        existing_artifact = existing_result.scalar_one_or_none()

        if existing_artifact:
            # Update existing artifact
            existing_artifact.body_json = artifact.body
            db_artifact = existing_artifact
        else:
            # Create new artifact
            db_artifact = TaskArtifact(
                task_pk=db_task.id,
                environment_id=build.environment_id,
                artifact_type=artifact.type,
                name=artifact.name,
                body_json=artifact.body,
            )
            db.add(db_artifact)
            new_artifact_count += 1

        await db.flush()
        created_artifacts.append(db_artifact)

    await db.commit()

    for _ in range(new_artifact_count):
        record_entity_created(auth.workspace_id, "artifacts")

    # Build response
    artifact_responses = [
        TaskArtifactResponse(
            id=db_artifact.id,
            task_id=db_task.task_id,
            artifact_type=db_artifact.artifact_type,
            name=db_artifact.name,
            body=db_artifact.body_json,
            created_at=db_artifact.created_at,
        )
        for db_artifact in created_artifacts
    ]

    return TaskArtifactListResponse(artifacts=artifact_responses)


@router.get("/{build_id}/tasks", response_model=list[TaskWithStatusResponse])
async def list_tasks_in_build(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """List all tasks in a build with their status.

    Statuses are global (events from all builds); ``attempt_count`` is
    per-build by construction — it answers "how many times did *this* build
    try since it was last resumed", which is what a UI or CLI showing a
    build wants, and what a global count could not express. See
    ``FrontierTaskRef.attempt_count`` for the counting rule and the round
    window.

    Requires authentication via API key or JWT token with environment_id.
    """
    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    # Verify build belongs to authenticated environment
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    # Order tasks by their first-event-in-this-build timestamp so the list
    # reflects per-build registration/discovery order — not Task.created_at,
    # which is the global "first ever seen in this environment" timestamp
    # and would surface previously-cached tasks at the top of every later
    # build. ``Task.id`` (UUID7, time-encoded) breaks the timestamp tie
    # deterministically. ``min(events.id)`` would be a more precise
    # tiebreaker but Postgres has no ``min(uuid)`` aggregate (unlike SQLite),
    # and the practical risk of two tasks having identical
    # ``min(events.created_at)`` is negligible.
    first_event_subquery = (
        select(
            Event.task_id.label("task_id"),
            func.min(Event.created_at).label("first_event_at"),
        )
        .where(Event.build_id == build_id)
        .where(Event.task_id.isnot(None))
        .group_by(Event.task_id)
        .subquery()
    )

    result = await db.execute(
        select(Task)
        .join(first_event_subquery, Task.id == first_event_subquery.c.task_id)
        .order_by(
            first_event_subquery.c.first_event_at.asc(),
            Task.id.asc(),
        )
    )
    tasks = result.scalars().all()
    task_ids = [t.id for t in tasks]

    # Get global statuses (considering events from ALL builds)
    statuses = await get_all_task_global_statuses(db, task_ids)

    # Get artifact counts per task
    artifact_counts: dict[UUID, int] = {}
    if task_ids:
        artifact_count_result = await db.execute(
            select(TaskArtifact.task_pk, func.count(TaskArtifact.id))
            .where(TaskArtifact.task_pk.in_(task_ids))
            .group_by(TaskArtifact.task_pk)
        )
        artifact_counts = {row[0]: row[1] for row in artifact_count_result.all()}

    # One grouped query for the whole page, mirroring artifact_counts above
    # (and never one per task).
    attempt_counts = await get_attempt_counts_in_build(db, build_id, task_ids)

    responses = []
    for task in tasks:
        (
            status,
            started_at,
            completed_at,
            error_message,
            status_build_id,
            waiting_for_lock,
            commit_hash,
        ) = statuses.get(
            task.id, (TaskStatus.PENDING, None, None, None, None, False, None)
        )
        responses.append(
            TaskWithStatusResponse(
                id=task.id,
                task_id=task.task_id,
                environment_id=task.environment_id,
                task_namespace=task.task_namespace,
                task_name=task.task_name,
                task_data=task.task_data,
                version=task.version,
                output_uri=task.output_uri,
                created_at=task.created_at,
                is_phantom=task.is_phantom,
                latest_executor=task.latest_executor,
                latest_executor_ref=task.latest_executor_ref,
                latest_executor_metadata=task.latest_executor_metadata,
                status=status,
                started_at=started_at,
                completed_at=completed_at,
                error_message=error_message,
                artifact_count=artifact_counts.get(task.id, 0),
                waiting_for_lock=waiting_for_lock,
                status_build_id=status_build_id,
                commit_hash=commit_hash,
                attempt_count=attempt_counts.get(task.id, 0),
            )
        )

    return responses


@router.get("/{build_id}/events", response_model=list[EventResponse])
async def list_build_events(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
):
    """List all events for a build.

    Requires authentication via API key or JWT token with environment_id.
    """
    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    # Verify build belongs to authenticated environment
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    result = await db.execute(
        select(Event).where(Event.build_id == build_id).order_by(Event.created_at.asc())
    )
    events = result.scalars().all()

    return [
        EventResponse(
            id=e.id,
            build_id=e.build_id,
            task_id=e.task_id,
            event_type=e.event_type,
            created_at=e.created_at,
            error_message=e.error_message,
            event_metadata=e.event_metadata,
        )
        for e in events
    ]


@router.get("/{build_id}/graph", response_model=TaskGraphExtendedResponse)
async def get_build_graph(
    build_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    auth: Annotated[SdkAuth, Depends(require_sdk_auth)],
    upstream_depth: Annotated[int, Query(ge=0, le=100)] = 0,
    downstream_depth: Annotated[int, Query(ge=0, le=100)] = 0,
    max_per_type_per_level: Annotated[int, Query(ge=1, le=200)] = 5,
    max_total_nodes: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> TaskGraphExtendedResponse:
    """Get the task graph for a build.

    Recursively traverses dependencies starting from tasks in the build.
    ``upstream_depth`` / ``downstream_depth`` control how far to traverse
    *beyond* the build boundary (both default 0 — just the build's own
    tasks are returned). ``max_per_type_per_level`` controls grouping:
    same-type tasks at the same traversal depth & status get collapsed
    into a single batch node when their count exceeds the threshold.
    Grouping applies regardless of traversal depth, including depth 0
    (so a build with many structurally-identical tasks renders tidily).

    Requires authentication via API key or JWT token with environment_id.
    """
    build = await db.get(Build, build_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    # Verify build belongs to authenticated environment
    if build.environment_id != auth.environment_id:
        raise HTTPException(
            status_code=403, detail="Build does not belong to this environment"
        )

    # Get distinct task IDs that have events in this build
    task_ids_subquery = (
        select(Event.task_id)
        .where(Event.build_id == build_id)
        .where(Event.task_id.isnot(None))
        .distinct()
        .scalar_subquery()
    )

    # Get all tasks by those IDs (IDs only — traverse_upstream re-fetches)
    result = await db.execute(select(Task.id).where(Task.id.in_(task_ids_subquery)))
    task_ids_list = [row[0] for row in result.all()]

    from stardag_api.services.graph import traverse_upstream

    return await traverse_upstream(
        db=db,
        environment_id=auth.environment_id,
        primary_task_pks=task_ids_list,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth,
        max_per_type_per_level=max_per_type_per_level,
        max_total_nodes=max_total_nodes,
    )
