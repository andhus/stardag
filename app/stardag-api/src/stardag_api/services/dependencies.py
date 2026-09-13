"""Retracting dependency edges that no longer describe how a task is built.

**A dependency edge is evidence asserted by an act.** A static edge is
declared, in full, by every build that registers the task. A dynamic edge is
*discovered*: one execution attempt of the downstream task yielded this
upstream and suspended. The two therefore go stale for different reasons and
need different rules, and this module implements the second one — an edge
stops counting when the attempt that produced it is abandoned.

Nothing abandons a static edge, so nothing here touches one.

Why it is needed at all: dynamic edges are written ``ON CONFLICT DO
NOTHING``, so a task's set only ever grew across attempts. Usually invisible,
because a previous generation's children are COMPLETED and completed
upstreams do not gate. It bites when an attempt is abandoned with its
children incomplete — a cancelled build is the ordinary way — and then the
children gate *their own parent* forever: the task cannot be scheduled until
they complete, so the next build that wants it resets and re-runs a whole
generation of work the task is no longer going to ask for, before it ever
gets to re-yield.

**All of the attempt's edges are retracted, not only the incomplete ones.**
The completed children are just as much a statement about that attempt, and
retracting only the stragglers would leave a task whose recorded generation
is half one attempt and half another — which is not a state any rule can
reason about afterwards. Nothing is lost by retracting a completed child:
its target still exists, so if the next attempt yields it again it is
complete on arrival, and the edge comes back with the next yield.
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from stardag_api.models import Event, EventType, Task, TaskDependency, TaskStatus
from stardag_api.models.base import utc_now

logger = logging.getLogger(__name__)


async def retract_dynamic_edges_if_new_attempt(
    db: AsyncSession,
    task: Task,
    *,
    previous_status: TaskStatus,
    previous_status_build_id: UUID | None,
    event: Event,
    now: datetime | None = None,
) -> None:
    """Supersede ``task``'s dynamic edges when a fresh attempt begins.

    Two transitions mean that, and only these two:

    **A reset to PENDING.** The task will run again from the top, so
    whatever the last attempt yielded is not what the next one will need.
    This is also the transition that *has* to carry the retraction rather
    than the start that follows it: the stale children gate the task
    itself, so it can never reach a start while they are recorded. Reaching
    for the start instead would be a rule that cannot fire.

    **A start of a SUSPENDED task by a different build.** Taking over an
    abandoned suspension is a new attempt of the same shape — the worker
    re-runs the generator from the top — so the previous attempt's
    generation goes with it. Takeover itself stays unrestricted: handing a
    suspension between builds is pure benefit when the code is identical,
    which is the normal case, and retracting makes the cost of a takeover
    proportional to how much the code actually diverges.

    A build resuming *its own* suspension is the ordinary multi-round walk,
    not a new attempt: its generator advances past the batches it has
    already completed and yields the next one, which is exactly what the
    accumulated edges describe. Retracting there would throw away the
    record of a walk still in progress.

    No-ops for every other transition, and cheap when it does run: one
    UPDATE that matches nothing for the overwhelming majority of tasks,
    which have no dynamic edges at all.
    """
    if not _begins_new_attempt(
        task,
        previous_status=previous_status,
        previous_status_build_id=previous_status_build_id,
        event=event,
    ):
        return

    result = await db.execute(
        update(TaskDependency)
        .where(
            TaskDependency.downstream_task_id == task.id,
            TaskDependency.is_dynamic.is_(True),
            TaskDependency.superseded_at.is_(None),
        )
        .values(superseded_at=now or utc_now())
    )
    retracted = getattr(result, "rowcount", 0) or 0
    if retracted:
        logger.info(
            "Task %s begins a new attempt; retracted %d dynamic dependency "
            "edge(s) from the abandoned one.",
            task.task_id,
            retracted,
        )


def _begins_new_attempt(
    task: Task,
    *,
    previous_status: TaskStatus,
    previous_status_build_id: UUID | None,
    event: Event,
) -> bool:
    if event.event_type == EventType.TASK_RETRIED:
        # Gated on the task actually having moved: TASK_RETRIED is recorded
        # whether or not the status was retryable (which is what makes
        # concurrent trigger/retry races benign), and a retry that changed
        # nothing has abandoned nothing.
        return (
            task.latest_status == TaskStatus.PENDING
            and previous_status != TaskStatus.PENDING
        )
    if event.event_type == EventType.TASK_STARTED:
        # A NULL previous owner counts as a takeover, not as "mine". The
        # column is ``ON DELETE SET NULL``, so a suspension whose build has
        # been deleted lands here, as does a row predating the owner
        # backfill — and in both cases whoever is starting it now is not the
        # build that suspended it. Plan closure already reads a missing
        # owner as abandoned; treating it as present here would keep an
        # abandoned generation current while the other half of the rule
        # assumes it is gone.
        return (
            previous_status == TaskStatus.SUSPENDED
            and previous_status_build_id != event.build_id
        )
    return False
