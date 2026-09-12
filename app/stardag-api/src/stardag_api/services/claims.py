"""Liveness of the per-task execution claim.

``Task.latest_status == RUNNING`` *is* the execution claim — arbitrated in
``routes/builds.py::_create_task_event`` under ``SELECT … FOR UPDATE``, in
the same transaction as the event and as the concurrency-limit slot rows.
That single-row design is deliberate and its advantages are load-bearing
(one transaction so claim, status, completion and slot occupancy cannot
drift; no join on a frontier that is re-read every few seconds; zero
liveness traffic). See the execution-claims design note before changing
any of it.

Its one defect is what this module addresses: the claim recorded no
liveness evidence a **third party** could evaluate. This adds an expiry —
one nullable column, written once, no heartbeats — and the two predicates
that honour it. Both must, and that is the easy half to forget: if the
claim check honours the expiry but the concurrency-limit count does not,
an abandoned task stops blocking re-execution yet keeps occupying its
slots, and half the healing is lost.

The two predicates are the same rule expressed twice, once in Python (for
the FOR-UPDATE-locked row already in hand) and once in SQL (for counting
rows we do not want to load):

    RUNNING AND (expires_at IS NULL OR expires_at > <now>)

where ``<now>`` is **application** time (``utc_now()``), not the database's
``now()``. Deliberate, and worth knowing before "simplifying" it: the same
clock has to decide both predicates, and the Python one has no database
session to ask. The cost is that a skewed app server mis-times expiries by
its skew — immaterial against TTLs measured in hours.

The design note this module implements lands with the accompanying work
(``docs/design/execution-claims-and-liveness.md``); it is not on this
branch.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import ColumnElement, or_

from stardag_api.config import claim_settings
from stardag_api.models import Task, TaskStatus
from stardag_api.models.base import as_utc, utc_now


# Statuses in which a task belongs to the build whose event produced them.
#
# RUNNING holds the execution claim and the concurrency-limit slots.
# SUSPENDED is an execution that yielded for dynamic dependencies and will
# be resumed; INTERRUPTED is one the platform stopped. Neither holds a
# slot, but all three describe work a *particular* build set in motion, and
# only that build may revoke it — see :func:`may_revoke`.
#
# PENDING is deliberately absent: it holds nothing, and a task one build
# registered may be referenced by a live build elsewhere
# (TASK_REFERENCED leaves ``latest_status_build_id`` alone, so ownership
# scoping cannot tell the two apart).
BUILD_OWNED_STATUSES = (
    TaskStatus.RUNNING,
    TaskStatus.SUSPENDED,
    TaskStatus.INTERRUPTED,
)


def may_revoke(task: Task, build_id: UUID) -> bool:
    """Whether ``build_id`` is entitled to cancel ``task``.

    The design note's *authority to revoke is build-scoped*, enforced
    rather than assumed. Cancelling a task in a :data:`BUILD_OWNED_STATUSES`
    status releases its execution claim and its limit slots; doing that on
    behalf of a build that did not put it there declares somebody else's
    live worker dead, and hands their task to the next claimant while their
    container is still writing into it. ``cascade_cancel_build_tasks``
    already scoped itself this way; the per-task route did not, which is
    how a cancelled build came to kill a later build's executions.

    Everything else stays cancellable. PENDING and the terminal statuses
    hold no claim, so a cancel there is bookkeeping, and a neighbour reads
    CANCELLED as a revocation it may reset and run.

    A NULL ``latest_status_build_id`` is permitted too: the owning build
    row is gone (the FK is ``ON DELETE SET NULL``), so nobody is left to
    revoke it and refusing would strand the claim forever.
    """
    if task.latest_status not in BUILD_OWNED_STATUSES:
        return True
    owner = task.latest_status_build_id
    return owner is None or owner == build_id


def claim_ttl(ttl_seconds: int | None) -> int:
    """Resolve a claim TTL: the caller's, else the server default.

    Client-supplied wins because the caller is the only party that knows how
    long the execution it is about to spawn may legitimately take — a claim
    should outlive its execution by a small grace and no more. The server
    default (``ClaimSettings.default_ttl_seconds``) is the fallback for
    callers that say nothing, and is generous on purpose.
    """
    return claim_settings.default_ttl_seconds if ttl_seconds is None else ttl_seconds


def claim_expires_at(granted_at: datetime, ttl_seconds: int | None) -> datetime:
    """When a claim granted at ``granted_at`` stops being believable.

    Measured from the granting event's timestamp rather than from "now" so
    the stored expiry matches the event that produced it, and so replaying
    the event stream yields the same value it did the first time.
    """
    return as_utc(granted_at) + timedelta(seconds=claim_ttl(ttl_seconds))


def claim_is_live(task: Task, now: datetime | None = None) -> bool:
    """Whether ``task`` currently holds a believable execution claim.

    The Python half of the predicate, for a task row already loaded (and,
    at the one call site that matters, locked FOR UPDATE). An expired claim
    is *not* a distinct state a caller has to handle: it simply is not a
    claim, so the task is claimable again by whoever asks next.

    A NULL expiry counts as live, i.e. never lapses. That is not a hole
    left open for abandoned claims — the migration backfilled every row
    RUNNING at the time from its ``latest_status_at`` — but the honest
    answer for the residue nothing can date: a claim stamped by a server
    predating the column and not re-started since. There is no timestamp
    from which to conclude it is dead, so it is not this predicate's place
    to guess; releasing it stays an operator action.
    """
    if task.latest_status != TaskStatus.RUNNING:
        return False
    expires_at = task.latest_status_expires_at
    return expires_at is None or as_utc(expires_at) > (now or utc_now())


def live_claim_filter(now: datetime | None = None) -> ColumnElement[bool]:
    """SQL for :func:`claim_is_live`, for counting or listing claim holders.

    ``latest_status`` leads the expression so an index on it (or on
    ``(environment_id, latest_status, …)``) still drives the scan; the
    expiry comparison then filters the few rows that survive.

    Compares against application time, not the database's ``now()`` — see
    the module docstring for why both predicates must share one clock.
    """
    return (Task.latest_status == TaskStatus.RUNNING) & or_(
        Task.latest_status_expires_at.is_(None),
        Task.latest_status_expires_at > (now or utc_now()),
    )
