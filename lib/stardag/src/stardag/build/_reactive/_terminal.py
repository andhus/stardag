from __future__ import annotations

import logging
import typing
from datetime import datetime, timezone
from typing import Sequence
from uuid import UUID

from stardag.build._base import (
    FailMode,
    TaskExecutorABC,
)
from stardag.build._task_store import BuildTaskStore
from stardag.exceptions import NotFoundError, is_missing_route_error
from stardag.registry import (
    BuildExecution,
    BuildFrontier,
    FrontierExternalBlocker,
    RegistryABC,
)

from stardag.build._reactive._budgets import _retry_allowed
from stardag.build._reactive._frontier_actions import (
    _INTERRUPTED_STATUS,
    _RUNNING_STATUSES,
    _TERMINAL_BUILD_STATUSES,
    _claim_has_lapsed,
    _load_task,
)

if typing.TYPE_CHECKING:
    from stardag.build._reactive._tick import TickConfig, TickSummary

logger = logging.getLogger(__name__)


def _format_age(seconds: float) -> str:
    """Render an age the way an operator reads it.

    The blocker message is the one a stalled build's owner acts on, and
    "RUNNING for 10889s" makes them do arithmetic before they can judge
    whether that is alarming. Coarse on purpose — nobody needs seconds
    once it has been running for hours.
    """
    seconds = int(max(0, seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m".replace(" 0m", "")
    return f"{seconds // 86400}d {(seconds % 86400) // 3600}h".replace(" 0h", "")


class _BlockerVerdict(typing.NamedTuple):
    """A blocker paired with the reason it landed in its bucket.

    The reason is carried rather than re-derived at rendering time so the
    message can say *why* a blocker is fatal ("its owning build is failed")
    without re-issuing the lookups the classification already made.
    """

    blocker: FrontierExternalBlocker
    # Short why-clause appended to this blocker's description.
    note: str


class _ExternalBlockers(typing.NamedTuple):
    """Partition of a frontier's ``blocked_by_external`` (see below)."""

    # RUNNING with a claim that has not lapsed: someone is executing it, and
    # their completion frees this build. Wait.
    executing: list[_BlockerVerdict]
    # Not running (so holding no claim, so carrying no expiry) and in a
    # status its owning build is still driving — that build is going to move
    # it. Wait.
    queued: list[_BlockerVerdict]
    # Nothing is going to move it and this build cannot reset it. Fail.
    fatal: list[_BlockerVerdict]
    # CANCELLED with attempt budget left: a revocation, not a verdict, so
    # this build's to run. Reset and schedule, don't fail — see
    # ``_classify_external_blockers``.
    recoverable: list[_BlockerVerdict]
    # A *result* (FAILED/SKIPPED), or a CANCELLED whose budget is spent: the
    # outcome belongs to ``fail_mode``, which already sees it in
    # ``status_counts``, so it must not influence the wait-or-fail decision.
    # Kept only to enrich the message.
    inert: list[_BlockerVerdict]

    @property
    def waiting(self) -> list[_BlockerVerdict]:
        """Every blocker this build should wait on rather than fail over."""
        return self.executing + self.queued


def _blocker_label(blocker: FrontierExternalBlocker) -> str:
    """``namespace.Name`` of a blocker (namespace is "" by default)."""
    if blocker.blocking_task_namespace:
        return f"{blocker.blocking_task_namespace}.{blocker.blocking_task_name}"
    return blocker.blocking_task_name


def _blocker_status_age_seconds(
    blocker: FrontierExternalBlocker, now: datetime
) -> float | None:
    """Seconds the blocker has been in its current status, None if unknown.

    Naive timestamps (a custom registry that drops the offset) are read as
    UTC rather than raising: this runs on the path that decides whether to
    fail a build, so a formatting quirk must not become a tick crash.
    """
    if blocker.blocking_status_at is None:
        return None
    status_at = blocker.blocking_status_at
    if status_at.tzinfo is None:
        status_at = status_at.replace(tzinfo=timezone.utc)
    return (now - status_at).total_seconds()


# Blocker statuses whose fate belongs to the build that owns them. None of
# them holds an execution claim, so the server clears the expiry with the
# claim and the owner's status is the only liveness evidence that exists:
# PENDING means that build has not scheduled it yet, SUSPENDED means that
# build is working through its dynamic dependencies, INTERRUPTED means the
# platform took its execution away and that build is due to start it again.
# All three are transient while the owner lives — and all three are a
# permanent wedge once it dies.
#
# INTERRUPTED belongs with these and NOT with the inert results: it is not
# a verdict on the task, so failing this build over it would be failing
# over a neighbour's timeout.
_OWNER_DRIVEN_STATUSES = ("pending", "suspended", "interrupted")


async def _classify_external_blockers(
    frontier: BuildFrontier,
    now: datetime,
    *,
    registry: RegistryABC,
    config: TickConfig,
) -> _ExternalBlockers:
    """Split the frontier's external blockers into reset / wait / fail / ignore.

    The frontier reports these only when the build has nothing actionable
    and nothing running — precisely the state the stuck-build check reads as
    "this build is dead". Each entry is an upstream of one of this build's
    tasks whose current status *another* build produced. The split decides,
    per blocker, whether anyone — this build included — is going to move it.

    **Plan membership is not one of the questions.** What decides is the
    blocker's *status*, because the status is what says whether it is a
    revocation, a result, or work in flight. A build's plan is closed under the
    dependency relation, so a gating upstream is *usually* this build's own
    task — but closure runs once, at registration, and an edge written after
    that is not in the plan. A concurrent build's worker yielding dynamic
    dependencies does exactly that, routinely. Such a blocker is still decided
    here, on the same evidence as any other, and the attempt budget is what
    stops the CANCELLED branch acting on one (see below). See
    ``docs/design/execution-claims-and-liveness.md``.

    **CANCELLED — reset it and run it.** A cancel releases the execution
    claim (that is the whole point of the fail-fast cascade) and leaves the
    task in a status nothing schedules. It revokes *permission to run*; it is
    not a verdict on the task, and permission is not build-scoped — a task in
    this build's plan is this build's to run, whatever build last touched it.
    Without this, one build's fail-fast became every overlapping build's
    failure. Bounded by the same per-task attempt budget an ordinary retry
    obeys, so a task that fails on every attempt cannot loop here. A blocker
    outside the plan excludes itself without a plan check: the server reports
    no attempt count for one, and a missing count refuses the retry (see
    :func:`_retry_allowed`).

    **FAILED / SKIPPED — leave them.** A failure is a *result*, and results
    belong to ``fail_mode``: FAIL_FAST has already failed the build on the
    same count, and CONTINUE means "finish what you can, then fail". A tick
    that reset them would override the policy the user chose, and would do it
    on nobody's request. They go to ``inert`` — named in the failure message,
    influencing nothing. A re-trigger, where the user *did* ask, resets the
    whole retryable set (see ``_RETRYABLE_STATUSES``).

    **RUNNING — read the claim's expiry.** A RUNNING task holds an execution
    claim, and the claim's expiry is the one piece of liveness evidence a
    third party can evaluate without probing an executor it has no access to:

    - live expiry → **wait** (treated like a concurrency-limit denial:
      return, don't fail; the blocker's completion wakes this scheduler).
    - lapsed expiry → **fail**. Not *presumed* abandoned: the server no
      longer honours the claim, has stopped counting it against concurrency
      limits, and will hand it to the next claimant.
    - no expiry (``None``) → **wait**, unbounded, logged as such. Chosen
      deliberately: ``None`` is the server's encoding of "never lapses"
      (older server, or a start predating the column), and reading missing
      evidence as death would fail builds whose blocker is perfectly alive —
      the exact spurious failure this path exists to remove. The window is
      self-closing (new starts all carry an expiry) and the escape hatch is
      a task cancel, which the log line names.

    **PENDING / SUSPENDED — ask the owning build** (``_OWNER_DRIVEN_STATUSES``).
    Neither holds a claim, so the server clears the expiry with it and there
    is nothing to read. The wedge is real — a task abandoned SUSPENDED blocks
    every downstream build — so this half still decides, the only way it can:

    - owning build still live → **wait**; it is going to move it. Without
      this a SUSPENDED shared task would fail every *other* build that
      depends on it while the owner is legitimately mid-flight through its
      dynamic dependencies, and a PENDING dynamic dependency of a healthy
      concurrent build would do the same.
    - owning build terminal → **fail**; nothing will move it.
    - ``blocking_status_build_id is None`` → **fail** without a lookup: no
      status-moving event was ever recorded against it, so there is nobody
      to ask.
    - owner status unresolvable (deleted build, unreachable registry, a
      registry that doesn't report it) → **fail**: unknown is not evidence
      of life, and a silent indefinite hang is the failure mode #208 exists
      to kill.

    So the owning-build lookup earns its place, for these statuses **only** —
    not out of caution but because it is the only evidence that exists for a
    status carrying no claim. What a live owner does not buy is a deadline: a
    build gone silent without transitioning is reaped server-side, not
    guessed at here from a task's age.

    **Why waiting on a SUSPENDED blocker is bounded**, and not the open-ended
    hang it looks like: SUSPENDED persists only while the owner progresses the
    dynamic dependencies it yielded, or while the owner is itself stuck on a
    RUNNING task — and a RUNNING task carries a claim with an expiry. Once
    that lapses the owner recovers or fails it, ``skip_blocked`` moves the
    suspended parent to SKIPPED, the owner goes terminal, and this build stops
    waiting. The wait ends on the same bound everything else here uses.
    """
    executing: list[_BlockerVerdict] = []
    queued: list[_BlockerVerdict] = []
    fatal: list[_BlockerVerdict] = []
    recoverable: list[_BlockerVerdict] = []
    inert: list[_BlockerVerdict] = []

    # Owning-build statuses resolved during THIS classification only. A wide
    # DAG stalled behind one build yields one blocker entry per blocked edge,
    # every one naming the same owner, so without the memo this would be N
    # requests for one answer. Deliberately not cached across calls: a later
    # pass must be able to see the owner go terminal.
    #
    # The whole lookup only happens on the stalled path — the frontier
    # populates blocked_by_external solely when the build has nothing
    # actionable and nothing running — and now only for the non-RUNNING
    # blockers within it, so a healthy build issues zero extra requests,
    # however often it polls. Please don't "optimise" this away on the
    # assumption that it runs per tick in steady state; it does not.
    owner_statuses: dict[UUID, str | None] = {}

    async def owner_status(owner_id: UUID) -> str | None:
        if owner_id not in owner_statuses:
            try:
                info = await registry.build_get_aio(owner_id)
                owner_statuses[owner_id] = info.status
            except Exception as e:
                # Swallowed on purpose: this is a diagnostic lookup on the
                # path that decides a build's fate, and an unreachable or
                # deleted owner must produce a precise failure, not an
                # exception out of the tick.
                logger.warning(
                    f"Could not resolve the status of build {owner_id}, which "
                    f"owns a task blocking this build: {e}"
                )
                owner_statuses[owner_id] = None
        return owner_statuses[owner_id]

    for blocker in frontier.blocked_by_external:
        if blocker.blocking_status == "cancelled":
            # A revocation, not a verdict: the cancel released the claim, and
            # a task in this build's plan is this build's to run whatever
            # build last touched it. Budget-bounded, and the budget also
            # excludes an out-of-plan blocker, which has no attempt count.
            if _retry_allowed(blocker.blocking_attempt_count, config.max_attempts):
                recoverable.append(
                    _BlockerVerdict(blocker, "cancelled, so this build's to reset")
                )
            else:
                inert.append(
                    _BlockerVerdict(
                        blocker,
                        "cancelled, but not resettable from here (its attempt "
                        "budget in this build is spent, or this build has no "
                        "attempts on it to count)",
                    )
                )
            continue

        if blocker.blocking_status in _RUNNING_STATUSES:
            # Two rows and no lookup: the claim says whether anyone is still
            # executing it. The owning build's status is not consulted even
            # when it is known — a live build proves nothing about one of its
            # claims, and a terminal one does not release them.
            expires_at = blocker.blocking_status_expires_at
            if _claim_has_lapsed(expires_at, now):
                fatal.append(
                    _BlockerVerdict(
                        blocker,
                        f"its execution claim lapsed at {expires_at}, so the "
                        "claim is abandoned and re-claimable — but a RUNNING "
                        "task is not schedulable until someone releases it",
                    )
                )
            elif expires_at is None:
                executing.append(
                    _BlockerVerdict(
                        blocker,
                        "another build claims to be executing it, and the "
                        "claim carries no expiry, so nothing here can show "
                        "it abandoned",
                    )
                )
            else:
                executing.append(
                    _BlockerVerdict(
                        blocker,
                        "another build is executing it under a claim live "
                        f"until {expires_at}",
                    )
                )
            continue

        if blocker.blocking_status not in _OWNER_DRIVEN_STATUSES:
            # A result — FAILED or SKIPPED — or a status no build drives at
            # all (an UNREGISTERED phantom, or one a future server adds).
            # Nothing is going to move it, but the decision is not this
            # path's to take: it is in this build's plan, so it is in
            # ``status_counts``, and ``fail_mode`` owns what happens to a
            # failure. Recorded for the message only.
            inert.append(
                _BlockerVerdict(
                    blocker,
                    "a result rather than a revocation, so this build's "
                    "fail_mode owns the outcome",
                )
            )
            continue

        # PENDING or SUSPENDED: no claim is held, so there is no expiry to
        # read. It moves only if the build owning its status is still going to
        # move it.
        owner_id = blocker.blocking_status_build_id
        if owner_id is None:
            fatal.append(
                _BlockerVerdict(
                    blocker,
                    "no build owns its status, so nothing has ever moved it",
                )
            )
            continue
        status = await owner_status(owner_id)
        if status is None:
            fatal.append(
                _BlockerVerdict(
                    blocker,
                    "its owning build's status is unknown (the lookup failed, "
                    "or this registry does not report it), which is not "
                    "evidence that anyone will run it",
                )
            )
        elif status in _TERMINAL_BUILD_STATUSES:
            fatal.append(_BlockerVerdict(blocker, f"its owning build is {status}"))
        else:
            # Includes an owner still PENDING: a build that has not started
            # yet may still start. A build that has gone silent *without*
            # transitioning is reaped server-side; a tick cannot tell the
            # difference from here and must not guess.
            queued.append(
                _BlockerVerdict(blocker, f"its owning build is still {status}")
            )

    return _ExternalBlockers(
        executing=executing,
        queued=queued,
        fatal=fatal,
        recoverable=recoverable,
        inert=inert,
    )


# How many blockers a log line or build error names before summarising the
# rest. The server already caps its list (hence blocked_by_external_
# truncated); this second cap keeps a build's error_message readable when a
# wide DAG is stalled behind a single upstream.
_MAX_REPORTED_BLOCKERS = 5


def _describe_blockers(
    verdicts: Sequence[_BlockerVerdict], now: datetime, truncated: bool
) -> str:
    """One-line, user-actionable rendering of blockers (names, not ids only).

    ``truncated`` is the frontier's ``blocked_by_external_truncated``: the
    server capped its list, so this must not read as an exhaustive account.
    """
    described = "; ".join(
        (
            f"task {verdict.blocker.task_id} is blocked by "
            f"{_blocker_label(verdict.blocker)} "
            f"({verdict.blocker.blocking_task_id}), "
            f"{verdict.blocker.blocking_status.upper()}"
            + (
                ""
                if (age := _blocker_status_age_seconds(verdict.blocker, now)) is None
                else f" for {_format_age(age)}"
            )
            + (
                f" under build {verdict.blocker.blocking_status_build_id}"
                if verdict.blocker.blocking_status_build_id is not None
                else " under no recorded build"
            )
            + f" — {verdict.note}"
        )
        for verdict in verdicts[:_MAX_REPORTED_BLOCKERS]
    )
    remaining = len(verdicts) - _MAX_REPORTED_BLOCKERS
    if remaining > 0:
        described += f"; and {remaining} more"
    if truncated:
        described += (
            "; the registry capped the blocker list, so there may be further "
            "blockers not shown"
        )
    return described


def _blocker_remedy(verdicts: Sequence[_BlockerVerdict]) -> str:
    """How to get out of it — the part the error used to lack.

    One remedy, because one covers it: re-triggering this build re-runs
    discovery, which closes the plan again over whatever edges exist *now* and
    resets the retryable set — so it reaches a blocker that was outside the
    plan as well as one inside it. The exception is a RUNNING blocker, which
    holds a claim no reset can take.

    Spelled in the **surfaces a user actually has** — the UI and the CLI — not
    as the REST routes underneath them. This text lands in a build's
    ``error_message``, read by someone whose build just died; a bare
    ``POST /api/v1/...`` leaves them to find a base URL, mint a token and
    assemble a body before they can act on it.

    The UI is named first because it cannot get the build id wrong: the
    scheduling panel addresses a claim action to the blocker's
    ``blocking_status_build_id``. That matters more than it looks. Any build in
    the environment is accepted by the route — it does not require the task to
    be in it — but the id given becomes the task's ``latest_status_build_id``,
    so cancelling under the *stuck* build makes that build the owner of the
    CANCELLED status. The frontier then stops reporting the task as an external
    blocker at all, so the very reset this message is steering towards never
    happens and the build has to be re-triggered anyway. Under the owner, the
    next tick resets it and runs it — which is why the CLI form spells the
    argument ``<owning-build-id>`` and says which build that is.
    """
    remedy = (
        "Re-trigger this build to reset the blocker and run it here — a "
        "trigger resets failed/cancelled/skipped/suspended tasks in the plan, "
        "which a mid-flight tick deliberately does not. Trigger it with this "
        "same build id: build_trigger(..., build_id=<this build>, "
        "reactive=True). A task-level Retry (in the UI, or 'stardag tasks "
        "retry') is not the same thing and will not do it"
    )
    if any(
        verdict.blocker.blocking_status in _RUNNING_STATUSES for verdict in verdicts
    ):
        remedy += (
            ". A blocker stuck RUNNING holds an execution claim no reset can "
            "take — and while a lapsed claim is re-claimable, no build is "
            "claiming it, so release it first: in the UI, open the blocking "
            "build's scheduling panel and use the blocker's 'Release claim' "
            "action, which addresses it to the owning build for you; from the "
            "CLI, 'stardag tasks cancel <owning-build-id> "
            "<blocking-task-id>'. It has to be the build that owns the blocker "
            "(named above), not this one — cancelling it under this build "
            "would make this build the owner of the cancelled status, which "
            "stops the next tick from picking the task up"
        )
    return remedy + "."


async def _handle_terminal(
    frontier: BuildFrontier,
    *,
    build_id: UUID,
    registry: RegistryABC,
    task_executor: TaskExecutorABC,
    task_store: BuildTaskStore,
    config: TickConfig,
    summary: TickSummary,
    denied_this_round: int = 0,
) -> str | None:
    """Evaluate terminal conditions; emit build events. Returns terminal status.

    Returning ``None`` means "not terminal — keep waiting", which covers
    both a build with work in flight and a build with nothing of its own to
    do that is legitimately waiting on another build (see
    :func:`_classify_external_blockers`).
    """
    if frontier.build_status in _TERMINAL_BUILD_STATUSES:
        if frontier.build_status == "cancelled":
            # Cancelled externally (e.g. UI): stop the running work.
            await _cancel_running(
                frontier, build_id, registry, task_executor, task_store, summary
            )
        return frontier.build_status

    counts = frontier.status_counts
    running = sum(counts.get(status, 0) for status in _RUNNING_STATUSES)
    failed = counts.get("failed", 0)

    if failed > 0 and config.fail_mode == FailMode.FAIL_FAST:
        await _cancel_running(
            frontier, build_id, registry, task_executor, task_store, summary
        )
        await _skip_blocked(registry, build_id, summary)
        await registry.build_fail_aio(
            build_id, f"{failed} task(s) failed (fail_mode=FAIL_FAST)"
        )
        return "failed"

    roots_known = len(frontier.roots) == len(frontier.root_task_ids) > 0
    if roots_known and all(r.latest_status == "completed" for r in frontier.roots):
        await registry.build_complete_aio(build_id)
        return "completed"

    if denied_this_round > 0:
        # Tasks denied by concurrency limits in THIS pass are waiting for
        # slots held possibly by OTHER builds (running == 0 here doesn't
        # mean the env is idle) — never declare the build stuck. Scoped to
        # the current pass: a cumulative count would keep suppressing the
        # stuck check long after the denied tasks have run. The watchdog
        # re-ticks periodically; same-build slot releases notify directly.
        return None

    # Note: spawns within this iteration imply frontier.actionable was
    # non-empty, so this check can't misfire on the pre-spawn snapshot.
    if not frontier.actionable and running == 0:
        # Nothing runnable and nothing *in this build* running. That is not
        # the same as "the build can't progress": dependency gating is
        # environment-global while these counts are build-scoped, so a task
        # some other build is executing gates this build's tasks while
        # showing up in neither. Ask the frontier which it is before
        # declaring the build dead (#208 A1) — the list is populated only in
        # this exact state, and is empty against servers predating it, in
        # which case everything below degrades to the old unconditional
        # failure.
        now = datetime.now(timezone.utc)
        truncated = frontier.blocked_by_external_truncated
        blockers = await _classify_external_blockers(
            frontier, now, registry=registry, config=config
        )

        # Recoverable (cancelled) blockers first: this build can reset them
        # and run them itself, so there is nothing to wait for and nothing to
        # fail on. Done before the wait/fail decision because it *removes*
        # the reason for both — the next tick finds them actionable.
        if blockers.recoverable:
            # Deduplicated, because the frontier reports one entry per
            # (blocked, blocker) *edge*: a shared upstream appears once for
            # every task of this build that depends on it, which is the normal
            # shape for the fan-out this path exists to unblock. Without the
            # dedupe a diamond retries the same task N times — the second call
            # hitting a row that is already PENDING, so it fails and logs — and
            # ``in_build_blockers_reset`` counts edges rather than tasks.
            # ``dict.fromkeys`` rather than a set: registration order is what
            # makes the log line's truncated list stable.
            reset_ids = list(
                dict.fromkeys(v.blocker.blocking_task_id for v in blockers.recoverable)
            )
            # Successes only, and the distinction is load-bearing rather
            # than cosmetic: the caller reads this counter as "the frontier
            # changed, act again immediately". Counting a reset that raised
            # would send the tick round the loop to re-read the same
            # blocker, fail the same reset and refresh its own linger
            # deadline — a hot loop for as long as the retry keeps failing,
            # which a transient registry error or an unsupported route is
            # enough to produce.
            reset = 0
            for task_id in reset_ids:
                try:
                    await registry.task_retry_by_id_aio(build_id, task_id)
                    reset += 1
                except Exception as e:
                    # Best-effort: another tick may have reset it already, or
                    # completed it outright. Either way the next frontier read
                    # tells the truth, and failing the build over a lost race
                    # is the outcome this whole path exists to avoid.
                    logger.warning(f"Could not reset in-build blocker {task_id}: {e}")
            summary.in_build_blockers_reset += reset
            logger.info(
                f"Build {build_id}: reset {len(reset_ids)} cancelled blocker(s) "
                f"in this build's own plan so this build can run them: "
                f"{', '.join(reset_ids[:_MAX_REPORTED_BLOCKERS])}"
            )
            return None
        waiting = blockers.waiting
        summary.external_blockers += len(frontier.blocked_by_external)
        summary.external_blockers_waited += len(waiting)
        summary.external_blockers_fatal += len(blockers.fatal)

        if waiting and not blockers.fatal:
            # Waiting on work another build is doing or is about to do — the
            # same call the denied_this_round branch above makes, and for the
            # same reason: running == 0 here does not mean the environment is
            # idle. The blocker's completion wakes this scheduler; a lost
            # wake-up is covered by the watchdog. Logged every pass on
            # purpose: a build that sits here needs to be diagnosable from
            # the tick logs alone.
            # A RUNNING blocker whose claim carries no expiry cannot be shown
            # abandoned from here, so this particular wait has no end the tick
            # can see. Called out every pass rather than silently: it is the
            # one shape where waiting is a choice rather than a reading, and
            # the operator is the only one who can break the tie (by
            # cancelling the blocker) or remove the shape (by upgrading the
            # server, after which new starts carry an expiry).
            if any(
                verdict.blocker.blocking_status in _RUNNING_STATUSES
                and verdict.blocker.blocking_status_expires_at is None
                for verdict in waiting
            ):
                bound_note = (
                    " (a RUNNING blocker's claim carries no expiry, so this "
                    "wait cannot be shown to end — cancel the blocking task "
                    "to release its claim)"
                )
            else:
                bound_note = ""
            # Spelled out as "executing" vs "queued in a live build" because
            # the two are different operational situations: one is work in
            # progress, the other is work another build has not started yet.
            held_by = " and ".join(
                part
                for part in (
                    f"{len(blockers.executing)} being executed elsewhere"
                    if blockers.executing
                    else "",
                    f"{len(blockers.queued)} queued in a build that is still live"
                    if blockers.queued
                    else "",
                )
                if part
            )
            logger.info(
                f"Build {build_id} has nothing runnable or running of its own "
                f"but is waiting on {len(waiting)} upstream task(s) owned by "
                f"other builds ({held_by}); waiting rather than failing"
                f"{bound_note}: {_describe_blockers(waiting, now, truncated)}"
            )
            return None

        await _skip_blocked(registry, build_id, summary)
        if blockers.fatal:
            # Precise, actionable failure: which task, its name, its status,
            # how long it has been in it, which build owns it, why that owner
            # is not going to move it — plus how to get it moving. The status
            # counts alone (the whole of the old message) point nowhere near
            # the cause when the cause is an upstream in a status no tick of
            # this build will touch.
            reason = (
                f"Build cannot progress: it has nothing runnable or running "
                f"of its own, and {len(blockers.fatal)} of its task(s) are "
                "blocked by an upstream that nothing is going to move "
                f"(status counts: {counts}). Blocked by: "
                f"{_describe_blockers(blockers.fatal, now, truncated)}"
                f". {_blocker_remedy(blockers.fatal)}"
            )
        else:
            # Nothing to wait for and nothing fatal: genuinely stuck (failed
            # deps in CONTINUE mode, a lost task pickle, a blocker whose
            # status is a result this tick will not override). Fail rather
            # than idle forever — naming the inert blockers, whose role in it
            # the status counts do not reveal.
            reason = (
                "No runnable or running tasks left but roots are not "
                f"complete (status counts: {counts})"
            )
            if blockers.inert:
                reason += ". Blocked by: " + _describe_blockers(
                    blockers.inert, now, truncated
                )
                reason += f". {_blocker_remedy(blockers.inert)}"
        logger.error(f"Failing build {build_id}: {reason}")
        await registry.build_fail_aio(build_id, reason)
        return "failed"

    return None


async def _skip_blocked(
    registry: RegistryABC, build_id: UUID, summary: TickSummary
) -> None:
    """Mark tasks transitively blocked by failures as skipped (best-effort).

    Cosmetic-but-important: without it, blocked tasks dangle PENDING in the
    registry/UI forever while the build shows failed. Old servers without
    the endpoint are tolerated (missing-route 404 → skip silently omitted);
    app-level 404s (e.g. the build no longer exists) are re-raised — they
    signal a registry inconsistency the tick must not paper over.
    """
    try:
        skipped = await registry.build_skip_blocked_aio(build_id)
        summary.skipped += len(skipped)
    except NotFoundError as e:
        if not is_missing_route_error(e):
            raise
        logger.warning(
            "Registry server does not support skip-blocked; tasks blocked "
            "by the failure will remain pending."
        )
    except Exception as e:
        logger.warning(f"Failed to skip blocked tasks for build {build_id}: {e}")


async def _cancel_running(
    frontier: BuildFrontier,
    build_id: UUID,
    registry: RegistryABC,
    task_executor: TaskExecutorABC,
    task_store: BuildTaskStore,
    summary: TickSummary,
) -> None:
    """Stop the detached executions **this build** is responsible for.

    Authority to revoke is build-scoped (see the execution-claims design
    note). What this function may act on is therefore not "everything that
    looks alive in the frontier" but "the executions this build started and
    has not seen end" — which the registry answers from its event log
    (``GET .../executions``), with the backend and ref needed to stop each.

    Reading it from the frontier instead was wrong in both directions, and
    both cost real damage:

    - ``running`` is every RUNNING task in the build's *plan*, and after
      plan closure that includes tasks another build claimed and is
      executing. Cancelling one killed a live worker and released its
      claim, so the task was handed to a third build while the second was
      still writing its target. A cancelled build did this on *every* tick
      it received, for as long as neighbours kept touching its tasks.
    - A cascading build cancel writes TASK_CANCELLED for the claims this
      build held — releasing them — while the containers keep running. Such
      a task is in neither ``running`` nor ``actionable``, so nothing
      reached it, and ``cancel_detached`` has exactly one caller: this one.
      The claim was released and the execution was not stopped, which is
      how two builds came to run the same task at once.

    Asking about the task's *current* state does not fix the second one
    either, which a live run had to demonstrate: the whole point of
    releasing the claim is that the next build may take the task over, and
    it did so three seconds later — long before the cancelled build's tick
    ran. By then the task row named the new execution. The ref this build
    recorded when it started the task is the only thing that stays true,
    and cancelling it cannot touch anybody else's container.

    Each stopped execution is also recorded as TASK_CANCELLED, unless the
    registry already shows it cancelled — a worker killed by the backend
    cannot reliably self-report, and without the event the task dangles
    RUNNING, keeping its pending descendants out of the skip-blocked
    closure and holding its concurrency-limit slots forever.

    Cancelling is best-effort throughout and idempotent at the backend, so
    a ref stopped twice is harmless. What must not happen is stopping one
    this build does not own.
    """
    for item in await _executions_to_stop(frontier, build_id, registry, summary):
        task = await _load_task(item.task_id, registry, task_store, quiet=True)
        if task is None:
            continue
        try:
            await task_executor.cancel_detached(task, item.executor, item.executor_ref)
            summary.cancelled_refs += 1
        except Exception as e:
            logger.warning(
                f"Failed to cancel detached execution "
                f"{item.executor_ref!r} for task {item.task_id}: {e}"
            )
            continue
        try:
            # ``only_if_held``: the registry re-checks, on the locked row,
            # that this build still holds the task in a status with an
            # execution to revoke, and records nothing otherwise. Two things
            # need that, and neither is visible from the listing. A task the
            # cascade already cancelled needs no second event; and by now
            # another build may have reset this one and be about to run it,
            # where writing CANCELLED takes no claim but does stamp a
            # neighbour's freshly scheduled task dead — the exact churn this
            # whole pass exists to stop causing.
            await registry.task_cancel_aio(build_id, task, only_if_held=True)
        except Exception as e:
            logger.warning(f"Failed to record cancellation of task {item.task_id}: {e}")


async def _executions_to_stop(
    frontier: BuildFrontier,
    build_id: UUID,
    registry: RegistryABC,
    summary: TickSummary,
) -> list[BuildExecution]:
    """Ask the registry what is this build's to stop; fall back if it can't.

    The fallback is for a server predating the route, and it is the old
    frontier-derived list with the ownership filter the frontier can now
    support — ``latest_status_build_id``. A server old enough to lack
    *that* too reports None, and None cannot be read as "not mine": it
    means "this server cannot say", so the item is acted on exactly as
    before. That keeps an old server no worse off than it is today, which
    is the rule every version-skew decision here follows, while the two
    shapes only the route can see (a foreign claim on a server that does
    report the owner, and this build's own cascaded executions) are
    handled as soon as the server is new enough to know about them.
    """
    try:
        listed = await registry.build_get_executions_aio(build_id)
        if listed.truncated:
            logger.info(
                f"Build {build_id} has more executions to stop than the "
                "registry returns at once; stopping this batch and leaving "
                "the rest to the next pass."
            )
        return list(listed.executions)
    except NotFoundError as e:
        if not is_missing_route_error(e):
            raise
        logger.warning(
            "Registry server does not support the build executions route; "
            "falling back to the frontier, which cannot see executions this "
            "build has already cancelled."
        )
    except NotImplementedError:
        pass
    except Exception as e:
        logger.warning(
            f"Could not read the executions of build {build_id} ({e}); "
            "falling back to the frontier."
        )

    cancellable = _RUNNING_STATUSES + (_INTERRUPTED_STATUS,)
    # Re-read, because the snapshot the caller holds is the PRE-action one.
    # ``_act_on_frontier`` has already run by the time terminal handling
    # decides to cancel, so that snapshot can be wrong in both directions:
    # a task it resumed or spawned this pass is live under a ref the
    # snapshot has never seen, and a task the snapshot lists as INTERRUPTED
    # may now be RUNNING under a *different* ref.
    try:
        frontier = await registry.build_get_frontier_aio(build_id)
    except Exception as e:
        logger.warning(
            f"Could not re-read the frontier of build {build_id} before "
            f"cancelling ({e}); falling back to the pre-action snapshot, "
            "which may miss executions started in this pass."
        )
    items = list(frontier.running or [])
    seen = {item.task_id for item in items}
    items += [
        item
        for item in frontier.actionable
        if item.latest_status in cancellable and item.task_id not in seen
    ]
    executions: list[BuildExecution] = []
    for item in items:
        if item.latest_status not in cancellable:
            continue
        if item.latest_executor is None or item.latest_executor_ref is None:
            continue
        owner = item.latest_status_build_id
        if owner is not None and owner != build_id:
            logger.info(
                f"Not stopping the execution of task {item.task_id}: it is "
                f"held by build {owner}, not this one."
            )
            continue
        executions.append(
            BuildExecution(
                task_id=item.task_id,
                latest_status=item.latest_status,
                executor=item.latest_executor,
                executor_ref=item.latest_executor_ref,
                executor_metadata=item.latest_executor_metadata,
                latest_status_at=item.latest_status_at,
            )
        )
    return executions
