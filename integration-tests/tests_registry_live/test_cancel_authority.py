"""Cancelling a build stops its own work, and only its own work.

Two defects met here in production and neither is visible from one build.

A build was cancelled with ``--cascade``. That releases the claims it held
-- which is what lets the next build take those tasks over -- but nothing
stopped the containers, because the only caller of ``cancel_detached``
reads the frontier, and a cascaded task is CANCELLED and therefore in
neither ``running`` nor ``actionable``. So the old execution kept going
while a second build started its own copy of the same task: two executions
of one task id, the one thing the claim exists to prevent.

Then the cancelled build ticked again -- neighbours kept flagging it -- and
cancelled every RUNNING task in its *plan*. After plan closure that
includes tasks the second build had claimed, so it killed their containers
and released their claims. The second build recorded failures, retried,
and was killed again on the next tick; the loop never converged.

The scenario is both halves at once, because in production they were the
same incident:

1. A runs a slow shared task. Cancel A with ``--cascade``.
2. B is triggered, resets the cancelled task and runs it.
3. A is ticked again, deliberately, while B's copy is running.

What must then hold is that B's execution is untouched *and* that A's own
container is gone. The second is what the completion owner proves: both
containers sleep the same duration and A's started first, so if A's were
still alive it would complete the task before B's and COMPLETED is sticky.
An owner of A on that row means A's container outlived its cancel.

Against the code this fixes, step 3 kills B's container and step 1 leaves
A's alive -- so both assertions fail, from opposite directions.
"""

from __future__ import annotations

import uuid

import pytest

from stardag_integration_tests.registry_live._guard import registry_live_guard
from stardag_integration_tests.registry_live._wait import (
    describe,
    find_task,
    task_status,
    tick_summaries,
    wait_for_task_status,
    wait_for_terminal,
    wait_until,
)

registry_live_guard()

pytestmark = [
    pytest.mark.registry_live,
    pytest.mark.timeout(900),
]

# Long enough that the shared task is still RUNNING when A is cancelled and
# when B is ticked, and short enough that B's own copy finishes inside the
# scenario. It does NOT have to outlive B's takeover: if A's container
# survives its cancel it completes the task on its own schedule, and that
# is precisely what the completion-owner assertion catches.
SHARED_SLEEP_SECONDS = 60

# A only has to get the shared task started before it is cancelled.
A_LINGER_SECONDS = 30
# B has to see a 60s task through a container start, so it stays resident.
B_LINGER_SECONDS = 180

STATUS_TIMEOUT_SECONDS = 300
TICK_TIMEOUT_SECONDS = 180
BUILD_TIMEOUT_SECONDS = 600


def _owner(task_id: str):
    """The build whose event produced the task's current status."""
    return find_task(task_id, task_name="Slow").latest_status_build_id


def test_a_cancelled_build_stops_its_own_executions_and_no_others() -> None:
    from stardag.integration.modal._spawn import spawn_tick
    from stardag.registry import registry_provider
    from stardag_integration_tests.registry_live.dag_app import APP_NAME, app
    from stardag_integration_tests.registry_live.tasks import (
        get_range,
        get_sum,
        slow,
        square,
    )

    salt = uuid.uuid4().hex
    leaf = get_range(limit=4, salt=salt)
    shared = slow(values=leaf, seconds=SHARED_SLEEP_SECONDS)
    shared_id = str(shared.id)

    build_a = app.build_trigger(
        get_sum(integers=shared),
        reactive=True,
        tick_kwargs={"linger_seconds": A_LINGER_SECONDS, "poll_interval_seconds": 3},
    ).build_id

    wait_for_task_status(
        shared.id,
        expected="running",
        build_id=build_a,
        timeout=STATUS_TIMEOUT_SECONDS,
    )
    assert _owner(shared_id) == build_a, describe(build_a)

    # The cascade releases the claim. Stopping the container it belonged to
    # is not something the server can do -- it can only record that the
    # claim is gone -- so from here the execution is A's engine's to stop,
    # and a tick is the only thing that will.
    registry = registry_provider.get()
    cancelled = registry.build_cancel(build_a, cascade=True)
    assert cancelled is not None
    assert shared_id in cancelled.cascaded_task_ids, (
        "The cascade did not release the shared task's claim, so the rest "
        f"of this scenario cannot happen.\n{describe(build_a)}"
    )

    # B inherits the cancelled task: a revocation is not a result, so B
    # resets it and runs it. Reaching RUNNING under B is what proves the
    # hand-over happened at all.
    build_b = app.build_trigger(
        square(values=shared, offset=11),
        reactive=True,
        tick_kwargs={"linger_seconds": B_LINGER_SECONDS, "poll_interval_seconds": 3},
    ).build_id
    wait_for_task_status(
        shared.id,
        expected="running",
        build_id=build_b,
        timeout=STATUS_TIMEOUT_SECONDS,
    )
    assert _owner(shared_id) == build_b, describe(build_b)

    # Tick A again, while B's copy is running. In production this arrived
    # on its own -- a neighbour's drain hands a flagged build out, and A's
    # own workers kept re-flagging it -- but waiting for that would be
    # waiting on a race. Spawning it directly puts the interleaving under
    # test rather than hoping for it.
    ticks_before = len(tick_summaries(build_a))
    spawn_tick(build_a, APP_NAME)
    wait_until(
        lambda: len(tick_summaries(build_a)) > ticks_before,
        build_id=build_a,
        timeout=TICK_TIMEOUT_SECONDS,
        what=f"build {build_a} to report a tick after being cancelled",
    )

    status = task_status(shared.id)
    assert status in ("running", "completed"), (
        "A cancelled build ticked and revoked a task it does not hold. The "
        "execution belonged to another build, which is now running a task "
        "the registry has declared dead.\n"
        f"--- build A (cancelled) ---\n{describe(build_a)}\n"
        f"--- build B ---\n{describe(build_b)}"
    )
    assert _owner(shared_id) == build_b, (
        "The shared task's status is no longer B's doing, so the cancelled "
        "build rewrote it.\n" + describe(build_a)
    )

    status_b = wait_for_terminal(build_b, timeout=BUILD_TIMEOUT_SECONDS)
    assert status_b == "completed", (
        "Build B did not finish. Its executions were being cancelled out "
        "from under it by a build that no longer holds them.\n"
        f"--- build A (cancelled) ---\n{describe(build_a)}\n"
        f"--- build B ---\n{describe(build_b)}"
    )

    # The other half: A's own container really was stopped. Both sleep the
    # same duration and A's started first, so a surviving A would have
    # completed the task before B's copy and taken the row (COMPLETED is
    # sticky). B owning the completion is the evidence that it did not.
    assert _owner(shared_id) == build_b, (
        "The shared task was completed by the cancelled build, so its "
        "container outlived the cancel and ran to the end -- alongside the "
        "copy B was running at the same time.\n"
        f"--- build A (cancelled) ---\n{describe(build_a)}\n"
        f"--- build B ---\n{describe(build_b)}"
    )
