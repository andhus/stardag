"""Who may revoke a task, and what a build is left to stop.

Two halves of one rule. ``docs/design/execution-claims-and-liveness.md``
states that *authority to revoke is build-scoped*: cascade-cancel already
enforced it (``test_build_cleanup.test_cascade_never_cancels_another_builds_running_task``),
the per-task route did not, and a cancelled reactive build used the per-task
route to cancel every RUNNING task in its *plan* — including the ones a
later build had claimed and was executing.

The second half is what makes the first half safe to add: once a build may
only cancel what it owns, it needs to be told what that is. Its frontier
cannot say — ``running`` is plan-scoped, and a cascading build cancel moves
its own tasks to CANCELLED, out of both ``running`` and ``actionable``,
while their containers keep going.
"""

import pytest
from httpx import AsyncClient


def _register(task_id: str, deps: list[str] | None = None) -> dict:
    return {
        "task_id": task_id,
        "task_namespace": "",
        "task_name": "T",
        "task_data": {},
        "dependency_task_ids": deps or [],
    }


async def _new_build(client: AsyncClient, **body) -> str:
    return (await client.post("/api/v1/builds", json=body)).json()["id"]


async def _start(client: AsyncClient, build_id: str, task_id: str) -> None:
    await client.post(f"/api/v1/builds/{build_id}/tasks", json=_register(task_id))
    await client.post(
        f"/api/v1/builds/{build_id}/tasks/{task_id}/start",
        params={"executor": "modal", "executor_ref": f"fc-{task_id}"},
    )


async def _reference(client: AsyncClient, build_id: str, task_id: str) -> None:
    """Register an existing task under another build: TASK_REFERENCED, no
    status change — exactly what plan closure does."""
    await client.post(f"/api/v1/builds/{build_id}/tasks", json=_register(task_id))


async def _task_status(client: AsyncClient, task_id: str) -> str:
    return (await client.get(f"/api/v1/tasks/{task_id}")).json()["latest_status"]


async def _cancel(client: AsyncClient, build_id: str, task_id: str):
    return await client.post(f"/api/v1/builds/{build_id}/tasks/{task_id}/cancel")


async def _executions(client: AsyncClient, build_id: str) -> dict:
    response = await client.get(f"/api/v1/builds/{build_id}/executions")
    assert response.status_code == 200, response.text
    return response.json()


# ---------------------------------------------------------------------------
# authority to revoke
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_referencing_build_cannot_cancel_a_running_task(client: AsyncClient):
    """The reported bug, at the route: build B is executing the task, build A
    merely has it in its plan, and A's cancel would both kill B's container
    and release B's claim."""
    owner = await _new_build(client)
    await _start(client, owner, "shared")

    referencer = await _new_build(client)
    await _reference(client, referencer, "shared")

    response = await _cancel(client, referencer, "shared")
    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["error_code"] == "not_claim_holder"
    assert detail["latest_status_build_id"] == owner
    assert await _task_status(client, "shared") == "running"

    # The owner is refused nothing.
    assert (await _cancel(client, owner, "shared")).status_code == 200
    assert await _task_status(client, "shared") == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "status"), [("suspend", "suspended"), ("interrupt", "interrupted")]
)
async def test_ownership_covers_every_status_a_task_is_held_in(
    client: AsyncClient, route: str, status: str
):
    """Not just RUNNING. A suspension holds no claim, but it is a build's
    execution mid-flight all the same, and the cascade treats all three
    alike — the two definitions are now literally the same tuple."""
    owner = await _new_build(client)
    await _start(client, owner, "held")
    await client.post(f"/api/v1/builds/{owner}/tasks/held/{route}")
    assert await _task_status(client, "held") == status

    other = await _new_build(client)
    await _reference(client, other, "held")
    assert (await _cancel(client, other, "held")).status_code == 409
    assert await _task_status(client, "held") == status


@pytest.mark.asyncio
async def test_a_pending_task_is_cancellable_by_any_build(client: AsyncClient):
    """PENDING holds no claim and no execution, so nobody owns it. Refusing
    here would take away the one way to retire work nothing will run."""
    first = await _new_build(client)
    await client.post(f"/api/v1/builds/{first}/tasks", json=_register("idle"))

    other = await _new_build(client)
    await _reference(client, other, "idle")
    assert (await _cancel(client, other, "idle")).status_code == 200
    assert await _task_status(client, "idle") == "cancelled"


@pytest.mark.asyncio
async def test_a_terminal_task_is_cancellable_by_any_build(client: AsyncClient):
    """A cancel over a failure is bookkeeping, not revocation — and a
    COMPLETED task is sticky, so the event is recorded and the status is
    not moved. Neither case has a claim to protect."""
    owner = await _new_build(client)
    await _start(client, owner, "done")
    await client.post(f"/api/v1/builds/{owner}/tasks/done/fail")

    other = await _new_build(client)
    await _reference(client, other, "done")
    assert (await _cancel(client, other, "done")).status_code == 200
    assert await _task_status(client, "done") == "cancelled"


# ---------------------------------------------------------------------------
# what a build is left to stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executions_lists_only_what_this_build_started(client: AsyncClient):
    owner = await _new_build(client)
    await _start(client, owner, "mine")

    referencer = await _new_build(client)
    await _reference(client, referencer, "mine")

    mine = await _executions(client, owner)
    assert [e["task_id"] for e in mine["executions"]] == ["mine"]
    assert mine["executions"][0]["executor"] == "modal"
    assert mine["executions"][0]["executor_ref"] == "fc-mine"
    assert mine["truncated"] is False

    assert (await _executions(client, referencer))["executions"] == []


@pytest.mark.asyncio
async def test_a_cascading_cancel_leaves_its_executions_to_be_stopped(
    client: AsyncClient,
):
    """The defect underneath the reported loop. The cascade releases the
    claim — which is what lets the next build take the task over — while the
    container is still running, and only the cancelling build's own engine
    can stop it. Before this endpoint the task was CANCELLED, therefore in
    neither ``running`` nor ``actionable``, therefore invisible to the one
    caller of ``cancel_detached``."""
    build = await _new_build(client)
    await _start(client, build, "leftover")
    await client.post(f"/api/v1/builds/{build}/cancel", params={"cascade": "true"})
    assert await _task_status(client, "leftover") == "cancelled"

    listed = await _executions(client, build)
    assert listed["build_status"] == "cancelled"
    assert [e["task_id"] for e in listed["executions"]] == ["leftover"]
    assert listed["executions"][0]["executor_ref"] == "fc-leftover"


@pytest.mark.asyncio
async def test_a_task_this_build_cancelled_is_still_its_to_stop(client: AsyncClient):
    """A cancel is a request to stop, not evidence that anything stopped.

    The server cannot reach a container; it can only record that the claim
    is gone. So a task this build cancelled is exactly a task whose
    execution it still has to go and kill — and listing it by status would
    do the opposite, since CANCELLED is terminal.
    """
    build = await _new_build(client)
    await _start(client, build, "revoked")
    await _cancel(client, build, "revoked")

    listed = await _executions(client, build)
    assert [e["task_id"] for e in listed["executions"]] == ["revoked"]
    assert listed["executions"][0]["executor_ref"] == "fc-revoked"


@pytest.mark.asyncio
async def test_a_takeover_does_not_hide_the_execution_from_its_owner(
    client: AsyncClient,
):
    """The defect this endpoint was rewritten for, and it is not a corner.

    A cascading cancel releases the claim precisely so the next build can
    take the task over, and the next build can claim it within seconds —
    before the cancelled build's tick has run. From that moment the task
    row names the *new* execution. Answering from the row therefore hands
    the cancelled build either nothing or somebody else's container; what
    it needs is the ref it started, which is in the event log and stays
    true however the claim moves.
    """
    owner = await _new_build(client)
    await _start(client, owner, "shared")
    await client.post(f"/api/v1/builds/{owner}/cancel", params={"cascade": "true"})

    taker = await _new_build(client)
    await _reference(client, taker, "shared")
    await client.post(f"/api/v1/builds/{taker}/tasks/shared/retry")
    await client.post(
        f"/api/v1/builds/{taker}/tasks/shared/start",
        params={"executor": "modal", "executor_ref": "fc-theirs"},
    )
    assert await _task_status(client, "shared") == "running"

    mine = await _executions(client, owner)
    assert [e["executor_ref"] for e in mine["executions"]] == ["fc-shared"], (
        "the cancelled build must still be handed its own execution, and "
        "never the one that took the task over"
    )
    theirs = await _executions(client, taker)
    assert [e["executor_ref"] for e in theirs["executions"]] == ["fc-theirs"]


@pytest.mark.asyncio
async def test_a_conditional_cancel_does_not_stamp_a_task_someone_reset(
    client: AsyncClient,
):
    """An engine cleaning up after itself decides what to cancel from a
    listing it read a moment ago. By the time it gets here another build may
    have reset the task and be about to run it — and PENDING holds no claim,
    so the ownership guard would let the write through. It takes nothing,
    but it stamps a neighbour's freshly scheduled task dead and sends it
    round the reset loop, which is the damage this endpoint exists to stop
    causing."""
    owner = await _new_build(client)
    await _start(client, owner, "shared")
    await client.post(f"/api/v1/builds/{owner}/cancel", params={"cascade": "true"})

    taker = await _new_build(client)
    await _reference(client, taker, "shared")
    await client.post(f"/api/v1/builds/{taker}/tasks/shared/retry")
    assert await _task_status(client, "shared") == "pending"

    response = await client.post(
        f"/api/v1/builds/{owner}/tasks/shared/cancel",
        params={"if_executor_ref": "fc-shared"},
    )
    assert response.status_code == 200, response.text
    assert await _task_status(client, "shared") == "pending", (
        "the cancelled build stamped a task another build had already reset"
    )


@pytest.mark.asyncio
async def test_a_conditional_cancel_does_not_revoke_a_newer_execution(
    client: AsyncClient,
):
    """Ownership is not enough — the execution's identity has to match too.

    Between the listing and this call, *this* build can have started the
    task again: a retry it spawned, or a worker of the old attempt
    self-reporting late. Status and owner still say "held by me", so a
    check on those alone would revoke the claim of an execution nobody
    stopped."""
    build = await _new_build(client)
    await _start(client, build, "restarted")
    await client.post(
        f"/api/v1/builds/{build}/tasks/restarted/start",
        params={"executor": "modal", "executor_ref": "fc-newer"},
    )

    response = await client.post(
        f"/api/v1/builds/{build}/tasks/restarted/cancel",
        params={"if_executor_ref": "fc-restarted"},
    )
    assert response.status_code == 200, response.text
    assert await _task_status(client, "restarted") == "running", (
        "the cleanup pass revoked an execution it had never listed"
    )


@pytest.mark.asyncio
async def test_a_conditional_cancel_still_revokes_what_this_build_holds(
    client: AsyncClient,
):
    """The narrowing must not cost the thing the record is for: a worker
    killed by the backend cannot self-report, so without the event the task
    dangles RUNNING and holds its claim and limit slots forever."""
    build = await _new_build(client)
    await _start(client, build, "mine")

    response = await client.post(
        f"/api/v1/builds/{build}/tasks/mine/cancel",
        params={"if_executor_ref": "fc-mine"},
    )
    assert response.status_code == 200, response.text
    assert await _task_status(client, "mine") == "cancelled"


@pytest.mark.asyncio
async def test_a_conditional_cancel_of_an_already_cancelled_task_is_a_no_op(
    client: AsyncClient,
):
    """The cascade already wrote that event. A second one says nothing new,
    and the cleanup pass should not have to know which."""
    build = await _new_build(client)
    await _start(client, build, "revoked")
    await client.post(f"/api/v1/builds/{build}/cancel", params={"cascade": "true"})

    before = (await client.get("/api/v1/tasks/revoked")).json()["latest_status_at"]
    response = await client.post(
        f"/api/v1/builds/{build}/tasks/revoked/cancel",
        params={"if_executor_ref": "fc-revoked"},
    )
    assert response.status_code == 200, response.text
    after = (await client.get("/api/v1/tasks/revoked")).json()["latest_status_at"]
    assert after == before, "a second cancel event was recorded"


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["complete", "fail", "suspend"])
async def test_an_execution_a_worker_reported_over_is_not_listed(
    client: AsyncClient, outcome: str
):
    """A worker said the execution ended, so there is no container left."""
    build = await _new_build(client)
    await _start(client, build, "done")
    await client.post(f"/api/v1/builds/{build}/tasks/done/{outcome}")

    assert (await _executions(client, build))["executions"] == []


@pytest.mark.asyncio
async def test_an_interrupted_execution_is_still_listed(client: AsyncClient):
    """An interruption ended one attempt, and the backend may be retrying
    under the same call — the premise the tick's backend-retry guard rests
    on. So the ref can still be live and is still this build's to stop."""
    build = await _new_build(client)
    await _start(client, build, "taken")
    await client.post(f"/api/v1/builds/{build}/tasks/taken/interrupt")

    listed = await _executions(client, build)
    assert [e["task_id"] for e in listed["executions"]] == ["taken"]


@pytest.mark.asyncio
async def test_only_the_latest_execution_of_a_task_is_listed(client: AsyncClient):
    """A retried task has been started more than once by the same build.
    The earlier call is over — its failure was recorded — and the ref that
    matters is the one running now."""
    build = await _new_build(client)
    await _start(client, build, "flaky")
    await client.post(f"/api/v1/builds/{build}/tasks/flaky/fail")
    await client.post(f"/api/v1/builds/{build}/tasks/flaky/retry")
    await client.post(
        f"/api/v1/builds/{build}/tasks/flaky/start",
        params={"executor": "modal", "executor_ref": "fc-second"},
    )

    listed = await _executions(client, build)
    assert [e["executor_ref"] for e in listed["executions"]] == ["fc-second"]


@pytest.mark.asyncio
async def test_execution_identity_comes_from_the_start_event(client: AsyncClient):
    """Never from the task's current row, which after a takeover describes
    somebody else's execution. Pairing this build's historical ref with the
    successor's backend or metadata would hand back something that
    identifies no execution at all."""
    owner = await _new_build(client)
    await client.post(f"/api/v1/builds/{owner}/tasks", json=_register("shared"))
    await client.post(
        f"/api/v1/builds/{owner}/tasks/shared/start",
        params={
            "executor": "modal",
            "executor_ref": "fc-mine",
            "executor_metadata": '{"app": "mine"}',
        },
    )
    await client.post(f"/api/v1/builds/{owner}/cancel", params={"cascade": "true"})

    taker = await _new_build(client)
    await _reference(client, taker, "shared")
    await client.post(f"/api/v1/builds/{taker}/tasks/shared/retry")
    await client.post(
        f"/api/v1/builds/{taker}/tasks/shared/start",
        params={
            "executor": "other-backend",
            "executor_ref": "fc-theirs",
            "executor_metadata": '{"app": "theirs"}',
        },
    )

    mine = (await _executions(client, owner))["executions"][0]
    assert mine["executor_ref"] == "fc-mine"
    assert mine["executor"] == "modal", "the successor's backend leaked in"
    assert mine["executor_metadata"] == {"app": "mine"}


@pytest.mark.asyncio
async def test_executions_page_through_a_cursor(client: AsyncClient, monkeypatch):
    """Paging, not a bare cap. Stopping an execution records nothing — a
    cancel is a request, not an end — so the answer does not shrink as a
    caller works through it, and asking again without a cursor would return
    the same page forever, leaving a wide build's tail running."""
    from stardag_api.routes import builds as builds_routes

    monkeypatch.setattr(builds_routes, "_MAX_BUILD_EXECUTIONS", 2)
    build = await _new_build(client)
    for index in range(5):
        await _start(client, build, f"wide-{index}")

    seen: list[str] = []
    cursor: str | None = None
    for _ in range(5):
        params = {"cursor": cursor} if cursor else {}
        page = (
            await client.get(f"/api/v1/builds/{build}/executions", params=params)
        ).json()
        seen += [e["task_id"] for e in page["executions"]]
        cursor = page["next_cursor"]
        if not page["truncated"]:
            break

    assert sorted(seen) == [f"wide-{i}" for i in range(5)]
    assert len(seen) == len(set(seen)), f"a task was handed out twice: {seen}"
    assert cursor is None


@pytest.mark.asyncio
async def test_a_malformed_cursor_is_rejected(client: AsyncClient):
    """Rather than silently restarting from the top, which is the loop this
    paging exists to remove."""
    build = await _new_build(client)
    response = await client.get(
        f"/api/v1/builds/{build}/executions", params={"cursor": "nonsense"}
    )
    assert response.status_code == 400, response.text


@pytest.mark.asyncio
async def test_executions_skips_a_task_with_no_recorded_ref(client: AsyncClient):
    """The window between the claiming start and the one that records the
    ref. There is nothing to cancel, and the claim is released by the
    lapsed-claim path instead."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("claimed"))
    await client.post(
        f"/api/v1/builds/{build}/tasks/claimed/start", params={"claim": "true"}
    )
    assert await _task_status(client, "claimed") == "running"
    assert (await _executions(client, build))["executions"] == []
