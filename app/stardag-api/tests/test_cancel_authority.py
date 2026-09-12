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
async def test_a_running_builds_own_cancelled_tasks_are_history(client: AsyncClient):
    """A retry cycle cancels and re-runs; those cancels are spent, not
    pending. Listing them would grow without bound over a long build and
    hand the engine refs it stopped hours ago."""
    build = await _new_build(client)
    await _start(client, build, "retried")
    await _cancel(client, build, "retried")

    listed = await _executions(client, build)
    assert listed["build_status"] == "running"
    assert listed["executions"] == []


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
