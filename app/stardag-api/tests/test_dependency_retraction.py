"""An abandoned execution attempt takes its dynamic dependencies with it.

Dynamic edges are written ``ON CONFLICT DO NOTHING``, so a task's set only
ever grew. Usually invisible, because a previous generation's children are
COMPLETED and completed upstreams do not gate — it bites when an attempt is
abandoned with its children incomplete, and then they gate *their own
parent* forever. The parent cannot be scheduled until they complete, so the
next build that wants it resets and re-runs a whole generation of work the
task is no longer going to ask for, before it ever gets to re-yield.

The unit under test is which transitions count as a new attempt, and what
stops gating when one does.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from stardag_api.models import Task, TaskDependency


def _register(task_id: str, deps: list[str] | None = None) -> dict:
    return {
        "task_id": task_id,
        "task_namespace": "",
        "task_name": "T",
        "task_data": {},
        "dependency_task_ids": deps or [],
    }


async def _new_build(client: AsyncClient) -> str:
    return (await client.post("/api/v1/builds", json={})).json()["id"]


async def _start(client: AsyncClient, build_id: str, task_id: str) -> None:
    response = await client.post(
        f"/api/v1/builds/{build_id}/tasks/{task_id}/start",
        params={"executor": "test", "executor_ref": f"ref-{task_id}"},
    )
    assert response.status_code == 200, response.text


async def _yield_children(
    client: AsyncClient, build_id: str, parent: str, children: list[str]
) -> None:
    """What a worker does when it yields: register the children, record the
    dynamic edges, suspend."""
    for child in children:
        await client.post(f"/api/v1/builds/{build_id}/tasks", json=_register(child))
    response = await client.post(
        f"/api/v1/builds/{build_id}/tasks/{parent}/dependencies",
        json={"upstream_task_ids": children, "is_dynamic": True},
    )
    assert response.status_code == 200, response.text
    await client.post(f"/api/v1/builds/{build_id}/tasks/{parent}/suspend")


async def _actionable(client: AsyncClient, build_id: str) -> list[str]:
    frontier = (await client.get(f"/api/v1/builds/{build_id}/frontier")).json()
    return [ref["task_id"] for ref in frontier["actionable"]]


async def _edges(session: AsyncSession, downstream: str) -> dict[str, bool]:
    """``{upstream task_id: is current}`` for one downstream task."""
    rows = (
        await session.execute(
            select(Task.task_id, TaskDependency.superseded_at)
            .select_from(TaskDependency)
            .join(Task, Task.id == TaskDependency.upstream_task_id)
            .join(
                _Downstream := Task.__table__.alias("downstream"),
                _Downstream.c.id == TaskDependency.downstream_task_id,
            )
            .where(_Downstream.c.task_id == downstream)
        )
    ).all()
    return {task_id: superseded_at is None for task_id, superseded_at in rows}


# ---------------------------------------------------------------------------
# a reset abandons the attempt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_reset_retracts_the_abandoned_generation(
    client: AsyncClient, async_session: AsyncSession
):
    """The reported shape. The parent is suspended on children that are then
    cancelled; without retraction it is gated on them forever and the next
    build reruns them to un-gate it."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("parent"))
    await _start(client, build, "parent")
    await _yield_children(client, build, "parent", ["p1", "p2"])

    # The build is cancelled: the parent and its children are abandoned.
    await client.post(f"/api/v1/builds/{build}/cancel", params={"cascade": "true"})
    for child in ("p1", "p2"):
        await client.post(f"/api/v1/builds/{build}/tasks/{child}/cancel")

    assert await _edges(async_session, "parent") == {"p1": True, "p2": True}
    assert "parent" not in await _actionable(client, build), (
        "the parent is gated on its abandoned children, which is the state "
        "the retraction exists to end"
    )

    await client.post(f"/api/v1/builds/{build}/tasks/parent/retry")

    async_session.expire_all()
    assert await _edges(async_session, "parent") == {"p1": False, "p2": False}
    assert "parent" in await _actionable(client, build), (
        "a reset parent must be schedulable at once: it will re-yield "
        "whatever this attempt actually needs"
    )


@pytest.mark.asyncio
async def test_a_reset_retracts_completed_children_too(
    client: AsyncClient, async_session: AsyncSession
):
    """All of the attempt, not the stragglers. A generation half retracted
    and half kept is a state no later rule can reason about — and nothing is
    lost, since a completed child's target still exists and the edge returns
    with the next yield."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("parent"))
    await _start(client, build, "parent")
    await _yield_children(client, build, "parent", ["done", "abandoned"])
    await _start(client, build, "done")
    await client.post(f"/api/v1/builds/{build}/tasks/done/complete")

    await client.post(f"/api/v1/builds/{build}/tasks/parent/retry")

    async_session.expire_all()
    assert await _edges(async_session, "parent") == {
        "done": False,
        "abandoned": False,
    }


@pytest.mark.asyncio
async def test_static_edges_are_never_retracted(
    client: AsyncClient, async_session: AsyncSession
):
    """A static edge is *declared*, in full, at every registration — nothing
    about an execution attempt withdraws one. Only STA-41's rule touches
    these."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("upstream"))
    await client.post(
        f"/api/v1/builds/{build}/tasks", json=_register("child", ["upstream"])
    )
    await _start(client, build, "child")
    await _yield_children(client, build, "child", ["dyn"])

    await client.post(f"/api/v1/builds/{build}/tasks/child/retry")

    async_session.expire_all()
    assert await _edges(async_session, "child") == {"upstream": True, "dyn": False}


@pytest.mark.asyncio
async def test_a_retry_that_moves_nothing_retracts_nothing(
    client: AsyncClient, async_session: AsyncSession
):
    """TASK_RETRIED is recorded whether or not the status was retryable —
    which is what makes concurrent trigger/retry races benign. A retry that
    changed nothing has abandoned nothing, so the gate is on the transition
    and not on the event."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("parent"))
    await _start(client, build, "parent")
    await _yield_children(client, build, "parent", ["p1"])
    # COMPLETED is sticky, so the retry below is recorded and moves nothing.
    await client.post(f"/api/v1/builds/{build}/tasks/parent/complete")

    await client.post(f"/api/v1/builds/{build}/tasks/parent/retry")

    async_session.expire_all()
    assert await _edges(async_session, "parent") == {"p1": True}


# ---------------------------------------------------------------------------
# a takeover is a new attempt; a build's own resume is not
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_another_build_taking_over_a_suspension_retracts_it(
    client: AsyncClient, async_session: AsyncSession
):
    """Taking over an abandoned suspension re-runs the generator from the
    top, so the previous attempt's generation goes with it. Takeover itself
    stays allowed — the cost is now proportional to how much the code
    actually diverges."""
    owner = await _new_build(client)
    await client.post(f"/api/v1/builds/{owner}/tasks", json=_register("parent"))
    await _start(client, owner, "parent")
    await _yield_children(client, owner, "parent", ["p1"])
    await _start(client, owner, "p1")
    await client.post(f"/api/v1/builds/{owner}/tasks/p1/complete")

    taker = await _new_build(client)
    await client.post(f"/api/v1/builds/{taker}/tasks", json=_register("parent"))
    await _start(client, taker, "parent")

    async_session.expire_all()
    assert await _edges(async_session, "parent") == {"p1": False}


@pytest.mark.asyncio
async def test_the_owning_build_resuming_its_own_suspension_keeps_them(
    client: AsyncClient, async_session: AsyncSession
):
    """The ordinary multi-round walk. The generator advances past the
    batches it has already completed and yields the next one — which is
    exactly what the accumulated edges describe, so retracting here would
    throw away a walk still in progress."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("parent"))
    await _start(client, build, "parent")
    await _yield_children(client, build, "parent", ["round1"])
    await _start(client, build, "round1")
    await client.post(f"/api/v1/builds/{build}/tasks/round1/complete")

    await _start(client, build, "parent")  # resumed by its own build

    async_session.expire_all()
    assert await _edges(async_session, "parent") == {"round1": True}


# ---------------------------------------------------------------------------
# what a retracted edge stops doing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_retracted_edge_admits_nothing_into_a_new_builds_plan(
    client: AsyncClient,
):
    """Plan closure follows recorded edges so a build is never gated on
    something outside its plan. A retracted edge does not gate, so following
    it would pull an abandoned generation into the plan for no benefit."""
    first = await _new_build(client)
    await client.post(f"/api/v1/builds/{first}/tasks", json=_register("parent"))
    await _start(client, first, "parent")
    await _yield_children(client, first, "parent", ["stale1", "stale2"])
    await client.post(f"/api/v1/builds/{first}/cancel", params={"cascade": "true"})
    for child in ("stale1", "stale2"):
        await client.post(f"/api/v1/builds/{first}/tasks/{child}/cancel")
    await client.post(f"/api/v1/builds/{first}/tasks/parent/retry")

    later = await _new_build(client)
    await client.post(
        f"/api/v1/builds/{later}/tasks", json=_register("root", ["parent"])
    )

    frontier = (await client.get(f"/api/v1/builds/{later}/frontier")).json()
    assert frontier["status_counts"] == {"pending": 2}, (
        "the fresh build's plan is its root and the parent — the abandoned "
        f"generation must not be in it: {frontier['status_counts']}"
    )
    assert [ref["task_id"] for ref in frontier["actionable"]] == ["parent"]


@pytest.mark.asyncio
async def test_a_cancelled_parents_generation_is_not_admitted_either(
    client: AsyncClient,
):
    """Retraction alone is not enough, and a live run is what showed it.

    A cascade cancels the claims a build holds — RUNNING, SUSPENDED,
    INTERRUPTED — and deliberately leaves PENDING tasks alone. So a build
    cancelled shortly after a fan-out yields leaves its children *pending*,
    not cancelled. Plan closure then admits them to the next build, where
    they are immediately actionable, and it runs the whole abandoned
    generation before it ever resets the parent and retracts them.

    So closure must not follow the dynamic edges of a cancelled task. The
    two rules are a pair: retraction un-gates the parent, and this keeps
    what it retracts out of the plan in the first place.
    """
    first = await _new_build(client)
    await client.post(f"/api/v1/builds/{first}/tasks", json=_register("parent"))
    await _start(client, first, "parent")
    await _yield_children(client, first, "parent", ["kid1", "kid2"])
    # The cascade reaches the suspended parent and leaves the pending
    # children exactly as a real one does.
    await client.post(f"/api/v1/builds/{first}/cancel", params={"cascade": "true"})

    later = await _new_build(client)
    await client.post(
        f"/api/v1/builds/{later}/tasks", json=_register("root", ["parent"])
    )

    frontier = (await client.get(f"/api/v1/builds/{later}/frontier")).json()
    admitted = sorted(frontier["status_counts"].items())
    assert admitted == [("cancelled", 1), ("pending", 1)], (
        "the fresh build's plan must be its root and the cancelled parent "
        f"alone — the abandoned generation is not its work: {admitted}"
    )
    assert [ref["task_id"] for ref in frontier["actionable"]] == [], (
        "and nothing of it may be actionable"
    )


@pytest.mark.asyncio
async def test_a_re_yielded_child_gates_again(
    client: AsyncClient, async_session: AsyncSession
):
    """The dangerous half of retraction, if the edge could not come back.

    The next attempt of a parent usually yields the very same children —
    the fan-out changing is the interesting case, not the common one. Left
    superseded, those edges would no longer gate: the parent would be
    schedulable the instant it was reset, re-yield the same incomplete
    batch, suspend, and go round again, never once waiting for the work it
    is waiting for.
    """
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("parent"))
    await _start(client, build, "parent")
    await _yield_children(client, build, "parent", ["child"])
    await client.post(f"/api/v1/builds/{build}/tasks/parent/retry")
    async_session.expire_all()
    assert await _edges(async_session, "parent") == {"child": False}

    # The new attempt yields the same child.
    await _start(client, build, "parent")
    await _yield_children(client, build, "parent", ["child"])

    async_session.expire_all()
    assert await _edges(async_session, "parent") == {"child": True}
    assert "parent" not in await _actionable(client, build), (
        "the parent must be gated on the child it just asked for again"
    )


@pytest.mark.asyncio
async def test_a_statically_declared_edge_outranks_an_earlier_yield(
    client: AsyncClient, async_session: AsyncSession
):
    """``is_dynamic`` decides what retraction may touch, so it has to mean
    "nothing has declared this". It kept the first observation, which was
    harmless while only the DAG view read it: an edge first yielded and
    later named in ``requires()`` would stay marked dynamic and be retracted
    out from under the build that statically requires it."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("upstream"))
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("child"))
    await _start(client, build, "child")
    await _yield_children(client, build, "child", ["upstream"])

    # A later registration declares the same upstream statically.
    await client.post(
        f"/api/v1/builds/{build}/tasks", json=_register("child", ["upstream"])
    )
    await client.post(f"/api/v1/builds/{build}/tasks/child/retry")

    async_session.expire_all()
    assert await _edges(async_session, "child") == {"upstream": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["fail", "skip", "interrupt"])
async def test_every_retryable_parent_keeps_its_generation_out(
    client: AsyncClient, outcome: str
):
    """Not just CANCELLED. A trigger resets the whole retryable set, and it
    does so *after* registration has closed the plan — so for any of these a
    stale generation would already be in the plan, PENDING and actionable,
    by the time the reset retracted its edges."""
    first = await _new_build(client)
    await client.post(f"/api/v1/builds/{first}/tasks", json=_register("parent"))
    await _start(client, first, "parent")
    await _yield_children(client, first, "parent", ["kid1", "kid2"])
    await _start(client, first, "parent")
    await client.post(f"/api/v1/builds/{first}/tasks/parent/{outcome}")

    later = await _new_build(client)
    await client.post(
        f"/api/v1/builds/{later}/tasks", json=_register("root", ["parent"])
    )

    counts = (await client.get(f"/api/v1/builds/{later}/frontier")).json()[
        "status_counts"
    ]
    assert sum(counts.values()) == 2, (
        f"the abandoned generation entered the plan of a {outcome}ed parent: {counts}"
    )


@pytest.mark.asyncio
async def test_a_suspension_whose_owner_is_gone_admits_nothing(client: AsyncClient):
    """A deleted owning build leaves ``latest_status_build_id`` NULL, and
    the same reasoning applies: nobody is progressing it."""
    first = await _new_build(client)
    await client.post(f"/api/v1/builds/{first}/tasks", json=_register("parent"))
    await _start(client, first, "parent")
    await _yield_children(client, first, "parent", ["kid"])
    await client.post(f"/api/v1/builds/{first}/cancel")

    later = await _new_build(client)
    await client.post(
        f"/api/v1/builds/{later}/tasks", json=_register("root", ["parent"])
    )
    counts = (await client.get(f"/api/v1/builds/{later}/frontier")).json()[
        "status_counts"
    ]
    assert sum(counts.values()) == 2, counts


@pytest.mark.asyncio
async def test_an_abandoned_suspension_admits_nothing_either(client: AsyncClient):
    """The same trap one status over. A suspension is the one state where a
    task is legitimately mid-flight, so closure keeps admitting its children
    while the owning build is alive — that is scenario S2, and the case this
    closure was written for. Once the owner is gone it is an abandoned
    attempt like any other, and the next build's trigger resets it."""
    first = await _new_build(client)
    await client.post(f"/api/v1/builds/{first}/tasks", json=_register("parent"))
    await _start(client, first, "parent")
    await _yield_children(client, first, "parent", ["kid"])
    # Terminal *without* a cascade, so the parent is left SUSPENDED.
    await client.post(f"/api/v1/builds/{first}/fail")

    later = await _new_build(client)
    await client.post(
        f"/api/v1/builds/{later}/tasks", json=_register("root", ["parent"])
    )

    counts = (await client.get(f"/api/v1/builds/{later}/frontier")).json()[
        "status_counts"
    ]
    assert sorted(counts.items()) == [("pending", 1), ("suspended", 1)], counts


@pytest.mark.asyncio
async def test_a_live_owners_suspension_still_admits_its_children(
    client: AsyncClient,
):
    """The half that must not change. The children of a suspension a live
    build is progressing have to enter the waiting build's plan, or it is
    gated on tasks it cannot schedule and nothing can clear — the permanent
    deadlock plan closure exists to prevent."""
    owner = await _new_build(client)
    await client.post(f"/api/v1/builds/{owner}/tasks", json=_register("parent"))
    await _start(client, owner, "parent")
    await _yield_children(client, owner, "parent", ["kid"])

    later = await _new_build(client)
    await client.post(
        f"/api/v1/builds/{later}/tasks", json=_register("root", ["parent"])
    )

    counts = (await client.get(f"/api/v1/builds/{later}/frontier")).json()[
        "status_counts"
    ]
    assert sorted(counts.items()) == [("pending", 2), ("suspended", 1)], counts


@pytest.mark.asyncio
async def test_a_retracted_edge_does_not_propagate_a_failure(client: AsyncClient):
    """``skip-blocked`` walks down the edges from a failed task. A child of
    an abandoned attempt failing says nothing about a parent that is no
    longer waiting on it."""
    build = await _new_build(client)
    await client.post(f"/api/v1/builds/{build}/tasks", json=_register("parent"))
    await _start(client, build, "parent")
    await _yield_children(client, build, "parent", ["child"])
    await client.post(f"/api/v1/builds/{build}/tasks/parent/retry")
    await _start(client, build, "child")
    await client.post(f"/api/v1/builds/{build}/tasks/child/fail")

    skipped = (await client.post(f"/api/v1/builds/{build}/skip-blocked")).json()[
        "skipped_task_ids"
    ]
    assert skipped == [], (
        "the parent is not downstream of that failure any more — the "
        f"attempt that made it so was abandoned: {skipped}"
    )
