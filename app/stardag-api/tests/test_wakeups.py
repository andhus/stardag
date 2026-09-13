"""Cross-build wake-ups: the flag hook and ``POST /builds/wake-candidates``.

The two halves of a cross-build wake-up (see ``services.wakeups``):

- every path that changes a task's status flags every *other* live reactive
  build holding the task (and, on a release out of RUNNING, the builds
  queued on its concurrency-limit keys);
- ``wake-candidates`` hands flagged builds with no live scheduler to a
  caller that can spawn, once per window.

Each filter here corresponds to a line someone could delete and be told so
by a red test — the coverage the review of the previous attempt asked for.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from stardag_api.models import Build

WAKE = "/api/v1/builds/wake-candidates"


async def _build(client: AsyncClient, app: str | None = "app-a") -> str:
    build_id = (await client.post("/api/v1/builds", json={})).json()["id"]
    if app is not None:
        response = await client.put(
            f"/api/v1/builds/{build_id}/reactive-meta", json={"app_name": app}
        )
        assert response.status_code == 200, response.text
    return build_id


async def _register(
    client: AsyncClient,
    build_id: str,
    task_id: str,
    *,
    deps: list[str] | None = None,
    limit_keys: list[str] | None = None,
) -> None:
    payload: dict = {
        "task_id": task_id,
        "task_name": "T",
        "task_namespace": "",
        "task_data": {},
        "dependency_task_ids": deps or [],
    }
    if limit_keys is not None:
        payload["limit_keys"] = limit_keys
    response = await client.post(
        f"/api/v1/builds/{build_id}/tasks/bulk", json={"tasks": [payload]}
    )
    assert response.status_code in (200, 201), response.text


async def _start(client: AsyncClient, build_id: str, task_id: str, **params) -> None:
    response = await client.post(
        f"/api/v1/builds/{build_id}/tasks/{task_id}/start",
        params={"executor": "test", "executor_ref": f"ref-{task_id}", **params},
    )
    assert response.status_code == 200, response.text


async def _needs_tick(client: AsyncClient, build_id: str) -> bool:
    return (await client.get(f"/api/v1/builds/{build_id}/frontier")).json()[
        "needs_tick"
    ]


async def _clear(client: AsyncClient, *build_ids: str) -> None:
    for build_id in build_ids:
        await client.delete(f"/api/v1/builds/{build_id}/notify")


async def _hold_lease(client: AsyncClient, build_id: str, ttl: int = 60) -> str:
    """Take the build's scheduler lease, as a tick does. Returns the owner."""
    owner_id = str(uuid.uuid4())
    response = await client.post(
        f"/api/v1/builds/{build_id}/scheduler-lease",
        params={"owner_id": owner_id, "ttl_seconds": ttl},
    )
    assert response.status_code == 200, response.text
    assert response.json()["held"] is True, response.text
    return owner_id


async def _candidates(client: AsyncClient, **params) -> list[dict]:
    response = await client.post(WAKE, params=params)
    assert response.status_code == 200, response.text
    return response.json()["builds"]


async def _shared(client: AsyncClient, task_id: str = "shared") -> tuple[str, str]:
    """Two reactive builds holding one task; A has it RUNNING; B's flag is clear."""
    a, b = await _build(client), await _build(client, "app-b")
    await _register(client, a, task_id)
    await _register(client, b, task_id)
    await _start(client, a, task_id)
    await _clear(client, a, b)
    return a, b


# --- the flag hook, per status-writing path -------------------------------


@pytest.mark.asyncio
async def test_completion_in_one_build_flags_the_other(client: AsyncClient):
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    assert await _needs_tick(client, b) is True
    # The writer's own build is not flagged by this hook: its own worker
    # notifies it, and flagging it here would only double up.
    assert await _needs_tick(client, a) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["fail", "cancel", "interrupt", "suspend"])
async def test_every_release_out_of_running_flags_the_other(
    client: AsyncClient, route: str
):
    """Not only the terminal statuses: INTERRUPTED and SUSPENDED release the
    claim too, and a neighbour blocked on the claim is waiting for exactly
    that."""
    a, b = await _shared(client, f"shared-{route}")
    params = {"reason": "preempted"} if route == "interrupt" else {}
    response = await client.post(
        f"/api/v1/builds/{a}/tasks/shared-{route}/{route}", params=params
    )
    assert response.status_code == 200, response.text
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_a_start_flags_the_other_too(client: AsyncClient):
    """Every transition flags — including into RUNNING. Cheap, and a rule
    with no exceptions is one nobody has to re-derive."""
    a, b = await _build(client), await _build(client, "app-b")
    await _register(client, a, "t")
    await _register(client, b, "t")
    await _clear(client, a, b)
    await _start(client, a, "t")
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_an_event_that_changes_nothing_flags_nobody(client: AsyncClient):
    """Transition-gated, not state-gated: COMPLETED is sticky, so a late
    FAILED on a completed task changes nothing and must wake nobody."""
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    await _clear(client, a, b)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/fail")
    assert await _needs_tick(client, b) is False


@pytest.mark.asyncio
async def test_retry_flags_the_other(client: AsyncClient):
    """A retry makes a failed blocker runnable for the neighbour."""
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/fail")
    await _clear(client, a, b)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/retry")
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_skip_blocked_flags_the_other(client: AsyncClient):
    """The skip-blocked route writes statuses without going through the
    per-event route; it must run the same hook."""
    a, b = await _build(client), await _build(client, "app-b")
    await _register(client, a, "up")
    await _register(client, a, "down", deps=["up"])
    await _register(client, b, "down", deps=["up"])
    await _start(client, a, "up")
    await client.post(f"/api/v1/builds/{a}/tasks/up/fail")
    await _clear(client, a, b)
    response = await client.post(f"/api/v1/builds/{a}/skip-blocked")
    assert response.json()["skipped_task_ids"] == ["down"]
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_cascade_cancel_flags_the_other(client: AsyncClient):
    a, b = await _shared(client)
    response = await client.post(
        f"/api/v1/builds/{a}/cancel", params={"cascade": "true"}
    )
    assert response.json()["cascaded_task_ids"] == ["shared"]
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_cancelling_a_reactive_build_flags_it(client: AsyncClient):
    """Its running executions can only be stopped by a tick — so make sure
    one comes, rather than leaving it to the watchdog."""
    a = await _build(client)
    await _clear(client, a)
    await client.post(f"/api/v1/builds/{a}/cancel")
    assert await _needs_tick(client, a) is True


@pytest.mark.asyncio
async def test_a_cancelled_reactive_build_is_handed_out(client: AsyncClient):
    """The flag a cancel sets is only useful if a drainer can be handed the
    build: its executions are still running and only a tick stops them."""
    a = await _build(client)
    await client.post(f"/api/v1/builds/{a}/cancel")
    assert [c["build_id"] for c in await _candidates(client)] == [a]


@pytest.mark.asyncio
async def test_evicting_a_slot_holder_flags_the_builds_queued_on_the_key(
    client: AsyncClient,
):
    """The eviction route writes RUNNING→FAILED on its own path; same hook."""
    response = await client.put(
        "/api/v1/concurrency-limits/gpu", json={"max_concurrent": 1}
    )
    assert response.status_code == 200, response.text
    a, b = await _build(client), await _build(client, "app-b")
    await _register(client, a, "holder", limit_keys=["gpu"])
    await _register(client, b, "waiter", limit_keys=["gpu"])
    await _start(client, a, "holder", limit_key=["gpu"])
    await _clear(client, a, b)
    response = await client.post("/api/v1/concurrency-limits/gpu/holders/holder/evict")
    assert response.status_code == 200, response.text
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_notify_from_a_caller_that_cannot_spawn_does_not_mark_the_build(
    client: AsyncClient,
):
    """A notifier with no app to reach a tick must not block the drainers
    that can, for a whole window."""
    a = await _build(client)
    notify = (
        await client.post(f"/api/v1/builds/{a}/notify", params={"can_spawn": "false"})
    ).json()
    assert notify["scheduler_live"] is False
    assert [c["build_id"] for c in await _candidates(client)] == [a]


@pytest.mark.asyncio
async def test_notify_does_not_reflag_a_build_that_is_no_longer_running(
    client: AsyncClient,
):
    """The loop this closes. A cancelled build's workers keep running until a
    tick stops them, each notifies on its way out, and each notify used to
    re-flag the build — so every drain in the environment handed it out
    again, ran the cascade again, and the cancelled build never went quiet.
    Once its one tick has drained the flag, nothing may set it back."""
    a = await _build(client)
    await client.post(f"/api/v1/builds/{a}/cancel")
    await _clear(client, a)  # the one tick ran

    notify = (await client.post(f"/api/v1/builds/{a}/notify")).json()
    assert notify["needs_tick"] is False
    assert await _needs_tick(client, a) is False
    assert await _candidates(client) == []


@pytest.mark.asyncio
async def test_notify_still_reports_a_cancelled_builds_undrained_flag(
    client: AsyncClient,
):
    """The one tick is not lost. A cancel flags the build itself, and a
    worker notifying before that flag is drained is told a tick is wanted —
    so the notifier spawns it rather than leaving it to the next drain."""
    a = await _build(client)
    await client.post(f"/api/v1/builds/{a}/cancel")
    notify = (await client.post(f"/api/v1/builds/{a}/notify")).json()
    assert notify["needs_tick"] is True


@pytest.mark.asyncio
async def test_notify_does_not_flag_a_finished_build(client: AsyncClient):
    """Same rule, and here there is not even a cascade to run: a late
    lifecycle report from a straggler worker must not resurrect the build."""
    a = await _build(client)
    await client.post(f"/api/v1/builds/{a}/complete")
    notify = (await client.post(f"/api/v1/builds/{a}/notify")).json()
    assert notify["needs_tick"] is False
    assert await _needs_tick(client, a) is False


@pytest.mark.asyncio
async def test_cancelling_a_resident_build_does_not_flag_it(client: AsyncClient):
    a = await _build(client, app=None)
    await client.post(f"/api/v1/builds/{a}/cancel")
    assert await _needs_tick(client, a) is False


@pytest.mark.asyncio
async def test_lock_release_with_completion_flags_the_other(client: AsyncClient):
    """The global lock's release-with-completion writes TASK_COMPLETED on its
    own path; same hook."""
    a, b = await _shared(client, "locked")
    owner = str(uuid.uuid4())
    await client.post(
        "/api/v1/locks/locked/acquire",
        json={"owner_id": owner, "ttl_seconds": 60, "check_task_completion": False},
    )
    await _clear(client, a, b)
    response = await client.post(
        "/api/v1/locks/locked/release",
        json={"owner_id": owner, "task_completed": True, "build_id": a},
    )
    assert response.status_code == 200, response.text
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_only_live_reactive_builds_are_flagged(client: AsyncClient):
    """A resident build needs no flag; a terminal one has nothing to do with
    it; a build in another environment cannot be reached at all."""
    a = await _build(client)
    resident = await _build(client, app=None)
    finished = await _build(client, "app-b")
    for build_id in (a, resident, finished):
        await _register(client, build_id, "shared")
    await client.post(f"/api/v1/builds/{finished}/complete")
    await _start(client, a, "shared")
    await _clear(client, a, resident, finished)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    assert await _needs_tick(client, resident) is False
    assert await _needs_tick(client, finished) is False


# --- concurrency-limit keys: registered at plan time, woken on release ----


@pytest.mark.asyncio
async def test_a_released_slot_flags_the_builds_queued_on_the_key(
    client: AsyncClient,
):
    """The build waiting on a limit slot does not hold the releasing task;
    what it holds is a PENDING task registered under the same key."""
    a, b = await _build(client), await _build(client, "app-b")
    await _register(client, a, "holder", limit_keys=["gpu"])
    await _register(client, b, "waiter", limit_keys=["gpu"])
    await _start(client, a, "holder", limit_key=["gpu"])
    await _clear(client, a, b)
    await client.post(f"/api/v1/builds/{a}/tasks/holder/complete")
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_a_released_slot_does_not_flag_builds_on_other_keys(
    client: AsyncClient,
):
    a, b = await _build(client), await _build(client, "app-b")
    await _register(client, a, "holder", limit_keys=["gpu"])
    await _register(client, b, "waiter", limit_keys=["cpu"])
    await _start(client, a, "holder", limit_key=["gpu"])
    await _clear(client, a, b)
    await client.post(f"/api/v1/builds/{a}/tasks/holder/complete")
    assert await _needs_tick(client, b) is False


@pytest.mark.asyncio
async def test_plan_time_keys_do_not_occupy_a_slot(client: AsyncClient):
    """Registration-time rows are inert for occupancy: only a RUNNING task
    under a live claim counts against a limit."""
    response = await client.put(
        "/api/v1/concurrency-limits/gpu", json={"max_concurrent": 1}
    )
    assert response.status_code == 200, response.text
    a = await _build(client)
    await _register(client, a, "pending-one", limit_keys=["gpu"])
    await _register(client, a, "runner", limit_keys=["gpu"])
    response = await client.post(
        f"/api/v1/builds/{a}/tasks/runner/start",
        params={"claim": "true", "limit_key": ["gpu"], "enforce_limits": "true"},
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_registration_without_keys_leaves_recorded_keys_alone(
    client: AsyncClient, async_session: AsyncSession
):
    """An older SDK sends no ``limit_keys``; that must not wipe what a newer
    registration recorded, and a RUNNING task keeps the keys it occupies."""
    from stardag_api.models import Task, TaskLimitKey

    a, b = await _build(client), await _build(client, "app-b")
    await _register(client, a, "t", limit_keys=["gpu"])
    await _register(client, b, "t")  # no limit_keys field at all
    await _start(client, a, "t", limit_key=["gpu"])
    await _register(client, b, "t", limit_keys=[])  # RUNNING: must not clear
    task_pk = (
        await async_session.execute(select(Task.id).where(Task.task_id == "t"))
    ).scalar_one()
    keys = (
        (
            await async_session.execute(
                select(TaskLimitKey.key).where(TaskLimitKey.task_pk == task_pk)
            )
        )
        .scalars()
        .all()
    )
    assert keys == ["gpu"]


# --- wake-candidates -------------------------------------------------------


@pytest.mark.asyncio
async def test_wake_candidates_hands_out_a_flagged_unserved_build(
    client: AsyncClient,
):
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    got = await _candidates(client)
    assert got == [{"build_id": b, "reactive_app_name": "app-b"}]


@pytest.mark.asyncio
async def test_wake_candidates_hands_each_build_out_once_per_window(
    client: AsyncClient,
):
    """The debounce: a second caller inside the window gets nothing, so N
    concurrent schedulers produce one tick per flagged build."""
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    assert [c["build_id"] for c in await _candidates(client)] == [b]
    assert await _candidates(client) == []
    # The flag itself is untouched — only the tick that runs clears it.
    assert await _needs_tick(client, b) is True


@pytest.mark.asyncio
async def test_wake_candidates_offers_a_build_again_after_the_window(
    client: AsyncClient, async_session: AsyncSession
):
    """A handed-out spawn that never happened (app deleted, spawner died)
    must not strand the build: past the window it is offered again."""
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    assert [c["build_id"] for c in await _candidates(client)] == [b]
    build = await async_session.get(Build, uuid.UUID(b))
    assert build is not None
    build.tick_requested_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    await async_session.commit()
    assert [c["build_id"] for c in await _candidates(client)] == [b]


@pytest.mark.asyncio
async def test_wake_candidates_skips_a_build_with_a_live_scheduler(
    client: AsyncClient,
):
    """A lingering tick will see the flag itself; a spawn would only find
    the lease held."""
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    await _hold_lease(client, b)
    assert await _candidates(client) == []


@pytest.mark.asyncio
async def test_wake_candidates_ignores_an_expired_scheduler_lease(
    client: AsyncClient, async_session: AsyncSession
):
    from stardag_api.models import Build

    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    await _hold_lease(client, b, ttl=60)
    # Age the lease past its expiry. A tick whose container died leaves the
    # column set, and treating that as a live scheduler would suppress
    # wake-ups for exactly the build that most needs them.
    build = (
        await async_session.execute(select(Build).where(Build.id == uuid.UUID(b)))
    ).scalar_one()
    build.scheduler_lease_until = datetime.now(timezone.utc) - timedelta(seconds=1)
    await async_session.commit()
    assert [c["build_id"] for c in await _candidates(client)] == [b]


@pytest.mark.asyncio
async def test_wake_candidates_skips_unflagged_and_finished_builds(
    client: AsyncClient,
):
    a, b = await _shared(client)
    await _build(client, "app-c")  # reactive, running, never flagged
    finished = await _build(client, "app-d")
    await client.post(f"/api/v1/builds/{finished}/notify")
    await client.post(f"/api/v1/builds/{finished}/complete")
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    assert [c["build_id"] for c in await _candidates(client)] == [b]


@pytest.mark.asyncio
async def test_wake_candidates_never_lists_a_resident_build(client: AsyncClient):
    """The "never double-schedule a resident build" guarantee: a resident
    build's flag is set by its own workers' notifies and read by nobody."""
    resident = await _build(client, app=None)
    await client.post(f"/api/v1/builds/{resident}/notify")
    assert await _candidates(client) == []


@pytest.mark.asyncio
async def test_wake_candidates_is_scoped_to_the_environment(
    client: AsyncClient, as_environment_b
):
    a, b = await _shared(client)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    with as_environment_b():
        assert await _candidates(client) == []
    assert [c["build_id"] for c in await _candidates(client)] == [b]


@pytest.mark.asyncio
async def test_wake_candidates_respects_the_limit_oldest_first(
    client: AsyncClient,
):
    a = await _build(client)
    neighbours = [await _build(client, f"app-{i}") for i in range(3)]
    await _register(client, a, "shared")
    for n in neighbours:
        await _register(client, n, "shared")
    await _start(client, a, "shared")
    await _clear(client, a, *neighbours)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")
    first = await _candidates(client, limit=2)
    second = await _candidates(client, limit=2)
    assert len(first) == 2 and len(second) == 1
    assert {c["build_id"] for c in first + second} == set(neighbours)


@pytest.mark.asyncio
async def test_notify_marks_the_build_as_requested_when_no_scheduler_is_live(
    client: AsyncClient,
):
    """The notifying worker is about to spawn; wake-candidates must not hand
    the same build to a second spawner."""
    a = await _build(client)
    notify = (await client.post(f"/api/v1/builds/{a}/notify")).json()
    assert notify["scheduler_live"] is False
    assert await _candidates(client) == []


@pytest.mark.asyncio
async def test_notify_with_a_live_scheduler_does_not_mark_the_build(
    client: AsyncClient, async_session: AsyncSession
):
    a = await _build(client)
    await _hold_lease(client, a)
    notify = (await client.post(f"/api/v1/builds/{a}/notify")).json()
    assert notify["scheduler_live"] is True
    build = await async_session.get(Build, uuid.UUID(a))
    assert build is not None
    assert build.tick_requested_at is None


@pytest.mark.asyncio
async def test_reactive_meta_rejects_an_empty_app_name(client: AsyncClient):
    build_id = (await client.post("/api/v1/builds", json={})).json()["id"]
    response = await client.put(
        f"/api/v1/builds/{build_id}/reactive-meta", json={"app_name": ""}
    )
    assert response.status_code == 422


# --- registration flags nobody (STA-16) ---------------------------------
#
# The one behaviour change in routing every path through ``transition_task``:
# the two registration paths now run the wake-up hook, which they did not
# before. It is a no-op only because TASK_PENDING and TASK_REFERENCED are
# status-neutral, and nothing else in this file can catch a regression —
# every helper that registers calls ``_clear`` afterwards, wiping exactly
# the flag a spurious registration wake-up would set.


async def test_registering_a_new_task_flags_nobody(client: AsyncClient):
    a, b = await _shared(client)
    await _clear(client, a, b)

    await _register(client, a, "brand-new")

    assert await _needs_tick(client, a) is False
    assert await _needs_tick(client, b) is False


async def test_re_referencing_a_running_task_flags_nobody(client: AsyncClient):
    """The case most likely to break: build B registers a task that is
    already RUNNING under build A. The event is TASK_REFERENCED, and if it
    ever stopped being status-neutral this would flag A for a change that
    did not happen."""
    a, b = await _shared(client)
    await _start(client, a, "shared")
    await _clear(client, a, b)

    await _register(client, b, "shared")

    assert await _needs_tick(client, a) is False
    assert await _needs_tick(client, b) is False


async def test_bulk_registration_flags_nobody(client: AsyncClient):
    """Also the performance guard the bulk loop rests on.

    The hook returns at its first line for every task, so a 500-task plan
    issues no wake-up queries at all. If TASK_PENDING/TASK_REFERENCED ever
    stopped being status-neutral, that loop would turn into two queries per
    task against ``builds``, inside a transaction already holding
    ``FOR UPDATE`` on every task row.
    """
    a, b = await _shared(client)
    await _clear(client, a, b)

    payload = {
        "tasks": [
            {
                "task_id": f"bulk-{i}",
                "task_name": "T",
                "task_namespace": "",
                "task_data": {},
                "dependency_task_ids": [],
            }
            for i in range(25)
        ]
    }
    response = await client.post(f"/api/v1/builds/{a}/tasks/bulk", json=payload)
    assert response.status_code in (200, 201), response.text

    assert await _needs_tick(client, a) is False
    assert await _needs_tick(client, b) is False


# --- GET /builds/{id}/notify: the linger poll's one-row read (STA-18) ----


async def test_reading_the_flag_agrees_with_the_frontier(client: AsyncClient):
    """The whole point is that it is the *same* boolean, more cheaply.

    A second source of truth for "does this build need a tick" would be
    worse than the frontier read it replaces, so the two are asserted
    together rather than separately.
    """
    build_id, other_id = await _shared(client)
    await _clear(client, build_id, other_id)

    assert (await client.get(f"/api/v1/builds/{build_id}/notify")).json()[
        "needs_tick"
    ] is False
    assert await _needs_tick(client, build_id) is False

    await client.post(f"/api/v1/builds/{build_id}/notify")

    assert (await client.get(f"/api/v1/builds/{build_id}/notify")).json()[
        "needs_tick"
    ] is True
    assert await _needs_tick(client, build_id) is True


async def test_reading_the_flag_does_not_set_or_clear_it(client: AsyncClient):
    """A GET is a read. The linger poll calls it every few seconds while
    the tick still holds the lease; a read with a side effect on the flag
    would either wake the build forever or swallow a wake-up."""
    build_id = await _build(client)
    await client.post(f"/api/v1/builds/{build_id}/notify")

    for _ in range(3):
        assert (await client.get(f"/api/v1/builds/{build_id}/notify")).json()[
            "needs_tick"
        ] is True

    await client.delete(f"/api/v1/builds/{build_id}/notify")
    for _ in range(3):
        assert (await client.get(f"/api/v1/builds/{build_id}/notify")).json()[
            "needs_tick"
        ] is False


async def test_reading_the_flag_reports_no_scheduler_liveness(client: AsyncClient):
    """``scheduler_live`` is absent by design, not by omission: the caller
    holds the lease, so the answer would only ever be itself — and computing
    it costs the second table this endpoint exists to avoid. It must not be
    reported as ``False``, which a caller reads as "nobody is scheduling"."""
    build_id = await _build(client)
    await _hold_lease(client, build_id)

    body = (await client.get(f"/api/v1/builds/{build_id}/notify")).json()

    assert body["scheduler_live"] is None


async def test_reading_the_flag_of_an_unknown_build_is_404(client: AsyncClient):
    response = await client.get(f"/api/v1/builds/{uuid.uuid4()}/notify")
    assert response.status_code == 404


# --- The scheduler lease, on the build row (STA-17) ---------------------


LEASE = "/api/v1/builds/{}/scheduler-lease"


async def _lease(client: AsyncClient, method: str, build_id: str, **params) -> dict:
    response = await client.request(method, LEASE.format(build_id), params=params)
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
async def test_one_tick_holds_the_lease_and_the_next_is_refused(client: AsyncClient):
    """The single-flight guarantee: at most one tick drives a build."""
    build_id = await _build(client)

    first = await _lease(client, "POST", build_id, owner_id="tick-1", ttl_seconds=60)
    second = await _lease(client, "POST", build_id, owner_id="tick-2", ttl_seconds=60)

    assert first["held"] is True
    assert second["held"] is False
    # The denial reports the current holder's expiry, so a caller learns how
    # long the build is spoken for without a second read.
    assert second["expires_at"] == first["expires_at"]


@pytest.mark.asyncio
async def test_a_lapsed_lease_is_taken_over(
    client: AsyncClient, async_session: AsyncSession
):
    """A tick whose container vanished releases nothing, and across
    containers there is nothing to release it with. The takeover *is* the
    healing mechanism."""
    build_id = await _build(client)
    await _lease(client, "POST", build_id, owner_id="dead-tick", ttl_seconds=60)
    build = (
        await async_session.execute(
            select(Build).where(Build.id == uuid.UUID(build_id))
        )
    ).scalar_one()
    build.scheduler_lease_until = datetime.now(timezone.utc) - timedelta(seconds=1)
    await async_session.commit()

    taken = await _lease(client, "POST", build_id, owner_id="new-tick", ttl_seconds=60)

    assert taken["held"] is True
    # And the dead holder cannot renew its way back in.
    assert (
        await _lease(client, "PUT", build_id, owner_id="dead-tick", ttl_seconds=60)
    )["held"] is False


@pytest.mark.asyncio
async def test_only_the_holder_can_renew_or_release(client: AsyncClient):
    """Owner-checked, and that is what makes a slow tick safe: one whose
    lease lapsed and was taken over must not extend or clear its
    successor's."""
    build_id = await _build(client)
    await _lease(client, "POST", build_id, owner_id="holder", ttl_seconds=60)

    assert (await _lease(client, "PUT", build_id, owner_id="impostor", ttl_seconds=60))[
        "held"
    ] is False
    assert (await _lease(client, "DELETE", build_id, owner_id="impostor"))[
        "held"
    ] is False
    # The real holder still has it.
    assert (
        await _lease(client, "POST", build_id, owner_id="somebody-else", ttl_seconds=60)
    )["held"] is False

    assert (await _lease(client, "DELETE", build_id, owner_id="holder"))["held"] is True
    # Released: free for the next tick.
    assert (await _lease(client, "POST", build_id, owner_id="next", ttl_seconds=60))[
        "held"
    ] is True


@pytest.mark.asyncio
async def test_renewing_extends_the_expiry(client: AsyncClient):
    build_id = await _build(client)
    first = await _lease(client, "POST", build_id, owner_id="t", ttl_seconds=10)
    renewed = await _lease(client, "PUT", build_id, owner_id="t", ttl_seconds=600)

    assert renewed["held"] is True
    assert renewed["expires_at"] > first["expires_at"]


@pytest.mark.asyncio
async def test_a_held_lease_hides_the_build_from_wake_candidates(client: AsyncClient):
    """The same column answers both questions now — one query, no second
    table and no lock name assembled from a build id."""
    a, b = await _shared(client)
    await _lease(client, "POST", b, owner_id="tick-b", ttl_seconds=60)
    await client.post(f"/api/v1/builds/{a}/tasks/shared/complete")

    assert [c["build_id"] for c in await _candidates(client)] == []

    await _lease(client, "DELETE", b, owner_id="tick-b")
    assert [c["build_id"] for c in await _candidates(client)] == [b]


@pytest.mark.asyncio
async def test_lease_ttl_is_bounded(client: AsyncClient):
    """A lease so short it lapses mid-pass would let a second tick in; one
    long enough to outlive a person's patience holds a build hostage."""
    build_id = await _build(client)

    assert (
        await client.post(
            LEASE.format(build_id), params={"owner_id": "t", "ttl_seconds": 1}
        )
    ).status_code == 422
    assert (
        await client.post(
            LEASE.format(build_id), params={"owner_id": "t", "ttl_seconds": 99999}
        )
    ).status_code == 422
