"""Tests for the registry-backed resident-build concurrency limiter."""

from __future__ import annotations

import asyncio
import typing
from uuid import uuid4

import pytest

from stardag import auto_namespace
from stardag.exceptions import APIError, AuthenticationError
from stardag.build import (
    BuildExitStatus,
    FailMode,
    RegistryConcurrencyLimiter,
    build_aio,
)
from stardag.build._base import current_build_id_var
from stardag.registry import NoOpRegistry, StartClaimResult
from stardag.target import InMemoryFileTarget
from stardag.utils.testing.helper_tasks import SyncOnlyTask

auto_namespace(__name__)


class CountingLimitRegistry(NoOpRegistry):
    """Fake registry with the enforced-start slot semantics."""

    def __init__(self, caps: dict[str, int]):
        super().__init__()
        self.caps = caps
        self.running_keys: dict[str, set[str]] = {}  # task_id -> keys
        self.acquire_attempts = 0
        self.acquires_by_task: dict[str, int] = {}
        self.max_active_by_key: dict[str, int] = {}
        # Exceptions to raise on upcoming acquire attempts (fifo).
        self.pending_errors: list[Exception] = []

    async def task_start_claim_aio(
        self,
        build_id,
        task,
        executor=None,
        executor_ref=None,
        executor_metadata=None,
        limit_keys=None,
        claim_ttl_seconds=None,
        *,
        claim=True,
    ) -> StartClaimResult:
        # The limiter acquires slots for a task its own build has already
        # claimed, so it must not claim again; assert that here rather than
        # let a regression pass silently against a permissive double.
        assert claim is False, "the limiter must acquire without claiming"
        self.acquire_attempts += 1
        if self.pending_errors:
            raise self.pending_errors.pop(0)
        for key in limit_keys or []:
            cap = self.caps.get(key)
            if cap is None:
                continue
            active = sum(
                1
                for tid, keys in self.running_keys.items()
                if key in keys and tid != str(task.id)
            )
            if active >= cap:
                return StartClaimResult(
                    started=False, denied_reason="limit", denied_keys=[key]
                )
        self.running_keys[str(task.id)] = set(limit_keys or [])
        self.acquires_by_task[str(task.id)] = (
            self.acquires_by_task.get(str(task.id), 0) + 1
        )
        for key in limit_keys or []:
            active = sum(1 for keys in self.running_keys.values() if key in keys)
            self.max_active_by_key[key] = max(
                self.max_active_by_key.get(key, 0), active
            )
        return StartClaimResult(started=True)

    # Slot freed on ANY transition out of RUNNING (mirrors the server:
    # slot = RUNNING status + key rows).
    async def task_complete_aio(self, build_id, task) -> None:
        self.running_keys.pop(str(task.id), None)

    async def task_fail_aio(self, build_id, task, error_message=None) -> None:
        self.running_keys.pop(str(task.id), None)

    async def task_cancel_aio(self, build_id, task, *, only_if_held=False) -> None:
        self.running_keys.pop(str(task.id), None)

    async def task_suspend_aio(self, build_id, task) -> None:
        self.running_keys.pop(str(task.id), None)


class TestRegistryConcurrencyLimiter:
    async def test_no_keys_no_registry_calls(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        registry = CountingLimitRegistry(caps={})
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: [], registry=registry
        )
        token = current_build_id_var.set(uuid4())
        try:
            async with limiter.slot(SyncOnlyTask(name="nokeys")):
                pass
        finally:
            current_build_id_var.reset(token)
        assert registry.acquire_attempts == 0

    async def test_key_normalization(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """A plain-string selector result is one key (not iterated into
        characters) and duplicates are de-duplicated — same contract as
        ConcurrencyConfig.key_selector."""
        registry = CountingLimitRegistry(caps={"one": 5})
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: "one", registry=registry
        )
        task = SyncOnlyTask(name="strkey")
        token = current_build_id_var.set(uuid4())
        try:
            async with limiter.slot(task):
                assert registry.running_keys[str(task.id)] == {"one"}
            dup_limiter = RegistryConcurrencyLimiter(
                key_selector=lambda t: ["one", "one"], registry=registry
            )
            task2 = SyncOnlyTask(name="dupkeys")
            async with dup_limiter.slot(task2):
                assert registry.running_keys[str(task2.id)] == {"one"}
        finally:
            current_build_id_var.reset(token)
        assert limiter._keys_for(task) == ["one"]
        assert (
            RegistryConcurrencyLimiter(
                key_selector=lambda t: None, registry=registry
            )._keys_for(task)
            == []
        )

    async def test_blocks_until_slot_frees(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        registry = CountingLimitRegistry(caps={"one": 1})
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["one"],
            registry=registry,
            poll_interval_seconds=0.01,
        )
        holder = SyncOnlyTask(name="holder")
        waiter = SyncOnlyTask(name="waiter")
        build_id = uuid4()
        token = current_build_id_var.set(build_id)
        try:
            async with limiter.slot(holder):
                waiter_started = asyncio.Event()

                async def acquire_waiter():
                    async with limiter.slot(waiter):
                        waiter_started.set()

                acquire_task = asyncio.create_task(acquire_waiter())
                await asyncio.sleep(0.05)
                assert not waiter_started.is_set()  # blocked at capacity
                # Slot frees when the holder leaves RUNNING:
                await registry.task_complete_aio(build_id, holder)
                await asyncio.wait_for(acquire_task, timeout=2)
                assert waiter_started.is_set()
        finally:
            current_build_id_var.reset(token)

    async def test_timeout_raises(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        registry = CountingLimitRegistry(caps={"one": 1})
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["one"],
            registry=registry,
            poll_interval_seconds=0.01,
            max_wait_seconds=0.05,
        )
        token = current_build_id_var.set(uuid4())
        try:
            async with limiter.slot(SyncOnlyTask(name="t-holder")):
                with pytest.raises(TimeoutError, match="concurrency-limit"):
                    async with limiter.slot(SyncOnlyTask(name="t-waiter")):
                        pass
        finally:
            current_build_id_var.reset(token)

    async def test_end_to_end_build_respects_cap(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """A resident build with the registry limiter completes a fan-out
        while never holding more than the cap concurrently."""
        registry = CountingLimitRegistry(caps={"db": 1})
        deps = tuple(SyncOnlyTask(name=f"fan-{i}") for i in range(4))
        root = SyncOnlyTask(name="fan-root", deps=deps)
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["db"] if t.id != root.id else [],
            registry=registry,
            poll_interval_seconds=0.01,
        )

        summary = await build_aio(
            [root], registry=registry, concurrency_limiter=limiter
        )

        assert summary.status == BuildExitStatus.SUCCESS
        assert root.complete()
        # The invariant: never more than the cap held concurrently.
        assert registry.max_active_by_key["db"] == 1


class TestAcquireErrorHandling:
    async def test_transient_error_retried(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """A registry blip (5xx/network/429) during acquire is retried with
        backoff instead of failing the task."""
        registry = CountingLimitRegistry(caps={"one": 1})
        registry.pending_errors = [
            APIError("boom", status_code=503),
            APIError("conn reset"),  # no status: network-level
        ]
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["one"],
            registry=registry,
            poll_interval_seconds=0.01,
        )
        task = SyncOnlyTask(name="blip")
        token = current_build_id_var.set(uuid4())
        try:
            async with limiter.slot(task):
                pass
        finally:
            current_build_id_var.reset(token)
        assert registry.acquire_attempts == 3  # 2 errors + 1 success
        assert registry.acquires_by_task[str(task.id)] == 1

    async def test_non_transient_error_raises(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Auth/validation errors raise immediately — retrying can't help."""
        registry = CountingLimitRegistry(caps={"one": 1})
        registry.pending_errors = [AuthenticationError("bad token")]
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["one"], registry=registry
        )
        token = current_build_id_var.set(uuid4())
        try:
            with pytest.raises(AuthenticationError):
                async with limiter.slot(SyncOnlyTask(name="denied")):
                    pass
        finally:
            current_build_id_var.reset(token)
        assert registry.acquire_attempts == 1


class TestReleasePathsInBuild:
    async def test_failed_task_frees_slot(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """A task failure releases the slot (slot = RUNNING status): the
        next task under the same cap can proceed."""
        from stardag.utils.testing.helper_tasks import FailingTask

        registry = CountingLimitRegistry(caps={"one": 1})
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["one"],
            registry=registry,
            poll_interval_seconds=0.01,
        )
        failing = FailingTask(error_message="rel-fail")
        ok = SyncOnlyTask(name="rel-ok")

        summary = await build_aio(
            [failing, ok],
            registry=registry,
            concurrency_limiter=limiter,
            fail_mode=FailMode.CONTINUE,
        )

        assert summary.status == BuildExitStatus.FAILURE  # failing failed...
        assert ok.complete()  # ...but its slot was freed for ok
        assert registry.max_active_by_key["one"] == 1
        assert str(failing.id) not in registry.running_keys

    async def test_suspension_frees_slot_and_resume_reacquires(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """Dynamic-deps suspension releases the slot; resuming re-acquires
        (parity with LocalConcurrencyLimiter)."""
        from stardag.utils.testing.helper_tasks import DynamicDiamondTask

        registry = CountingLimitRegistry(caps={"gpu": 1})
        dep = DynamicDiamondTask(name="susp-dep", test_id="rl-susp")
        root = DynamicDiamondTask(
            name="susp-root", test_id="rl-susp", dynamic_task_deps=(dep,)
        )
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["gpu"] if t.id == root.id else [],
            registry=registry,
            poll_interval_seconds=0.01,
        )

        summary = await build_aio(
            [root], registry=registry, concurrency_limiter=limiter
        )

        assert summary.status == BuildExitStatus.SUCCESS
        assert root.complete()
        # Acquired before the first run segment AND again after resuming.
        assert registry.acquires_by_task[str(root.id)] == 2

    async def test_timeout_fails_task_not_build_crash(
        self, default_in_memory_fs_target: typing.Type[InMemoryFileTarget]
    ):
        """max_wait exceeded inside a build → the waiting task fails like
        any task error (no deadlock; under FAIL_FAST it would re-raise per
        the engine's normal contract)."""
        registry = CountingLimitRegistry(caps={"one": 1})
        # Slot pre-held by a task outside this build (e.g. another process).
        registry.running_keys["some-other-task"] = {"one"}
        limiter = RegistryConcurrencyLimiter(
            key_selector=lambda t: ["one"],
            registry=registry,
            poll_interval_seconds=0.01,
            max_wait_seconds=0.05,
        )
        starved = SyncOnlyTask(name="starved")

        summary = await build_aio(
            [starved],
            registry=registry,
            concurrency_limiter=limiter,
            fail_mode=FailMode.CONTINUE,
        )

        assert summary.status == BuildExitStatus.FAILURE
        assert not starved.complete()
