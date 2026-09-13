"""Fixtures for build tests."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from stardag import BaseTask
from stardag.registry import NoOpRegistry, RegisteredTaskInfo


@pytest.fixture
def noop_registry():
    """Provide a NoOpRegistry for tests.

    TODO remove? Could be wrapped by Mock to track called methods.
    """
    return NoOpRegistry()


class RecordingRegistry(NoOpRegistry):
    """NoOpRegistry that records every async lifecycle call.

    Use in tests that need to assert which registry events fired (and in
    what order). Each call is appended to ``self.calls`` as a tuple of
    ``(method_name, task_id_or_None, extra)`` where ``task_id_or_None`` is
    the ``BaseTask.id`` for task-scoped methods or ``None`` for build-
    scoped methods.
    """

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, UUID | None, dict[str, Any]]] = []
        # When set, returned from task_register_bulk_aio — lets tests
        # simulate a registry reporting re-attachable detached executions.
        self.bulk_register_response: list[RegisteredTaskInfo] | None = None

    def _record(
        self,
        method: str,
        task_id: UUID | None = None,
        **extra: Any,
    ) -> None:
        self.calls.append((method, task_id, extra))

    def call_methods_for(self, task_id: UUID) -> list[str]:
        return [m for (m, tid, _) in self.calls if tid == task_id]

    def has_call(self, method: str, task_id: UUID | None = None) -> bool:
        if task_id is None:
            return any(m == method for (m, _, _) in self.calls)
        return any(m == method and tid == task_id for (m, tid, _) in self.calls)

    # ----- async lifecycle overrides -----
    async def build_start_aio(
        self,
        root_tasks: list[BaseTask] | None = None,
        description: str | None = None,
        executor_metadata: dict | None = None,
    ) -> UUID:
        bid = await super().build_start_aio(
            root_tasks, description, executor_metadata=executor_metadata
        )
        self._record("build_start_aio")
        return bid

    async def build_resume_aio(
        self, build_id: UUID, executor_metadata: dict | None = None
    ) -> None:
        self._record("build_resume_aio")
        await super().build_resume_aio(build_id, executor_metadata=executor_metadata)

    def build_resume(
        self, build_id: UUID, executor_metadata: dict | None = None
    ) -> None:
        self._record("build_resume")
        super().build_resume(build_id, executor_metadata=executor_metadata)

    async def build_complete_aio(self, build_id: UUID) -> None:
        self._record("build_complete_aio")
        await super().build_complete_aio(build_id)

    async def build_fail_aio(
        self, build_id: UUID, error_message: str | None = None
    ) -> None:
        self._record("build_fail_aio", None, error_message=error_message)
        await super().build_fail_aio(build_id, error_message)

    async def task_register_aio(self, build_id: UUID, task: BaseTask) -> None:
        self._record("task_register_aio", task.id)
        await super().task_register_aio(build_id, task)

    async def task_register_bulk_aio(
        self, build_id: UUID, tasks, *, limit_keys=None
    ) -> list[RegisteredTaskInfo] | None:
        for t in tasks:
            self._record("task_register_aio", t.id, bulk=True)
        # Skip super (would emit per-task again).
        return self.bulk_register_response

    async def task_start_aio(
        self,
        build_id: UUID,
        task: BaseTask,
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict | None = None,
        claim_ttl_seconds: int | None = None,
    ) -> None:
        self._record(
            "task_start_aio",
            task.id,
            executor=executor,
            executor_ref=executor_ref,
            executor_metadata=executor_metadata,
        )
        await super().task_start_aio(
            build_id,
            task,
            executor=executor,
            executor_ref=executor_ref,
            executor_metadata=executor_metadata,
        )

    async def task_complete_aio(self, build_id: UUID, task: BaseTask) -> None:
        self._record("task_complete_aio", task.id)
        await super().task_complete_aio(build_id, task)

    async def task_fail_aio(
        self,
        build_id: UUID,
        task: BaseTask,
        error_message: str | None = None,
    ) -> None:
        self._record("task_fail_aio", task.id, error_message=error_message)
        await super().task_fail_aio(build_id, task, error_message)

    async def task_cancel_aio(
        self, build_id: UUID, task: BaseTask, *, if_executor_ref: str | None = None
    ) -> None:
        self._record("task_cancel_aio", task.id)
        await super().task_cancel_aio(build_id, task, if_executor_ref=if_executor_ref)

    async def task_skip_aio(self, build_id: UUID, task: BaseTask) -> None:
        self._record("task_skip_aio", task.id)
        await super().task_skip_aio(build_id, task)

    async def task_interrupt_aio(
        self, build_id: UUID, task: BaseTask, reason: str | None = None
    ) -> None:
        self._record("task_interrupt_aio", task.id, reason=reason)
        await super().task_interrupt_aio(build_id, task, reason)

    async def task_suspend_aio(self, build_id: UUID, task: BaseTask) -> None:
        self._record("task_suspend_aio", task.id)
        await super().task_suspend_aio(build_id, task)

    async def task_resume_aio(self, build_id: UUID, task: BaseTask) -> None:
        self._record("task_resume_aio", task.id)
        await super().task_resume_aio(build_id, task)


@pytest.fixture
def recording_registry() -> RecordingRegistry:
    """Recording registry that tracks every lifecycle call for assertions."""
    return RecordingRegistry()
