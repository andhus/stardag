import asyncio
import logging
import typing

from prefect import task as prefect_task
from prefect.artifacts import create_markdown_artifact
from prefect.futures import PrefectConcurrentFuture

from stardag.integration.prefect.utils import format_key
from stardag.task import Task, TaskDeps, flatten_task_struct

logger = logging.getLogger(__name__)


Callback = typing.Callable[[Task], typing.Awaitable[None]]


async def build(
    task: Task,
    before_run_callback: Callback | None = None,
    after_run_callback: Callback | None = None,
    wait_for_completion: bool = True,
) -> dict[str, PrefectConcurrentFuture]:
    task_id_to_future = {}
    task_id_to_dynamic_future = {}
    task_id_to_dynamic_deps = {}
    res = await build_dag_recursive(
        task,
        before_run_callback=before_run_callback,
        after_run_callback=after_run_callback,
        task_id_to_future=task_id_to_future,
        task_id_to_dynamic_future=task_id_to_dynamic_future,
        task_id_to_dynamic_deps=task_id_to_dynamic_deps,
        visited=set([]),
    )
    while res is None:
        # Get next completed dynamic task
        task_id, dynamic_future = await next(
            asyncio.as_completed(
                [
                    _completed_prefect_future(task_id, prefect_future)
                    for task_id, prefect_future in task_id_to_dynamic_future.items()
                ]
            )
        )
        del task_id_to_dynamic_future[task_id]  # important to avoid infinite loop
        result = dynamic_future.result()
        # TODO avoid duplicate task_id references
        task_id, dynamic_deps = result
        # TODO check for exceptions
        if dynamic_deps is None:
            # task completed
            task_id_to_future[task_id] = dynamic_future
        else:
            prev_dynamic_deps, _ = task_id_to_dynamic_deps.get(task_id, ([], None))
            task_id_to_dynamic_deps[task_id] = (
                prev_dynamic_deps + dynamic_deps,
                dynamic_future,
            )

        res = await build_dag_recursive(
            task,
            before_run_callback=before_run_callback,
            after_run_callback=after_run_callback,
            task_id_to_future=task_id_to_future,
            task_id_to_dynamic_future=task_id_to_dynamic_future,
            task_id_to_dynamic_deps=task_id_to_dynamic_deps,
            visited=set([]),
        )
    if wait_for_completion:
        for future in task_id_to_future.values():
            future.wait()

    return task_id_to_future


async def build_dag_recursive(
    task: Task,
    *,
    before_run_callback: Callback | None,
    after_run_callback: Callback | None,
    task_id_to_future: dict[str, PrefectConcurrentFuture | None],
    task_id_to_dynamic_future: dict[
        str, PrefectConcurrentFuture
    ],  # dynamic tasks tentative run
    task_id_to_dynamic_deps: dict[
        str, tuple[list[Task], PrefectConcurrentFuture]
    ],  # dynamic tasks' dependencies
    visited: set[str],  # check for cyclic dependencies
) -> (
    PrefectConcurrentFuture | None
):  # None means could not be scheduled yet, because has dynamic dep task as upstream
    """Translates a stardag task-DAG into Prefect flow logic."""
    # print(f"\nBuilding task {task.task_id}")
    # pprint(task_id_to_future)
    # pprint(task_id_to_dynamic_future)
    # pprint(task_id_to_dynamic_deps)
    # print()

    async def run(task: Task):
        if before_run_callback is not None:
            await before_run_callback(task)
        res = task.run()
        if after_run_callback is not None:
            await after_run_callback(task)
        return res

    # Cycle detection
    if task.task_id in visited:
        raise ValueError("Cyclic dependencies detected")

    # Hitting a built "branch"
    already_built_future = task_id_to_future.get(task.task_id, None)
    if already_built_future is not None:
        return already_built_future

    already_built_dynamic_future = task_id_to_dynamic_future.get(task.task_id, None)
    if already_built_dynamic_future is not None:
        return None

    # Recurse dependencies
    dynamic_deps, prev_dynamic_future = task_id_to_dynamic_deps.get(
        task.task_id, ([], None)
    )
    upstream_tasks = task.deps() + dynamic_deps
    upstream_build_results = [
        await build_dag_recursive(
            dep,
            before_run_callback=before_run_callback,
            after_run_callback=after_run_callback,
            task_id_to_future=task_id_to_future,
            task_id_to_dynamic_future=task_id_to_dynamic_future,
            task_id_to_dynamic_deps=task_id_to_dynamic_deps,
            visited=visited | {task.task_id},
        )
        for dep in upstream_tasks
    ]
    logger.debug(f"Upstream build results: {upstream_build_results}")
    if any([res is None for res in upstream_build_results]):
        # Task with dynamic deps upstream
        return None

    if task.has_dynamic_deps():

        @prefect_task(name=f"{task.id_ref.slug}-dynamic")
        async def stardag_dynamic_task():
            # TODO: concurrency lock
            if not task.complete():
                try:
                    gen = await run(task)
                    assert hasattr(gen, "__next__")
                    gen = typing.cast(typing.Generator[TaskDeps, None, None], gen)

                    requires = next(gen)
                    deps = flatten_task_struct(requires)
                    completed = [dep.complete() for dep in deps]
                    logger.debug(f"Initial deps: {deps}, completed: {completed}")
                    while all(completed):
                        logger.debug("All deps complete")
                        requires = next(gen)
                        deps = flatten_task_struct(requires)
                        completed = [dep.complete() for dep in deps]
                        logger.debug(f"Deps: {deps}, completed: {completed}")

                    return task.task_id, deps

                except StopIteration:
                    logger.debug("Task completed")
                    return task.task_id, None

        extra_deps = [prev_dynamic_future] if prev_dynamic_future is not None else []
        future = stardag_dynamic_task.submit(  # type: ignore
            wait_for=upstream_build_results + extra_deps
        )
        task_id_to_dynamic_future[task.task_id] = future

        # signal that this task has dynamic deps, we can't build downstream tasks yet
        return None

    @prefect_task(
        name=task.id_ref.slug,
        # TODO caching. Make sure to include environment (stardag root) in cache key
        # cache_key_fn=lambda *args, **kwargs: task.task_id
    )
    async def stardag_task():
        # TODO: concurrency lock
        if not task.complete():
            res = await run(task)
            # check if it's a generator
            if hasattr(res, "__next__"):
                raise AssertionError(
                    "Tasks with dynamic deps should be executed separately."
                )
            # TODO remove, should be handled by `on_complete_callback`
            # if hasattr(task, "prefect_on_complete_artifacts"):
            #     for artifact in task.prefect_on_complete_artifacts():
            #         await artifact.create()

        return task.task_id

    future = stardag_task.submit(wait_for=upstream_build_results)  # type: ignore
    task_id_to_future[task.task_id] = future

    return future


async def _completed_prefect_future(
    key, future: PrefectConcurrentFuture, timeout: float | None = None
):
    future.wait(timeout=timeout)
    return key, future


async def create_markdown(task: Task):
    output_path = getattr(task.output(), "path", None)
    markdown = f"""# {task.id_ref.slug}
**Task id**: `{task.task_id}`
**Task class**: `{task.__module__}.{task.__class__.__name__}`
**Output path**: [{output_path}]({output_path})
**Task spec**
```dict
{task.model_dump_json(indent=2)}
```
"""

    return await create_markdown_artifact(  # type: ignore
        key=format_key(f"{task.id_ref.slug}-spec"),
        description=f"Task spec for {task.task_id}",
        markdown=markdown,
    )
