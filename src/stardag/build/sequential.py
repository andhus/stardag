from stardag.task import Task, TaskDeps


def build(task: Task, completion_cache: set[str] | None = None) -> None:
    _build(task, completion_cache or set())


def _build(task: Task, completion_cache: set[str]) -> None:
    if _is_complete(task, completion_cache):
        return

    deps = _flatten(task.requires())
    for dep in deps:
        _build(dep, completion_cache)

    task.run()
    completion_cache.add(task.task_id)


def _is_complete(task: Task, completion_cache: set[str]) -> bool:
    if task.task_id in completion_cache:
        return True
    if task.complete():
        completion_cache.add(task.task_id)
        return True
    return False


def _flatten(requires_output: TaskDeps) -> list[Task]:
    if requires_output is None:
        return []
    if isinstance(requires_output, Task):
        return [requires_output]
    if isinstance(requires_output, (list, tuple)):
        return sum([_flatten(task) for task in requires_output], [])

    if isinstance(requires_output, dict):
        return sum([_flatten(task) for task in requires_output.values()], [])

    raise ValueError(f"Unsupported requires_output type: {requires_output!r}")
