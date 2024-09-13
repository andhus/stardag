from dcdag.core.task import Task


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


def _flatten(requires_tasks) -> list[Task]:
    # TODO general case!
    if requires_tasks is None:
        return []
    if isinstance(requires_tasks, Task):
        return [requires_tasks]
    return requires_tasks
