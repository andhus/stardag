from stardag.task import Task


def build(task: Task, completion_cache: set[str] | None = None) -> None:
    _build(task, completion_cache or set())


def _build(task: Task, completion_cache: set[str]) -> None:
    if _is_complete(task, completion_cache):
        return

    for dep in task.deps():
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
