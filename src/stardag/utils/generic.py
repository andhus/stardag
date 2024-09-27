from collections.abc import Mapping, Sequence

from stardag.task import Task, TaskStruct


def flatten_task_struct(task_struct: TaskStruct) -> list[Task]:
    """Flatten a TaskStruct into a list of Tasks.

    TaskStruct: TypeAlias = Union[
        "Task", Sequence["TaskStruct"], Mapping[str, "TaskStruct"]
    ]
    """
    if isinstance(task_struct, Task):
        return [task_struct]

    if isinstance(task_struct, Sequence):
        return [
            task
            for sub_task_struct in task_struct
            for task in flatten_task_struct(sub_task_struct)
        ]

    if isinstance(task_struct, Mapping):
        return [
            task
            for sub_task_struct in task_struct.values()
            for task in flatten_task_struct(sub_task_struct)
        ]
