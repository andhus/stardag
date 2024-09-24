"""Comparison of three levels of the task API.

The following three ways of specifying a root_task, its dependencies, persistent
targets and serialization are 100% equivalent.
"""

from stardag.task_parameter import TaskLoads


def decorator_api(limit: int) -> TaskLoads[int]:
    from stardag.decorator import Depends, task

    # @task(family="Range")  # TODO
    @task
    def get_range(limit: int) -> list[int]:
        return list(range(limit))

    # @task(family="Sum")  # TODO
    @task
    def get_sum(integers: Depends[list[int]]) -> int:
        return sum(integers)

    return get_sum(integers=get_range(limit=limit))


def auto_fst_task_api(limit: int) -> TaskLoads[int]:
    from stardag.auto_task import AutoFSTTask

    class Range(AutoFSTTask[list[int]]):
        limit: int

        def run(self):
            self.output().save(list(range(self.limit)))

    class Sum(AutoFSTTask[int]):
        integers: TaskLoads[list[int]]

        def requires(self):
            return self.integers

        def run(self):
            self.output().save(sum(self.integers.output().load()))

    return Sum(integers=Range(limit=limit))


def base_task_api(limit: int) -> TaskLoads[int]:
    from stardag.resources import get_target
    from stardag.target import LoadableSaveableFileSystemTarget
    from stardag.target.serialize import JSONSerializer, Serializable
    from stardag.task import Task

    def default_relpath(task: Task) -> str:
        return "/".join(
            [
                task.get_namespace().replace(".", "/"),
                task.get_family(),
                task.task_id[:2],
                task.task_id[2:4],
                f"{task.task_id}.json",
            ]
        )

    class Range(Task[LoadableSaveableFileSystemTarget[list[int]]]):
        limit: int

        def output(self) -> LoadableSaveableFileSystemTarget[list[int]]:
            return Serializable(
                wrapped=get_target(default_relpath(self), task=self),
                serializer=JSONSerializer(list[int]),
            )

        def run(self):
            self.output().save(list(range(self.limit)))

    class Sum(Task[LoadableSaveableFileSystemTarget[int]]):
        integers: TaskLoads[list[int]]

        def requires(self):
            return self.integers

        def output(self) -> LoadableSaveableFileSystemTarget[int]:
            return Serializable(
                wrapped=get_target(default_relpath(self), task=self),
                serializer=JSONSerializer(int),
            )

        def run(self):
            return self.output().save(sum(self.integers.output().load()))

    return Sum(integers=Range(limit=limit))
