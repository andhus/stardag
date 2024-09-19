from stardag.auto_task import AutoFSTTask
from stardag.target import LoadableTarget
from stardag.task import Task, auto_namespace
from stardag.task_parameter import TaskParam

LeafTaskLoadedT = dict[str, str | int | None]

auto_namespace(__name__)


class LeafTask(AutoFSTTask[LeafTaskLoadedT]):
    param_a: int
    param_b: str

    def run(self):
        self.output().save(self.model_dump(mode="json"))


ParentTaskLoadedT = dict[str, list[LeafTaskLoadedT]]


class ParentTask(AutoFSTTask[ParentTaskLoadedT]):
    param_ab_s: list[tuple[int, str]]

    def requires(self):
        return [LeafTask(param_a=a, param_b=b) for a, b in self.param_ab_s]

    def run(self):
        self.output().save(
            {"leaf_tasks": [task.output().load() for task in self.requires()]}
        )


RootTaskLoadedT = dict[str, ParentTaskLoadedT]


class RootTask(AutoFSTTask[RootTaskLoadedT]):
    parent_task: TaskParam[Task[LoadableTarget[ParentTaskLoadedT]]]

    def requires(self):
        return self.parent_task

    def run(self):
        self.output().save({"parent_task": self.parent_task.output().load()})


def get_simple_dag():
    return RootTask(
        parent_task=ParentTask(param_ab_s=[(1, "a"), (2, "b")]),
    )


def get_simple_dag_expected_root_output():
    return {
        "parent_task": {
            "leaf_tasks": [
                {"version": None, "param_a": 1, "param_b": "a"},
                {"version": None, "param_a": 2, "param_b": "b"},
            ]
        },
    }
