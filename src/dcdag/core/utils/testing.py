from dcdag.core.fsttask import AutoFSTTask
from dcdag.core.target import LoadableTarget
from dcdag.core.task import Task
from dcdag.core.task_parameter import TaskParam

TestTaskLeafLoadedT = dict[str, str | int | None]


class TestTaskLeaf(AutoFSTTask[TestTaskLeafLoadedT]):
    param_a: int
    param_b: str

    def run(self):
        self.output().save(self.model_dump(mode="json"))


TestTaskParentLoadedT = dict[str, list[TestTaskLeafLoadedT]]


class TestTaskParent(AutoFSTTask[TestTaskParentLoadedT]):
    param_ab_s: list[tuple[int, str]]

    def requires(self):
        return [TestTaskLeaf(param_a=a, param_b=b) for a, b in self.param_ab_s]

    def run(self):
        self.output().save(
            {"leaf_tasks": [task.output().load() for task in self.requires()]}
        )


TestTaskRootLoadedT = dict[str, TestTaskParentLoadedT]


class TestTaskRoot(AutoFSTTask[TestTaskRootLoadedT]):
    parent_task: TaskParam[Task[LoadableTarget[TestTaskParentLoadedT]]]

    def requires(self):
        return self.parent_task

    def run(self):
        self.output().save({"parent_task": self.parent_task.output().load()})


def get_simple_dag():
    return TestTaskRoot(
        parent_task=TestTaskParent(param_ab_s=[(1, "a"), (2, "b")]),
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
