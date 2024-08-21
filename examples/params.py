import json

from pydantic import BaseModel

from dcdag.core.parameter import ParamField
from dcdag.core.target import InMemoryTarget, LoadableTarget
from dcdag.core.task import Task, TaskParam


class TestModel(BaseModel):
    test: str = "testing"


class SubTask(Task[LoadableTarget[int]]):
    param: int

    def output(self) -> InMemoryTarget[int]:
        return InMemoryTarget(self.task_id)

    def run(self) -> None:
        self.output().save(self.param)


class MyTask(Task[LoadableTarget[str]]):
    a: int
    b: str
    c: str = ParamField(
        "C",
        significant=False,
        # id_hash_include=lambda x: x != "C",
    )
    model: TestModel = TestModel()
    sub_task: TaskParam[Task[LoadableTarget[int]]]
    sub_task_2: TaskParam[SubTask]

    def output(self) -> InMemoryTarget[str]:
        return InMemoryTarget(self.task_id)

    def run(self) -> None:
        self.output().save(f"{self.a}, {self.b}")


if __name__ == "__main__":
    my_task = MyTask(
        a=1,
        b="b",
        sub_task=SubTask(param=1),
        sub_task_2=SubTask(param=2),
    )
    print(json.dumps(my_task._id_hash_jsonable(), indent=2))
    print(my_task.task_id)
    print(my_task.id_ref)
    dumped = my_task.model_dump()
    print(dumped)
    print(MyTask.model_validate(dumped))
