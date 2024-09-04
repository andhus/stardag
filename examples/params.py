import json
import typing

from pydantic import BaseModel

from dcdag.core.fsttask import AutoFSTTask
from dcdag.core.parameter import IDHashInclude
from dcdag.core.resources import target_factory_provider
from dcdag.core.resources.target_factory import TargetFactory
from dcdag.core.target import (
    LSFST,
    InMemoryFileSystemTarget,
    InMemoryTarget,
    JSONSerializer,
    LoadableTarget,
    Serializable,
)
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
    c: typing.Annotated[str, IDHashInclude(False)] = "C"
    model: TestModel = TestModel()
    sub_task: TaskParam[Task[LoadableTarget[int]]]
    sub_task_2: TaskParam[SubTask]

    def output(self) -> InMemoryTarget[str]:
        return InMemoryTarget(self.task_id)

    def run(self) -> None:
        self.output().save(f"{self.a}, {self.b}")


class OtherTask(Task[LSFST[dict]]):
    a: int
    b: str

    def output(self) -> LSFST[dict]:
        return Serializable(
            InMemoryFileSystemTarget(self.task_id),
            serializer=JSONSerializer(annotation=dict),
        )

    def run(self) -> None:
        self.output().save({"a": self.a, "b": self.b})


class OtherTask2(AutoFSTTask[dict]):
    a: int
    b: str

    def run(self) -> None:
        self.output().save({"a": self.a, "b": self.b})


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

    other_task = OtherTask2(a=1, b="b")
    other_task.run()
    assert other_task.complete()
    print(other_task.output().load())
    print(InMemoryFileSystemTarget.path_to_bytes)

    with target_factory_provider.override(
        TargetFactory(
            target_roots={"default": "in-memory://"},
            target_class_by_prefix={"in-memory://": InMemoryFileSystemTarget},
        ),
    ):
        assert not other_task.complete()
        other_task.run()
        print(other_task.output().load())

    print(InMemoryFileSystemTarget.path_to_bytes)
