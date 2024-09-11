import typing

from dcdag.core.fsttask import AutoFSTTask
from dcdag.core.parameter import (
    IDHasher,
    IDHashExclude,
    IDHashInclude,
    _ParameterConfig,
    always_include,
)
from dcdag.core.task import TaskParam


class MockTask(AutoFSTTask[str]):
    a: int
    b: IDHashExclude[str]


def test_parameter():
    assert MockTask._param_configs["a"] == _ParameterConfig(
        id_hash_include=always_include, id_hasher=IDHasher().init(int)
    )
    assert MockTask._param_configs["b"] == _ParameterConfig(
        id_hash_include=IDHashInclude(False),
        id_hasher=IDHasher().init(
            typing.Annotated[str, IDHashInclude(False)],  # type: ignore
        ),
    )


def test_task_param():
    class ChildTask(AutoFSTTask[str]):
        a: str

        def run(self) -> None:
            return None

    class ParentTask(AutoFSTTask[str]):
        child: TaskParam[ChildTask]

        def run(self) -> None:
            return None

    parent = ParentTask(child=ChildTask(a="A"))
    assert parent.model_dump() == {
        "version": None,
        "child": {
            "__task_family": "ChildTask",
            "version": None,
            "a": "A",
        },
    }
