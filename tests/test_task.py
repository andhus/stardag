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


class ChildTask(AutoFSTTask[str]):
    a: str

    def run(self) -> None:
        return None


class ParentTask(AutoFSTTask[str]):
    child: TaskParam[ChildTask]

    def run(self) -> None:
        return None


def test_task_param():
    parent = ParentTask(child=ChildTask(a="A"))
    assert parent.model_dump() == {
        "version": None,
        "child": {
            "__task_family": "ChildTask",
            "version": None,
            "a": "A",
        },
    }
    assert parent._id_hash_jsonable() == {
        "task_family": "ParentTask",
        "parameters": {
            "version": None,
            "child": parent.child.task_id,
        },
    }


class ParentTask2(AutoFSTTask[str]):
    children: frozenset[TaskParam[ChildTask]]

    def run(self) -> None:
        return None


def test_set_of_task_params():
    parent = ParentTask2(children=frozenset([ChildTask(a="A"), ChildTask(a="B")]))
    assert parent.model_dump(mode="json") == {
        "version": None,
        "children": [
            {
                "__task_family": "ChildTask",
                "version": None,
                "a": "A",
            },
            {
                "__task_family": "ChildTask",
                "version": None,
                "a": "B",
            },
        ],
    }
    assert ParentTask2.model_validate_json(parent.model_dump_json()) == parent
    # assert parent._id_hash_jsonable() == {
    #     "task_family": "ParentTask2",
    #     "parameters": {
    #         "version": None,
    #         "children": sorted([child.task_id for child in parent.children]),
    #     },
    # }
