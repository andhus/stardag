import typing

import pytest

from stardag.auto_task import AutoFSTTask
from stardag.decorator import task as task_decorator
from stardag.parameter import (
    IDHasher,
    IDHashExclude,
    IDHashInclude,
    _ParameterConfig,
    always_include,
)
from stardag.task import (
    _REGISTER,
    Task,
    TaskStruct,
    flatten_task_struct,
    get_namespace_family,
)
from stardag.utils.testing.namepace import (
    ClearNamespaceByArg,
    ClearNamespaceByDunder,
    CustomFamilyByArgFromIntermediate,
    CustomFamilyByArgFromIntermediateChild,
    CustomFamilyByArgFromTask,
    CustomFamilyByArgFromTaskChild,
    CustomFamilyByDUnder,
    CustomFamilyByDUnderChild,
    OverrideNamespaceByArg,
    OverrideNamespaceByArgChild,
    OverrideNamespaceByDUnder,
    OverrideNamespaceByDUnderChild,
    UnspecifiedNamespace,
)
from stardag.utils.testing.simple_dag import LeafTask


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


_testing_module = "stardag.utils.testing"


@pytest.mark.parametrize(
    "task_class,expected_namespace_family",
    [
        # namespace
        (MockTask, "MockTask"),
        (LeafTask, f"{_testing_module}.simple_dag.LeafTask"),
        (UnspecifiedNamespace, f"{_testing_module}.UnspecifiedNamespace"),
        # namespace override by dunder
        (OverrideNamespaceByDUnder, "override_namespace.OverrideNamespaceByDUnder"),
        (ClearNamespaceByDunder, "ClearNamespaceByDunder"),
        (
            OverrideNamespaceByDUnderChild,
            "override_namespace.OverrideNamespaceByDUnderChild",
        ),
        # namespace override by arg
        (OverrideNamespaceByArg, "override_namespace.OverrideNamespaceByArg"),
        (ClearNamespaceByArg, "ClearNamespaceByArg"),
        (
            OverrideNamespaceByArgChild,
            f"{_testing_module}.OverrideNamespaceByArgChild",
        ),
        # family override
        (CustomFamilyByArgFromIntermediate, f"{_testing_module}.custom_family"),
        (
            CustomFamilyByArgFromIntermediateChild,
            f"{_testing_module}.CustomFamilyByArgFromIntermediateChild",
        ),
        (CustomFamilyByArgFromTask, f"{_testing_module}.custom_family_2"),
        (
            CustomFamilyByArgFromTaskChild,
            f"{_testing_module}.CustomFamilyByArgFromTaskChild",
        ),
        (CustomFamilyByDUnder, f"{_testing_module}.custom_family_3"),
        (CustomFamilyByDUnderChild, f"{_testing_module}.custom_family_3_child"),
    ],
)
def test_auto_namespace(task_class: typing.Type[Task], expected_namespace_family):
    assert task_class.get_namespace_family() == expected_namespace_family
    namespace = task_class.get_namespace()
    family = task_class.get_family()
    assert get_namespace_family(namespace, family) == expected_namespace_family
    assert _REGISTER.get(namespace, family) == task_class


@task_decorator
def mock_task(key: str) -> str:
    return key


@pytest.mark.parametrize(
    "task_struct, expected",
    [
        (
            mock_task(key="a"),
            [mock_task(key="a")],
        ),
        (
            [mock_task(key="a"), mock_task(key="b")],
            [mock_task(key="a"), mock_task(key="b")],
        ),
        (
            {"a": mock_task(key="a"), "b": mock_task(key="b")},
            [mock_task(key="a"), mock_task(key="b")],
        ),
        (
            {"a": mock_task(key="a"), "b": [mock_task(key="b"), mock_task(key="c")]},
            [mock_task(key="a"), mock_task(key="b"), mock_task(key="c")],
        ),
        (
            [mock_task(key="a"), {"b": mock_task(key="b"), "c": mock_task(key="c")}],
            [mock_task(key="a"), mock_task(key="b"), mock_task(key="c")],
        ),
    ],
)
def test_flatten_task_struct(task_struct: TaskStruct, expected: list[Task]):
    assert flatten_task_struct(task_struct) == expected
