import typing

import pytest

from dcdag.auto_task import AutoFSTTask
from dcdag.parameter import (
    IDHasher,
    IDHashExclude,
    IDHashInclude,
    _ParameterConfig,
    always_include,
)
from dcdag.task import _REGISTER, Task, get_namespace_family
from dcdag.utils.testing.namepace import (
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
from dcdag.utils.testing.simple_dag import LeafTask


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


@pytest.mark.parametrize(
    "task_class,expected_namespace_family",
    [
        # namespace
        (MockTask, "MockTask"),
        (LeafTask, "dcdag.utils.testing.simple_dag.LeafTask"),
        (UnspecifiedNamespace, "dcdag.utils.testing.UnspecifiedNamespace"),
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
            "dcdag.utils.testing.OverrideNamespaceByArgChild",
        ),
        # family override
        (CustomFamilyByArgFromIntermediate, "dcdag.utils.testing.custom_family"),
        (
            CustomFamilyByArgFromIntermediateChild,
            "dcdag.utils.testing.CustomFamilyByArgFromIntermediateChild",
        ),
        (CustomFamilyByArgFromTask, "dcdag.utils.testing.custom_family_2"),
        (
            CustomFamilyByArgFromTaskChild,
            "dcdag.utils.testing.CustomFamilyByArgFromTaskChild",
        ),
        (CustomFamilyByDUnder, "dcdag.utils.testing.custom_family_3"),
        (CustomFamilyByDUnderChild, "dcdag.utils.testing.custom_family_3_child"),
    ],
)
def test_auto_namespace(task_class: typing.Type[Task], expected_namespace_family):
    assert task_class.get_namespace_family() == expected_namespace_family
    namespace = task_class.get_namespace()
    family = task_class.get_family()
    assert get_namespace_family(namespace, family) == expected_namespace_family
    assert _REGISTER.get(namespace, family) == task_class
