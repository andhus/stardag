import pytest

from stardag.decorator import task
from stardag.task import Task, TaskStruct
from stardag.utils.generic import flatten_task_struct


@task
def mock(key: str) -> str:
    return key


@pytest.mark.parametrize(
    "task_struct, expected",
    [
        (
            mock(key="a"),
            [mock(key="a")],
        ),
        (
            [mock(key="a"), mock(key="b")],
            [mock(key="a"), mock(key="b")],
        ),
        (
            {"a": mock(key="a"), "b": mock(key="b")},
            [mock(key="a"), mock(key="b")],
        ),
        (
            {"a": mock(key="a"), "b": [mock(key="b"), mock(key="c")]},
            [mock(key="a"), mock(key="b"), mock(key="c")],
        ),
        (
            [mock(key="a"), {"b": mock(key="b"), "c": mock(key="c")}],
            [mock(key="a"), mock(key="b"), mock(key="c")],
        ),
    ],
)
def test_flatten_task_struct(task_struct: TaskStruct, expected: list[Task]):
    assert flatten_task_struct(task_struct) == expected
