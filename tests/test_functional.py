import pytest

from dcdag.core.functional import task


@task
def add(a: int, b: int) -> int:
    return a + b


def test_basic(default_in_memory_fs_target):
    add_b_task = add(a=2, b=3)
    add_task = add(a=1, b=add_b_task)
    assert add_task.requires() == {"b": add_b_task}
    assert not add_b_task.complete()
    assert not add_task.complete()

    with pytest.raises(FileNotFoundError):
        add_task.run()

    add_task.b.run()  # type: ignore
    assert add_task.b.result() == 5  # type: ignore

    add_task.run()
    assert add_task.result() == 6
