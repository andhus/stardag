import pytest

from dcdag.core.functional import task


def test_basic(default_in_memory_fs_target):
    @task
    def add(a: int, b: int) -> int:
        return a + b

    assert add.__version__ == "0"
    assert add.model_fields["version"].default == "0"

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


def test_with_params(default_in_memory_fs_target):
    @task(version="1", relpath_base="add_task")
    def add2(a: int, b: int) -> int:
        return a + b

    add_b_task = add2(a=2, b=3)
    add_task = add2(a=1, b=add_b_task)
    assert add_task.requires() == {"b": add_b_task}
    assert not add_b_task.complete()
    assert not add_task.complete()

    with pytest.raises(FileNotFoundError):
        add_task.run()

    add_task.b.run()  # type: ignore
    assert add_task.b.result() == 5  # type: ignore

    add_task.run()
    assert add_task.result() == 6

    assert add_task._relpath.startswith("add_task/")
    assert add_task.b._relpath.startswith("add_task/")  # type: ignore
    assert add_task.output().path.startswith("in-memory://add_task/add2/v1/")
