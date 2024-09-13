from dcdag.core.functional import task


@task
def add(a: int, b: int) -> int:
    return a + b


def test_basic():
    add_task = add(a=1, b=add(a=2, b=3))
    add_task.b.run()  # type: ignore
    add_task.run()
    assert add_task.b.result() == 5  # type: ignore
    assert add_task.result() == 6
