from typing import ClassVar

from stardag.auto_task import AutoFSTTask
from stardag.task import auto_namespace
from stardag.task_parameter import TaskLoads

auto_namespace(__name__)


class DynamicDepsTask(AutoFSTTask[str]):
    value: str
    static_deps: tuple[TaskLoads[str], ...] = ()
    dynamic_deps: tuple[TaskLoads[str], ...] = ()

    has_dynamic_deps: ClassVar[bool] = True

    def requires(self):  # type: ignore
        return self.static_deps

    def run(self):  # type: ignore
        for dep in sorted(self.dynamic_deps):  # type: ignore
            yield dep
        self.output().save(self.value)


def get_dynamic_deps_dag():
    dyn_and_static = DynamicDepsTask(
        value="1",
        static_deps=(
            DynamicDepsTask(value="20"),
            DynamicDepsTask(value="21"),
        ),
        dynamic_deps=(
            DynamicDepsTask(value="30"),
            DynamicDepsTask(value="31"),
        ),
    )
    parent = DynamicDepsTask(
        value="0",
        static_deps=(
            dyn_and_static,
            DynamicDepsTask(value="31"),
        ),
    )

    return parent


def assert_dynamic_deps_task_complete_recursive(
    task: DynamicDepsTask,
    is_complete: bool,
):
    assert task.complete() == is_complete
    for dep in task.static_deps + task.dynamic_deps:
        assert_dynamic_deps_task_complete_recursive(dep, is_complete)
