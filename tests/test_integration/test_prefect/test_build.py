from prefect import flow

from stardag.integration.prefect.build import build
from stardag.utils.testing.dynamic_deps_dag import (
    assert_dynamic_deps_task_complete_recursive,
    get_dynamic_deps_dag,
)


async def test_build_dag_dynamic_deps(default_in_memory_fs_target):
    dag = get_dynamic_deps_dag()
    assert_dynamic_deps_task_complete_recursive(dag, False)

    @flow
    async def dynamic_deps_dag():
        task_id_to_future = await build(dag)
        for future in task_id_to_future.values():
            future.wait()

    await dynamic_deps_dag()
    assert_dynamic_deps_task_complete_recursive(dag, True)
