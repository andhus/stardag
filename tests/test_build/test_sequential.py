import json
import typing

from stardag.build import sequential
from stardag.target import InMemoryFileSystemTarget
from stardag.utils.testing.simple_dag import RootTask, RootTaskLoadedT


def test_build_simple_dag(
    default_in_memory_fs_target: typing.Type[InMemoryFileSystemTarget],
    simple_dag: RootTask,
    simple_dag_expected_root_output: RootTaskLoadedT,
):
    sequential.build(simple_dag)
    assert simple_dag.output().load() == simple_dag_expected_root_output
    expected_root_path = f"in-memory://{simple_dag._relpath}"
    assert (
        InMemoryFileSystemTarget.path_to_bytes[expected_root_path]
        == json.dumps(simple_dag_expected_root_output, separators=(",", ":")).encode()
    )
