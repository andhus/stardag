import json
import typing

from dcdag.core.build import sequential
from dcdag.core.target import InMemoryFileSystemTarget
from dcdag.core.utils.testing import TestTaskRoot, TestTaskRootLoadedT


def test_build_simple_dag(
    default_in_memory_fs_target: typing.Type[InMemoryFileSystemTarget],
    simple_dag: TestTaskRoot,
    simple_dag_expected_root_output: TestTaskRootLoadedT,
):
    sequential.build(simple_dag)
    assert simple_dag.output().load() == simple_dag_expected_root_output
    expected_root_path = f"in-memory://{simple_dag.relpath}"
    assert (
        InMemoryFileSystemTarget.path_to_bytes[expected_root_path]
        == json.dumps(simple_dag_expected_root_output, separators=(",", ":")).encode()
    )
