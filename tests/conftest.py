import typing
from pathlib import Path

import pytest

from dcdag.resources import target_factory_provider
from dcdag.resources.target_factory import TargetFactory
from dcdag.target import InMemoryFileSystemTarget, LocalTarget
from dcdag.utils.testing import get_simple_dag, get_simple_dag_expected_root_output


@pytest.fixture(scope="session")
def simple_dag():
    return get_simple_dag()


@pytest.fixture(scope="session")
def simple_dag_expected_root_output():
    return get_simple_dag_expected_root_output()


@pytest.fixture(scope="function")
def default_local_target_tmp_path(
    tmp_path: Path,
) -> typing.Generator[Path, None, None]:
    default_root = tmp_path.absolute() / "default-root"
    default_root.mkdir(parents=True, exist_ok=False)
    with target_factory_provider.override(
        TargetFactory(
            target_roots={"default": str(default_root)},
            target_class_by_prefix={"/": LocalTarget},
        )
    ):
        yield default_root


@pytest.fixture(scope="session")
def default_in_memory_fs_target_prefix():
    return "in-memory://"


@pytest.fixture(scope="function")
def _default_in_memory_fs_target_factory(
    default_in_memory_fs_target_prefix,
) -> typing.Generator[TargetFactory, None, None]:
    with target_factory_provider.override(
        TargetFactory(
            target_roots={"default": default_in_memory_fs_target_prefix},
            target_class_by_prefix={
                default_in_memory_fs_target_prefix: InMemoryFileSystemTarget
            },
        )
    ) as target_factory:
        with InMemoryFileSystemTarget.cleared():
            yield target_factory


@pytest.fixture(scope="function")
def default_in_memory_fs_target(
    _default_in_memory_fs_target_factory,
) -> typing.Type[InMemoryFileSystemTarget]:
    return InMemoryFileSystemTarget
