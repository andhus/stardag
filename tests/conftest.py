import typing

import pytest

from dcdag.core.resources import target_factory_provider
from dcdag.core.resources.target_factory import TargetFactory
from dcdag.core.target import InMemoryFileSystemTarget
from dcdag.core.utils.testing import get_simple_dag, get_simple_dag_expected_root_output


@pytest.fixture(scope="session")
def simple_dag():
    return get_simple_dag()


@pytest.fixture(scope="session")
def simple_dag_expected_root_output():
    return get_simple_dag_expected_root_output()


@pytest.fixture(scope="function")
def in_memory_default_targets_target_factory() -> (
    typing.Generator[TargetFactory, None, None]
):
    with target_factory_provider.override(
        TargetFactory(
            target_roots={"default": "in-memory://"},
            target_class_by_prefix={"in-memory://": InMemoryFileSystemTarget},
        )
    ) as target_factory:
        with InMemoryFileSystemTarget.cleared():
            yield target_factory


@pytest.fixture(scope="function")
def in_memory_default_targets(
    in_memory_default_targets_target_factory,
) -> typing.Type[InMemoryFileSystemTarget]:
    return InMemoryFileSystemTarget
