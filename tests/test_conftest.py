from pathlib import Path

from dcdag.core.resources import get_target, target_factory_provider
from dcdag.core.target import InMemoryFileSystemTarget, LocalTarget


def test_default_local_target_tmp_path(default_local_target_tmp_path: Path):
    tmp_path = default_local_target_tmp_path
    assert isinstance(tmp_path, Path)
    target_factory = target_factory_provider.get()
    assert target_factory.target_class_by_prefix("/") == LocalTarget
    assert target_factory.target_roots["default"] == str(tmp_path) + "/"
    key = "mock/target.txt"
    target = get_target(key, task=None)
    assert isinstance(target, LocalTarget)
    assert target.path == str(tmp_path / key)


def test_test_default_local_target_tmp_path_matches_tmp_path(
    default_local_target_tmp_path,
    tmp_path,
):
    assert tmp_path / "default-root" == default_local_target_tmp_path


def test_default_in_memory_target(
    default_in_memory_fs_target,
    default_in_memory_fs_target_prefix,
):
    assert default_in_memory_fs_target == InMemoryFileSystemTarget
    target_factory = target_factory_provider.get()
    assert (
        target_factory.target_class_by_prefix(default_in_memory_fs_target_prefix)
        == InMemoryFileSystemTarget
    )
    assert target_factory.target_roots["default"] == default_in_memory_fs_target_prefix
    key = "mock/target.txt"
    target = get_target(key, task=None)
    assert isinstance(target, InMemoryFileSystemTarget)
    assert target.path == default_in_memory_fs_target_prefix + key

    assert InMemoryFileSystemTarget.path_to_bytes == {}
    test_data = b"test-test"
    with target.open("wb") as handle:
        handle.write(test_data)  # type: ignore  # TODO check overloads!

    assert InMemoryFileSystemTarget.path_to_bytes == {target.path: test_data}
