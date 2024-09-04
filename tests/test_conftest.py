from pathlib import Path

from dcdag.core.resources import get_target, target_factory_provider
from dcdag.core.target import LocalTarget


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
