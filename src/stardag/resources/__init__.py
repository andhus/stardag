from stardag.resources.target_factory import _DEFAULT_TARGET_ROOT_KEY, TargetFactory
from stardag.target import FileSystemTarget
from stardag.task import Task
from stardag.utils.resource_provider import resource_provider

__all__ = ["get_target", "target_factory_provider"]

target_factory_provider = resource_provider(
    type_=TargetFactory, default_factory=TargetFactory
)


def get_target(
    relpath: str,
    task: Task | None,
    target_root_key: str = _DEFAULT_TARGET_ROOT_KEY,
) -> FileSystemTarget:
    return target_factory_provider.get().get_target(
        relpath=relpath,
        task=task,
        target_root_key=target_root_key,
    )
