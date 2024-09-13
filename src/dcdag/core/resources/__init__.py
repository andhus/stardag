from dcdag.core.resources.resource_provider import resource_provider
from dcdag.core.resources.target_factory import _DEFAULT_TARGET_ROOT_KEY, TargetFactory
from dcdag.core.target import FileSystemTarget
from dcdag.core.task import Task

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
