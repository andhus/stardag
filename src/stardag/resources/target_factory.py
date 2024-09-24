import typing
from pathlib import Path

from stardag.target import FileSystemTarget, LocalTarget
from stardag.task import Task


@typing.runtime_checkable
class TargetClassFromURIProtocol(typing.Protocol):
    def __call__(self, uri: str) -> typing.Type[FileSystemTarget]: ...


PrefixToTargetClass = typing.Mapping[str, typing.Type[FileSystemTarget]]

_DEFAULT_PREFIX_TO_TARGET_CLASS = {
    "/": LocalTarget,
    # "s3://": S3Target
    # "gs://": GSTarget,
}


class TargetClassByPrefix(TargetClassFromURIProtocol):
    def __init__(
        self,
        prefix_to_target_class: PrefixToTargetClass = _DEFAULT_PREFIX_TO_TARGET_CLASS,
    ) -> None:
        self.prefix_to_target_class = prefix_to_target_class

    def __call__(self, uri: str) -> typing.Type[FileSystemTarget]:
        for prefix, target_class in self.prefix_to_target_class.items():
            if uri.startswith(prefix):
                return target_class
        raise ValueError(f"URI {uri} does not match any prefixes.")


_DEFAULT_TARGET_ROOT_KEY = "default"
_DEFAULT_TARGET_ROOTS = {
    _DEFAULT_TARGET_ROOT_KEY: str(
        Path("~/.stardag/target-roots/default").expanduser().absolute()
    ),
}


class TargetFactory:
    def __init__(
        self,
        target_roots: dict[str, str] = _DEFAULT_TARGET_ROOTS,
        target_class_by_prefix: (
            PrefixToTargetClass | TargetClassFromURIProtocol
        ) = TargetClassByPrefix(),
    ) -> None:
        self.target_roots = {
            key: value.removesuffix("/") + "/" for key, value in target_roots.items()
        }
        self.target_class_by_prefix = (
            target_class_by_prefix
            if isinstance(target_class_by_prefix, TargetClassFromURIProtocol)
            else TargetClassByPrefix(target_class_by_prefix)
        )

    def get_target(
        self,
        relpath: str,
        task: Task | None,  # noqa
        target_root_key: str = _DEFAULT_TARGET_ROOT_KEY,
    ) -> FileSystemTarget:
        """Get a file system target.

        Args:
            relpath: The path to the target, relative to the configured root path for
              `target_root_key`.
            task: The task that will use the target. NOTE: this can be used to for
              advanced configuration of targets, such as in-memory/local disk caching
              etc.
            target_root: The key to the target root to use.
        """
        path = self.get_path(relpath, target_root_key)
        target_class = self.target_class_by_prefix(path)
        return target_class(path=path)

    def get_path(
        self, relpath: str, target_root_key: str = _DEFAULT_TARGET_ROOT_KEY
    ) -> str:
        return f"{self.target_roots[target_root_key]}{relpath}"
