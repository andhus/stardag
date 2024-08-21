import typing
from contextlib import contextmanager
from pathlib import PosixPath


@typing.runtime_checkable
class Target(typing.Protocol):
    def exists(self) -> bool: ...


LoadedT = typing.TypeVar("LoadedT")
LoadedT_co = typing.TypeVar("LoadedT_co", covariant=True)
LoadedT_contra = typing.TypeVar("LoadedT_contra", contravariant=True)


@typing.runtime_checkable
class LoadableTarget(
    Target,
    typing.Generic[LoadedT_co],
    typing.Protocol,
):
    def load(self) -> LoadedT_co: ...


@typing.runtime_checkable
class SaveableTarget(
    Target,
    typing.Generic[LoadedT_contra],
    typing.Protocol,
):
    def save(self, obj: LoadedT_contra) -> None: ...


@typing.runtime_checkable
class LoadableSaveableTarget(
    LoadableTarget[LoadedT],
    SaveableTarget[LoadedT],
    typing.Generic[LoadedT],
    typing.Protocol,
): ...


class InMemoryTarget(LoadableSaveableTarget[LoadedT]):
    """Useful in testing :)"""

    key_to_target = {}  # Note class variable!

    @classmethod
    def clear_targets(cls):
        cls.key_to_target = {}

    @classmethod
    @contextmanager
    def cleared(cls):
        cls.clear_targets()
        try:
            yield cls.key_to_target
        finally:
            cls.clear_targets()

    def __init__(self, key):
        self.key = key

    def exists(self):  # type: ignore
        return self.key in self.key_to_target

    def save(self, obj: LoadedT) -> None:
        self.key_to_target[self.key] = obj

    def load(self) -> LoadedT:
        return self.key_to_target[self.key]


StreamT = typing.TypeVar("StreamT", bound=typing.Union[str, bytes])

OpenMode = typing.Literal["r", "w"]
# TODO consider adding ["rb", "wb"] to OpenMode instead of upfront?


@typing.runtime_checkable
class FileSystemTargetHandle(
    typing.Generic[StreamT],
    typing.Protocol,
):
    # TODO split up into readable and writable
    def read(self, size: int) -> StreamT: ...
    def write(self, data: StreamT) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> "FileSystemTargetHandle[StreamT]": ...
    def __exit__(self, *args) -> None: ...


class FileSystemTarget(
    Target,
    typing.Generic[StreamT],
    typing.Protocol,
):
    path: PosixPath

    def __init__(self, path: str | PosixPath) -> None:
        self.path = PosixPath(path)

    def exists(self) -> bool: ...

    def open(self, mode: OpenMode) -> FileSystemTargetHandle[StreamT]: ...
