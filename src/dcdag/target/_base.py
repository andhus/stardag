import typing
from pathlib import Path
from types import TracebackType


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


StreamT = typing.TypeVar("StreamT", bound=typing.Union[str, bytes])
StreamT_co = typing.TypeVar(
    "StreamT_co", bound=typing.Union[str, bytes], covariant=True
)
StreamT_contra = typing.TypeVar(
    "StreamT_contra", bound=typing.Union[str, bytes], contravariant=True
)

OpenMode = typing.Literal["r", "w", "rb", "wb"]


@typing.runtime_checkable
class FileSystemTargetHandle(typing.Protocol):
    def close(self) -> None: ...
    def __enter__(self) -> typing.Self: ...
    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...


@typing.runtime_checkable
class ReadableFileSystemTargetHandle(
    FileSystemTargetHandle,
    typing.Generic[StreamT_co],
    typing.Protocol,
):
    def read(self, size: int = -1) -> StreamT_co: ...


@typing.runtime_checkable
class WritableFileSystemTargetHandle(
    FileSystemTargetHandle,
    typing.Generic[StreamT_contra],
    typing.Protocol,
):
    def write(self, data: StreamT_contra) -> None: ...


BytesT = typing.TypeVar("BytesT", bound=bytes)


class _FileSystemTargetGeneric(
    Target,
    typing.Generic[BytesT],
    typing.Protocol,
):
    path: str

    def __init__(self, path: str) -> None:
        self.path = path

    def exists(self) -> bool: ...

    @typing.overload
    def open(
        self, mode: typing.Literal["r"]
    ) -> ReadableFileSystemTargetHandle[str]: ...

    @typing.overload
    def open(
        self, mode: typing.Literal["rb"]
    ) -> ReadableFileSystemTargetHandle[BytesT]: ...

    @typing.overload
    def open(
        self, mode: typing.Literal["w"]
    ) -> WritableFileSystemTargetHandle[str]: ...

    @typing.overload
    def open(
        self, mode: typing.Literal["wb"]
    ) -> WritableFileSystemTargetHandle[BytesT]: ...

    def open(self, mode: OpenMode) -> FileSystemTargetHandle:
        """For convenience, subclasses of FileSystemTarget can implement the private
        method _open without type hints to not having to repeat the overload:s."""
        return self._open(mode=mode)  # type: ignore


class FileSystemTarget(_FileSystemTargetGeneric[bytes], typing.Protocol):
    pass


class LoadableSaveableFileSystemTarget(
    LoadableSaveableTarget[LoadedT],
    _FileSystemTargetGeneric[bytes],
    typing.Generic[LoadedT],
    typing.Protocol,
): ...


LSFST = LoadableSaveableFileSystemTarget


class LocalTarget(FileSystemTarget):
    """TODO use luigi-style atomic writes."""

    def __init__(self, path: str) -> None:
        self.path = path

    @property
    def _path(self) -> Path:
        return Path(self.path)

    def exists(self) -> bool:
        return self._path.exists()

    def _open(self, mode: OpenMode) -> FileSystemTargetHandle:  # type: ignore
        if mode in ["r", "rb"]:
            return self._path.open(mode)
        if mode in ["w", "wb"]:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            return self._path.open(mode)  # type: ignore

        raise ValueError(f"Invalid mode {mode}")
