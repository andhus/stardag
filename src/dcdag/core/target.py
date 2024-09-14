import pickle
import typing
from contextlib import contextmanager
from io import BytesIO, StringIO
from pathlib import Path
from types import TracebackType

from pydantic import TypeAdapter


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

    key_to_object = {}  # Note class variable!

    @classmethod
    def clear_targets(cls):
        cls.key_to_object = {}

    @classmethod
    @contextmanager
    def cleared(cls):
        cls.clear_targets()
        try:
            yield cls.key_to_object
        finally:
            cls.clear_targets()

    def __init__(self, key):
        self.key = key

    def exists(self):  # type: ignore
        return self.key in self.key_to_object

    def save(self, obj: LoadedT) -> None:
        self.key_to_object[self.key] = obj

    def load(self) -> LoadedT:
        return self.key_to_object[self.key]


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
    def __enter__(self) -> "FileSystemTargetHandle": ...
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


class FileSystemTargetGeneric(
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

    def open(self, mode: OpenMode) -> FileSystemTargetHandle: ...


class FileSystemTarget(FileSystemTargetGeneric[bytes]):
    def open(self, mode):
        """For convenience, subclasses of FileSystemTarget can implement the private
        method _open without type hints to not having to repeat the overload:s."""
        return self._open(mode=mode)

    def _open(self, mode: OpenMode):
        raise NotImplementedError()


class LoadableSaveableFileSystemTarget(
    LoadableSaveableTarget[LoadedT],
    FileSystemTargetGeneric[bytes],
    typing.Generic[LoadedT],
    typing.Protocol,
): ...


LSFST = LoadableSaveableFileSystemTarget


class LocalTarget(FileSystemTarget):
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


class InMemoryFileSystemTarget(FileSystemTarget):
    """Useful in testing"""

    path_to_bytes: dict[str, bytes] = {}  # Note class variable!

    @classmethod
    def clear_targets(cls):
        cls.path_to_bytes = {}

    @classmethod
    @contextmanager
    def cleared(cls):
        cls.clear_targets()
        try:
            yield cls.path_to_bytes
        finally:
            cls.clear_targets()

    def __init__(self, path: str):
        self.path = path

    def exists(self):  # type: ignore
        return self.path in self.path_to_bytes

    def _open(self, mode: OpenMode) -> FileSystemTargetHandle:  # type: ignore
        try:
            if mode == "r":
                return _InMemoryStrReadableFileSystemTargetHandle(
                    self.path_to_bytes[self.path]
                )
            if mode == "rb":
                return _InMemoryBytesReadableFileSystemTargetHandle(
                    self.path_to_bytes[self.path]
                )
        except KeyError:
            raise FileNotFoundError(f"No such file: {self.path}")

        if mode == "w":
            return _InMemoryStrWritableFileSystemTargetHandle(self.path)
        if mode == "wb":
            return _InMemoryBytesWritableFileSystemTargetHandle(self.path)

        raise ValueError(f"Invalid mode {mode}")


class _InMemoryBytesWritableFileSystemTargetHandle(
    WritableFileSystemTargetHandle[bytes]
):
    def __init__(self, path: str) -> None:
        self.path = path

    def write(self, data: bytes) -> None:
        path_to_bytes = InMemoryFileSystemTarget.path_to_bytes
        path_to_bytes[self.path] = path_to_bytes.setdefault(self.path, b"") + data

    def close(self) -> None:
        pass

    def __enter__(self) -> "_InMemoryBytesWritableFileSystemTargetHandle":
        return self

    def __exit__(self, *args) -> None:
        pass


class _InMemoryStrWritableFileSystemTargetHandle(WritableFileSystemTargetHandle[str]):
    def __init__(self, path: str) -> None:
        self.path = path

    def write(self, data: str) -> None:
        path_to_bytes = InMemoryFileSystemTarget.path_to_bytes
        path_to_bytes[self.path] = (
            path_to_bytes.setdefault(self.path, b"") + data.encode()
        )

    def close(self) -> None:
        pass

    def __enter__(self) -> "_InMemoryStrWritableFileSystemTargetHandle":
        return self

    def __exit__(self, *args) -> None:
        pass


class _InMemoryBytesReadableFileSystemTargetHandle(
    ReadableFileSystemTargetHandle[bytes]
):
    def __init__(self, data: bytes) -> None:
        self.bytes_io = BytesIO(data)

    def read(self, size: int = -1) -> bytes:
        return self.bytes_io.read(size)

    def close(self) -> None:
        pass

    def __enter__(self) -> "_InMemoryBytesReadableFileSystemTargetHandle":
        return self

    def __exit__(self, *args) -> None:
        pass


class _InMemoryStrReadableFileSystemTargetHandle(ReadableFileSystemTargetHandle[str]):
    def __init__(self, data: bytes) -> None:
        self.string_io = StringIO(data.decode())

    def read(self, size: int = -1) -> str:
        return self.string_io.read(size)

    def close(self) -> None:
        pass

    def __enter__(self) -> "_InMemoryStrReadableFileSystemTargetHandle":
        return self

    def __exit__(self, *args) -> None:
        pass


class Serializer(typing.Generic[LoadedT], typing.Protocol):
    def dump(
        self, obj: LoadedT, handle: WritableFileSystemTargetHandle[bytes]
    ) -> None: ...
    def load(self, handle: ReadableFileSystemTargetHandle[bytes]) -> LoadedT: ...


class Serializable(
    LoadableSaveableFileSystemTarget[LoadedT],
    typing.Generic[LoadedT],
):
    def __init__(
        self,
        wrapped: FileSystemTarget,
        serializer: Serializer[LoadedT],
    ) -> None:
        self.serializer = serializer
        self.wrapped = wrapped

    @property
    def path(self) -> str:  # type: ignore
        return self.wrapped.path

    def load(self) -> LoadedT:
        with self.open("rb") as handle:
            return self.serializer.load(handle)  # type: ignore  # TODO?

    def save(self, obj: LoadedT) -> None:
        with self.open("wb") as handle:
            self.serializer.dump(obj, handle)  # type: ignore  # TODO?

    def exists(self) -> bool:
        return self.wrapped.exists()

    @typing.overload
    def open(
        self, mode: typing.Literal["r"]
    ) -> ReadableFileSystemTargetHandle[str]: ...

    @typing.overload
    def open(
        self, mode: typing.Literal["rb"]
    ) -> ReadableFileSystemTargetHandle[bytes]: ...

    @typing.overload
    def open(
        self, mode: typing.Literal["w"]
    ) -> WritableFileSystemTargetHandle[str]: ...

    @typing.overload
    def open(
        self, mode: typing.Literal["wb"]
    ) -> WritableFileSystemTargetHandle[bytes]: ...

    def open(self, mode: OpenMode) -> FileSystemTargetHandle:
        return self.wrapped.open(mode)


class JSONSerializer(Serializer[LoadedT]):
    def __init__(self, annotation: typing.Type[LoadedT]) -> None:
        self.type_adapter = TypeAdapter(annotation)

    def dump(self, obj: LoadedT, handle: WritableFileSystemTargetHandle[bytes]) -> None:
        handle.write(self.type_adapter.dump_json(obj))

    def load(self, handle: ReadableFileSystemTargetHandle[bytes]) -> LoadedT:
        return self.type_adapter.validate_json(handle.read())


class PickleSerializer(Serializer[LoadedT]):
    def dump(self, obj: LoadedT, handle: WritableFileSystemTargetHandle[bytes]) -> None:
        pickle.dump(obj, handle)

    def load(self, handle: ReadableFileSystemTargetHandle[bytes]) -> LoadedT:
        return pickle.loads(handle.read())
