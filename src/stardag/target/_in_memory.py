from contextlib import contextmanager
from io import BytesIO, StringIO

from stardag.target._base import (
    FileSystemTarget,
    FileSystemTargetHandle,
    LoadableSaveableTarget,
    LoadedT,
    OpenMode,
    ReadableFileSystemTargetHandle,
    WritableFileSystemTargetHandle,
)


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
