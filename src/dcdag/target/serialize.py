import pickle
import typing

from pydantic import TypeAdapter

from dcdag.target._base import (
    FileSystemTarget,
    FileSystemTargetHandle,
    LoadableSaveableFileSystemTarget,
    LoadedT,
    OpenMode,
    ReadableFileSystemTargetHandle,
    WritableFileSystemTargetHandle,
)


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
