import abc
import pickle
import typing
import warnings

from pydantic import PydanticSchemaGenerationError, TypeAdapter

from dcdag.resources.resource_provider import resource_provider
from dcdag.target._base import (
    FileSystemTarget,
    FileSystemTargetHandle,
    LoadableSaveableFileSystemTarget,
    LoadedT,
    OpenMode,
    ReadableFileSystemTargetHandle,
    WritableFileSystemTargetHandle,
)

if typing.TYPE_CHECKING:
    DataFrame = typing.Annotated[typing.Any, "pandas.DataFrame placeholder"]

    def pd_read_csv(*args, **kwargs): ...

else:
    try:
        from pandas import DataFrame as DataFrame
        from pandas import read_csv as pd_read_csv
    except ImportError:
        pass


@typing.runtime_checkable
class Serializer(typing.Generic[LoadedT], typing.Protocol):
    def dump(
        self,
        obj: LoadedT,
        target: FileSystemTarget,
    ) -> None: ...
    def load(
        self,
        target: FileSystemTarget,
    ) -> LoadedT: ...


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
        return self.serializer.load(self.wrapped)

    def save(self, obj: LoadedT) -> None:
        self.serializer.dump(obj, self.wrapped)

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
    @classmethod
    def type_checked_init(cls, annotation: typing.Type[LoadedT]) -> typing.Self:
        return cls(annotation)

    def __init__(self, annotation: typing.Type[LoadedT]) -> None:
        try:
            self.type_adapter = TypeAdapter(annotation)
        except PydanticSchemaGenerationError as e:
            raise ValueError(f"Failed to generate schema for {annotation}") from e

    def dump(
        self,
        obj: LoadedT,
        target: FileSystemTarget,
    ) -> None:
        with target.open("wb") as handle:
            handle.write(self.type_adapter.dump_json(obj))

    def load(self, target: FileSystemTarget) -> LoadedT:
        with target.open("rb") as handle:
            return self.type_adapter.validate_json(handle.read())


class PickleSerializer(Serializer[LoadedT]):
    @classmethod
    def type_checked_init(cls, annotation: typing.Type[LoadedT]) -> typing.Self:
        # always ok
        return cls()

    def dump(
        self,
        obj: LoadedT,
        target: FileSystemTarget,
    ) -> None:
        with target.open("wb") as handle:
            pickle.dump(obj, handle)

    def load(self, target: FileSystemTarget) -> LoadedT:
        with target.open("rb") as handle:
            return pickle.loads(handle.read())


class PandasDataFrameCSVSerializer(Serializer[DataFrame]):
    """Serializer for pandas.DataFrame to CSV.

    NOTE this is mainly a proof of concept. Other formats are recommended for large
    data frames. See e.g.
        https://matthewrocklin.com/blog/work/2015/03/16/Fast-Serialization
    """

    @classmethod
    def type_checked_init(cls, annotation: typing.Type[DataFrame]) -> typing.Self:
        if strip_annotation(annotation) != DataFrame:
            raise ValueError(f"{annotation} must be DataFrame.")
        return cls()

    def dump(
        self,
        obj: DataFrame,
        target: FileSystemTarget,
    ) -> None:
        with target.open("w") as handle:
            obj.to_csv(handle, index=True)  # type: ignore

    def load(self, target: FileSystemTarget) -> DataFrame:
        with target.open("r") as handle:
            return pd_read_csv(handle, index_col=0)  # type: ignore


@typing.runtime_checkable
class SelfSerializing(typing.Protocol):
    def dump(self, target: FileSystemTarget) -> None: ...
    @classmethod
    def load(cls, target: FileSystemTarget) -> typing.Self: ...


class SelfSerializer(Serializer[SelfSerializing]):
    """Serializer for objects that themselves implements the `Serializer` protocol."""

    @classmethod
    def type_checked_init(cls, annotation: typing.Type[SelfSerializing]) -> typing.Self:
        return cls(strip_annotation(annotation))

    def __init__(self, class_) -> None:
        try:
            is_subclass_ = issubclass(class_, SelfSerializing)
        except TypeError:
            warnings.warn(f"{class_} must be a class.")
            raise ValueError(f"{class_} must be a class.")

        if not is_subclass_:
            raise ValueError(f"{class_} must comply with the SelfSerializing protocol.")
        self.class_ = class_

    def dump(
        self,
        obj: SelfSerializing,
        target: FileSystemTarget,
    ) -> None:
        obj.dump(target)

    def load(self, target: FileSystemTarget) -> SelfSerializing:
        return self.class_.load(target)


def strip_annotation(annotation: typing.Type[LoadedT]) -> typing.Type[LoadedT]:
    # TODO complete?
    origin = typing.get_origin(annotation)
    if origin is None:
        return annotation

    if origin != typing.Annotated:
        return typing.get_args(annotation)[0]

    return annotation


class SerializerFactoryProtocol(typing.Protocol):
    @abc.abstractmethod
    def __call__(self, annotation: typing.Type[LoadedT]) -> Serializer[LoadedT]: ...


_DEFAULT_SERIALIZER_CANDIDATES: tuple[SerializerFactoryProtocol] = (
    SelfSerializer.type_checked_init,  # type: ignore
    PandasDataFrameCSVSerializer.type_checked_init,
    JSONSerializer.type_checked_init,
    PickleSerializer.type_checked_init,
)


class SerializerFactory(SerializerFactoryProtocol):
    def __init__(
        self,
        candidates: typing.Iterable[
            SerializerFactoryProtocol
        ] = _DEFAULT_SERIALIZER_CANDIDATES,
    ) -> None:
        self.candidates = candidates

    def __call__(self, annotation: typing.Type[LoadedT]) -> Serializer[LoadedT]:
        for candidate in self.candidates:
            try:
                return candidate(annotation)
            except ValueError:
                pass
        raise ValueError(f"No serializer found for {annotation}")


serializer_factory_provider = resource_provider(
    SerializerFactoryProtocol,
    default_factory=SerializerFactory,
    doc_str="Provides a factory for serializers based on type annotations.",
)


def get_serializer(annotation: typing.Type[LoadedT]) -> Serializer[LoadedT]:
    return serializer_factory_provider.get()(annotation)