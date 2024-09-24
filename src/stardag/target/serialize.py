import abc
import pickle
import typing

from pydantic import PydanticSchemaGenerationError, TypeAdapter

from stardag.target._base import (
    FileSystemTarget,
    FileSystemTargetHandle,
    LoadableSaveableFileSystemTarget,
    LoadedT,
    OpenMode,
    ReadableFileSystemTargetHandle,
    WritableFileSystemTargetHandle,
)
from stardag.utils.resource_provider import resource_provider

try:
    from pandas import DataFrame as DataFrame  # type: ignore
    from pandas import read_csv as pd_read_csv  # type: ignore
except ImportError:

    class DataFrame: ...

    def pd_read_csv(*args, **kwargs): ...


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


class PlainTextSerializer(Serializer[str]):
    @classmethod
    def type_checked_init(cls, annotation: typing.Type[str]) -> typing.Self:
        if strip_annotation(annotation) != str:
            raise ValueError(f"{annotation} must be str.")
        return cls()

    def dump(
        self,
        obj: str,
        target: FileSystemTarget,
    ) -> None:
        with target.open("w") as handle:
            handle.write(obj)

    def load(self, target: FileSystemTarget) -> str:
        with target.open("r") as handle:
            return handle.read()

    def get_default_extension(self) -> str:
        return "txt"

    def __eq__(self, value: object) -> bool:
        return type(self) == type(value)


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

    def get_default_extension(self) -> str:
        return "json"

    def __eq__(self, value: object) -> bool:
        return (
            type(self) == type(value)
            and isinstance(value, JSONSerializer)
            and self.type_adapter.core_schema == value.type_adapter.core_schema
        )


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

    def get_default_extension(self) -> str:
        return "pkl"

    def __eq__(self, value: object) -> bool:
        return type(self) == type(value)


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

    def get_default_extension(self) -> str:
        return "csv"

    def __eq__(self, value: object) -> bool:
        return type(self) == type(value)


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

    def get_default_extension(self) -> str | None:
        return getattr(self.class_, "default_serialized_extension", None)

    def __eq__(self, value: object) -> bool:
        return (
            type(self) == type(value)
            and isinstance(value, SelfSerializer)
            and self.class_ == value.class_
        )


def strip_annotation(annotation: typing.Type[LoadedT]) -> typing.Type[LoadedT]:
    # TODO complete?
    origin = typing.get_origin(annotation)
    if origin is None:
        return annotation

    if origin == typing.Annotated:
        return typing.get_args(annotation)[0]

    return annotation


class SerializerFactoryProtocol(typing.Protocol):
    @abc.abstractmethod
    def __call__(self, annotation: typing.Type[LoadedT]) -> Serializer[LoadedT]: ...


def get_explicitly_annotated_serializer(
    annotation: typing.Type[LoadedT],
) -> Serializer[LoadedT]:
    origin = typing.get_origin(annotation)
    if origin == typing.Annotated:
        args = typing.get_args(annotation)
        for arg in args[1:]:  # NOTE important to skip the first arg
            if isinstance(arg, Serializer):
                return arg

    raise ValueError(f"No explicit serializer found for {annotation}")


_DEFAULT_SERIALIZER_CANDIDATES: tuple[SerializerFactoryProtocol] = (
    get_explicitly_annotated_serializer,
    SelfSerializer.type_checked_init,  # type: ignore
    # specific type serializers
    PandasDataFrameCSVSerializer.type_checked_init,
    PlainTextSerializer.type_checked_init,
    # generic serializers
    JSONSerializer.type_checked_init,
    # fallback
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
