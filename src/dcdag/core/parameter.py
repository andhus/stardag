from __future__ import annotations

from abc import abstractmethod
import datetime
import inspect
import json
import typing
from enum import Enum
from numbers import Number
from types import NoneType, UnionType
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Literal,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Self,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

from pydantic import BaseModel, Field, TypeAdapter, ConfigDict
from pydantic.config import JsonDict, JsonValue
from pydantic_core import PydanticUndefined, PydanticUndefinedType


ParameterT = TypeVar("ParameterT")


class IDHasherABC(Generic[ParameterT]):

    def init(self, annotation: type[Any] | None) -> Self:
        return self

    @abstractmethod
    def __call__(self, value: ParameterT) -> JsonValue: ...


class IDHasher(IDHasherABC[ParameterT]):

    def __init__(self) -> NoneType:
        self._type_adapter: TypeAdapter = PydanticUndefined

    def init(self, annotation: Type[Any] | NoneType) -> Self:
        self._type_adapter = TypeAdapter(annotation)
        return self

    def __call__(self, value: ParameterT) -> JsonValue:
        # TODO except for Assets! use id_hash...
        return self.type_adapter.dump_python(value, mode="json")

    @property
    def type_adapter(self) -> TypeAdapter:
        if self._type_adapter is PydanticUndefined:
            raise ValueError("The init method has not been called.")

        return self._type_adapter


def _always_include(value: Any) -> bool:
    return True


def _always_exclude(value: Any) -> bool:
    return False


class _ParameterConfig(BaseModel, Generic[ParameterT]):
    id_hash_include: Callable[[ParameterT], bool] = _always_include
    id_hasher: Callable[[ParameterT], JsonValue] | IDHasher[ParameterT] = Field(
        default_factory=IDHasher
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __call__(self, schema: JsonDict) -> None:
        """Just a placeholder for a callable json_schema_extra.

        NOTE if we wanted to support json_schema_extra *as well*, we could
        forward it to `schema` here.
        """
        pass

    def init(self, annotation: type[Any] | None) -> "_ParameterConfig":
        update = {}
        if isinstance(self.id_hasher, IDHasherABC):
            update["id_hasher"] = self.id_hasher.init(annotation)

        return self.model_copy(update=update)


def ParamField(  # noqa: C901
    default: ParameterT = PydanticUndefined,
    *,
    default_factory: typing.Callable[[], ParameterT] | None = None,
    significant: bool = True,
    id_hash_include: Callable[[ParameterT], bool] = _always_include,
    id_hasher: Callable[[ParameterT], JsonValue] = PydanticUndefined,
    **kwargs: Any,  # TODO typing?
) -> Any:
    """TODO: docstring

    Usage:
    ```python
    class MyTask(Task):
        param: str = ParamField("my_default", significant=False)
    ```
    """

    return Field(
        default=default,
        default_factory=default_factory,
        json_schema_extra=_ParameterConfig(
            id_hash_include=id_hash_include if significant else _always_exclude,
            id_hasher=IDHasher() if id_hasher is PydanticUndefined else id_hasher,
        ),
        **kwargs,
    )
