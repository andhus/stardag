from __future__ import annotations

import abc
from abc import abstractmethod
from types import NoneType
from typing import Annotated, Any, Callable, Generic, Self, Type, TypeVar

from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic.config import JsonDict, JsonValue

ParameterT = TypeVar("ParameterT")


class IDHasherABC(Generic[ParameterT]):
    def init(self, annotation: type[Any] | None) -> Self:
        return self

    @abstractmethod
    def __call__(self, value: ParameterT) -> JsonValue: ...


class IDHasher(IDHasherABC[ParameterT]):
    def __init__(self) -> NoneType:
        self._type_adapter: TypeAdapter | None = None
        self.annotation: Type[Any] | None = None

    def init(self, annotation: Type[Any] | NoneType) -> Self:
        self.annotation = annotation
        self._type_adapter = TypeAdapter(annotation)
        return self

    def __call__(self, value: ParameterT) -> JsonValue:
        from stardag.task import Task

        if isinstance(value, Task):
            return value.task_id

        return self.type_adapter.dump_python(value, mode="json")

    @property
    def type_adapter(self) -> TypeAdapter:
        if self._type_adapter is None:
            raise ValueError("The init method has not been called.")

        return self._type_adapter

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(self, IDHasher)
            and isinstance(other, IDHasher)
            and self.annotation == other.annotation
        )


class IDHashIncludeABC(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __call__(self, value: Any) -> bool: ...


class IDHashInclude(IDHashIncludeABC):
    def __init__(self, include: bool | Callable[[Any], bool] = True) -> NoneType:
        self._include = include

    def __call__(self, value: Any) -> bool:
        if callable(self._include):
            return self._include(value)
        return self._include

    def __eq__(self, other: object) -> bool:
        return isinstance(other, IDHashInclude) and self._include == other._include


always_include = IDHashInclude(True)
always_exclude = IDHashInclude(False)


IDHashExclude = Annotated[ParameterT, always_exclude]


class _ParameterConfig(BaseModel, Generic[ParameterT]):
    id_hash_include: Callable[[ParameterT], bool]
    id_hasher: Callable[[ParameterT], JsonValue] | IDHasher[ParameterT]

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


# def ParamField(  # noqa: C901
#     default: ParameterT = PydanticUndefined,
#     *,
#     default_factory: typing.Callable[[], ParameterT] | None = None,
#     significant: bool = True,
#     id_hash_include: Callable[[ParameterT], bool] = _always_include,
#     id_hasher: Callable[[ParameterT], JsonValue] | None = None,
#     **kwargs: Any,  # TODO typing?
# ) -> Any:
#     """TODO: docstring

#     Usage:
#     ```python
#     class MyTask(Task):
#         param: str = ParamField("my_default", significant=False)
#     ```
#     """

#     return Field(
#         default=default,
#         default_factory=default_factory,
#         json_schema_extra=_ParameterConfig(
#             id_hash_include=id_hash_include if significant else _always_exclude,
#             id_hasher=IDHasher() if id_hasher is None else id_hasher,
#         ),
#         **kwargs,
#     )
