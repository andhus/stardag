import typing


from dcdag.resources import get_target
from dcdag.target import LoadableSaveableFileSystemTarget, Serializable
from dcdag.target.serialize import (
    get_serializer,
)
from dcdag.task import Task

LoadedT = typing.TypeVar("LoadedT")


class AutoFSTTask(
    Task[LoadableSaveableFileSystemTarget[LoadedT]],
    typing.Generic[LoadedT],
):
    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        # get generic type of self
        loaded_t = typing.get_args(cls.__orig_class__)[0]
        if type(loaded_t) != typing.TypeVar:
            cls._serializer = get_serializer(loaded_t)

    @property
    def _relpath_base(self) -> str:
        return ""

    @property
    def _relpath_extra(self) -> str:
        return ""

    @property
    def _relpath_filename(self) -> str:
        return ""

    @property
    def _relpath(self) -> str:
        return "/".join(
            [
                part
                for part in [
                    self._relpath_base,
                    self.get_task_family(),
                    f"v{self.version}" if self.version else "",
                    self._relpath_extra,
                    self.task_id[:2],
                    self.task_id[2:4],
                    self.task_id,
                    self._relpath_filename,
                ]
                if part
            ]
        )

    def output(self) -> LoadableSaveableFileSystemTarget[LoadedT]:
        return Serializable(
            wrapped=get_target(self._relpath, task=self),
            serializer=self._serializer,
        )
