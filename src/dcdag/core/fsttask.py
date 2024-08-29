import typing

from dcdag.core.resources import get_target
from dcdag.core.target import (
    JSONSerializer,
    LoadableSaveableFileSystemTarget,
    Serializable,
)
from dcdag.core.task import Task

LoadedT = typing.TypeVar("LoadedT")


class AutoFSTTask(
    Task[LoadableSaveableFileSystemTarget[LoadedT]],
    typing.Generic[LoadedT],
):
    @property
    def relpath_base(self) -> str:
        return ""

    @property
    def relpath_extra(self) -> str:
        return ""

    @property
    def relpath_filename(self) -> str:
        return ""

    @property
    def relpath(self) -> str:
        return "/".join(
            [
                part
                for part in [
                    self.relpath_base,
                    self.get_task_family(),
                    f"v{self.version}" if self.version else "",
                    self.relpath_extra,
                    self.task_id[:2],
                    self.task_id[2:4],
                    self.task_id,
                    self.relpath_filename,
                ]
                if part
            ]
        )

    def output(self) -> LoadableSaveableFileSystemTarget[LoadedT]:
        # get generic type of self
        loaded_t = typing.get_args(self.__orig_class__)[0]
        return Serializable(
            wrapped=get_target(self.relpath, task=self),
            serializer=JSONSerializer(annotation=loaded_t),
        )
