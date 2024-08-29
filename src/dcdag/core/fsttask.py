import typing

from dcdag.core.target import (
    InMemoryFileSystemTarget,
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
    def file_system_target_class(self) -> typing.Type[InMemoryFileSystemTarget]:
        return InMemoryFileSystemTarget

    @property
    def base_path(self) -> str:
        return ""

    @property
    def params_path(self) -> str:
        return ""

    @property
    def filename(self) -> str:
        return ""

    @property
    def target_path(self) -> str:
        return "/".join(
            [
                part
                for part in [
                    self.base_path,
                    self.get_task_family(),
                    f"v{self.version}" if self.version else "",
                    self.params_path,
                    self.task_id,
                    self.filename,
                ]
                if part
            ]
        )

    def output(self) -> LoadableSaveableFileSystemTarget[LoadedT]:
        # get generic type of self
        loaded_t = typing.get_args(self.__orig_class__)[0]
        return Serializable(
            self.file_system_target_class(self.task_id),
            serializer=JSONSerializer(annotation=loaded_t),
        )
