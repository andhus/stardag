import typing

from pydantic import PydanticSchemaGenerationError

from dcdag.resources import get_target
from dcdag.target import LoadableSaveableFileSystemTarget, Serializable
from dcdag.target.serialize import JSONSerializer, PickleSerializer
from dcdag.task import Task

LoadedT = typing.TypeVar("LoadedT")


class AutoFSTTask(
    Task[LoadableSaveableFileSystemTarget[LoadedT]],
    typing.Generic[LoadedT],
):
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
        # get generic type of self
        loaded_t = typing.get_args(self.__orig_class__)[0]

        try:
            serializer = JSONSerializer(annotation=loaded_t)
        except PydanticSchemaGenerationError:
            serializer = PickleSerializer[loaded_t]()

        return Serializable(
            wrapped=get_target(self._relpath, task=self),
            serializer=serializer,
        )
