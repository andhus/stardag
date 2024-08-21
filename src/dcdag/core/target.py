import abc
import typing
from contextlib import contextmanager


class Target(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def exists(self) -> bool: ...


LoadedT = typing.TypeVar("LoadedT")


class LoadableTarget(Target, typing.Generic[LoadedT]):
    @abc.abstractmethod
    def load(self) -> LoadedT: ...


class InMemoryTarget(LoadableTarget[LoadedT]):
    """Useful in testing :)"""

    key_to_target = {}  # Note class variable!

    @classmethod
    def clear_targets(cls):
        cls.key_to_target = {}

    @classmethod
    @contextmanager
    def cleared(cls):
        cls.clear_targets()
        try:
            yield cls.key_to_target
        finally:
            cls.clear_targets()

    def __init__(self, key):
        self.key = key

    def exists(self):  # type: ignore
        return self.key in self.key_to_target

    def save(self, obj: LoadedT) -> None:
        self.key_to_target[self.key] = obj

    def load(self) -> LoadedT:
        return self.key_to_target[self.key]


class FileSystemTarget(Target):
    def __init__(self, path: str) -> None:
        self.path = path

    def exists(self) -> bool:
        return False
