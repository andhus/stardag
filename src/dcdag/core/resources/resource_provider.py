# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager
from typing import Callable, Generator, Generic, Type, TypeVar

__all__ = ["ResourceProvider"]

logger = logging.getLogger(__name__)


_ResourceType = TypeVar("_ResourceType")


class ResourceProvider(Generic[_ResourceType]):
    def __init__(self):
        self._resource: _ResourceType | None = None

    def get(self) -> _ResourceType:
        if self._resource is None:
            self._resource = self.default_factory()
        return self._resource

    def set(self, resource: _ResourceType):
        self._resource = resource

    def default_factory(self, **kwargs) -> _ResourceType:
        """Needs to be implemented by subclasses.

        NOTE when called by the constructor, kwargs will be empty.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement a default_factory"
        )

    @contextmanager
    def override(
        self, resource: _ResourceType, context: str | None = None
    ) -> Generator[_ResourceType, None, None]:
        initial = self._resource
        logger.debug(
            f"Overriding resource. Initial: {initial}, new: {resource}, "
            f"context: {context}"
        )
        try:
            self.set(resource)
            yield resource
        finally:
            logger.debug(f"Restoring resource: {initial}, context: {context}")
            self._resource = initial


def resource_provider(
    type_: Type[_ResourceType],
    default_factory: Callable[[], _ResourceType] | None = None,
    doc_str: str = "",
) -> ResourceProvider[_ResourceType]:
    """Functional creation of a ResourceProvider for a specific type.

    Reduces boilerplate for simple default_factory implementations.

    Example:

    ```python
    provider = resource_provider(str, default_factory=lambda: "default")
    assert provider.get() == "default"
    provider.set("test")
    assert provider.get() == "test"
    ```
    """

    class FunctionalResourceProvider(ResourceProvider[type_]):
        __doc__ = doc_str

        def default_factory(self) -> type_:  # type: ignore
            if default_factory is None:
                raise NotImplementedError(
                    f"{self.__class__.__name__} does not implement a default_factory"
                )
            return default_factory()

    return FunctionalResourceProvider()
