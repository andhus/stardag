import inspect
import typing

from pydantic import create_model

from dcdag.core.fsttask import AutoFSTTask
from dcdag.core.task import Task
from dcdag.core.task_parameter import TaskLoads

LoadedT = typing.TypeVar("LoadedT")


class FunctionTask(AutoFSTTask[LoadedT], typing.Generic[LoadedT]):
    _func: typing.Callable

    @classmethod
    def call(cls, *args, **kwds):
        return cls._func(*args, **kwds)

    def run(self) -> None:
        result = self.__class__._func(**self._get_inputs())
        self.output().save(result)

    def _get_inputs(self):
        def get_input(name):
            value = getattr(self, name)
            if isinstance(value, Task):
                return value.output().load()
            return value

        return {
            name: get_input(name)
            for name in self.model_fields.keys()
            if name != "version"
        }

    def result(self) -> LoadedT:
        return self.output().load()


def task(func):
    """Decorator to turn a function into a task."""
    # get the function signature
    sig = inspect.signature(func)
    # get the function return type
    return_type = sig.return_annotation
    if return_type == inspect.Parameter.empty:
        raise ValueError("Return type must be annotated")

    # get the function arguments
    args = sig.parameters
    # create the model
    # TODO
    if any(arg.annotation == inspect.Parameter.empty for arg in args.values()):
        raise ValueError("All arguments must have annotations")

    task_class = create_model(
        func.__name__,
        __base__=FunctionTask[return_type],
        __module__=func.__module__,
        **{  # type: ignore
            name: (
                arg.annotation | TaskLoads[arg.annotation],
                arg.default if arg.default != inspect.Parameter.empty else ...,
            )
            for name, arg in args.items()
        },
    )
    task_class._func = func

    return task_class


@task
def add(a: int, b: int) -> int:
    return a + b
