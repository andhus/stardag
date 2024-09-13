import inspect
import typing

from pydantic import create_model

from dcdag.core.fsttask import AutoFSTTask
from dcdag.core.task import Task
from dcdag.core.task_parameter import TaskLoads

LoadedT = typing.TypeVar("LoadedT")
FuncT = typing.TypeVar("FuncT", bound=typing.Callable)

_PWrapped = typing.ParamSpec("_PWrapped")


class FunctionTask(AutoFSTTask[LoadedT], typing.Generic[LoadedT, _PWrapped]):
    _func: typing.Callable[_PWrapped, LoadedT]

    if typing.TYPE_CHECKING:

        def __init__(
            self,
            # TODO not really possible to type hint this (?) :/ Below would only allow
            # the same signature as the function, not TaskLoads[<type>]
            #    *args: _PWrapped.args, **kwargs: _PWrapped.kwargs
            # and if the user is forced to type hit the function with
            # <type> | TaskLoads[<type>], then it doesn't make sense inside the function
            **kwargs: typing.Any,
        ) -> None: ...

    @classmethod
    def call(cls, *args: _PWrapped.args, **kwargs: _PWrapped.kwargs) -> LoadedT:
        return cls._func(*args, **kwargs)  # type: ignore

    def requires(self) -> typing.Mapping[str, Task] | None:
        requires = {
            name: getattr(self, name)
            for name in self.model_fields.keys()
            if isinstance(getattr(self, name), Task)
        }
        return requires or None

    def run(self) -> None:
        result = self.call(**self._get_inputs())  # type: ignore
        self.output().save(result)

    def _get_inputs(self) -> _PWrapped.kwargs:  # type: ignore
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


def task(
    func: typing.Callable[_PWrapped, LoadedT],
) -> typing.Type[FunctionTask[LoadedT, _PWrapped]]:
    """Decorator to turn a function into a task."""

    signature = inspect.signature(func)
    return_type = signature.return_annotation
    if return_type == inspect.Parameter.empty:
        raise ValueError("Return type must be annotated")
    args = signature.parameters
    if any(arg.annotation == inspect.Parameter.empty for arg in args.values()):
        raise ValueError("All arguments must have annotations")

    task_class = create_model(
        func.__name__,
        __base__=FunctionTask[LoadedT, _PWrapped],
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
