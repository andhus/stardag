import inspect
import typing

from pydantic import create_model

from stardag.auto_task import AutoFSTTask
from stardag.task import Task
from stardag.task_parameter import TaskLoads

LoadedT = typing.TypeVar("LoadedT")
FuncT = typing.TypeVar("FuncT", bound=typing.Callable)

_PWrapped = typing.ParamSpec("_PWrapped")


class _FunctionTask(AutoFSTTask[LoadedT], typing.Generic[LoadedT, _PWrapped]):
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


class _TaskWrapper(typing.Protocol):
    def __call__(
        self,
        _func: typing.Callable[_PWrapped, LoadedT],
    ) -> typing.Type[_FunctionTask[LoadedT, _PWrapped]]: ...


_Relpath = str | typing.Callable[[AutoFSTTask[LoadedT]], str] | None


@typing.overload
def task(
    _func: typing.Callable[_PWrapped, LoadedT],
    *,
    version: str = "0",
    relpath: _Relpath = None,
    relpath_base: str | None = None,
) -> typing.Type[_FunctionTask[LoadedT, _PWrapped]]: ...


@typing.overload
def task(
    *,
    version: str = "0",
    relpath: _Relpath = None,
    relpath_base: str | None = None,
) -> _TaskWrapper: ...


def task(
    _func: typing.Callable[_PWrapped, LoadedT] | None = None,
    *,
    version: str = "0",
    relpath: _Relpath = None,
    relpath_base: str | None = None,
    # TODO remaining kwargs!
) -> typing.Type[_FunctionTask[LoadedT, _PWrapped]] | _TaskWrapper:
    def wrapper(
        _func: typing.Callable[_PWrapped, LoadedT],
    ) -> typing.Type[_FunctionTask[LoadedT, _PWrapped]]:
        """Decorator to turn a function into a task."""

        signature = inspect.signature(_func)
        return_type = signature.return_annotation
        if return_type == inspect.Parameter.empty:
            raise ValueError("Return type must be annotated")
        args = signature.parameters
        if any(arg.annotation == inspect.Parameter.empty for arg in args.values()):
            raise ValueError("All arguments must have annotations")

        task_class = create_model(
            _func.__name__,
            __base__=_FunctionTask[return_type, _PWrapped],
            __module__=_func.__module__,
            version=(str | None, version),
            **{  # type: ignore
                name: (
                    _get_param_annotation(arg.annotation),
                    arg.default if arg.default != inspect.Parameter.empty else ...,
                )
                for name, arg in args.items()
            },
        )
        task_class._func = _func
        task_class.__version__ = "0"

        # extra properties
        if relpath:
            if callable(relpath):
                task_class._relpath = property(relpath)
            else:
                assert isinstance(relpath, str)
                task_class._relpath = relpath

        if relpath_base:
            task_class._relpath_base = relpath_base

        return task_class

    if _func is None:
        return wrapper  # type: ignore

    return wrapper(_func)


_DependsT = typing.TypeVar("_DependsT")


class _DependsOnMarker:
    pass


Depends = typing.Annotated[_DependsT, _DependsOnMarker]


def _get_param_annotation(func_annotation: typing.Any) -> typing.Any:
    args = typing.get_args(func_annotation)
    if _DependsOnMarker in args:
        stripped_args = tuple(arg for arg in args if arg != _DependsOnMarker)
        if len(stripped_args) > 1:
            stripped_annotation = typing.Annotated[*stripped_args]  # type: ignore
        else:
            stripped_annotation = stripped_args[0]  # type: ignore
        return TaskLoads[stripped_annotation]
    return func_annotation | TaskLoads[func_annotation]
