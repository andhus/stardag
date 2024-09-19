import typing

from pydantic import (
    PlainSerializer,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    WithJsonSchema,
    WrapValidator,
)

from stardag.parameter import IDHasherABC
from stardag.target import LoadableTarget
from stardag.task import _REGISTER, Task

_TaskT = typing.TypeVar("_TaskT", bound=Task)


# See: https://github.com/pydantic/pydantic/issues/8202#issuecomment-2264669699
class _TaskParam:
    def __class_getitem__(cls, item):
        return typing.Annotated[
            item,
            WrapValidator(_get_task_param_validate(item)),
            PlainSerializer(
                lambda x: {
                    **x.model_dump(),
                    _TASK_FAMILY_KEY: x.get_family(),
                    _TASK_NAMESPACE_KEY: x.get_namespace(),
                },
            ),
            WithJsonSchema(
                {
                    "type": "object",
                    "properties": {
                        _TASK_FAMILY_KEY: {"type": "string"},
                        _TASK_NAMESPACE_KEY: {"type": "string"},
                    },
                    "additionalProperties": True,
                },
                mode="serialization",
            ),
        ]


if typing.TYPE_CHECKING:
    TaskParam: typing.TypeAlias = typing.Annotated[_TaskT, "task_param"]
else:
    TaskParam = _TaskParam


_TASK_FAMILY_KEY = "__family__"
_TASK_NAMESPACE_KEY = "__namespace__"


def _get_task_param_validate(annotation):
    def _task_param_validate(
        x: typing.Any,
        handler: ValidatorFunctionWrapHandler,
        info: ValidationInfo,
    ) -> Task:
        if isinstance(x, dict):
            namespace = x.get(_TASK_NAMESPACE_KEY)
            if namespace is None:
                raise ValueError(
                    f"Task parameter dict must have a '{_TASK_NAMESPACE_KEY}' key."
                )
            family = x.get(_TASK_FAMILY_KEY)
            if family is None:
                raise ValueError(
                    f"Task parameter dict must have a '{_TASK_NAMESPACE_KEY}' key."
                )
            class_ = _REGISTER.get(namespace=namespace, family=family)
            instance = class_(
                **{key: value for key, value in x.items() if key != _TASK_FAMILY_KEY}
            )
        elif isinstance(x, Task):
            instance = x
        else:
            raise ValueError(f"Invalid task parameter type: {type(x)}")

        try:
            return handler(instance)
        except ValidationError:
            # print(
            #     f"Error in task parameter validation: {e}"
            #     f"\nAnnotation: {annotation}"
            # )
            # check that the annotation is correct
            if not isinstance(instance, Task):
                raise ValueError(
                    f"Task parameter must be of type {Task}, got {type(instance)}."
                )

            meta: dict = annotation.__pydantic_generic_metadata__
            origin = meta.get("origin")
            if not origin == Task:  # TODO subclass check?
                raise ValueError(
                    f"Task parameter must be of type {Task}, got {origin}."
                )

            (target_t,) = meta.get("args", (typing.Any,))

            if target_t is not typing.Any:
                # TODO check must be loosened and improved, check libs...
                if not instance.output.__annotations__["return"] == target_t:
                    pass
                    # TODO!
                    # warnings.warn(
                    #     "Could not verify task parameter type compatibility."
                    #     f"Input Task.output() must be compatible with {target_t}, "
                    #     f"got {instance.output.__annotations__['return']}."
                    # )

        return instance

    return _task_param_validate


_LoadedT = typing.TypeVar("_LoadedT")


class _TaskLoads:
    def __class_getitem__(cls, item):
        return _TaskParam[Task[LoadableTarget[item]]]


if typing.TYPE_CHECKING:
    TaskLoads = typing.Annotated[
        TaskParam[Task[LoadableTarget[_LoadedT]]], "task_loads"
    ]
else:
    TaskLoads = _TaskLoads


class TaskSetHasher(IDHasherABC[typing.Set[Task]]):
    def __call__(self, value: typing.Set[Task]) -> typing.List[str]:  # type: ignore
        return sorted([child.task_id for child in value])


TaskSet = typing.Annotated[typing.FrozenSet[TaskParam[_TaskT]], TaskSetHasher()]
