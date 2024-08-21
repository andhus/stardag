import json
from abc import abstractmethod
from functools import cached_property
from hashlib import sha1
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Dict, Generic, Type, TypeVar

from pydantic import (
    BaseModel,
    Field,
    PlainSerializer,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    WithJsonSchema,
    WrapValidator,
)
from typing_extensions import List, TypeAlias, Union

from dcdag.core.parameter import _ParameterConfig
from dcdag.core.target import Target

TargetT = TypeVar("TargetT", bound=Union[Target, None], covariant=True)

TaskStruct: TypeAlias = Union["Task", List["TaskStruct"], Dict[str, "TaskStruct"]]

# The type allowed for tasks to declare their dependencies. Note that it would be
# enough with just list[Task], but allowing these types are only for visualization
# purposes and dev UX - it allows for grouping and labeling of the incoming "edges"
# in the DAG.
TaskDeps: TypeAlias = Union[
    None, "Task", List["Task"], Dict[str, "Task"], Dict[str, List["Task"]]
]


class _Register:
    def __init__(self):
        self._family_to_class: dict[str, Type["Task"]] = {}

    def add(self, task_class: Type["Task"]):
        # TODO support luigi style name spacing
        if self._family_to_class.get(task_class.family_name()):
            raise ValueError(
                f"Task family name {task_class.family_name()} already registered."
            )
        self._family_to_class[task_class.family_name()] = task_class

    def get(self, family_name: str) -> Type["Task"]:
        return self._family_to_class[family_name]


_REGISTER = _Register()


class TaskIDRef(BaseModel):
    family_name: str
    version: str | None
    id_hash: str


class Task(BaseModel, Generic[TargetT]):
    __version__: ClassVar[str | None] = None

    version: str | None = Field(default=None, description="Version of the task code.")

    if TYPE_CHECKING:
        _param_configs: ClassVar[Dict[str, _ParameterConfig]] = {}
    else:
        _param_configs = {}

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        _REGISTER.add(cls)
        cls._param_configs = {
            name: (
                field_info.json_schema_extra.init(
                    annotation=field_info.rebuild_annotation()
                )
                if isinstance(field_info.json_schema_extra, _ParameterConfig)
                else _ParameterConfig().init(annotation=field_info.rebuild_annotation())
            )
            for name, field_info in cls.model_fields.items()
        }
        # TODO automatically set version default to __version__.

    @abstractmethod
    def output(self) -> TargetT: ...

    @abstractmethod
    def run(self) -> None:
        """Execute the task logic."""
        # TODO dynamic deps, including type hint
        ...

    def requires(self) -> TaskDeps:
        return None

    @classmethod
    def family_name(cls) -> str:
        return cls.__name__

    @cached_property
    def id_hash(self) -> str:
        return get_str_hash(self._id_hash_json())

    @property
    def id_ref(self) -> TaskIDRef:
        return TaskIDRef(
            family_name=self.family_name(),
            version=self.version,
            id_hash=self.id_hash,
        )

    def run_version_checked(self):
        if not self.version == self.__version__:
            raise ValueError("TODO")

        self.run()

    def _id_hash_jsonable(self) -> dict:
        return {
            "family_name": self.family_name(),
            "parameters": {
                name: config.id_hasher(getattr(self, name))
                for name, config in self._param_configs.items()
                if config.id_hash_include(getattr(self, name))
            },
        }

    def _id_hash_json(self) -> str:
        return _hash_safe_json_dumps(self._id_hash_jsonable())


_TASK_FAMILY_KEY = "__family_name"


def _get_task_param_validate(annotation):
    def _task_param_validate(
        x: Any,
        handler: ValidatorFunctionWrapHandler,
        info: ValidationInfo,
    ) -> Task:
        if isinstance(x, dict):
            if _TASK_FAMILY_KEY not in x:
                raise ValueError(
                    f"Task parameter dict must have a '{_TASK_FAMILY_KEY}' key."
                )

            instance = _REGISTER.get(x[_TASK_FAMILY_KEY])(
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

            (target_t,) = meta.get("args", (Any,))

            if target_t is not Any:
                # TODO check must be loosened.
                if not isinstance(instance.output.__annotations__["return"], target_t):
                    raise ValueError(
                        f"Task parameter target method must return {target_t}, got "
                        f"{instance.output.__annotations__['return']}."
                    )

        return instance

    return _task_param_validate


_TaskT = TypeVar("_TaskT", bound=Task)


# See: https://github.com/pydantic/pydantic/issues/8202#issuecomment-2264669699
class _TaskParam:
    def __class_getitem__(cls, item):
        return Annotated[
            item,
            WrapValidator(_get_task_param_validate(item)),
            PlainSerializer(
                lambda x: {**x.model_dump(), _TASK_FAMILY_KEY: x.family_name()}
            ),
            WithJsonSchema(
                {
                    "type": "object",
                    "properties": {
                        _TASK_FAMILY_KEY: {"type": "string"},
                    },
                    "additionalProperties": True,
                },
                mode="serialization",
            ),
        ]


if TYPE_CHECKING:
    TaskParam: TypeAlias = Annotated[_TaskT, "task_param"]  # TODO?
else:
    TaskParam = _TaskParam


def _hash_safe_json_dumps(obj):
    """Fixed separators and (deep) sort_keys for stable hash."""
    return json.dumps(
        obj,
        separators=(",", ":"),
        sort_keys=True,
    )


def get_str_hash(str_: str) -> str:
    # TODO truncate / convert to UUID?
    return sha1(str_.encode("utf-8")).hexdigest()
