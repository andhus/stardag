import json
import warnings
from abc import abstractmethod
from functools import cached_property
from hashlib import sha1
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Dict,
    Generic,
    Mapping,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

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
from pydantic.fields import FieldInfo
from typing_extensions import List, TypeAlias, Union

from dcdag.core.parameter import (
    IDHasher,
    IDHasherABC,
    IDHashInclude,
    IDHashIncludeABC,
    _ParameterConfig,
)
from dcdag.core.target import Target

TargetT = TypeVar("TargetT", bound=Union[Target, None], covariant=True)

TaskStruct: TypeAlias = Union["Task", List["TaskStruct"], Dict[str, "TaskStruct"]]

# The type allowed for tasks to declare their dependencies. Note that it would be
# enough with just list[Task], but allowing these types are only for visualization
# purposes and dev UX - it allows for grouping and labeling of the incoming "edges"
# in the DAG.
TaskDeps: TypeAlias = Union[
    None, "Task", Sequence["Task"], Mapping[str, "Task"], Mapping[str, Sequence["Task"]]
]


class _Register:
    def __init__(self):
        self._family_to_class: dict[str, Type["Task"]] = {}

    def add(self, task_class: Type["Task"]):
        # TODO support luigi style name spacing
        if self._family_to_class.get(task_class.get_task_family()):
            raise ValueError(
                f"Task family name {task_class.get_task_family()} already registered."
            )
        self._family_to_class[task_class.get_task_family()] = task_class

    def get(self, task_family: str) -> Type["Task"]:
        return self._family_to_class[task_family]


_REGISTER = _Register()


class TaskIDRef(BaseModel):
    task_family: str
    version: str | None
    task_id: str


class _Generic(Generic[TargetT]):
    pass


class Task(BaseModel, Generic[TargetT]):
    __version__: ClassVar[str | None] = None

    version: str | None = Field(default=None, description="Version of the task code.")

    if TYPE_CHECKING:
        _param_configs: ClassVar[Dict[str, _ParameterConfig]] = {}
        __orig_class__: ClassVar[Any]  # _Generic[TargetT]
    else:
        _param_configs = {}

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        _REGISTER.add(cls)

        def get_one(field_info: FieldInfo, class_or_tuple, default_factory):
            matches = [
                value
                for value in field_info.metadata
                if isinstance(value, class_or_tuple)
            ]
            if len(matches) > 1:
                raise ValueError(f"Multiple {class_or_tuple} found in metadata.")
            if len(matches) == 1:
                return matches[0]

            return default_factory()

        def get_parameter_config(field_info: FieldInfo):
            id_hasher = get_one(field_info, IDHasherABC, IDHasher)
            id_hash_include = get_one(field_info, IDHashIncludeABC, IDHashInclude)
            return _ParameterConfig(
                id_hasher=id_hasher,
                id_hash_include=id_hash_include,
            ).init(annotation=field_info.rebuild_annotation())

        cls._param_configs = {
            name: get_parameter_config(field_info)
            for name, field_info in cls.model_fields.items()
        }
        # TODO automatically set version default to __version__.

    def __class_getitem__(
        cls: Type[BaseModel], params: Union[Type[Any], Tuple[Type[Any], ...]]
    ) -> Type[Any]:
        """Hack to be able to access the generic type of the class from subclasses. See:
        https://github.com/pydantic/pydantic/discussions/4904#discussioncomment-4592052
        """
        create_model = super().__class_getitem__(params)  # type: ignore

        create_model.__orig_class__ = _Generic[params]  # type: ignore
        return create_model

    def complete(self) -> bool:
        """Check if the task is complete."""
        target = self.output()
        if target is None:
            raise NotImplementedError("Tasks must implement output() or complete().")

        return target.exists()

    def output(self) -> TargetT:
        return None  # type: ignore

    @abstractmethod
    def run(self) -> None:
        """Execute the task logic."""
        # TODO dynamic deps, including type hint
        ...

    def requires(self) -> TaskDeps:
        return None

    @classmethod
    def get_task_family(cls) -> str:
        return cls.__name__

    @cached_property
    def task_id(self) -> str:
        return get_str_hash(self._id_hash_json())

    @property
    def id_ref(self) -> TaskIDRef:
        return TaskIDRef(
            task_family=self.get_task_family(),
            version=self.version,
            task_id=self.task_id,
        )

    def run_version_checked(self):
        if not self.version == self.__version__:
            raise ValueError("TODO")

        self.run()

    def _id_hash_jsonable(self) -> dict:
        return {
            "task_family": self.get_task_family(),
            "parameters": {
                name: config.id_hasher(getattr(self, name))
                for name, config in self._param_configs.items()
                if config.id_hash_include(getattr(self, name))
            },
        }

    def _id_hash_json(self) -> str:
        return _hash_safe_json_dumps(self._id_hash_jsonable())


_TASK_FAMILY_KEY = "__task_family"


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
                # TODO check must be loosened and improved, check libs...
                if not instance.output.__annotations__["return"] == target_t:
                    warnings.warn(
                        "Could not verify task parameter type compatibility."
                        f"Input Task.output() must be compatible with {target_t}, "
                        f"got {instance.output.__annotations__['return']}."
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
                lambda x: {**x.model_dump(), _TASK_FAMILY_KEY: x.get_task_family()}
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
