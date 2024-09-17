import json
import logging
from abc import abstractmethod
from functools import cached_property
from hashlib import sha1
from typing import (
    TYPE_CHECKING,
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

from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo
from typing_extensions import List, TypeAlias, Union

from dcdag.parameter import (
    IDHasher,
    IDHasherABC,
    IDHashInclude,
    IDHashIncludeABC,
    _ParameterConfig,
)
from dcdag.target import Target

logger = logging.getLogger(__name__)

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
        self._namespace_family_to_class: dict[str, Type["Task"]] = {}
        self._module_to_namespace: dict[str, str] = {}

    def add(self, task_class: Type["Task"]):
        self._finalize_namespace(task_class)
        namespace_family = task_class.get_namespace_family()
        logger.debug(
            f"\nRegistering task class: {task_class}\n"
            f"  namespace_family: {namespace_family}\n"
            f"  module.name: {task_class.__module__}.{task_class.__name__}\n"
            f"  __orig_bases__: {getattr(task_class, '__orig_bases__')}\n"
            "  __pydantic_generic_metadata__: "
            f"{task_class.__pydantic_generic_metadata__}\n"
        )
        existing = self._namespace_family_to_class.get(namespace_family)
        if existing:
            raise ValueError(
                "A task is already registered for the "
                f'namespace_family "{namespace_family}".\n'
                f"Existing: {existing.__module__}.{existing.__name__}\n"
                f"New: {task_class.__module__}.{task_class.__name__}"
            )
        self._namespace_family_to_class[namespace_family] = task_class

    def get(self, namespace, family: str) -> Type["Task"]:
        namespace_family = get_namespace_family(namespace, family)
        return self._namespace_family_to_class[namespace_family]

    def add_module_namespace(self, module: str, namespace: str):
        self._module_to_namespace[module] = namespace

    def _finalize_namespace(self, task_class: Type["Task"]):
        if task_class.__namespace__ is not None:
            # Already set explicitly on task
            return
        # check if set by module
        namespace = self._module_to_namespace.get(task_class.__module__)
        if namespace:
            task_class.__namespace__ = namespace
        else:
            task_class.__namespace__ = ""


def get_namespace_family(namespace: str, family: str) -> str:
    if namespace:  # NOTE: empty string is "no namespace"
        return f"{namespace}.{family}"
    return family


def is_generic_task_class(cls: Type["Task"]) -> bool:
    meta = cls.__pydantic_generic_metadata__
    if meta["origin"] or meta["parameters"]:
        return True
    return False


_REGISTER = _Register()


def auto_namespace(scope: str):
    """Set the task namespace for the module to the module import path.

    Usage:
    ```python
    from dcdag.task import auto_namespace

    auto_namespace(__name__)

    class MyTask(Task):
        ...
    ```
    """
    module = scope.rstrip(".")
    _REGISTER.add_module_namespace(module, module)


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
        __namespace__: ClassVar[str]
        __family__: ClassVar[str]
    else:
        _param_configs = {}
        __namespace__: ClassVar[str | None] = None
        __family__: ClassVar[str | None] = None

    @classmethod
    def __init_subclass__(
        cls,
        family: str | None = None,
        **kwargs: Any,
    ) -> None:
        # Need to avoid forwarding the family kwarg to the BaseModel
        super().__init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(
        cls,
        family: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__pydantic_init_subclass__(**kwargs)

        # Register (including set namespace) and set family for *non generic* task
        # classes
        if not is_generic_task_class(cls):
            cls.__family__ = family or cls.__name__
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
        if isinstance(params, tuple):
            params = params[0]
        create_model.__orig_class__ = _Generic[params]  # type: ignore
        return create_model

    @classmethod
    def get_namespace(cls) -> str:
        if cls.__namespace__ is None:
            raise ValueError("Namespace not set.")
        return cls.__namespace__

    @classmethod
    def get_family(cls) -> str:
        return cls.__family__

    @classmethod
    def get_namespace_family(cls) -> str:
        if cls.get_namespace():  # NOTE: empty string is "no namespace"
            return f"{cls.get_namespace()}.{cls.get_family()}"
        return cls.get_family()

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

    @cached_property
    def task_id(self) -> str:
        return get_str_hash(self._id_hash_json())

    @property
    def id_ref(self) -> TaskIDRef:
        return TaskIDRef(
            task_family=self.get_family(),
            version=self.version,
            task_id=self.task_id,
        )

    def run_version_checked(self):
        if not self.version == self.__version__:
            raise ValueError("TODO")

        self.run()

    def _id_hash_jsonable(self) -> dict:
        return {
            "task_family": self.get_family(),
            "parameters": {
                name: config.id_hasher(getattr(self, name))
                for name, config in self._param_configs.items()
                if config.id_hash_include(getattr(self, name))
            },
        }

    def _id_hash_json(self) -> str:
        return _hash_safe_json_dumps(self._id_hash_jsonable())

    def __hash__(self) -> int:
        # TODO?
        return hash(self.task_id)


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
