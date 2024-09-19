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

from stardag.parameter import (
    IDHasher,
    IDHasherABC,
    IDHashInclude,
    IDHashIncludeABC,
    _ParameterConfig,
)
from stardag.target import Target

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
        self._class_to_family_and_namespace: dict[Type["Task"], Tuple[str, str]] = {}
        self._module_to_namespace: dict[str, str] = {}

    def add(
        self,
        task_class: Type["Task"],
        family_override: str | None,
        namespace_override: str | None,
    ):
        if task_class in self._class_to_family_and_namespace:
            raise ValueError(f"Task class already registered: {task_class}")

        family = self._get_family(
            task_class,
            family_override=family_override,
        )
        namespace = self._get_namespace(
            task_class,
            namespace_override=namespace_override,
        )
        self._class_to_family_and_namespace[task_class] = (family, namespace)
        namespace_family = get_namespace_family(namespace, family)
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

    def get_task_family_and_namespace(
        self,
        task_class: Type["Task"],
    ) -> Tuple[str, str]:
        res = self._class_to_family_and_namespace.get(task_class)
        if res is None:
            raise ValueError(f"Task class not registered: {task_class}")
        return res

    def _get_family(
        self,
        task_class: Type["Task"],
        family_override: str | None,
    ) -> str:
        if family_override is not None:
            return family_override

        if task_class.__family__ is not None:
            # Already set explicitly on task
            return task_class.__family__

        # No family set
        return task_class.__name__

    def _get_namespace(
        self,
        task_class: Type["Task"],
        namespace_override: str | None,
    ) -> str:
        if namespace_override is not None:
            return namespace_override

        if task_class.__namespace__ is not None:
            # Already set explicitly on task class
            return task_class.__namespace__

        # check if set by module or any parent module
        module_parts = task_class.__module__.split(".")
        for idx in range(len(module_parts), 0, -1):
            module = ".".join(module_parts[:idx])
            namespace = self._module_to_namespace.get(module)
            if namespace:
                return namespace

        # No namespace set
        return ""


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
    module = scope
    _REGISTER.add_module_namespace(module, module)


def namespace(namespace: str, scope: str):
    _REGISTER.add_module_namespace(scope, namespace)


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
        family_override: str | None = None,
        namespace_override: str | None = None,
        **kwargs: Any,
    ) -> None:
        # Need to avoid forwarding the family and namespace kwarg to the BaseModel
        super().__init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(
        cls,
        family_override: str | None = None,
        namespace_override: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__pydantic_init_subclass__(**kwargs)

        # Register (including set namespace) and set family for *non generic* task
        # classes
        if not is_generic_task_class(cls):
            _REGISTER.add(
                cls,
                family_override=family_override,
                namespace_override=namespace_override,
            )

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
        return _REGISTER.get_task_family_and_namespace(cls)[1]

    @classmethod
    def get_family(cls) -> str:
        return _REGISTER.get_task_family_and_namespace(cls)[0]

    @classmethod
    def get_namespace_family(cls) -> str:
        family, namespace = _REGISTER.get_task_family_and_namespace(cls)
        return get_namespace_family(namespace=namespace, family=family)

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
            "namespace": self.get_namespace(),
            "family": self.get_family(),
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
