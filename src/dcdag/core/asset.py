from abc import abstractmethod
from functools import cached_property
from hashlib import sha1
import json
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Generic, TypeVar
from pydantic import BaseModel, Field

from typing_extensions import TypeAlias, Union, List

from dcdag.core.parameter import _ParameterConfig


LoadedT = TypeVar("LoadedT")
TargetT = TypeVar("TargetT")

AssetStruct: TypeAlias = Union["Asset", List["AssetStruct"], Dict[str, "AssetStruct"]]

# The type allowed for assets to declare their dependencies. Note that it would be
# enough with just list[Asset], but allowing these types are only for visualization
# purposes and dev UX - it allows for grouping and labeling of the incoming "edges"
# in the DAG.
AssetDeps: TypeAlias = Union[
    None, "Asset", List["Asset"], Dict[str, "Asset"], Dict[str, List["Asset"]]
]


class AssetIDRef(BaseModel):
    family_name: str
    version: str | None
    id_hash: str


class Asset(BaseModel, Generic[LoadedT, TargetT]):

    __version__: ClassVar[str | None] = None

    version: str | None = Field(None, description="Version of the task code.")

    if TYPE_CHECKING:
        _param_configs: Dict[str, _ParameterConfig]

    else:
        _param_configs = {}

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        # TODO register class?
        super().__pydantic_init_subclass__(**kwargs)
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
    def load(self) -> LoadedT: ...

    @abstractmethod
    def target(self) -> TargetT: ...

    @abstractmethod
    def run(self) -> None: ...

    def requires(self) -> AssetDeps:
        return None

    @classmethod
    def family_name(cls) -> str:
        return cls.__name__

    @cached_property
    def id_hash(self) -> str:
        return get_str_hash(self._id_hash_json())

    @property
    def id_ref(self) -> str:
        return AssetIDRef(
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
