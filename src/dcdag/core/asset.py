from abc import abstractmethod
from functools import cached_property
from hashlib import sha1
import json
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Dict,
    Generic,
    Type,
    TypeVar,
    get_origin,
)
from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    PlainSerializer,
    WithJsonSchema,
    ValidationInfo,
    ValidationError,
    ValidatorFunctionWrapHandler,
    WrapValidator,
)
from typing_extensions import TypeAlias, Union, List

from dcdag.core.parameter import _ParameterConfig


LoadedT = TypeVar("LoadedT", covariant=True)
TargetT = TypeVar("TargetT", covariant=True)

AssetStruct: TypeAlias = Union["Asset", List["AssetStruct"], Dict[str, "AssetStruct"]]

# The type allowed for assets to declare their dependencies. Note that it would be
# enough with just list[Asset], but allowing these types are only for visualization
# purposes and dev UX - it allows for grouping and labeling of the incoming "edges"
# in the DAG.
AssetDeps: TypeAlias = Union[
    None, "Asset", List["Asset"], Dict[str, "Asset"], Dict[str, List["Asset"]]
]


class _Register:

    def __init__(self):
        self._family_to_class: dict[str, Type["Asset"]] = {}

    def add(self, asset_class: Type["Asset"]):
        # TODO support luigi style namespacing
        if self._family_to_class.get(asset_class.family_name()):
            raise ValueError(
                f"Asset family name {asset_class.family_name()} already registered."
            )
        self._family_to_class[asset_class.family_name()] = asset_class

    def get(self, family_name: str) -> Type["Asset"]:
        return self._family_to_class[family_name]


_REGISTER = _Register()


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
    def id_ref(self) -> AssetIDRef:
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


_ASSET_FAMILY_KEY = "__family_name"


def _get_asset_param_validate(annotation):
    def _asset_param_validate(
        x: Any,
        handler: ValidatorFunctionWrapHandler,
        info: ValidationInfo,
    ) -> Asset:
        if isinstance(x, dict):
            if _ASSET_FAMILY_KEY not in x:
                raise ValueError(
                    f"Asset parameter dict must have a '{_ASSET_FAMILY_KEY}' key."
                )

            instance = _REGISTER.get(x[_ASSET_FAMILY_KEY])(
                **{key: value for key, value in x.items() if key != _ASSET_FAMILY_KEY}
            )
        elif isinstance(x, Asset):
            instance = x
        else:
            raise ValueError(f"Invalid asset parameter type: {type(x)}")

        try:
            return handler(instance)
        except ValidationError as e:
            # print(
            #     f"Error in asset parameter validation: {e}, {info}"
            #     f"\nAnnotation: {annotation}"
            # )
            # check that the annotation is correct
            if not isinstance(instance, Asset):
                raise ValueError(
                    f"Asset parameter must be of type {Asset}, got {type(instance)}."
                )

        meta: dict = annotation.__pydantic_generic_metadata__
        origin = meta.get("origin")
        if not origin == Asset:
            raise ValueError(f"Asset parameter must be of type {Asset}, got {origin}.")

        load_t, target_t = meta.get("args")

        if not load_t is Any:
            if not instance.load.__annotations__["return"] == load_t:
                raise ValueError(
                    f"Asset parameter load method must return {load_t}, got "
                    f"{instance.load.__annotations__['return']}."
                )
        if not target_t is Any:
            if not instance.target.__annotations__["return"] == target_t:
                raise ValueError(
                    f"Asset parameter target method must return {target_t}, got "
                    f"{instance.target.__annotations__['return']}."
                )

        return instance

    return _asset_param_validate


# LoadedT_ = TypeVar("LoadedT_", bound=Any, covariant=True)
# TargetT_ = TypeVar("TargetT_", bound=Any, covariant=True)

# _AssetT = TypeVar("_AssetT", bound=Asset[LoadedT_, TargetT_])

_AssetT = TypeVar("_AssetT", bound=Asset)

_AssetParam = Annotated[
    _AssetT,
    # WrapValidator(_asset_param_validate),
    PlainSerializer(lambda x: {**x.model_dump(), _ASSET_FAMILY_KEY: x.family_name()}),
    WithJsonSchema(
        {
            "type": "object",
            "properties": {
                _ASSET_FAMILY_KEY: {"type": "string"},
            },
            "additionalProperties": True,
        },
        mode="serialization",
    ),
]


# TODO add "if TYPE_CHECKING"...
# See: https://github.com/pydantic/pydantic/issues/8202#issuecomment-2264669699
class AssetParam:
    def __class_getitem__(cls, item):
        return Annotated[
            item,
            WrapValidator(_get_asset_param_validate(item)),
            PlainSerializer(
                lambda x: {**x.model_dump(), _ASSET_FAMILY_KEY: x.family_name()}
            ),
            WithJsonSchema(
                {
                    "type": "object",
                    "properties": {
                        _ASSET_FAMILY_KEY: {"type": "string"},
                    },
                    "additionalProperties": True,
                },
                mode="serialization",
            ),
        ]


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
