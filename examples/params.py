import json
from typing import Any

from pydantic import BaseModel
from pytest import param
from dcdag.core.asset import Asset, AssetParam
from dcdag.core.parameter import ParamField


class TestModel(BaseModel):
    test: str = "testing"


class SubAsset(Asset[int, None]):
    param: int

    def load(self) -> int:
        return self.param

    def target(self) -> None:
        return

    def run(self) -> None:
        pass


class MyAsset(Asset[str, None]):
    a: int
    b: str
    c: str = ParamField(
        "C",
        significant=False,
        # id_hash_include=lambda x: x != "C",
    )
    model: TestModel = TestModel()
    sub_asset: AssetParam[Asset[int, Any]]

    def load(self) -> str:
        return f"{self.a}, {self.b}"

    def target(self) -> None:
        return

    def run(self) -> None:
        pass


if __name__ == "__main__":
    my_asset = MyAsset(a=1, b="b", sub_asset=SubAsset(param=1))
    print(json.dumps(my_asset._id_hash_jsonable(), indent=2))
    print(my_asset.id_hash)
    print(my_asset.id_ref)
    dumped = my_asset.model_dump()
    print(dumped)
    print(MyAsset.model_validate(dumped))
