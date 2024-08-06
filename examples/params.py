import json

from pydantic import BaseModel
from dcdag.core.asset import Asset
from dcdag.core.parameter import ParamField


class TestModel(BaseModel):
    test: str = "testing"


class MyAsset(Asset[str, None]):
    a: int
    b: str
    c: str = ParamField(
        "C",
        significant=False,
        # id_hash_include=lambda x: x != "C",
    )
    model: TestModel = TestModel()

    def load(self) -> str:
        return f"{self.a}, {self.b}"

    def target(self) -> None:
        return

    def run(self) -> None:
        pass


if __name__ == "__main__":
    my_asset = MyAsset(a=1, b="b")
    print(json.dumps(my_asset._id_hash_jsonable(), indent=2))
    print(my_asset.id_hash)
    print(my_asset.id_ref)
