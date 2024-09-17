import typing

import pytest

from dcdag.target._base import FileSystemTarget
from dcdag.target.serialize import (
    DataFrame,
    JSONSerializer,
    PandasDataFrameCSVSerializer,
    PickleSerializer,
    PlainTextSerializer,
    SelfSerializer,
    SelfSerializing,
    get_serializer,
)


class _SelfSerializing(SelfSerializing):
    def __init__(self, value: str) -> None:
        self.value = value

    def dump(self, target: FileSystemTarget) -> None:
        with target.open("w") as f:
            f.write(str(self.value))

    @classmethod
    def load(cls, target: FileSystemTarget) -> typing.Self:
        with target.open("r") as f:
            return cls(f.read())


class _NoDefaultSerializerType:
    def __init__(self, value: str) -> None:
        self.value = value


@pytest.mark.parametrize(
    "annotation,expected_serializer",
    [
        (str, PlainTextSerializer()),
        (int, JSONSerializer(int)),
        (float, JSONSerializer(float)),
        (dict[str, int], JSONSerializer(dict[str, int])),
        (dict[str, str], JSONSerializer(dict[str, str])),
        (DataFrame, PandasDataFrameCSVSerializer()),
        (_SelfSerializing, SelfSerializer(_SelfSerializing)),
        (_NoDefaultSerializerType, PickleSerializer()),
    ],
)
def test_get_serializer(annotation, expected_serializer):
    serializer = get_serializer(annotation)
    assert serializer == expected_serializer
