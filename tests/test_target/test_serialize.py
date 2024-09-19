import typing

import pytest

from stardag.target._base import FileSystemTarget
from stardag.target.serialize import (
    DataFrame,
    JSONSerializer,
    PandasDataFrameCSVSerializer,
    PickleSerializer,
    PlainTextSerializer,
    SelfSerializer,
    SelfSerializing,
    Serializer,
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


class CustomMockSerializer(Serializer[str]):
    def dump(self, obj: str, target: FileSystemTarget) -> None:
        with target.open("w") as f:
            f.write(obj)

    def load(self, target: FileSystemTarget) -> str:
        with target.open("r") as f:
            return f.read()

    def __eq__(self, value: object) -> bool:
        return isinstance(value, CustomMockSerializer)


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
        (typing.Annotated[str, CustomMockSerializer()], CustomMockSerializer()),
    ],
)
def test_get_serializer(annotation, expected_serializer):
    serializer = get_serializer(annotation)
    assert serializer == expected_serializer

    extra_annotation = typing.Annotated[annotation, "extra"]
    serializer_from_extra_annotated = get_serializer(extra_annotation)  # type: ignore
    assert serializer_from_extra_annotated == expected_serializer
