from dcdag.core.fsttask import AutoFSTTask
from dcdag.core.parameter import (
    IDHasher,
    ParamField,
    _always_exclude,
    _always_include,
    _ParameterConfig,
)


class MockTask(AutoFSTTask[str]):
    a: int = ParamField()
    b: str = ParamField(significant=False)


def test_parameter():
    assert MockTask._param_configs["a"] == _ParameterConfig(
        id_hash_include=_always_include, id_hasher=IDHasher().init(int)
    )
    assert MockTask._param_configs["b"] == _ParameterConfig(
        id_hash_include=_always_exclude, id_hasher=IDHasher().init(str)
    )
