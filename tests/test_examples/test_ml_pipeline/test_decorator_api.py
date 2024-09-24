import pytest

from stardag.build.sequential import build as build_sequential
from stardag.target.serialize import JSONSerializer, PandasDataFrameCSVSerializer

try:
    import pandas as pd
except ImportError:
    pd = None


@pytest.mark.skipif(pd is None, reason="pandas is not installed")
def test_build_metrics_dag(default_in_memory_fs_target, examples_in_sys_path):
    from ml_pipeline.decorator_api import get_metrics_dag  # type: ignore

    metrics = get_metrics_dag()
    assert isinstance(metrics._serializer, JSONSerializer)
    assert metrics.output().path.endswith(".json")
    assert metrics.output().path.startswith(
        "in-memory://examples/ml_pipeline/decorator_api/metrics/v0/"
    )
    assert isinstance(metrics.predictions._serializer, PandasDataFrameCSVSerializer)
    assert metrics.predictions.output().path.endswith(".csv")
    build_sequential(metrics)
    assert metrics.complete()
    assert metrics.output().exists()
    metrics_dict = metrics.output().load()
    assert set(metrics_dict.keys()) == {"accuracy", "precision", "recall", "f1"}
    assert metrics_dict["f1"] > 0.50  # TODO fix seeds!
