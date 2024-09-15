from dcdag.build.sequential import build as build_sequential
from dcdag.target.serialize import JSONSerializer, PandasDataFrameCSVSerializer


def test_build_metrics_dag(default_in_memory_fs_target, examples_in_sys_path):
    from ml_pipeline.decorator_api import get_metrics_dag  # type: ignore

    metrics = get_metrics_dag()
    assert isinstance(metrics._serializer, JSONSerializer)
    assert isinstance(metrics.predictions._serializer, PandasDataFrameCSVSerializer)
    build_sequential(metrics)
    assert metrics.complete()
    assert metrics.output().exists()
    metrics_dict = metrics.output().load()
    assert set(metrics_dict.keys()) == {"accuracy", "precision", "recall", "f1"}
    assert metrics_dict["f1"] > 0.55
