from dcdag.core.build.sequential import build as build_sequential
from dcdag.examples.ml_pipeline.decorator_api import get_metrics_dag


def test_build_metrics_dag(default_in_memory_fs_target):
    metrics = get_metrics_dag()
    build_sequential(metrics)
    assert metrics.complete()
    assert metrics.output().exists()
    metrics_dict = metrics.output().load()
    assert set(metrics_dict.keys()) == {"accuracy", "precision", "recall", "f1"}
    assert metrics_dict["f1"] > 0.55
