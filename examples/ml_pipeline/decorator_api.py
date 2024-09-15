# -*- coding: utf-8 -*-
import datetime
import json
import logging
import tempfile
from functools import partial
from pathlib import Path

import pandas as pd

from dcdag.build.sequential import build as build_sequential
from dcdag.decorator import Depends, task

from .base import (
    DatasetFilter,
    HyperParameters,
    LogisticRegressionHyperParameters,
    ModelFitContext,
    ProcessParams,
    RandomPartition,
    SKLearnClassifierModel,
    generate_data,
    get_metrics,
    predict_model,
    process_data,
    train_model,
    utc_today,
)

logger = logging.getLogger(__name__)


base_task = partial(task, version="0", relpath_base="examples/ml_pipeline")


@base_task(
    relpath=lambda self: (
        f"{self._relpath_base}/DumpData/v{self.version}/{self.date}/"  # type: ignore
        f"{self.snapshot_slug}.pkl"  # type: ignore
    )
)
def dump(
    date: datetime.date = utc_today(),
    snapshot_slug: str = "default",
) -> pd.DataFrame:
    """Dump data.

    Args:
        date: The date of the dump.
        snapshot_slug: The slug for the dump. If you want to create multiple dumps
            for the same date, this must be used to differentiate them.
    """
    if not date == utc_today():
        raise ValueError("Date must be today")

    data = generate_data()
    return data


@base_task
def dataset(
    dump: Depends[pd.DataFrame],
    params: ProcessParams = ProcessParams(),
) -> pd.DataFrame:
    """Process data."""
    print("Processing data...")
    return process_data(dump, params=params)


@base_task
def subset(
    dataset: Depends[pd.DataFrame],
    filter: DatasetFilter,
) -> pd.DataFrame:
    """Sub setting data..."""
    return filter(dataset)


@base_task
def trained_model(
    dataset: Depends[pd.DataFrame],
    model: HyperParameters = LogisticRegressionHyperParameters(),
    seed: int = 0,
) -> SKLearnClassifierModel:
    """Training model..."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        model_dir = tmp_path / "model_dir"
        model_instance = SKLearnClassifierModel(hyper_parameters=model)
        model_instance = train_model(
            model=model_instance,
            dataset=dataset,
            context=ModelFitContext(
                model_dir=model_dir,
                seed=seed,
            ),
        )
    return model_instance


@base_task
def predictions(
    dataset: Depends[pd.DataFrame],
    model: Depends[SKLearnClassifierModel],
) -> pd.DataFrame:
    """Predicting..."""
    return predict_model(model=model, dataset=dataset)


@base_task
def metrics(
    dataset: Depends[pd.DataFrame],
    predictions: Depends[pd.DataFrame],
) -> dict[str, float]:
    """Calculating metrics..."""
    return get_metrics(dataset, predictions)


def get_metrics_dag():
    dataset_ = dataset(dump=dump(), params=ProcessParams())
    train_filter = DatasetFilter(
        random_partition=RandomPartition(
            num_buckets=3,
            include_buckets=(0, 1),
        )
    )
    test_filter = DatasetFilter(
        random_partition=RandomPartition(
            num_buckets=3,
            include_buckets=(2,),
        )
    )

    predictions_ = predictions(
        model=trained_model(
            model=LogisticRegressionHyperParameters(),
            dataset=subset(dataset=dataset_, filter=train_filter),
            seed=0,
        ),
        dataset=subset(dataset=dataset_, filter=test_filter),
    )

    return metrics(
        dataset=subset(dataset=dataset_, filter=test_filter),
        predictions=predictions_,
    )


def build_metrics_dag():
    metrics = get_metrics_dag()
    print(metrics.model_dump_json(indent=2))
    build_sequential(metrics)
    print(json.dumps(metrics.output().load(), indent=2))
    return metrics
