# -*- coding: utf-8 -*-
import datetime
import json
import logging
import tempfile
from functools import partial
from pathlib import Path

import pandas as pd

from stardag.build.sequential import build as build_sequential
from stardag.decorator import Depends, task
from stardag.task import namespace

from . import base

logger = logging.getLogger(__name__)

namespace("examples.ml_pipeline.decorator_api", scope=__name__)

base_task = partial(task, version="0")


@base_task(
    relpath=lambda self: (
        f"dump/v{self.version}/{self.date}/"  # type: ignore
        f"{self.snapshot_slug}.pkl"  # type: ignore
    )
)
def dump(
    date: datetime.date = base.utc_today(),
    snapshot_slug: str = "default",
) -> pd.DataFrame:
    """Dump data.

    Args:
        date: The date of the dump.
        snapshot_slug: The slug for the dump. If you want to create multiple dumps
            for the same date, this must be used to differentiate them.
    """
    if not date == base.utc_today():
        raise ValueError("Date must be today")

    data = base.generate_data()
    return data


@base_task
def dataset(
    dump: Depends[pd.DataFrame],
    params: base.ProcessParams = base.ProcessParams(),
) -> pd.DataFrame:
    """Process data."""
    print("Processing data...")
    return base.process_data(dump, params=params)


@base_task
def subset(
    dataset: Depends[pd.DataFrame],
    filter: base.DatasetFilter,
) -> pd.DataFrame:
    """Sub setting data..."""
    return filter(dataset)


@base_task
def trained_model(
    dataset: Depends[pd.DataFrame],
    model: base.HyperParameters = base.LogisticRegressionHyperParameters(),
    seed: int = 0,
) -> base.SKLearnClassifierModel:
    """Training model..."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        model_dir = tmp_path / "model_dir"
        model_instance = base.SKLearnClassifierModel(hyper_parameters=model)
        model_instance = base.train_model(
            model=model_instance,
            dataset=dataset,
            context=base.ModelFitContext(
                model_dir=model_dir,
                seed=seed,
            ),
        )
    return model_instance


@base_task
def predictions(
    dataset: Depends[pd.DataFrame],
    model: Depends[base.SKLearnClassifierModel],
) -> pd.DataFrame:
    """Predicting..."""
    return base.predict_model(model=model, dataset=dataset)


@base_task
def metrics(
    dataset: Depends[pd.DataFrame],
    predictions: Depends[pd.DataFrame],
) -> dict[str, float]:
    """Calculating metrics..."""
    return base.get_metrics(dataset, predictions)


def get_metrics_dag():
    dataset_ = dataset(dump=dump(), params=base.ProcessParams())
    train_filter = base.DatasetFilter(
        random_partition=base.RandomPartition(
            num_buckets=3,
            include_buckets=(0, 1),
        )
    )
    test_filter = base.DatasetFilter(
        random_partition=base.RandomPartition(
            num_buckets=3,
            include_buckets=(2,),
        )
    )

    predictions_ = predictions(
        model=trained_model(
            model=base.LogisticRegressionHyperParameters(),
            dataset=subset(dataset=dataset_, filter=train_filter),
            seed=0,
        ),
        dataset=subset(dataset=dataset_, filter=test_filter),
    )

    return metrics(
        dataset=subset(dataset=dataset_, filter=test_filter),
        predictions=predictions_,
    )


if __name__ == "__main__":
    metrics_task = get_metrics_dag()
    print(metrics_task.model_dump_json(indent=2))
    build_sequential(metrics_task)
    print(json.dumps(metrics_task.output().load(), indent=2))
