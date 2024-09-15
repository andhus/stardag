# -*- coding: utf-8 -*-
import datetime
import json
import logging
import tempfile
import typing
from pathlib import Path

import pandas as pd
from pydantic import Field
from traitlets import Any

from dcdag.auto_task import AutoFSTTask
from dcdag.build.sequential import build as build_sequential
from dcdag.examples.ml_pipeline.base import (
    DatasetFilter,
    DecisionTreeHyperParameters,
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
from dcdag.target import LoadedT
from dcdag.task_parameter import TaskLoads

logger = logging.getLogger(__name__)


class ExamplesMLPipelineBase(AutoFSTTask[LoadedT], typing.Generic[LoadedT]):
    __version__ = "0"
    version: str | None = __version__

    @property
    def _relpath_base(self) -> str:
        return "examples/ml_pipeline"


class Dump(ExamplesMLPipelineBase[pd.DataFrame]):
    date: datetime.date = Field(default_factory=utc_today)
    snapshot_slug: str = Field(
        default="default",
        description=(
            "The slug for the dump. If you want to create multiple dumps "
            "for the same date, this must be used to differentiate them"
        ),
    )

    @property
    def _relpath(self) -> str:
        return (
            f"{self._relpath_base}/DumpData/v{self.version}/{self.date}/"
            f"{self.snapshot_slug}.csv"  # TODO extension and format
        )

    def run(self):
        if not self.date == utc_today():
            raise ValueError("Date must be today")

        data = generate_data()
        self.output().save(data)


class Dataset(ExamplesMLPipelineBase[pd.DataFrame]):
    dump: TaskLoads[pd.DataFrame] = Field(default_factory=Dump)
    params: ProcessParams = ProcessParams()

    def requires(self):
        return self.dump

    def run(self):
        print("Processing data...")
        data = self.dump.output().load()
        processed_data = process_data(data, params=self.params)
        self.output().save(processed_data)


class Subset(ExamplesMLPipelineBase[pd.DataFrame]):
    dataset: TaskLoads[pd.DataFrame]
    filter: DatasetFilter

    def requires(self):  # type: ignore
        return self.dataset

    def run(self):
        print("Sub setting data...")
        data = self.dataset.output().load()  # type: ignore
        subset = self.filter(data)
        self.output().save(subset)


class TrainedModel(ExamplesMLPipelineBase[SKLearnClassifierModel]):
    model: HyperParameters = LogisticRegressionHyperParameters()
    dataset: Subset
    seed: int = 0

    def requires(self):  # type: ignore
        return self.dataset

    # TODO directory target!

    def run(self):
        print("Training model...")
        dataset = self.dataset.output().load()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            model_dir = tmp_path / "model_dir"
            model = SKLearnClassifierModel(hyper_parameters=self.model)
            model = train_model(
                model=model,
                dataset=dataset,
                context=ModelFitContext(
                    model_dir=model_dir,
                    seed=self.seed,
                ),
            )
        self.output().save(model)


class Predictions(ExamplesMLPipelineBase[pd.DataFrame]):
    trained_model: TrainedModel
    dataset: Subset

    def requires(self):
        return {
            "trained_model": self.trained_model,
            "dataset": self.dataset,
        }

    def run(self):
        print("Predicting...")
        model = self.trained_model.output().load()
        dataset = self.dataset.output().load()
        predictions = predict_model(model=model, dataset=dataset)
        self.output().save(predictions)


class Metrics(ExamplesMLPipelineBase[dict[str, float]]):
    predictions: Predictions

    def requires(self):
        return {
            "predictions": self.predictions,
            "dataset": self.predictions.dataset,
        }

    def run(self):
        print("Calculating metrics...")
        dataset = self.predictions.dataset.output().load()
        predictions = self.predictions.output().load()
        metrics = get_metrics(dataset, predictions)
        self.output().save(metrics)


#     def prefect_on_complete_artifacts(self):
#         from prefect.artifacts import MarkdownArtifact

#         metrics = self.load()
#         markdown = f"""# Metrics Summary

# | Metric    | Value |
# |-----------|-------|
# | Accuracy  | {metrics['accuracy']} |
# | Precision | {metrics['precision']} |
# | Recall    | {metrics['recall']} |
# | F1        | {metrics['f1']} |
# """

#         return [
#             MarkdownArtifact(
#                 markdown=markdown,
#                 key=format_key(f"{self.task_id}-result"),
#                 description="Metrics",
#             )
#         ]


class Benchmark(ExamplesMLPipelineBase[list[dict[str, Any]]]):
    train_dataset: Subset
    test_dataset: Subset
    models: frozenset[HyperParameters]
    seed: int = 0

    def requires(self):  # type: ignore
        return [
            Metrics(
                predictions=Predictions(
                    trained_model=TrainedModel(
                        model=model,
                        dataset=self.train_dataset,
                        seed=self.seed,
                    ),
                    dataset=self.test_dataset,
                )
            )
            for model in self.models
        ]

    def run(self):
        metrics_s = [task.output().load() for task in self.requires()]
        metrics_and_params_s = [
            {**metrics, **hyper_parameters.model_dump(mode="json")}
            for metrics, hyper_parameters in zip(metrics_s, self.models)
        ]
        self.output().save(metrics_and_params_s)

    # def prefect_on_complete_artifacts(self):
    #     from prefect.artifacts import TableArtifact

    #     rows = self.output().load()

    #     return [
    #         TableArtifact(
    #             table=rows,
    #             key=format_key(f"{self.task_id}-result"),
    #             description="Metrics by model parameters",
    #         )
    #     ]


def get_metrics_dag(
    dump: Dump | None = None,
    preprocess_params: ProcessParams = ProcessParams(),
):
    dump = dump or Dump()

    dataset = Dataset(dump=dump, params=preprocess_params)

    train_dataset = Subset(
        dataset=dataset,
        filter=DatasetFilter(
            random_partition=RandomPartition(
                num_buckets=3,
                include_buckets=(0, 1),
            )
        ),
    )
    test_dataset = Subset(
        dataset=dataset,
        filter=DatasetFilter(
            random_partition=RandomPartition(
                num_buckets=3,
                include_buckets=(2,),
            )
        ),
    )

    trained_model = TrainedModel(
        model=LogisticRegressionHyperParameters(),
        dataset=train_dataset,
        seed=0,
    )

    predictions = Predictions(trained_model=trained_model, dataset=test_dataset)

    metrics = Metrics(predictions=predictions)

    return metrics


def get_benchmark_dag(
    dump: Dump | None = None,
    preprocess_params: ProcessParams = ProcessParams(),
):
    dump = dump or Dump()

    dataset = Dataset(dump=dump, params=preprocess_params)

    train_dataset = Subset(
        dataset=dataset,
        filter=DatasetFilter(
            random_partition=RandomPartition(
                num_buckets=3,
                include_buckets=(0, 1),
            )
        ),
    )
    test_dataset = Subset(
        dataset=dataset,
        filter=DatasetFilter(
            random_partition=RandomPartition(
                num_buckets=3,
                include_buckets=(2,),
            )
        ),
    )

    benchmark = Benchmark(
        train_dataset=train_dataset,
        test_dataset=test_dataset,
        models=frozenset(
            [
                LogisticRegressionHyperParameters(penalty="l2"),
                DecisionTreeHyperParameters(criterion="gini", max_depth=3),
                DecisionTreeHyperParameters(criterion="gini", max_depth=10),
                DecisionTreeHyperParameters(criterion="entropy", max_depth=3),
                DecisionTreeHyperParameters(criterion="entropy", max_depth=10),
            ]
        ),
        seed=0,
    )

    return benchmark


def build_metrics_dag():
    metrics = get_metrics_dag()
    print(metrics.model_dump_json(indent=2))
    build_sequential(metrics)
    print(json.dumps(metrics.output().load(), indent=2))
    return metrics
