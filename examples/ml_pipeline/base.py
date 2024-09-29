import datetime
import json
import logging
import pickle
import tempfile
import uuid
from pathlib import Path
from typing import Annotated, ClassVar, Literal, Type, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.tree import DecisionTreeClassifier

logger = logging.getLogger(__name__)

Category = Literal["A", "B", "C"]
Segment = Literal["X", "Y", "Z"]


def generate_data(
    num_samples: int = 1000,
    segment_flip_probs: tuple[float, float, float] = (0.1, 0.2, 0.3),
) -> pd.DataFrame:
    """Simulates export from a *mutable* data source.
    Returns data frame with index of UUIDs and columns:
    - `number`: random normal distribution float
    - `category`: random choice from a ["A", "B", "C"]
    - `segment`: random choice from a ["X", "Y", "Z"]
    - `_target_flip`: noise variable which True or False based on segment
    The probability of flipping the target based on segment:
    - Segment "X": p(random flip) = 0.1
    - Segment "Y": p(random flip) = 0.2
    - Segment "Z": p(random flip) = 0.3
    """

    # set seed based on current time
    np.random.seed(int((pd.Timestamp.now().timestamp() * 1e6) % 2**32))

    logger.info("Generating data...")
    df = pd.DataFrame(
        data={
            "number": np.random.normal(size=num_samples),
            "category": np.random.choice(["A", "B", "C"], num_samples),
            "segment": np.random.choice(["X", "Y", "Z"], num_samples),
        },
        index=[str(uuid.uuid4()) for _ in range(num_samples)],  # type: ignore
    )
    # add random target flip
    df["_target_flip"] = 0
    for segment in ["X", "Y", "Z"]:
        mask = df["segment"] == segment
        flip_mask = (
            np.random.rand(mask.sum()) < segment_flip_probs["XYZ".index(segment)]
        )
        df.loc[mask, "_target_flip"] = flip_mask.astype(int)

    df["_target_flip"] = df["_target_flip"].astype(bool)

    return df


class ProcessParams(BaseModel):
    category_thresholds: tuple[float, float, float] = (0.0, 0.5, 1.0)


def process_data(df: pd.DataFrame, params: ProcessParams):
    """Adds a target column to the data frame.
    - `target`: False or True
    Target is computed as follows based on category and segment:
    - Category "A" target = number > 0.0
    - Category "B" target = number > 0.5
    - Category "C" target = number > 1.0
    But then target is flipped based on the _target_flip column:
    """
    df["target"] = 0
    for category in ["A", "B", "C"]:
        mask = df["category"] == category
        target = (
            df["number"] > params.category_thresholds["ABC".index(category)]
        ).astype(int)
        df.loc[mask, "target"] = target

    df["target"] = df["target"] ^ df["_target_flip"]
    df["target"] = df["target"].astype(bool)

    return df


class RandomPartition(BaseModel):
    num_buckets: int
    include_buckets: tuple[int, ...]
    seed_salt: str = "default"


class DatasetFilter(BaseModel):
    categories: tuple[Category, ...] | None = None
    segments: tuple[Segment, ...] | None = None
    random_partition: RandomPartition | None = None

    def __call__(self, dataset: pd.DataFrame) -> pd.DataFrame:
        if self.categories is not None:
            dataset = dataset[dataset["category"].isin(self.categories)]  # type: ignore

        if self.segments is not None:
            dataset = dataset[dataset["segment"].isin(self.segments)]  # type: ignore

        if self.random_partition is not None:
            rp: RandomPartition = self.random_partition
            dataset = dataset[  # type: ignore
                dataset.index.map(
                    lambda x: hash(x + rp.seed_salt) % rp.num_buckets
                    in rp.include_buckets
                )
            ]

        return dataset


class LogisticRegressionHyperParameters(BaseModel):
    class_: ClassVar[Type[LogisticRegression]] = LogisticRegression

    type: Literal["LogisticRegression"] = "LogisticRegression"
    penalty: Literal["l1", "l2", "elasticnet"] = "l2"

    def init(self):
        return self.class_(**self.model_dump(exclude={"type"}))


class DecisionTreeHyperParameters(BaseModel):
    class_: ClassVar[Type[DecisionTreeClassifier]] = DecisionTreeClassifier

    type: Literal["DecisionTreeClassifier"] = "DecisionTreeClassifier"
    criterion: Literal["gini", "entropy", "log_loss"] = "gini"
    max_depth: int = 3

    def init(self):
        return self.class_(**self.model_dump(exclude={"type"}))


HyperParameters = Annotated[
    Union[LogisticRegressionHyperParameters, DecisionTreeHyperParameters],
    Field(discriminator="type"),
]


class ModelFitContext(BaseModel):
    model_dir: Path
    seed: int

    model_config = ConfigDict(
        protected_namespaces=(),
        **BaseModel.model_config,  # type: ignore
    )


class SKLearnClassifierModel:
    def __init__(self, hyper_parameters: HyperParameters) -> None:
        self.hyper_parameters = hyper_parameters
        self.model = self.hyper_parameters.init()

    def fit(self, X: pd.DataFrame, y: pd.Series, context: ModelFitContext):
        # encode categorical columns
        self.model.fit(self._preprocess(X), y)

    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        X_ = self._preprocess(X)
        y_pred_proba = self.model.predict_proba(X_)[:, 1]  # type: ignore
        df = pd.DataFrame(
            data={"y_pred_proba": y_pred_proba},
            index=X.index,
        )
        df["y_pred"] = y_pred_proba > 0.5

        return df

    def _preprocess(
        self, X: pd.DataFrame
    ) -> pd.DataFrame:  # encode categorical columns
        return pd.get_dummies(X)

    def save(self, model_dir: Path):
        with (model_dir / "model.pkl").open("wb") as file:
            pickle.dump(self.model, file)
        with (model_dir / "hyper_parameters.json").open("w") as file:
            file.write(self.hyper_parameters.model_dump_json(indent=2))

    @classmethod
    def load(cls, model_dir: Path):
        with (model_dir / "model.pkl").open("rb") as file:
            model = pickle.load(file)
        with (model_dir / "hyper_parameters.json").open("r") as file:
            hyper_parameters = TypeAdapter(HyperParameters).validate_json(file.read())
        instance = cls(hyper_parameters)
        instance.model = model
        return instance


FIT_COLUMNS: list[str] = ["number", "category", "segment"]


def train_model(
    model: SKLearnClassifierModel,
    dataset: pd.DataFrame,
    context: ModelFitContext,
    fit_columns: list[str] = FIT_COLUMNS,
):
    X = dataset[fit_columns]
    y = dataset["target"]
    context.model_dir.mkdir(exist_ok=False, parents=False)
    model.fit(X, y, context)  # type: ignore
    model.save(context.model_dir)

    return model


def predict_model(
    model: SKLearnClassifierModel,
    dataset: pd.DataFrame,
    fit_columns: list[str] = FIT_COLUMNS,
) -> pd.DataFrame:
    X = dataset[fit_columns]
    return model.predict(X)  # type: ignore


def get_metrics(dataset: pd.DataFrame, predictions: pd.DataFrame) -> dict[str, float]:
    y_true = dataset["target"]
    y_pred = predictions["y_pred"]
    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred),
        "recall": recall_score(y_true, y_pred),
        "f1": f1_score(y_true, y_pred),
    }  # type: ignore


def utc_today() -> datetime.date:
    """Return today's date in UTC timezone."""
    return datetime.datetime.now(tz=datetime.timezone.utc).date()


def run():
    raw_df = generate_data()

    df = process_data(raw_df, ProcessParams())

    with tempfile.TemporaryDirectory() as tmp_dir:
        model_dir = Path(tmp_dir) / "model_dir"
        context = ModelFitContext(model_dir=model_dir, seed=42)
        model = SKLearnClassifierModel(hyper_parameters=DecisionTreeHyperParameters())
        trained_model = train_model(
            model=model,
            dataset=df,
            context=context,
        )

    predictions = predict_model(trained_model, df)

    metrics = get_metrics(df, predictions)  # type: ignore
    print(json.dumps(metrics, indent=2))
    return metrics


if __name__ == "__main__":
    metrics = run()
