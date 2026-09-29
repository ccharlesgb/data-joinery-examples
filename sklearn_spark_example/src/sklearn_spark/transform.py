from typing import Annotated, cast

import numpy as np
import polars as pl
from data_joinery import Context, Project, ProjectCast, Strict, transform
from numpy.typing import NDArray
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline as SklearnPipeline
from sklearn.preprocessing import OneHotEncoder

from .context import AdvertEventsPath
from .schemas import (
    AdvertEvents,
    AdvertFeatures,
    AdvertPredictions,
    PredictionMetrics,
)

SEARCH_VIEW = "SEARCH_VIEW"
ADVERT_VIEW = "ADVERT_VIEW"
CATEGORICAL_FEATURE_COLUMNS = ["postcode", "vehicle_make", "vehicle_model"]
TARGET_COLUMNS = ["number_of_search_views", "number_of_advert_views"]


@transform
def read_advert_events(
    spark: Annotated[SparkSession, Context()],
    path: Annotated[AdvertEventsPath, Context()],
) -> Annotated[DataFrame, ProjectCast(AdvertEvents)]:
    return spark.read.parquet(path)


@transform
def create_advert_features(
    advert_events: Annotated[DataFrame, Project(AdvertEvents)],
) -> Annotated[DataFrame, Strict(AdvertFeatures)]:
    dimensions = ["advert_id", "postcode", "vehicle_make", "vehicle_model"]
    return (
        advert_events.groupBy(
            F.to_date("event_timestamp").alias("snapshot_date"), *dimensions
        )
        .agg(
            F.sum(
                F.when(F.col("event_type") == SEARCH_VIEW, F.lit(1)).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("number_of_search_views"),
            F.sum(
                F.when(F.col("event_type") == ADVERT_VIEW, F.lit(1)).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("number_of_advert_views"),
        )
        .select(
            "snapshot_date",
            "advert_id",
            "postcode",
            "vehicle_make",
            "vehicle_model",
            "number_of_search_views",
            "number_of_advert_views",
        )
    )


@transform
def collect_advert_features(
    advert_features: Annotated[DataFrame, Strict(AdvertFeatures)],
) -> Annotated[pl.DataFrame, ProjectCast(AdvertFeatures)]:
    """Collect the aggregated, bounded feature set onto the driver."""
    return cast(pl.DataFrame, pl.from_arrow(advert_features.toArrow()))


@transform
def fit_model(
    advert_features: Annotated[pl.DataFrame, Strict(AdvertFeatures)],
) -> SklearnPipeline:
    if advert_features.is_empty():
        raise ValueError("Cannot fit a linear regression model without advert features")

    preprocessor = ColumnTransformer(
        [
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                CATEGORICAL_FEATURE_COLUMNS,
            )
        ]
    )
    model = SklearnPipeline(
        [("preprocessor", preprocessor), ("regressor", LinearRegression())]
    )
    return model.fit(
        advert_features.select(CATEGORICAL_FEATURE_COLUMNS),
        advert_features.select(TARGET_COLUMNS),
    )


@transform
def predict_training_set(
    advert_features: Annotated[pl.DataFrame, Strict(AdvertFeatures)],
    model: SklearnPipeline,
) -> Annotated[pl.DataFrame, Strict(AdvertPredictions)]:
    predictions = cast(
        NDArray[np.float64],
        model.predict(advert_features.select(CATEGORICAL_FEATURE_COLUMNS)),
    )
    return advert_features.with_columns(
        pl.Series("predicted_number_of_search_views", predictions[:, 0]),
        pl.Series("predicted_number_of_advert_views", predictions[:, 1]),
    )


@transform
def calculate_prediction_metrics(
    advert_predictions: Annotated[pl.DataFrame, Strict(AdvertPredictions)],
) -> PredictionMetrics:
    actual = advert_predictions.select(TARGET_COLUMNS)
    predicted = advert_predictions.select(
        "predicted_number_of_search_views", "predicted_number_of_advert_views"
    )
    return PredictionMetrics(
        mean_absolute_error=float(mean_absolute_error(actual, predicted)),
        mean_squared_error=float(mean_squared_error(actual, predicted)),
        r2_score=float(r2_score(actual, predicted)),
    )


@transform
def print_prediction_metrics(prediction_metrics: PredictionMetrics) -> None:
    print(prediction_metrics)
