from pathlib import Path

from data_joinery import Pipeline

from .context import AdvertPipelineContext
from .transform import (
    calculate_prediction_metrics,
    collect_advert_features,
    create_advert_features,
    fit_model,
    predict_training_set,
    print_prediction_metrics,
    read_advert_events,
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"


def build_pipeline() -> Pipeline[AdvertPipelineContext]:
    pipeline = Pipeline(AdvertPipelineContext)
    events = pipeline.add_step(read_advert_events, name="read_advert_events")
    spark_features = pipeline.add_step(create_advert_features)
    polars_features = pipeline.add_step(collect_advert_features)
    model = pipeline.add_step(fit_model)
    predictions = pipeline.add_step(predict_training_set)
    metrics = pipeline.add_step(calculate_prediction_metrics)
    printed_metrics = pipeline.add_step(print_prediction_metrics)

    pipeline.connect(events, spark_features)
    pipeline.connect(spark_features, polars_features)
    pipeline.connect(polars_features, model)
    pipeline.connect(polars_features, predictions, param="advert_features")
    pipeline.connect(model, predictions)
    pipeline.connect(predictions, metrics)
    pipeline.connect(metrics, printed_metrics)

    return pipeline
