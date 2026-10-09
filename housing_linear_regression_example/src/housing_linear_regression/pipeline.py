from pathlib import Path

from data_joinery.pipeline import Pipeline

from housing_linear_regression.context import PipelineContext

from .transform import fit_model, prepare_features, print_coefficients, read_data

DATA_DIR = Path(__file__).parent.parent.parent / "data"


def build_pipeline() -> Pipeline[PipelineContext]:
    pipeline = Pipeline(PipelineContext)
    housing = pipeline.add_step(read_data, name="read_data")
    prepared_housing = pipeline.add_step(prepare_features, name="prepare_features")
    model = pipeline.add_step(fit_model, name="fit_model")
    coefficients = pipeline.add_step(print_coefficients, name="print_coefficients")

    pipeline.connect(housing, prepared_housing)
    pipeline.connect(prepared_housing, model)
    pipeline.connect(model, coefficients)

    return pipeline
