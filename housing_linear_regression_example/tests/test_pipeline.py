import pytest
from data_joinery import Schema
from housing_linear_regression.context import HousingPath, PipelineContext
from housing_linear_regression.pipeline import build_pipeline
from housing_linear_regression.schemas import Housing
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame, SparkSession


def test_pipeline_passes_fitted_model_to_reporting_step(
    spark: SparkSession, tmp_path, capsys: pytest.CaptureFixture[str]
):
    housing = Schema(Housing).create_dataframe(
        [
            Housing(1, 100, 250.0),
            Housing(2, 100, 350.0),
            Housing(1, 200, 450.0),
            Housing(2, 200, 550.0),
        ],
        DataFrame,
        session=spark,
    )
    source_path = tmp_path / "housing"
    housing.write.parquet(str(source_path))

    result = build_pipeline().run(
        PipelineContext(spark=spark, housing_path=HousingPath(str(source_path)))
    )

    model = result.get_input("print_coefficients", "model", LinearRegressionModel)
    assert model.coefficients.toArray() == pytest.approx([100.0, 2.0], abs=1e-3)
    assert "coefficients:" in capsys.readouterr().out
