import pytest
from data_joinery import Schema
from housing_linear_regression.context import HousingPath
from housing_linear_regression.schemas import Housing, HousingWithFeatures
from housing_linear_regression.transform import (
    fit_model,
    prepare_features,
    print_coefficients,
    read_data,
)
from pyspark.ml.linalg import Vectors
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual


def housing_rows() -> list[Housing]:
    return [
        Housing(1, 100, 250.0),
        Housing(2, 100, 350.0),
        Housing(1, 200, 450.0),
        Housing(2, 200, 550.0),
    ]


def test_read_data_reads_parquet(spark: SparkSession, tmp_path):
    expected = Schema(Housing).create_dataframe(
        housing_rows(), DataFrame, session=spark
    )
    path = tmp_path / "housing"
    expected.write.parquet(str(path))

    assertDataFrameEqual(read_data(spark, HousingPath(str(path))), expected)


def test_prepare_features_assembles_bedrooms_and_area(spark: SparkSession):
    source = Schema(Housing).create_dataframe(
        [Housing(2, 150, 400.0)], DataFrame, session=spark
    )
    expected = Schema(HousingWithFeatures).create_dataframe(
        [HousingWithFeatures(2, 150, 400.0, Vectors.dense(2.0, 150.0))],
        DataFrame,
        session=spark,
    )

    assertDataFrameEqual(prepare_features(source), expected)


def test_fit_model_learns_housing_feature_coefficients(spark: SparkSession):
    housing = Schema(Housing).create_dataframe(housing_rows(), DataFrame, session=spark)

    model = fit_model(prepare_features(housing))

    assert model.coefficients.toArray()[0] == pytest.approx(100.0)
    assert model.coefficients.toArray()[1] == pytest.approx(2.0)
    assert model.intercept == pytest.approx(-50.0, abs=1e-3)


def test_print_coefficients_reports_model(
    spark: SparkSession, capsys: pytest.CaptureFixture[str]
):
    housing = Schema(Housing).create_dataframe(housing_rows(), DataFrame, session=spark)
    model = fit_model(prepare_features(housing))

    print_coefficients(model)

    assert capsys.readouterr().out == (
        f"coefficients: {model.coefficients}\nintercept: {model.intercept}\n"
    )
