from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("order-product-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
