"""CLI entry point for the Spark to scikit-learn pipeline."""

import logging

from sklearn_spark.pipeline import build_pipeline

logging.basicConfig()
logging.getLogger("data_joinery").setLevel(logging.INFO)

__all__ = ["build_pipeline"]
