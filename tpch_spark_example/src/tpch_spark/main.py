"""CLI-discoverable TPC-H Spark pipeline."""

import logging

from tpch_spark.pipeline import build_pipeline

logging.basicConfig()
logging.getLogger("data_joinery").setLevel(logging.INFO)

__all__ = ["build_pipeline"]
