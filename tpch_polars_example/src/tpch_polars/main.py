"""CLI-discoverable TPC-H Polars pipeline."""

import logging

from tpch_polars.pipeline import build_pipeline

logging.basicConfig()
logging.getLogger("data_joinery").setLevel(logging.INFO)

__all__ = ["build_pipeline"]
