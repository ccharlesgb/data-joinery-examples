"""CLI entry point for the housing regression pipeline."""

import logging

from housing_linear_regression.pipeline import build_pipeline

logging.basicConfig()
logging.getLogger("data_joinery").setLevel(logging.INFO)

__all__ = ["build_pipeline"]
