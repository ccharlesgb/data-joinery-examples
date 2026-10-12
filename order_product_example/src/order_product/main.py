"""CLI entry point for the order product pipeline."""

import logging

from order_product.pipeline import build_pipeline

logging.basicConfig()
logging.getLogger("data_joinery").setLevel(logging.INFO)

__all__ = ["build_pipeline"]
