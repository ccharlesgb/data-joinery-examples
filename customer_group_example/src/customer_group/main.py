"""CLI entry point for the customer group pipeline."""

import logging

from customer_group.pipeline import build_pipeline

logging.basicConfig()
logging.getLogger("data_joinery").setLevel(logging.INFO)

__all__ = ["build_pipeline"]
