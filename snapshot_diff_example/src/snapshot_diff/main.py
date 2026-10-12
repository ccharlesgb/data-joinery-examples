"""CLI entry point for the snapshot diff pipeline."""

import logging

from snapshot_diff.pipeline import build_pipeline

logging.basicConfig()
logging.getLogger("data_joinery").setLevel(logging.INFO)

__all__ = ["build_pipeline"]
