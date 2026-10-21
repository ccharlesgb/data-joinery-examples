"""Data Joinery CLI entry point; importing this module starts no services."""

from .pipeline import build_pipeline

__all__ = ["build_pipeline"]
