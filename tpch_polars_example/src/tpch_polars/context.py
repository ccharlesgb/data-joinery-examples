"""Input and output paths for the lazy Polars TPC-H pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class DataDir(str):
    """Directory containing the eight TPC-H Parquet sources."""


class OutputDir(str):
    """Directory containing the analytical mart tables."""


@dataclass(frozen=True)
class PipelineContext:
    data_dir: DataDir
    output_dir: OutputDir

    @classmethod
    def from_kwargs(
        cls, *, data_dir: str | None = None, output_dir: str | None = None
    ) -> PipelineContext:
        example_dir = Path(__file__).resolve().parents[2]
        return cls(
            data_dir=DataDir(
                data_dir
                if data_dir is not None
                else str(example_dir.parent / "datasets" / "tpch")
            ),
            output_dir=OutputDir(
                output_dir if output_dir is not None else str(example_dir / "__output")
            ),
        )
