"""Build the TPC-H star-schema mart with lazy Polars queries."""

import logging

from .context import PipelineContext
from .pipeline import build_pipeline


def main() -> None:
    logging.basicConfig()
    logging.getLogger("data_joinery").setLevel(logging.DEBUG)

    build_pipeline().run(PipelineContext.from_kwargs())


if __name__ == "__main__":
    main()
