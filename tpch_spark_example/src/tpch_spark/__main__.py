"""Build the TPC-H star-schema mart from local Parquet sources."""

from .context import PipelineContext
from .pipeline import build_pipeline


def main() -> None:
    context = PipelineContext.from_kwargs()
    try:
        build_pipeline().run(context)
    finally:
        context.spark.stop()


if __name__ == "__main__":
    main()
