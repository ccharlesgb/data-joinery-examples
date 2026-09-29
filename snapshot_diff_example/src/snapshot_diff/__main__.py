from pyspark.sql import SparkSession

from .context import OutputPath, PipelineContext, RunDate, SnapshottedDimensionPath
from .pipeline import DATA_DIR, OUTPUT_DIR, build_pipeline

spark = (
    SparkSession.builder.appName("snapshot-diff-pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = build_pipeline()

context = PipelineContext(
    spark=spark,
    snapshotted_dimension_path=SnapshottedDimensionPath(
        str(DATA_DIR / "snapshotted_dimension.parquet")
    ),
    output_path=OutputPath(str(OUTPUT_DIR / "output.parquet")),
    run_date=RunDate(2026, 1, 1),
)
outputs = pipeline.run(context)
outputs["compute_dimension_transitions"].show()

spark.stop()
