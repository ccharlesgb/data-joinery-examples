# TPC-H analytical mart (Polars)

This pipeline builds the same five-table star schema as the [Spark example](../tpch_spark_example/README.md) from the eight TPC-H Parquet sources in `../tpch_spark_example/data/`. The sources and intermediate tables stay as Polars `LazyFrame` queries. The writers use `sink_parquet` to materialize the five mart tables in this example's `__output/` directory. The date dimension derives from the lazy fact table.

![TPC-H Polars pipeline](pipeline.png)

| Output | Grain |
| --- | --- |
| `fact_sales.parquet` | One row per order line. |
| `dim_customers.parquet` | One row per customer. |
| `dim_suppliers.parquet` | One row per supplier. |
| `dim_part.parquet` | One row per part and supplier. |
| `dim_date.parquet` | One row per day across the fact date range. |

The money and measure columns use the same decimal precision and scale as the Spark mart. Reader steps project and cast source columns, then Data Joinery validates their `LazyFrame` schema contracts without collecting rows. The same contracts validate each transform input and output. Source value checks run as streaming queries and collect only the first invalid row, if one exists; writers stream the completed plans to Parquet.

This example requires a Data Joinery build with Polars `LazyFrame` contract support. When using a local checkout, install that build into the environment before running the pipeline.

From the example directory, run with the Data Joinery CLI:

```sh
cd tpch_polars_example
uv run --package tpch-polars data-joinery run
```

To use other input and output directories, pass context values. To validate and build the intermediate lazy plans without writing mart files, add `--skip-side-effects`:

```sh
uv run --package tpch-polars data-joinery run \
  --context data_dir=/path/to/tpch/data \
  --context output_dir=/path/to/mart \
  --skip-side-effects
```

Draw the graph with `uv run --package tpch-polars data-joinery visualize --output pipeline.svg`. The existing `just run-example tpch_polars` command also runs the pipeline with its default paths.

Generate the source Parquet files in `tpch_spark_example/data/` first. Each run replaces the five Polars output files. Large TPC-H scale factors still require substantial disk and memory.
