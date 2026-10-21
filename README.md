# Data Joinery Examples

This repository contains runnable pipelines for the data-joinery package. Each example README follows the same path: run it, inspect its inputs and outputs, follow the pipeline, then try a variation.

---

**Repository**: [https://github.com/ccharlesgb/data-joinery-examples](https://github.com/ccharlesgb/data-joinery-examples)

**Documentation**: [https://ccharlesgb.github.io/data-joinery/](https://ccharlesgb.github.io/data-joinery/)

---

## Setup

Install Python 3.14 or later, `uv`, `just`, and a Java runtime. From the repository root, run:

```sh
just install
```

## Examples

The TPC-H pipelines share generated Parquet data in `datasets/tpch/`. The video leaderboard example initializes its own local Delta input; the other examples read their own `data/` directories. Run these commands from the repository root:

| Example | Run | Learn |
| --- | --- | --- |
| [Customer groups](customer_group_example/README.md) | `just run-example customer_group` | Enrich a hierarchy with a self-join. |
| [Housing regression](housing_linear_regression_example/README.md) | `just run-example housing_linear_regression` | Pass a Spark ML model between steps. |
| [Orders and customers](order_product_example/README.md) | `just run-example order_product` | Join two inputs using a run date. |
| [Spark to scikit-learn](sklearn_spark_example/README.md) | `just run-example sklearn_spark` | Cross DataFrame backends and model steps. |
| [Live video leaderboards](video_topk_example/README.md) | `just run-example video_topk` | Maintain per-category TOPK rankings in stateful Delta streaming windows. |
| [TPC-H analytical mart (Spark)](tpch_spark_example/README.md) | `just run-example tpch_spark` | Build a star schema from generated TPC-H data with Spark. |
| [TPC-H analytical mart (Polars)](tpch_polars_example/README.md) | `just run-example tpch_polars` | Build the same mart with deferred Polars queries. |

Each example defines `build_pipeline()` in `src/<package>/pipeline.py` and exposes it through `src/<package>/main.py` for the Data Joinery CLI. `just run-example` accepts a short name or an example directory (such as `tpch_polars_example/`) and forwards extra CLI options. For example, `just run-example order_product --context run_date=2026-01-02` changes the run date, and `just run-example customer_group --skip-side-effects` runs without writing or displaying output. You can also invoke the CLI directly: `uv run --package customer-group data-joinery run customer_group.main`.

Run `just generate-datasets` before running either TPC-H pipeline. It replaces existing Parquet files in `datasets/tpch/` with scale factor 30 files using ZSTD level 1 compression. These large files are gitignored. The TPC-H examples resolve their default input and output paths from the repository location; their READMEs show how to override them.

To regenerate the diagrams embedded in the example READMEs, run:

```sh
uv run python scripts/update_pipeline_visualisations.py
```

## Test the pipelines

Run `just test` from the repository root. The pipeline tests build small schema-valid
fixtures, use temporary paths for local input and output, and compare complete
DataFrames or result objects. They inspect a named step with
`PipelineResult.get_output("step_name", ValueType)` or inspect a writer's bound
input with `PipelineResult.get_input("write_output", "parameter", ValueType)`.
