# Data Joinery Examples

This repo contains a collection of example pipelines for the data-joinery package.

---

**Repository**: [https://github.com/ccharlesgb/data-joinery-examples](https://github.com/ccharlesgb/data-joinery-examples)

**Documentation**: [https://ccharlesgb.github.io/data-joinery/](https://ccharlesgb.github.io/data-joinery/)

---

# Setup

Install `uv`, `just`, and a Java runtime, then run:

```sh
just install
```

# Examples

Each pipeline reads the small parquet fixture in its `data/` directory. Run these commands from the repository root:

| Example | Run | Learn |
| --- | --- | --- |
| [Customer groups](customer_group_example/README.md) | `just run-example customer_group` | Enrich a hierarchy with a self-join. |
| [Housing regression](housing_linear_regression_example/README.md) | `just run-example housing_linear_regression` | Pass a Spark ML model between steps. |
| [Orders and customers](order_product_example/README.md) | `just run-example order_product` | Join two inputs using a run date. |
| [Spark to scikit-learn](sklearn_spark_example/README.md) | `just run-example sklearn_spark` | Cross DataFrame backends and model steps. |
| [Snapshot transitions](snapshot_diff_example/README.md) | `just run-example snapshot_diff` | Compare two daily snapshots. |

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
