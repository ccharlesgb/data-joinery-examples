# TPC-H analytical mart (Spark)

This Spark Data Joinery pipeline reads the eight existing TPC-H Parquet tables in `data/`, projects and casts them to the declared schemas, validates non-null values and one-character flags, and writes a star schema to `__output/`. Cleanup happens in the reader transforms, with no separate staging output.

![TPC-H star-schema pipeline](pipeline.png)

| Output | Grain | Key details |
| --- | --- | --- |
| `fact_sales` | One row per order line | `(order_key, line_number)`; customer, supplier and part keys; order, ship, commit and receipt dates; revenue, total cost and late flag. |
| `dim_customers` | One row per customer | Customer attributes with nation and region. |
| `dim_suppliers` | One row per supplier | Supplier attributes with nation and region. |
| `dim_part` | One row per part and supplier | Composite `(part_key, supplier_key)` key retains `partsupp` attributes without multiplying sales rows. |
| `dim_date` | One row per calendar day | Covers the full range of dates in `fact_sales`; year, quarter, month name and weekend flag. |

`revenue = extended_price × (1 - discount)` and `total_cost = revenue × (1 + tax)`. `is_late_shipment` is true when receipt date is after commit date. Money inputs use `DECIMAL(15,2)`, and calculated measures use `DECIMAL(20,4)` and `DECIMAL(22,6)`. TPC-H flags are stored as Spark strings and checked to contain exactly one character. `order_total_price` is an order attribute repeated on each line; do not sum it across fact rows.

From the example directory, run with the Data Joinery CLI:

```sh
cd tpch_spark_example
uv run --package tpch-spark data-joinery run
```

Override the default paths with `--context data_dir=/path/to/tpch/data` and `--context output_dir=/path/to/mart`. Add `--skip-side-effects` to compute the graph without writing mart tables. Draw the graph with `uv run --package tpch-spark data-joinery visualize --output pipeline.svg`. The existing `just run-example tpch_spark` command also runs the pipeline with its default paths.

The pipeline overwrites the five mart output directories on each run. At scale factor 10, the data and joins require substantial disk and memory; the example uses ten local Spark tasks and a 4 GiB driver heap. The fact table is cached on Spark executor disk so writing it and building `dim_date` share the same order-line join.
