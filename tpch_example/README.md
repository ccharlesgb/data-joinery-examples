# TPC-H analytical mart

This Spark Data Joinery pipeline reads the eight TPC-H Parquet tables from `data/` and writes two analytical tables to `__output/`.

| Output | Grain | Contents |
| --- | --- | --- |
| `sales_lines` | One row per order line | Order dates, customer and supplier geography, part details, quantity, net revenue, and charge. |
| `order_summary` | One row per order | Customer geography, order attributes, line count, quantity, net revenue, and charge. |

The measures use `net_revenue = extended_price × (1 - discount)` and `charge = net_revenue × (1 + tax)`. The joins use TPC-H keys, including both part and supplier keys for `partsupp`.

Generate all eight tables in `data/` first. From the repository root, run:

```sh
just run-example tpch
```

The pipeline overwrites its two output directories on each run. At scale factor 10, the source data is several gigabytes and the joins can require substantial memory and disk space.
