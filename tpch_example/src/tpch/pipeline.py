"""Graph for the TPC-H analytical mart."""

from data_joinery.pipeline import Pipeline

from .context import PipelineContext
from .transform import (
    build_order_summary,
    build_sales_lines,
    read_customer,
    read_lineitem,
    read_nation,
    read_orders,
    read_part,
    read_partsupp,
    read_region,
    read_supplier,
    write_order_summary,
    write_sales_lines,
)


def build_pipeline() -> Pipeline[PipelineContext]:
    pipeline = Pipeline(PipelineContext)
    sources = {
        "customer": pipeline.add_step(read_customer, name="read_customer"),
        "lineitem": pipeline.add_step(read_lineitem, name="read_lineitem"),
        "nation": pipeline.add_step(read_nation, name="read_nation"),
        "orders": pipeline.add_step(read_orders, name="read_orders"),
        "part": pipeline.add_step(read_part, name="read_part"),
        "partsupp": pipeline.add_step(read_partsupp, name="read_partsupp"),
        "region": pipeline.add_step(read_region, name="read_region"),
        "supplier": pipeline.add_step(read_supplier, name="read_supplier"),
    }
    sales_lines = pipeline.add_step(build_sales_lines, name="build_sales_lines")
    order_summary = pipeline.add_step(build_order_summary, name="build_order_summary")
    sales_writer = pipeline.add_step(write_sales_lines, name="write_sales_lines")
    summary_writer = pipeline.add_step(write_order_summary, name="write_order_summary")

    for name, step in sources.items():
        pipeline.connect(step, sales_lines, param=name)
    pipeline.connect(sales_lines, order_summary, param="sales_lines")
    pipeline.connect(sources["orders"], order_summary, param="orders")
    pipeline.connect(sales_lines, sales_writer)
    pipeline.connect(order_summary, summary_writer)
    return pipeline
