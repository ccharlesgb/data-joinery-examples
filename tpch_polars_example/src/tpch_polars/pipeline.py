"""Graph for the TPC-H star-schema mart."""

from data_joinery.pipeline import Pipeline

from .context import PipelineContext
from .transforms import models, readers, writers


def build_pipeline() -> Pipeline[PipelineContext]:
    pipeline = Pipeline(PipelineContext)
    sources = {
        name: pipeline.add_step(getattr(readers, f"read_{name}"), name=f"read_{name}")
        for name in (
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        )
    }
    dim_customers = pipeline.add_step(
        models.build_dim_customers, name="build_dim_customers"
    )
    dim_suppliers = pipeline.add_step(
        models.build_dim_suppliers, name="build_dim_suppliers"
    )
    dim_part = pipeline.add_step(models.build_dim_part, name="build_dim_part")
    fact_sales = pipeline.add_step(models.build_fact_sales, name="build_fact_sales")
    fact_writer = pipeline.add_step(writers.write_fact_sales, name="write_fact_sales")
    dim_date = pipeline.add_step(models.build_dim_date, name="build_dim_date")
    customer_writer = pipeline.add_step(
        writers.write_dim_customers, name="write_dim_customers"
    )
    supplier_writer = pipeline.add_step(
        writers.write_dim_suppliers, name="write_dim_suppliers"
    )
    part_writer = pipeline.add_step(writers.write_dim_part, name="write_dim_part")
    date_writer = pipeline.add_step(writers.write_dim_date, name="write_dim_date")
    collect_sinks = pipeline.add_step(
        writers.collect_mart_sinks, name="collect_mart_sinks"
    )

    for name in ("customer", "nation", "region"):
        pipeline.connect(sources[name], dim_customers, param=name)
    for name in ("supplier", "nation", "region"):
        pipeline.connect(sources[name], dim_suppliers, param=name)
    for name in ("part", "partsupp"):
        pipeline.connect(sources[name], dim_part, param=name)
    for name in ("lineitem", "orders"):
        pipeline.connect(sources[name], fact_sales, param=name)
    pipeline.connect(dim_customers, customer_writer)
    pipeline.connect(dim_suppliers, supplier_writer)
    pipeline.connect(dim_part, part_writer)
    pipeline.connect(fact_sales, fact_writer)
    pipeline.connect(fact_sales, dim_date, param="fact_sales")
    pipeline.connect(dim_date, date_writer)
    pipeline.connect(fact_writer, collect_sinks, param="fact_sales")
    pipeline.connect(customer_writer, collect_sinks, param="dim_customers")
    pipeline.connect(supplier_writer, collect_sinks, param="dim_suppliers")
    pipeline.connect(part_writer, collect_sinks, param="dim_part")
    pipeline.connect(date_writer, collect_sinks, param="dim_date")
    return pipeline
