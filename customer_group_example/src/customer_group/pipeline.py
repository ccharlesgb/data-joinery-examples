from data_joinery.pipeline import Pipeline

from customer_group.context import PipelineContext

from .transform import (
    denormalise_group_id,
    read_customers,
    show_output,
    write_output,
)


def build_pipeline() -> Pipeline[PipelineContext]:
    pipeline = Pipeline(PipelineContext)

    read_customers_step = pipeline.add_step(read_customers, "read_customers")
    denormalise_group_id_step = pipeline.add_step(
        denormalise_group_id, "denormalise_group_id"
    )
    write_output_step = pipeline.add_step(write_output, "write_output")
    show_output_step = pipeline.add_step(show_output, "show_output")

    pipeline.connect(read_customers_step, denormalise_group_id_step)
    pipeline.connect(denormalise_group_id_step, write_output_step)
    pipeline.connect(denormalise_group_id_step, show_output_step)

    return pipeline
