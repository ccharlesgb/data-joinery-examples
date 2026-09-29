from dataclasses import dataclass

from data_joinery import SparkContext


class CustomersPath(str):
    """
    Marker class for the customers path context.
    """


class OutputPath(str):
    """
    Marker class for the output path context.
    """


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    customers_path: CustomersPath
    output_path: OutputPath
