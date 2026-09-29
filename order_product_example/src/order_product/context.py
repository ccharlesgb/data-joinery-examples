from dataclasses import dataclass
from datetime import date

from data_joinery import SparkContext


class OrdersPath(str):
    """
    Marker class for the orders path context.
    """


class CustomersPath(str):
    """
    Marker class for the customers path context.
    """


class OutputPath(str):
    """
    Marker class for the output path context.
    """


class RunDate(date):
    """
    Marker class for the run date context.
    """


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    orders_path: OrdersPath
    customers_path: CustomersPath
    output_path: OutputPath
    run_date: RunDate
