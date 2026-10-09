"""Columns exported by the TPC-H analytical mart."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass
class SalesLine:
    order_key: int
    line_number: int
    order_date: date
    ship_date: date
    customer_key: int
    customer_name: str
    customer_segment: str
    customer_nation: str
    customer_region: str
    supplier_key: int
    supplier_name: str
    supplier_nation: str
    supplier_region: str
    part_key: int
    part_name: str
    part_brand: str
    part_type: str
    quantity: Decimal
    extended_price: Decimal
    discount: Decimal
    tax: Decimal
    net_revenue: Decimal
    charge: Decimal
    return_flag: str
    line_status: str


@dataclass
class OrderSummary:
    order_key: int
    order_date: date
    customer_key: int
    customer_name: str
    customer_segment: str
    customer_nation: str
    customer_region: str
    order_status: str
    order_priority: str
    order_total_price: Decimal
    line_count: int
    total_quantity: Decimal
    net_revenue: Decimal
    total_charge: Decimal
