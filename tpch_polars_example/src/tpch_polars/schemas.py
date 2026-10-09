"""Typed TPC-H source records and star-schema mart records."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Annotated

import polars as pl

Money = Annotated[Decimal, pl.Decimal(15, 2)]
SmallInt = Annotated[int, pl.Int32]
Revenue = Annotated[Decimal, pl.Decimal(20, 4)]
TotalCost = Annotated[Decimal, pl.Decimal(22, 6)]


@dataclass
class Customer:
    c_custkey: int
    c_name: str
    c_address: str
    c_nationkey: int
    c_phone: str
    c_acctbal: Money
    c_mktsegment: str
    c_comment: str


@dataclass
class LineItem:
    l_orderkey: int
    l_partkey: int
    l_suppkey: int
    l_linenumber: SmallInt
    l_quantity: Money
    l_extendedprice: Money
    l_discount: Money
    l_tax: Money
    l_returnflag: str
    l_linestatus: str
    l_shipdate: date
    l_commitdate: date
    l_receiptdate: date
    l_shipinstruct: str
    l_shipmode: str
    l_comment: str


@dataclass
class Nation:
    n_nationkey: int
    n_name: str
    n_regionkey: int
    n_comment: str


@dataclass
class Orders:
    o_orderkey: int
    o_custkey: int
    o_orderstatus: str
    o_totalprice: Money
    o_orderdate: date
    o_orderpriority: str
    o_clerk: str
    o_shippriority: SmallInt
    o_comment: str


@dataclass
class Part:
    p_partkey: int
    p_name: str
    p_mfgr: str
    p_brand: str
    p_type: str
    p_size: SmallInt
    p_container: str
    p_retailprice: Money
    p_comment: str


@dataclass
class PartSupp:
    ps_partkey: int
    ps_suppkey: int
    ps_availqty: SmallInt
    ps_supplycost: Money
    ps_comment: str


@dataclass
class Region:
    r_regionkey: int
    r_name: str
    r_comment: str


@dataclass
class Supplier:
    s_suppkey: int
    s_name: str
    s_address: str
    s_nationkey: int
    s_phone: str
    s_acctbal: Money
    s_comment: str


SOURCE_MODELS = {
    "customer": Customer,
    "lineitem": LineItem,
    "nation": Nation,
    "orders": Orders,
    "part": Part,
    "partsupp": PartSupp,
    "region": Region,
    "supplier": Supplier,
}


@dataclass
class DimCustomer:
    customer_key: int
    customer_name: str
    address: str
    phone: str
    account_balance: Money
    market_segment: str
    nation: str
    region: str


@dataclass
class DimSupplier:
    supplier_key: int
    supplier_name: str
    address: str
    phone: str
    account_balance: Money
    nation: str
    region: str


@dataclass
class DimPart:
    part_key: int
    supplier_key: int
    part_name: str
    manufacturer: str
    brand: str
    part_type: str
    size: SmallInt
    container: str
    retail_price: Money
    available_quantity: SmallInt
    supply_cost: Money


@dataclass
class FactSales:
    order_key: int
    line_number: SmallInt
    customer_key: int
    supplier_key: int
    part_key: int
    order_date: date
    ship_date: date
    commit_date: date
    receipt_date: date
    order_status: str
    order_priority: str
    order_total_price: Money
    quantity: Money
    extended_price: Money
    discount: Money
    tax: Money
    revenue: Revenue
    total_cost: TotalCost
    is_late_shipment: bool
    return_flag: str
    line_status: str


@dataclass
class DimDate:
    date_key: date
    calendar_year: SmallInt
    quarter: SmallInt
    month_number: SmallInt
    month_name: str
    is_weekend: bool
