from datetime import datetime

from data_joinery.schemas.joinery_model import Column, JoineryModel


class Order(JoineryModel):
    order_id: Column[int]
    customer_id: Column[str]
    order_timestamp: Column[datetime]
    product_id: Column[int]
    quantity: Column[int]


class Customer(JoineryModel):
    customer_id: Column[str]
    name: Column[str]


class OrderWithCustomerDimension(Order, Customer):
    pass
