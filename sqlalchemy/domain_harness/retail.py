from _types import KEY, BIGINT, BOOL, INT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from cerbos_sqlalchemy import Relation, relation_overrides

Base = declarative_base()


class Store(Base):
    __tablename__ = "stores"
    id = Column(KEY, primary_key=True)
    region = Column(STR, nullable=False)
    country = Column(STR, nullable=False)


class Customer(Base):
    __tablename__ = "customers"
    id = Column(KEY, primary_key=True)
    email = Column(STR, nullable=False)
    tier = Column(STR, nullable=False)
    loyalty_points = Column(INT, nullable=False)


class Order(Base):
    __tablename__ = "order"
    id = Column(KEY, primary_key=True)
    customer_id = Column(KEY, ForeignKey("customers.id"), nullable=False)
    store_id = Column(KEY, ForeignKey("stores.id"), nullable=True)
    status = Column(STR, nullable=False)
    channel = Column(STR, nullable=False)
    total_cents = Column(BIGINT, nullable=False)
    currency = Column(STR, nullable=False)
    coupon_code = Column(STR, nullable=True)
    placed_at = Column(TS, nullable=False)
    gift = Column(BOOL, nullable=False)
    customer = relationship(Customer, viewonly=True)
    store = relationship(Store, viewonly=True)
    lines = relationship("OrderLine", viewonly=True)


class OrderLine(Base):
    __tablename__ = "order_line"
    id = Column(KEY, primary_key=True)
    order_id = Column(KEY, ForeignKey("order.id"), nullable=False)
    line_no = Column(INT, nullable=False)
    sku = Column(STR, nullable=False)
    category = Column(STR, nullable=False)
    quantity = Column(INT, nullable=False)
    unit_price_cents = Column(BIGINT, nullable=False)


ROOT = Order
R = "request.resource.attr."

INFERRED = {
    R + "isGift": Order.gift,
}

O = Order  # noqa: E741
LINES = Relation([OrderLine.order_id == O.id], [O])
DECLARED = {
    R + "status": O.status,
    R + "channel": O.channel,
    R + "totalCents": O.total_cents,
    R + "currency": O.currency,
    R + "couponCode": O.coupon_code,
    R + "placedAt": O.placed_at,
    R + "isGift": O.gift,
    R + "customer.id": O.customer_id,
    R + "customer.tier": scalar(O, Customer.tier, Customer.id == O.customer_id),
    R + "customer.loyaltyPoints": scalar(
        O, Customer.loyalty_points, Customer.id == O.customer_id
    ),
    R + "store.region": scalar(O, Store.region, Store.id == O.store_id),
    R + "store.country": scalar(O, Store.country, Store.id == O.store_id),
    R + "lines": LINES,
    "l": LINES,
    "l.sku": OrderLine.sku,
    "l.category": OrderLine.category,
    "l.quantity": OrderLine.quantity,
    "l.unitPriceCents": OrderLine.unit_price_cents,
}


def mapping(style, dialect):
    if style == "inferred":
        return {"attr_map": INFERRED}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides()}
