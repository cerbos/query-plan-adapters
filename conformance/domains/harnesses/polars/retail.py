from _frames import many, one

R = "request.resource.attr."
ID = "id"


def frame(t):
    o = one(
        t["order"],
        "customer_id",
        t["customers"],
        "id",
        "customer",
        id="id",
        tier="tier",
        loyaltyPoints="loyalty_points",
    )
    o = one(
        o, "store_id", t["stores"], "id", "store", region="region", country="country"
    )
    return many(
        o,
        "id",
        t["order_line"],
        "order_id",
        "lines",
        "line_no",
        sku="sku",
        category="category",
        quantity="quantity",
        unitPriceCents="unit_price_cents",
    )


ATTR_MAP = {
    R + "totalCents": "total_cents",
    R + "couponCode": "coupon_code",
    R + "placedAt": "placed_at",
    R + "isGift": "gift",
}
