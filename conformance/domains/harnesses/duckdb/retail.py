from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "order_v", "id"

VIEWS = """
CREATE VIEW order_v AS
SELECT o.*,
    (SELECT coalesce(list({'sku': l.sku, 'category': l.category, 'quantity': l.quantity,
                           'unit_price_cents': l.unit_price_cents} ORDER BY l.line_no), [])
        FROM order_line l WHERE l.order_id = o.id) AS lines
FROM "order" o
"""


def mapping(nulls):
    customer = ("customers", "customer_id", "id")
    store = ("stores", "store_id", "id")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.status", "status", STR)
        .col("attr.channel", "channel", STR)
        .col("attr.totalCents", "total_cents", NUM)
        .col("attr.currency", "currency", STR)
        .col("attr.couponCode", "coupon_code", STR)
        .col("attr.placedAt", "placed_at", TS)
        .col("attr.isGift", "gift", BOOL)
        .via("customer.id", path(customer), "id", STR)
        .via("customer.tier", path(customer), "tier", STR)
        .via("customer.loyaltyPoints", path(customer), "loyalty_points", NUM)
        .via("store.region", path(store), "region", STR)
        .via("store.country", path(store), "country", STR)
        .objects(
            "lines",
            "lines",
            {
                "sku": f("sku", STR),
                "category": f("category", STR),
                "quantity": f("quantity", NUM),
                "unitPriceCents": f("unit_price_cents", NUM),
            },
        )
        .m
    )
