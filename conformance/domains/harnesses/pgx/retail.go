package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var Retail = func() Domain {
	customer := join{"customers", "customer_id", "id"}
	store := join{"stores", "store_id", "id"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "id", str).
		col("attr.status", "status", str).
		col("attr.channel", "channel", str).
		col("attr.totalCents", "total_cents", num).
		col("attr.currency", "currency", str).
		col("attr.couponCode", "coupon_code", str).
		col("attr.placedAt", "placed_at", ts).
		col("attr.isGift", "gift", boo).
		via("customer.id", path(customer), "id", str).
		via("customer.tier", path(customer), "tier", str).
		via("customer.loyaltyPoints", path(customer), "loyalty_points", num).
		via("store.region", path(store), "region", str).
		via("store.country", path(store), "country", str).
		objects("lines", path(join{"order_line", "id", "order_id"}), map[string]cerbospgx.Entry{
			"sku": f("sku", str), "category": f("category", str),
			"quantity": f("quantity", num), "unitPriceCents": f("unit_price_cents", num),
		})
	return Domain{Name: "retail", Table: "order", ID: "id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()
