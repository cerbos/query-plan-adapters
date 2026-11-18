package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var Logistics = func() Domain {
	origin := join{"hubs", "origin_hub_id", "id"}
	destination := join{"hubs", "destination_hub_id", "id"}
	carrier := join{"carriers", "carrier_id", "id"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "id", str).
		col("attr.trackingNumber", "tracking_no", str).
		col("attr.status", "status", str).
		col("attr.weightKg", "weight_kg", num).
		col("attr.hazmat", "hazmat", boo).
		col("attr.customsHold", "customs_hold", boo).
		col("attr.shipper", "shipper_account", str).
		col("attr.eta", "eta", ts).
		col("attr.declaredValue", "declared_value", num).
		via("origin.code", path(origin), "code", str).
		via("origin.country", path(origin), "country", str).
		via("origin.region", path(origin), "region", str).
		via("destination.code", path(destination), "code", str).
		via("destination.country", path(destination), "country", str).
		via("destination.region", path(destination), "region", str).
		via("carrier.scac", path(carrier), "scac", str).
		list("handling", path(join{"shipment_handling", "id", "shipment_id"}), "code", str).
		objects("checkpoints", path(join{"checkpoints", "id", "shipment_id"}), map[string]cerbospgx.Entry{
			"event":     f("event", str),
			"scannedAt": f("scanned_at", ts),
			"hub.code":  {Column: "code", ValueType: str.ValueType, ScalarRelation: path(join{"hubs", "hub_id", "id"})},
		})
	return Domain{Name: "logistics", Table: "shipments", ID: "id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()
