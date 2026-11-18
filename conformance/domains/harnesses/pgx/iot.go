package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

// labels is a JSONB object, which no Entry can read by key: it stays unmapped.
var IoT = func() Domain {
	site := join{"sites", "site_id", "id"}
	org := join{"orgs", "org_id", "id"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "id", str).
		col("attr.serial", "serial", str).
		col("attr.model", "model", str).
		col("attr.firmware", "firmware", str).
		col("attr.lastSeenAt", "last_seen_at", ts).
		col("attr.online", "online", boo).
		col("attr.batteryPct", "battery_pct", num).
		col("attr.decommissioned", "decommissioned", boo).
		via("site.region", path(site), "region", str).
		via("site.org.id", path(site, org), "id", str).
		via("site.org.plan", path(site, org), "plan", str).
		objects("alerts", path(join{"device_alerts", "id", "device_id"}), map[string]cerbospgx.Entry{
			"severity": f("severity", num), "acknowledged": f("acknowledged", boo), "code": f("code", str),
		})
	return Domain{Name: "iot", Table: "devices", ID: "id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()
