from _frames import many, one

R = "request.resource.attr."
ID = "id"


def frame(t):
    sites = one(t["sites"], "org_id", t["orgs"], "id", "org", id="id", plan="plan")
    d = one(t["devices"], "site_id", sites, "id", "site", region="region", org="org")
    return many(
        d,
        "id",
        t["device_alerts"],
        "device_id",
        "alerts",
        "id",
        severity="severity",
        acknowledged="acknowledged",
        code="code",
    )


ATTR_MAP = {
    R + "lastSeenAt": "last_seen_at",
    R + "batteryPct": "battery_pct",
}
