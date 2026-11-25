from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "devices_v", "id"

# labels is a JSON object read by key, which no Entry can express: it stays unmapped.
VIEWS = """
CREATE VIEW devices_v AS
SELECT d.*,
    (SELECT coalesce(list({'severity': a.severity, 'acknowledged': a.acknowledged, 'code': a.code}
                          ORDER BY a.id), [])
        FROM device_alerts a WHERE a.device_id = d.id) AS alerts
FROM devices d
"""


def mapping(nulls):
    site = ("sites", "site_id", "id")
    org = ("orgs", "org_id", "id")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.serial", "serial", STR)
        .col("attr.model", "model", STR)
        .col("attr.firmware", "firmware", STR)
        .col("attr.lastSeenAt", "last_seen_at", TS)
        .col("attr.online", "online", BOOL)
        .col("attr.batteryPct", "battery_pct", NUM)
        .col("attr.decommissioned", "decommissioned", BOOL)
        .via("site.region", path(site), "region", STR)
        .via("site.org.id", path(site, org), "id", STR)
        .via("site.org.plan", path(site, org), "plan", STR)
        .objects(
            "alerts",
            "alerts",
            {
                "severity": f("severity", NUM),
                "acknowledged": f("acknowledged", BOOL),
                "code": f("code", STR),
            },
        )
        .m
    )
