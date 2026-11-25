from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "shipments_v", "id"

# Each checkpoint carries its hub's code, read through the hub as the application does.
VIEWS = """
CREATE VIEW shipments_v AS
SELECT s.*,
    (SELECT coalesce(list(h.code ORDER BY h.code), []) FROM shipment_handling h
        WHERE h.shipment_id = s.id) AS handling,
    (SELECT coalesce(list({'event': c.event, 'scanned_at': c.scanned_at, 'hub': {'code': hub.code}}
                          ORDER BY c.scanned_at), [])
        FROM checkpoints c JOIN hubs hub ON hub.id = c.hub_id WHERE c.shipment_id = s.id) AS checkpoints
FROM shipments s
"""


def mapping(nulls):
    origin = ("hubs", "origin_hub_id", "id")
    destination = ("hubs", "destination_hub_id", "id")
    carrier = ("carriers", "carrier_id", "id")
    a = (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.trackingNumber", "tracking_no", STR)
        .col("attr.status", "status", STR)
        .col("attr.weightKg", "weight_kg", NUM)
        .col("attr.hazmat", "hazmat", BOOL)
        .col("attr.customsHold", "customs_hold", BOOL)
        .col("attr.shipper", "shipper_account", STR)
        .col("attr.eta", "eta", TS)
        .col("attr.declaredValue", "declared_value", NUM)
        .via("carrier.scac", path(carrier), "scac", STR)
        .values("handling", "handling", STR)
        .objects(
            "checkpoints",
            "checkpoints",
            {
                "event": f("event", STR),
                "scannedAt": f("scanned_at", TS),
                "hub.code": f(("hub", "code"), STR),
            },
        )
    )
    for name, hub in (("origin", origin), ("destination", destination)):
        for attr in ("code", "country", "region"):
            a.via(f"{name}.{attr}", path(hub), attr, STR)
    return a.m
