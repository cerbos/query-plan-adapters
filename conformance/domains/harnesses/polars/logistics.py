from _frames import many, one

R = "request.resource.attr."
ID = "id"


def frame(t):
    hubs = t["hubs"]
    s = one(
        t["shipments"],
        "origin_hub_id",
        hubs,
        "id",
        "origin",
        code="code",
        country="country",
        region="region",
    )
    s = one(
        s,
        "destination_hub_id",
        hubs,
        "id",
        "destination",
        code="code",
        country="country",
        region="region",
    )
    s = one(s, "carrier_id", t["carriers"], "id", "carrier", scac="scac")
    s = many(s, "id", t["shipment_handling"], "shipment_id", "handling", "code", "code")
    checkpoints = one(t["checkpoints"], "hub_id", hubs, "id", "hub", code="code")
    return many(
        s,
        "id",
        checkpoints,
        "shipment_id",
        "checkpoints",
        "scanned_at",
        event="event",
        scannedAt="scanned_at",
        hub="hub",
    )


ATTR_MAP = {
    R + "trackingNumber": "tracking_no",
    R + "weightKg": "weight_kg",
    R + "customsHold": "customs_hold",
    R + "shipper": "shipper_account",
    R + "declaredValue": "declared_value",
}
