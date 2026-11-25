from _frames import many, one

R = "request.resource.attr."
ID = "id"
NULLS = "explicit"


def frame(t):
    matters = one(
        t["matters"],
        "client_id",
        t["clients"],
        "id",
        "client",
        id="id",
        industry="industry",
    )
    matters = many(
        matters, "id", t["matter_team"], "matter_id", "team", "user_id", "user_id"
    )
    matters = many(
        matters,
        "id",
        t["matter_screens"],
        "matter_id",
        "screened",
        "user_id",
        "user_id",
    )
    docs = one(
        t["documents"],
        "matter_id",
        matters,
        "id",
        "matter",
        status="status",
        practiceArea="practice_area",
        leadPartnerId="lead_partner_id",
        team="team",
        screened="screened",
        client="client",
    )
    return many(
        docs,
        "id",
        t["document_shares"],
        "document_id",
        "shares",
        "id",
        partyType="party_type",
        partyId="party_id",
    )


ATTR_MAP = {
    R + "legalHold": "legal_hold",
    R + "createdBy": "created_by",
    R + "retentionUntil": "retention_until",
}
