from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "documents_v", "id"

VIEWS = """
CREATE VIEW documents_v AS
SELECT d.*,
    (SELECT coalesce(list({'party_type': s.party_type, 'party_id': s.party_id} ORDER BY s.id), [])
        FROM document_shares s WHERE s.document_id = d.id) AS shares
FROM documents d;

CREATE VIEW matters_v AS
SELECT m.*,
    (SELECT coalesce(list(t.user_id ORDER BY t.user_id), []) FROM matter_team t
        WHERE t.matter_id = m.id) AS team,
    (SELECT coalesce(list(s.user_id ORDER BY s.user_id), []) FROM matter_screens s
        WHERE s.matter_id = m.id) AS screened
FROM matters m
"""


def mapping(nulls):
    matter = ("matters_v", "matter_id", "id")
    client = ("clients", "client_id", "id")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.title", "title", STR)
        .col("attr.privilege", "privilege", STR)
        .col("attr.classification", "classification", NUM)
        .col("attr.legalHold", "legal_hold", BOOL)
        .col("attr.createdBy", "created_by", STR)
        .col("attr.retentionUntil", "retention_until", TS)
        .objects(
            "shares",
            "shares",
            {"partyType": f("party_type", STR), "partyId": f("party_id", STR)},
        )
        .via("matter.status", path(matter), "status", STR)
        .via("matter.practiceArea", path(matter), "practice_area", STR)
        .via("matter.leadPartnerId", path(matter), "lead_partner_id", STR)
        .values("matter.team", "team", STR, join=path(matter))
        .values("matter.screened", "screened", STR, join=path(matter))
        .via("matter.client.id", path(matter, client), "id", STR)
        .via("matter.client.industry", path(matter, client), "industry", STR)
        .m
    )
