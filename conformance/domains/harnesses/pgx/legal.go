package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var Legal = func() Domain {
	matter := join{"matters", "matter_id", "id"}
	client := join{"clients", "client_id", "id"}
	a := newAttrs(cerbospgx.NullConventionExplicit).
		col("id", "id", str).
		col("attr.title", "title", str).
		col("attr.privilege", "privilege", str).
		col("attr.classification", "classification", num).
		col("attr.legalHold", "legal_hold", boo).
		col("attr.createdBy", "created_by", str).
		col("attr.retentionUntil", "retention_until", ts).
		objects("shares", path(join{"document_shares", "id", "document_id"}), map[string]cerbospgx.Entry{
			"partyType": f("party_type", str), "partyId": f("party_id", str),
		}).
		via("matter.status", path(matter), "status", str).
		via("matter.practiceArea", path(matter), "practice_area", str).
		via("matter.leadPartnerId", path(matter), "lead_partner_id", str).
		list("matter.team", path(matter, join{"matter_team", "id", "matter_id"}), "user_id", str).
		list("matter.screened", path(matter, join{"matter_screens", "id", "matter_id"}), "user_id", str).
		via("matter.client.id", path(matter, client), "id", str).
		via("matter.client.industry", path(matter, client), "industry", str)
	return Domain{Name: "legal", Table: "documents", ID: "id", Nulls: cerbospgx.NullExplicit, Mapper: a.m}
}()
