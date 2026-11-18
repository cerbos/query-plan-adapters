package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var Healthcare = func() Domain {
	patient := join{"patients", "patient_id", "id"}
	ward := join{"wards", "ward_id", "id"}
	hospital := join{"hospitals", "hospital_id", "id"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "note_id", str).
		col("attr.noteType", "type", str).
		col("attr.authorId", "author_id", str).
		col("attr.sensitivity", "sensitivity", num).
		col("attr.signedAt", "signed_at", ts).
		col("attr.deleted", "is_deleted", boo).
		list("careTeam", path(join{"note_care_team", "note_id", "note_id"}), "clinician_id", str).
		objects("codes", path(join{"note_codes", "note_id", "note_id"}), map[string]cerbospgx.Entry{
			"system": f("system", str), "code": f("code", str),
		}).
		via("patient.mrn", path(patient), "mrn", str).
		via("patient.consentResearch", path(patient), "consent_research", boo).
		via("patient.vip", path(patient), "vip", boo).
		via("patient.birthYear", path(patient), "birth_year", num).
		via("patient.ward.id", path(patient, ward), "id", str).
		via("patient.ward.secure", path(patient, ward), "secure", boo).
		via("patient.ward.hospital.region", path(patient, ward, hospital), "region", str)
	return Domain{Name: "healthcare", Table: "clinical_notes", ID: "note_id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()
