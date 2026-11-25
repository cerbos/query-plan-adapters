from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "clinical_notes_v", "note_id"

VIEWS = """
CREATE VIEW clinical_notes_v AS
SELECT n.*,
    (SELECT coalesce(list(t.clinician_id ORDER BY t.clinician_id), []) FROM note_care_team t
        WHERE t.note_id = n.note_id) AS care_team,
    (SELECT coalesce(list({'system': c.system, 'code': c.code} ORDER BY c.id), []) FROM note_codes c
        WHERE c.note_id = n.note_id) AS codes
FROM clinical_notes n
"""


def mapping(nulls):
    patient = ("patients", "patient_id", "id")
    ward = ("wards", "ward_id", "id")
    hospital = ("hospitals", "hospital_id", "id")
    return (
        Attrs(nulls)
        .col("id", "note_id", STR)
        .col("attr.noteType", "type", STR)
        .col("attr.authorId", "author_id", STR)
        .col("attr.sensitivity", "sensitivity", NUM)
        .col("attr.signedAt", "signed_at", TS)
        .col("attr.deleted", "is_deleted", BOOL)
        .values("careTeam", "care_team", STR)
        .objects("codes", "codes", {"system": f("system", STR), "code": f("code", STR)})
        .via("patient.mrn", path(patient), "mrn", STR)
        .via("patient.consentResearch", path(patient), "consent_research", BOOL)
        .via("patient.vip", path(patient), "vip", BOOL)
        .via("patient.birthYear", path(patient), "birth_year", NUM)
        .via("patient.ward.id", path(patient, ward), "id", STR)
        .via("patient.ward.secure", path(patient, ward), "secure", BOOL)
        .via(
            "patient.ward.hospital.region", path(patient, ward, hospital), "region", STR
        )
        .m
    )
