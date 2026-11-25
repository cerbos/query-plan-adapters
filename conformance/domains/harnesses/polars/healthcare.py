from _frames import many, one

R = "request.resource.attr."
ID = "note_id"


def frame(t):
    wards = one(
        t["wards"], "hospital_id", t["hospitals"], "id", "hospital", region="region"
    )
    patients = one(
        t["patients"],
        "ward_id",
        wards,
        "id",
        "ward",
        id="id",
        secure="secure",
        hospital="hospital",
    )
    notes = one(
        t["clinical_notes"],
        "patient_id",
        patients,
        "id",
        "patient",
        mrn="mrn",
        consentResearch="consent_research",
        vip="vip",
        birthYear="birth_year",
        ward="ward",
    )
    notes = many(
        notes,
        "note_id",
        t["note_care_team"],
        "note_id",
        "careTeam",
        "clinician_id",
        "clinician_id",
    )
    return many(
        notes,
        "note_id",
        t["note_codes"],
        "note_id",
        "codes",
        "id",
        system="system",
        code="code",
    )


ATTR_MAP = {
    "request.resource.id": "note_id",
    R + "noteType": "type",
    R + "authorId": "author_id",
    R + "signedAt": "signed_at",
    R + "deleted": "is_deleted",
}
