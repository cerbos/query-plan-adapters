from _types import KEY, BOOL, INT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from cerbos_sqlalchemy import Relation, related, relation_overrides

Base = declarative_base()


class Hospital(Base):
    __tablename__ = "hospitals"
    id = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    region = Column(STR, nullable=False)


class Ward(Base):
    __tablename__ = "wards"
    id = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    hospital_id = Column(KEY, ForeignKey("hospitals.id"), nullable=False)
    secure = Column(BOOL, nullable=False)
    hospital = relationship(Hospital, viewonly=True)


class Patient(Base):
    __tablename__ = "patients"
    id = Column(KEY, primary_key=True)
    mrn = Column(STR, nullable=False)
    ward_id = Column(KEY, ForeignKey("wards.id"), nullable=True)
    consent_research = Column(BOOL, nullable=False)
    vip = Column(BOOL, nullable=True)
    birth_year = Column(INT, nullable=False)
    ward = relationship(Ward, viewonly=True)


class ClinicalNote(Base):
    __tablename__ = "clinical_notes"
    note_id = Column(KEY, primary_key=True)
    patient_id = Column(KEY, ForeignKey("patients.id"), nullable=False)
    author_id = Column(STR, nullable=False)
    type = Column(STR, nullable=False)
    sensitivity = Column(INT, nullable=True)
    signed_at = Column(TS, nullable=True)
    is_deleted = Column(BOOL, nullable=False)
    patient = relationship(Patient, viewonly=True)
    care_team = relationship("NoteCareTeam", viewonly=True)
    codes = relationship("NoteCode", viewonly=True)


class NoteCareTeam(Base):
    __tablename__ = "note_care_team"
    note_id = Column(KEY, ForeignKey("clinical_notes.note_id"), primary_key=True)
    clinician_id = Column(KEY, primary_key=True)


class NoteCode(Base):
    __tablename__ = "note_codes"
    id = Column(KEY, primary_key=True)
    note_id = Column(KEY, ForeignKey("clinical_notes.note_id"), nullable=False)
    system = Column(STR, nullable=False)
    code = Column(STR, nullable=False)


ROOT = ClinicalNote
R = "request.resource.attr."

INFERRED = {
    R + "noteType": ClinicalNote.type,
    R + "deleted": ClinicalNote.is_deleted,
    R + "careTeam": related(ClinicalNote.care_team, NoteCareTeam.clinician_id),
}

N = ClinicalNote
_patient = (Patient.id == N.patient_id,)
_ward = (Ward.id == Patient.ward_id, *_patient)
CODES = Relation([NoteCode.note_id == N.note_id], [N])
DECLARED = {
    R + "noteType": N.type,
    R + "authorId": N.author_id,
    R + "sensitivity": N.sensitivity,
    R + "signedAt": N.signed_at,
    R + "deleted": N.is_deleted,
    R + "careTeam": Relation(
        [NoteCareTeam.note_id == N.note_id], [N], member=NoteCareTeam.clinician_id
    ),
    R + "codes": CODES,
    "c": CODES,
    "c.system": NoteCode.system,
    "c.code": NoteCode.code,
    R + "patient.mrn": scalar(N, Patient.mrn, *_patient),
    R + "patient.consentResearch": scalar(N, Patient.consent_research, *_patient),
    R + "patient.vip": scalar(N, Patient.vip, *_patient),
    R + "patient.birthYear": scalar(N, Patient.birth_year, *_patient),
    R + "patient.ward.id": scalar(N, Ward.id, *_ward),
    R + "patient.ward.secure": scalar(N, Ward.secure, *_ward),
    R + "patient.ward.hospital.region": scalar(
        N, Hospital.region, Hospital.id == Ward.hospital_id, *_ward
    ),
}


def mapping(style, dialect):
    if style == "inferred":
        return {"attr_map": INFERRED}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides()}
