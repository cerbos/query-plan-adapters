from _types import KEY, BOOL, DECIMAL, INT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from cerbos_sqlalchemy import Relation, related, relation_overrides

Base = declarative_base()


class Agency(Base):
    __tablename__ = "agencies"
    code = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    state = Column(STR, nullable=False)


class Applicant(Base):
    __tablename__ = "applicants"
    id = Column(KEY, primary_key=True)
    county_fips = Column(STR, nullable=False)
    household_size = Column(INT, nullable=False)
    citizenship = Column(STR, nullable=True)


class BenefitCase(Base):
    __tablename__ = "cases"
    id = Column(KEY, primary_key=True)
    case_number = Column(STR, nullable=False)
    agency_code = Column(KEY, ForeignKey("agencies.code"), nullable=False)
    applicant_id = Column(KEY, ForeignKey("applicants.id"), nullable=False)
    status = Column(STR, nullable=False)
    clearance_level = Column(INT, nullable=False)
    program = Column(STR, nullable=False)
    opened_on = Column(TS, nullable=False)
    closed_on = Column(TS, nullable=True)
    pii = Column(BOOL, nullable=False)
    assigned_to = Column(STR, nullable=True)
    monthly_amount = Column(DECIMAL, nullable=True)
    agency = relationship(Agency, viewonly=True)
    applicant = relationship(Applicant, viewonly=True)
    flags = relationship("CaseFlag", viewonly=True)


class CaseFlag(Base):
    __tablename__ = "case_flags"
    case_id = Column(KEY, ForeignKey("cases.id"), primary_key=True)
    flag = Column(KEY, primary_key=True)


ROOT = BenefitCase
R = "request.resource.attr."
C = BenefitCase

INFERRED = {
    R + "flags": related(C.flags, CaseFlag.flag),
}

NULLABLE = {
    R + name: "explicit"
    for name in ("closedOn", "assignedTo", "monthlyAmount", "applicant.citizenship")
}
_applicant = (Applicant.id == C.applicant_id,)
DECLARED = {
    R + "caseNumber": C.case_number,
    R + "status": C.status,
    R + "clearanceLevel": C.clearance_level,
    R + "program": C.program,
    R + "openedOn": C.opened_on,
    R + "closedOn": C.closed_on,
    R + "pii": C.pii,
    R + "assignedTo": C.assigned_to,
    R + "monthlyAmount": C.monthly_amount,
    R + "flags": Relation([CaseFlag.case_id == C.id], [C], member=CaseFlag.flag),
    R + "agency.code": C.agency_code,
    R + "agency.state": scalar(C, Agency.state, Agency.code == C.agency_code),
    R + "applicant.countyFips": scalar(C, Applicant.county_fips, *_applicant),
    R + "applicant.householdSize": scalar(C, Applicant.household_size, *_applicant),
    R + "applicant.citizenship": scalar(C, Applicant.citizenship, *_applicant),
}


def mapping(style, dialect):
    common = {
        "null_attribute_representation": "explicit",
        "attribute_null_representation": NULLABLE,
    }
    if style == "inferred":
        return {"attr_map": INFERRED, **common}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides(), **common}
