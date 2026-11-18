from _types import KEY, BOOL, INT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from cerbos_sqlalchemy import Relation, related, relation_overrides

Base = declarative_base()


class Client(Base):
    __tablename__ = "clients"
    id = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    industry = Column(STR, nullable=True)


class Matter(Base):
    __tablename__ = "matters"
    id = Column(KEY, primary_key=True)
    client_id = Column(KEY, ForeignKey("clients.id"), nullable=False)
    practice_area = Column(STR, nullable=False)
    status = Column(STR, nullable=False)
    opened_at = Column(TS, nullable=False)
    lead_partner_id = Column(STR, nullable=True)
    client = relationship(Client, viewonly=True)
    team = relationship("MatterTeam", viewonly=True)
    screened = relationship("MatterScreen", viewonly=True)


class MatterTeam(Base):
    __tablename__ = "matter_team"
    matter_id = Column(KEY, ForeignKey("matters.id"), primary_key=True)
    user_id = Column(KEY, primary_key=True)


class MatterScreen(Base):
    __tablename__ = "matter_screens"
    matter_id = Column(KEY, ForeignKey("matters.id"), primary_key=True)
    user_id = Column(KEY, primary_key=True)


class Document(Base):
    __tablename__ = "documents"
    id = Column(KEY, primary_key=True)
    matter_id = Column(KEY, ForeignKey("matters.id"), nullable=False)
    title = Column(STR, nullable=False)
    privilege = Column(STR, nullable=True)
    classification = Column(INT, nullable=False)
    legal_hold = Column(BOOL, nullable=False)
    created_by = Column(STR, nullable=False)
    retention_until = Column(TS, nullable=True)
    matter = relationship(Matter, viewonly=True)
    shares = relationship("DocumentShare", viewonly=True)


class DocumentShare(Base):
    __tablename__ = "document_shares"
    id = Column(KEY, primary_key=True)
    document_id = Column(KEY, ForeignKey("documents.id"), nullable=False)
    party_type = Column(STR, nullable=False)
    party_id = Column(STR, nullable=False)


ROOT = Document
R = "request.resource.attr."

INFERRED = {
    R + "matter.team": related(Document.matter, Matter.team, MatterTeam.user_id),
    R + "matter.screened": related(Document.matter, Matter.screened, MatterScreen.user_id),
}

D = Document
_matter = (Matter.id == D.matter_id,)
SHARES = Relation([DocumentShare.document_id == D.id], [D])
DECLARED = {
    R + "title": D.title,
    R + "privilege": D.privilege,
    R + "classification": D.classification,
    R + "legalHold": D.legal_hold,
    R + "createdBy": D.created_by,
    R + "retentionUntil": D.retention_until,
    R + "shares": SHARES,
    "s": SHARES,
    "s.partyType": DocumentShare.party_type,
    "s.partyId": DocumentShare.party_id,
    R + "matter.status": scalar(D, Matter.status, *_matter),
    R + "matter.practiceArea": scalar(D, Matter.practice_area, *_matter),
    R + "matter.leadPartnerId": scalar(D, Matter.lead_partner_id, *_matter),
    R + "matter.team": Relation(
        [MatterTeam.matter_id == Matter.id, *_matter],
        [D],
        member=MatterTeam.user_id,
        hop_correlation=list(_matter),
    ),
    R + "matter.screened": Relation(
        [MatterScreen.matter_id == Matter.id, *_matter],
        [D],
        member=MatterScreen.user_id,
        hop_correlation=list(_matter),
    ),
    R + "matter.client.id": scalar(D, Matter.client_id, *_matter),
    R + "matter.client.industry": scalar(
        D, Client.industry, Client.id == Matter.client_id, *_matter
    ),
}


NULLABLE = {
    R + name: "explicit"
    for name in (
        "privilege",
        "retentionUntil",
        "matter.leadPartnerId",
        "matter.client.industry",
    )
}


def mapping(style, dialect):
    common = {
        "null_attribute_representation": "explicit",
        "attribute_null_representation": NULLABLE,
    }
    if style == "inferred":
        return {"attr_map": INFERRED, **common}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides(), **common}
