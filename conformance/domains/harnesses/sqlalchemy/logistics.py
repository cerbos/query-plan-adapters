from _types import KEY, BOOL, DECIMAL, FLOAT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import aliased, declarative_base, relationship

from cerbos_sqlalchemy import Relation, related, relation_overrides

Base = declarative_base()


class Hub(Base):
    __tablename__ = "hubs"
    id = Column(KEY, primary_key=True)
    code = Column(STR, nullable=False)
    country = Column(STR, nullable=False)
    region = Column(STR, nullable=False)


class Carrier(Base):
    __tablename__ = "carriers"
    id = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    scac = Column(STR, nullable=False)


class Shipment(Base):
    __tablename__ = "shipments"
    id = Column(KEY, primary_key=True)
    tracking_no = Column(STR, nullable=False)
    status = Column(STR, nullable=False)
    origin_hub_id = Column(KEY, ForeignKey("hubs.id"), nullable=False)
    destination_hub_id = Column(KEY, ForeignKey("hubs.id"), nullable=False)
    carrier_id = Column(KEY, ForeignKey("carriers.id"), nullable=True)
    weight_kg = Column(FLOAT, nullable=False)
    hazmat = Column(BOOL, nullable=False)
    customs_hold = Column(BOOL, nullable=True)
    shipper_account = Column(STR, nullable=False)
    eta = Column(TS, nullable=True)
    declared_value = Column(DECIMAL, nullable=False)
    origin = relationship(Hub, foreign_keys=[origin_hub_id], viewonly=True)
    destination = relationship(Hub, foreign_keys=[destination_hub_id], viewonly=True)
    carrier = relationship(Carrier, viewonly=True)
    handling = relationship("ShipmentHandling", viewonly=True)
    checkpoints = relationship("Checkpoint", viewonly=True)


class ShipmentHandling(Base):
    __tablename__ = "shipment_handling"
    shipment_id = Column(KEY, ForeignKey("shipments.id"), primary_key=True)
    code = Column(KEY, primary_key=True)


class Checkpoint(Base):
    __tablename__ = "checkpoints"
    id = Column(KEY, primary_key=True)
    shipment_id = Column(KEY, ForeignKey("shipments.id"), nullable=False)
    hub_id = Column(KEY, ForeignKey("hubs.id"), nullable=False)
    event = Column(STR, nullable=False)
    scanned_at = Column(TS, nullable=False)
    hub = relationship(Hub, viewonly=True)


ROOT = Shipment
R = "request.resource.attr."

INFERRED = {
    R + "trackingNumber": Shipment.tracking_no,
    R + "shipper": Shipment.shipper_account,
    R + "handling": related(Shipment.handling, ShipmentHandling.code),
}

S = Shipment
Origin, Destination, CheckpointHub = aliased(Hub), aliased(Hub), aliased(Hub)
CHECKPOINTS = Relation([Checkpoint.shipment_id == S.id], [S])


def _hub(alias, fk, column):
    return scalar(S, getattr(alias, column), alias.id == fk)


DECLARED = {
    R + "trackingNumber": S.tracking_no,
    R + "status": S.status,
    R + "weightKg": S.weight_kg,
    R + "hazmat": S.hazmat,
    R + "customsHold": S.customs_hold,
    R + "shipper": S.shipper_account,
    R + "eta": S.eta,
    R + "declaredValue": S.declared_value,
    **{R + f"origin.{c}": _hub(Origin, S.origin_hub_id, c) for c in ("code", "country", "region")},
    **{
        R + f"destination.{c}": _hub(Destination, S.destination_hub_id, c)
        for c in ("code", "country", "region")
    },
    R + "carrier.scac": scalar(S, Carrier.scac, Carrier.id == S.carrier_id),
    R + "handling": Relation(
        [ShipmentHandling.shipment_id == S.id], [S], member=ShipmentHandling.code
    ),
    R + "checkpoints": CHECKPOINTS,
    "c": CHECKPOINTS,
    "c.event": Checkpoint.event,
    "c.scannedAt": Checkpoint.scanned_at,
    "c.hub.code": scalar(Checkpoint, CheckpointHub.code, CheckpointHub.id == Checkpoint.hub_id),
}


def mapping(style, dialect):
    if style == "inferred":
        return {"attr_map": INFERRED}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides()}
