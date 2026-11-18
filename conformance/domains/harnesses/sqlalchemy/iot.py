from _types import KEY, BOOL, INT, JSONMAP, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.types import Boolean

from cerbos_sqlalchemy import Relation, UnsupportedPlanError, relation_overrides

Base = declarative_base()


class Org(Base):
    __tablename__ = "orgs"
    id = Column(KEY, primary_key=True)
    name = Column(STR, nullable=False)
    plan = Column(STR, nullable=False)


class Site(Base):
    __tablename__ = "sites"
    id = Column(KEY, primary_key=True)
    org_id = Column(KEY, ForeignKey("orgs.id"), nullable=False)
    region = Column(STR, nullable=False)
    name = Column(STR, nullable=False)
    org = relationship(Org, viewonly=True)


class Device(Base):
    __tablename__ = "devices"
    id = Column(KEY, primary_key=True)
    serial = Column(STR, nullable=False)
    model = Column(STR, nullable=False)
    firmware = Column(STR, nullable=False)
    site_id = Column(KEY, ForeignKey("sites.id"), nullable=False)
    last_seen_at = Column(TS, nullable=True)
    online = Column(BOOL, nullable=False)
    battery_pct = Column(INT, nullable=True)
    labels = Column(JSONMAP, nullable=False)
    decommissioned = Column(BOOL, nullable=False)
    site = relationship(Site, viewonly=True)
    alerts = relationship("DeviceAlert", viewonly=True)


class DeviceAlert(Base):
    __tablename__ = "device_alerts"
    id = Column(KEY, primary_key=True)
    device_id = Column(KEY, ForeignKey("devices.id"), nullable=False)
    severity = Column(INT, nullable=False)
    acknowledged = Column(BOOL, nullable=False)
    code = Column(STR, nullable=False)


ROOT = Device
R = "request.resource.attr."
D = Device


class json_has_key(FunctionElement):
    type = Boolean()
    inherit_cache = True


@compiles(json_has_key, "postgresql")
def _pg_has_key(element, compiler, **kw):
    column, key = element.clauses
    return f"({compiler.process(column, **kw)} ? {compiler.process(key, **kw)})"


@compiles(json_has_key, "sqlite")
def _sqlite_has_key(element, compiler, **kw):
    column, key = element.clauses
    path = f"'$.\"' || {compiler.process(key, **kw)} || '\"'"
    return f"(json_type({compiler.process(column, **kw)}, {path}) IS NOT NULL)"


@compiles(json_has_key, "mysql")
def _mysql_has_key(element, compiler, **kw):
    column, key = element.clauses
    path = f"CONCAT('$.\"', {compiler.process(key, **kw)}, '\"')"
    return f"JSON_CONTAINS_PATH({compiler.process(column, **kw)}, 'one', {path})"


class LabelMap:
    """`R.attr.labels` read whole: only `"key" in labels` reaches it."""


LABELS = LabelMap()


def _overrides():
    overrides = relation_overrides()
    default_in = overrides["in"]

    def in_(left, right):
        if isinstance(left, LabelMap):
            if not isinstance(right, str):
                raise UnsupportedPlanError("only a string literal key is tested in labels")
            return json_has_key(D.labels, right)
        return default_in(left, right)

    def has(column, _):
        return column.is_not(None)

    overrides.update({"in": in_, "has": has})
    return overrides


def _label(key):
    return D.labels[key].as_string()


LABEL_KEYS = {
    R + "labels": LABELS,
    R + "labels.env": _label("env"),
    R + "labels.restricted": _label("restricted"),
}

INFERRED = dict(LABEL_KEYS)

ALERTS = Relation([DeviceAlert.device_id == D.id], [D])
DECLARED = {
    **LABEL_KEYS,
    R + "serial": D.serial,
    R + "model": D.model,
    R + "firmware": D.firmware,
    R + "lastSeenAt": D.last_seen_at,
    R + "online": D.online,
    R + "batteryPct": D.battery_pct,
    R + "decommissioned": D.decommissioned,
    R + "site.region": scalar(D, Site.region, Site.id == D.site_id),
    R + "site.org.id": scalar(D, Site.org_id, Site.id == D.site_id),
    R + "site.org.plan": scalar(D, Org.plan, Org.id == Site.org_id, Site.id == D.site_id),
    R + "alerts": ALERTS,
    "a": ALERTS,
    "a.severity": DeviceAlert.severity,
    "a.acknowledged": DeviceAlert.acknowledged,
    "a.code": DeviceAlert.code,
}


def mapping(style, dialect):
    return {
        "attr_map": INFERRED if style == "inferred" else DECLARED,
        "operator_override_fns": _overrides(),
    }
