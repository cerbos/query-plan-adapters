# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Relations read from mapped ``relationship()`` metadata instead of written by hand.

See "Collections in a related table" in the README.
"""

import itertools
import re
from typing import Any

from sqlalchemy import and_, inspect, select
from sqlalchemy.orm import (
    ColumnProperty,
    Mapper,
    RelationshipProperty,
    configure_mappers,
)
from sqlalchemy.orm.attributes import QueryableAttribute
from sqlalchemy.orm.interfaces import MANYTOMANY
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList
from sqlalchemy.sql.schema import Column, Index, UniqueConstraint
from sqlalchemy.sql.selectable import ScalarSelect

from cerbos_sqlalchemy._plan import (
    Expr,
    Operand,
    Value,
    Variable,
    lambda_binding,
)
from cerbos_sqlalchemy.errors import UnsupportedPlanError
from cerbos_sqlalchemy.relations import Relation

__all__ = ["related"]


class RelatedRow:
    """A to-one relationship read whole: a map-valued attribute, which no operator lowers."""

    def __init__(self, path: tuple[Any, ...]) -> None:
        self.path = path

    def __repr__(self) -> str:
        return f"RelatedRow({' -> '.join(str(a) for a in self.path)})"


def is_relationship(value: Any) -> bool:
    return isinstance(value, QueryableAttribute) and isinstance(
        _property(value), RelationshipProperty
    )


def _property(attribute: Any) -> Any:
    return getattr(attribute, "property", None)


class _Hop:
    def __init__(self, attribute: Any) -> None:
        configure_mappers()
        prop = attribute.property
        self.name = f"{prop.parent.class_.__name__}.{prop.key}"
        self.prop = prop
        target = prop.mapper
        self.owner_tables = frozenset(prop.parent.tables)
        self.target_tables = frozenset(target.tables)
        if self.owner_tables & self.target_tables:
            raise UnsupportedPlanError(
                f"{self.name} is self-referential; map it with a hand-written Relation "
                "over an aliased() target"
            )
        self.own = set(self.target_tables)
        self.predicates = [prop.primaryjoin]
        if prop.secondary is not None:
            self.own.add(prop.secondary)
            self.predicates.append(prop.secondaryjoin)
        self.predicates.extend(_target_criteria(target))
        self.many = bool(prop.uselist)

    def require_to_one(self) -> None:
        if self.many:
            raise UnsupportedPlanError(
                f"{self.name} is a collection; only the last hop of a path may be one"
            )
        if self.prop.direction is MANYTOMANY:
            raise UnsupportedPlanError(
                f"{self.name} is a to-one over a secondary table; nothing proves it "
                "holds one row"
            )
        keyed = _equated_target_columns(self.prop.primaryjoin, self.target_tables)
        if not any(key <= keyed for key in _unique_keys(self.prop.mapper)):
            raise UnsupportedPlanError(
                f"{self.name} is uselist=False but no unique key of its target is "
                "equated in its join, so the database does not hold it to one row"
            )


def _target_criteria(mapper: Any) -> list[Any]:
    criteria = []
    for level in mapper.iterate_to_root():
        if level.inherit_condition is not None and not level.single:
            criteria.append(level.inherit_condition)
    if mapper.single and mapper.polymorphic_on is not None:
        identities = [m.polymorphic_identity for m in mapper.self_and_descendants]
        criteria.append(mapper.polymorphic_on.in_(identities))
    return criteria


def _conjuncts(clause: Any) -> list[Any]:
    if isinstance(clause, BooleanClauseList) and clause.operator is operators.and_:
        return [c for part in clause.clauses for c in _conjuncts(part)]
    return [clause]


def _equated_target_columns(join: Any, target_tables: frozenset[Any]) -> set[Any]:
    columns = set()
    for clause in _conjuncts(join):
        if (
            not isinstance(clause, BinaryExpression)
            or clause.operator is not operators.eq
        ):
            continue
        for side in (clause.left, clause.right):
            if isinstance(side, Column) and side.table in target_tables:
                columns.add(side)
    return columns


def _unique_keys(mapper: Any) -> list[frozenset[Any]]:
    keys = []
    for table in mapper.tables:
        keys.append(frozenset(table.primary_key.columns))
        for constraint in table.constraints:
            if isinstance(constraint, UniqueConstraint):
                keys.append(frozenset(constraint.columns))
        for index in table.indexes:
            if isinstance(index, Index) and index.unique:
                keys.append(frozenset(index.columns))
        keys.extend(frozenset([c]) for c in table.columns if c.unique)
    return [key for key in keys if key]


def related(*path: Any) -> "Relation | RelatedRow | ScalarSelect[Any]":
    """An ``attr_map`` value read through mapped ``relationship()`` attributes.

    ``path`` is one or more relationship attributes, each starting where the last
    ended, optionally followed by a column of the last target:

    - ending in a to-many relationship: a collection, as a ``Relation``;
    - ending in a to-many relationship and a column: a list of that column's
      values, as a ``Relation`` with that ``member``;
    - every hop to-one and ending in a column: that column as a correlated scalar
      subquery, NULL when a hop is absent;
    - every hop to-one, no column: the row read whole, which no operator lowers.

    The join is the relationship's own ``primaryjoin`` and ``secondaryjoin``, with
    the target's joined-inheritance condition and single-table discriminator, so a
    filtered association is honoured. Every hop before the last must be to-one and
    held to one row by a unique key the join equates; ``require_hops`` guards those
    hops. Subqueries correlate against every enclosing table except their own.

    Raises:
        UnsupportedPlanError: For a self-referential hop, a to-many hop before the
            last, a to-one the schema does not hold to one row, or a column outside
            the last target.
    """
    if not path:
        raise UnsupportedPlanError("related() needs at least one relationship")
    *attributes, last = path
    column = None
    if is_relationship(last):
        attributes.append(last)
    else:
        column = last
    if not attributes or not all(is_relationship(a) for a in attributes):
        raise UnsupportedPlanError(
            "related() takes relationship attributes, optionally then one column"
        )
    hops = [_Hop(a) for a in attributes]
    for before, after in itertools.pairwise(hops):
        if not issubclass(before.prop.mapper.class_, after.prop.parent.class_):
            raise UnsupportedPlanError(
                f"{after.name} does not start where {before.name} ends"
            )
    for hop in hops[:-1]:
        hop.require_to_one()
    tail = hops[-1]
    if column is not None and getattr(column, "table", None) not in tail.target_tables:
        raise UnsupportedPlanError(f"{column} is not a column of {tail.name}'s target")

    own = [t for hop in hops for t in hop.own]
    predicates = [p for hop in hops for p in hop.predicates]

    if tail.many:
        return Relation._inferred(
            predicates,
            member=column,
            hop_correlation=[p for hop in hops[:-1] for p in hop.predicates],
            own=own,
            hop_own=[t for hop in hops[:-1] for t in hop.own],
            target=tail.prop.mapper,
        )

    tail.require_to_one()
    if column is None:
        return RelatedRow(tuple(path))
    return (
        select(column).where(and_(*predicates)).correlate_except(*own).scalar_subquery()
    )


def infer(value: Any) -> Any:
    return related(value) if is_relationship(value) else value


_ROOT = "request.resource.attr."


def _mapper(table: Any) -> Any:
    mapper = inspect(table, raiseerr=False)
    return mapper if isinstance(mapper, Mapper) else None


def _snake(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def _mapped_property(mapper: Any, segment: str) -> Any:
    found = {
        key: mapper.attrs[key]
        for key in (segment, _snake(segment))
        if key in mapper.attrs
    }
    if len(found) > 1:
        raise UnsupportedPlanError(
            f"{segment!r} names both {' and '.join(sorted(found))} on "
            f"{mapper.class_.__name__}; map it in attr_map"
        )
    return next(iter(found.values()), None)


def _resolve(mapper: Any, segments: list[str]) -> Any:
    relationships = []
    for position, segment in enumerate(segments):
        prop = _mapped_property(mapper, segment)
        last = position == len(segments) - 1
        attribute = getattr(mapper.class_, prop.key) if prop is not None else None
        if isinstance(prop, RelationshipProperty):
            relationships.append(attribute)
            mapper = prop.mapper
            if last:
                return related(*relationships)
        elif isinstance(prop, ColumnProperty) and last and len(prop.columns) == 1:
            return related(*relationships, attribute) if relationships else attribute
        else:
            return None
    return None


def _root_segments(name: str) -> list[str] | None:
    if name.startswith(_ROOT):
        return name[len(_ROOT) :].split(".")
    if name == "request.resource.id":
        return ["id"]
    return None


def _element(collection: Any) -> tuple[Any, Any] | None:
    if not isinstance(collection, Relation) or collection._target is None:
        return None
    if collection.member is not None:
        return collection.member, None
    return collection, collection._target


def resolvable(table: Any, name: str) -> bool:
    """Whether ``name`` could be read from ``table``'s mapping, for option checks."""
    mapper = _mapper(table)
    if mapper is None:
        return False
    segments = _root_segments(name)
    if segments is None:
        return not name.startswith("request.")
    try:
        return _resolve(mapper, segments) is not None
    except UnsupportedPlanError:
        return False


def infer_attributes(
    condition: Operand,
    table: Any,
    attr_map: dict[str, Any],
    skip: frozenset[str],
) -> dict[str, Any]:
    """The plan's attributes ``attr_map`` leaves out, read from ``table``'s mapping.

    ``request.resource.attr.a.b`` follows ``a`` then ``b`` through the model's columns
    and relationships, by name or its snake_case. A lambda variable takes the element
    of the collection it ranges over: the member column of a list of scalars, else the
    related row, whose fields resolve on its model. A name that resolves nowhere is
    left out, so the plan fails as it would without inference. A name in ``skip``
    (a declared collection column) is added only as a relation.
    """
    mapper = _mapper(table)
    if mapper is None:
        return {}
    added: dict[str, Any] = {}
    origin: dict[str, str] = {}

    def lookup(name: str) -> Any:
        return attr_map[name] if name in attr_map else added.get(name)

    def resolve(name: str, scope: dict[str, tuple[str, Any, Any]]) -> None:
        if name in attr_map:
            return
        head, _, rest = name.partition(".")
        if head in scope:
            collection, element, target = scope[head]
            if name in origin and origin[name] != collection:
                raise UnsupportedPlanError(
                    f"{head!r} ranges over both {origin[name]} and {collection}; "
                    f"map {name!r} in attr_map"
                )
            origin[name] = collection
            if name in added:
                return
            value = element if not rest else None
            if rest and target is not None:
                value = _resolve(target, rest.split("."))
        else:
            if name in added:
                return
            segments = _root_segments(name)
            value = _resolve(mapper, segments) if segments is not None else None
            if value is not None and name.startswith(_ROOT):
                # Map each hop too, so has() below one sees a parent that can be absent.
                for end in range(1, len(segments)):
                    resolve(_ROOT + ".".join(segments[:end]), scope)
        if value is not None and (name not in skip or isinstance(value, Relation)):
            added[name] = value

    def walk(operand: Operand, scope: dict[str, tuple[str, Any, Any]]) -> None:
        if isinstance(operand, Variable):
            resolve(operand.name, scope)
            return
        if not isinstance(operand, Expr):
            return
        children = operand.operands
        binding = lambda_binding(operand)
        if binding is not None:
            collection, body, variable = binding
            walk(collection, scope)
            inner = {k: v for k, v in scope.items() if k != variable.name}
            if isinstance(collection, Variable):
                element = _element(lookup(collection.name))
                if element is not None:
                    inner[variable.name] = (collection.name, *element)
            resolve(variable.name, inner)
            walk(body, inner)
            return
        if (
            operand.operator == "index"
            and len(children) == 2
            and isinstance(children[0], Variable)
            and isinstance(children[1], Value)
            and isinstance(children[1].value, str)
            and "." not in children[1].value
        ):
            resolve(f"{children[0].name}.{children[1].value}", scope)
        for child in children:
            walk(child, scope)

    walk(condition, {})
    return added
