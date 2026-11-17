# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Relations inferred from ``relationship()`` metadata, against real SQLite rows.

The corpus proves the inferred correlation for plain one-to-many and to-one
chains. These are the mapping hazards it has no model for. Each way inference
could return a row the PDP denies:

1. A filtered ``primaryjoin`` dropped: a soft-deleted tag still matches.
2. A ``secondaryjoin`` filter dropped: an inactive label still matches.
3. A single-table subclass target read without its discriminator: a sibling
   subtype's row matches.
4. A joined-inheritance target read without its inherit condition: the subclass
   table cross-joins, so another resource's row matches.
5. A ``uselist=False`` with no unique key read as one row: SQLite picks any.
6. A self-referential relationship correlated against itself: it compares a row
   with itself.
7. A to-many hop before the last: CEL cannot dot through a list.
"""

from typing import Any, ClassVar

import pytest
from cerbos.response.v1 import response_pb2
from google.protobuf.json_format import ParseDict
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    Table,
    create_engine,
    insert,
)
from sqlalchemy.orm import declarative_base, relationship

from cerbos_sqlalchemy import UnsupportedPlanError, get_query, related

Base = declarative_base()

doc_label = Table(
    "inf_doc_label",
    Base.metadata,
    Column("doc_id", ForeignKey("inf_doc.id"), primary_key=True),
    Column("label_id", ForeignKey("inf_label.id"), primary_key=True),
)


class Doc(Base):
    __tablename__ = "inf_doc"
    id = Column(Integer, primary_key=True)
    parent_id = Column(ForeignKey("inf_doc.id"))

    live_tags = relationship(
        "Tag",
        primaryjoin="and_(Tag.doc_id == Doc.id, Tag.deleted == False)",
        viewonly=True,
    )
    labels = relationship(
        "Label",
        secondary=doc_label,
        secondaryjoin="and_(inf_doc_label.c.label_id == Label.id, Label.active == True)",
        viewonly=True,
    )
    notes = relationship("Note", viewonly=True)
    reviews = relationship("Review", viewonly=True)
    loose_owner = relationship("Owner", uselist=False, viewonly=True)
    parent = relationship("Doc", remote_side=[id], viewonly=True)
    owners = relationship("Owner", viewonly=True)


class Tag(Base):
    __tablename__ = "inf_tag"
    id = Column(Integer, primary_key=True)
    doc_id = Column(ForeignKey("inf_doc.id"), nullable=False)
    name = Column(String, nullable=False)
    deleted = Column(Boolean, nullable=False)

    doc = relationship("Doc", viewonly=True)


class Label(Base):
    __tablename__ = "inf_label"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    active = Column(Boolean, nullable=False)


class Item(Base):
    __tablename__ = "inf_item"
    id = Column(Integer, primary_key=True)
    doc_id = Column(ForeignKey("inf_doc.id"), nullable=False)
    kind = Column(String, nullable=False)
    body = Column(String, nullable=False)
    __mapper_args__: ClassVar[dict[str, Any]] = {
        "polymorphic_on": kind,
        "polymorphic_identity": "item",
    }


class Note(Item):
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "note"}


class Todo(Item):
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "todo"}


class Entry(Base):
    __tablename__ = "inf_entry"
    id = Column(Integer, primary_key=True)
    doc_id = Column(ForeignKey("inf_doc.id"), nullable=False)
    kind = Column(String, nullable=False)
    __mapper_args__: ClassVar[dict[str, Any]] = {
        "polymorphic_on": kind,
        "polymorphic_identity": "entry",
    }


class Review(Entry):
    __tablename__ = "inf_review"
    id = Column(ForeignKey("inf_entry.id"), primary_key=True)
    score = Column(Integer, nullable=False)
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "review"}


class Owner(Base):
    __tablename__ = "inf_owner"
    id = Column(Integer, primary_key=True)
    doc_id = Column(ForeignKey("inf_doc.id"), nullable=False)
    name = Column(String, nullable=False)


@pytest.fixture(scope="module")
def engine():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(insert(Doc), [{"id": 1}, {"id": 2}])
        conn.execute(
            insert(Tag),
            [
                {"id": 1, "doc_id": 1, "name": "x", "deleted": True},
                {"id": 2, "doc_id": 2, "name": "x", "deleted": False},
            ],
        )
        conn.execute(
            insert(Label),
            [
                {"id": 1, "name": "x", "active": False},
                {"id": 2, "name": "x", "active": True},
            ],
        )
        conn.execute(
            insert(doc_label),
            [{"doc_id": 1, "label_id": 1}, {"doc_id": 2, "label_id": 2}],
        )
        conn.execute(
            insert(Item.__table__),
            [
                {"id": 1, "doc_id": 1, "kind": "todo", "body": "x"},
                {"id": 2, "doc_id": 2, "kind": "note", "body": "x"},
            ],
        )
        conn.execute(
            insert(Entry.__table__),
            [
                {"id": 1, "doc_id": 1, "kind": "entry"},
                {"id": 2, "doc_id": 2, "kind": "review"},
            ],
        )
        conn.execute(insert(Review.__table__), [{"id": 2, "score": 9}])
        conn.execute(
            insert(Owner),
            [
                {"id": 1, "doc_id": 1, "name": "a"},
                {"id": 2, "doc_id": 1, "name": "b"},
            ],
        )
    return engine


def _plan(condition):
    return ParseDict(
        {"filter": {"kind": "KIND_CONDITIONAL", "condition": condition}},
        response_pb2.PlanResourcesResponse(),
    )


def _exists(collection, field, operator, value):
    return {
        "expression": {
            "operator": "exists",
            "operands": [
                {"variable": f"request.resource.attr.{collection}"},
                {
                    "expression": {
                        "operator": "lambda",
                        "operands": [
                            {
                                "expression": {
                                    "operator": operator,
                                    "operands": [
                                        {"variable": f"e.{field}"},
                                        {"value": value},
                                    ],
                                }
                            },
                            {"variable": "e"},
                        ],
                    }
                },
            ],
        }
    }


def _ids(engine, expression, attr_map):
    query = get_query(_plan(expression), Doc, attr_map)
    with engine.connect() as conn:
        return {row.id for row in conn.execute(query)}


HAZARDS = [
    ("liveTags", Doc.live_tags, "name", Tag.name, "eq", "x"),
    ("labels", Doc.labels, "name", Label.name, "eq", "x"),
    ("notes", Doc.notes, "body", Note.body, "eq", "x"),
    ("reviews", Doc.reviews, "score", Review.score, "gt", 5),
]


@pytest.mark.parametrize("mapped", [True, False], ids=["named", "unmapped"])
@pytest.mark.parametrize(
    "collection,relationship_attribute,field,column,operator,value",
    HAZARDS,
    ids=[h[0] for h in HAZARDS],
)
def test_the_relationship_join_decides_which_rows_are_elements(
    engine, collection, relationship_attribute, field, column, operator, value, mapped
):
    # Doc 1's only candidate element is the one the relationship excludes. Unmapped,
    # the collection and the lambda's field resolve through Doc's mapping by name.
    attr_map = (
        {
            f"request.resource.attr.{collection}": relationship_attribute,
            "e": relationship_attribute,
            f"e.{field}": column,
        }
        if mapped
        else None
    )
    expression = _exists(collection, field, operator, value)
    assert _ids(engine, expression, attr_map) == {2}
    assert _ids(
        engine, {"expression": {"operator": "not", "operands": [expression]}}, attr_map
    ) == {1}


def test_a_to_one_without_a_unique_key_is_refused():
    with pytest.raises(UnsupportedPlanError, match="unique key"):
        related(Doc.loose_owner, Owner.name)
    with pytest.raises(UnsupportedPlanError, match="unique key"):
        get_query(
            _plan({"variable": "request.resource.attr.owner"}),
            Doc,
            {"request.resource.attr.owner": Doc.loose_owner},
        )


def test_a_self_referential_relationship_is_refused():
    with pytest.raises(UnsupportedPlanError, match="self-referential"):
        related(Doc.parent, Doc.id)


def test_a_to_many_hop_before_the_last_is_refused():
    with pytest.raises(UnsupportedPlanError, match="only the last hop"):
        related(Doc.live_tags, Tag.doc, Doc.id)


class Ambiguous(Base):
    __tablename__ = "inf_ambiguous"
    id = Column(Integer, primary_key=True)
    aValue = Column(String)
    a_value = Column(String)


def test_a_name_matching_two_attributes_is_refused():
    plan = _plan(
        {
            "expression": {
                "operator": "eq",
                "operands": [
                    {"variable": "request.resource.attr.aValue"},
                    {"value": "x"},
                ],
            }
        }
    )
    with pytest.raises(UnsupportedPlanError, match="names both"):
        get_query(plan, Ambiguous)
    get_query(plan, Ambiguous, {"request.resource.attr.aValue": Ambiguous.a_value})
