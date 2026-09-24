# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Translator unit test: what this adapter can be asked without a store. Offline.

Rows are the adversarial harness's job. This covers what it cannot vary or see: caller
options, the SDK transport, the model style, ``now()`` precision and PostgreSQL's bound
parameters. Plans come from ``conformance/golden/`` (ADR 0010); ``test_query.py`` covers
shapes no policy can produce.
"""

import math
from datetime import datetime, timezone
from typing import ClassVar

import pytest
from corpus import (
    ATTR_MAP,
    ATTRIBUTE_NULL_REPRESENTATION,
    COLLECTION_COLUMNS,
    OPERATOR_OVERRIDES,
    PDP_TAGS,
    PG_ARRAY_COLLECTION_COLUMNS,
    AdvResource,
    AdvTag,
    golden_case,
    golden_cases,
    grpc_plan_from_golden,
    now_minus_24h,
    plan_from_golden,
    render,
)
from sqlalchemy import any_
from sqlalchemy.exc import CompileError
from sqlalchemy.orm.attributes import InstrumentedAttribute

from cerbos_sqlalchemy import CollectionColumn, UnsupportedPlanError, get_query

CURRENT = golden_cases(PDP_TAGS[0])

#: One ``__NOW_MINUS_24H__`` value per run, so two translations of a case agree.
PLANNED_AT = now_minus_24h()


def translate(
    case_id,
    *,
    planned_at=None,
    attr_map=None,
    operator_override_fns=OPERATOR_OVERRIDES,
    null_attribute_representation="explicit",
    attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
    collection_columns=COLLECTION_COLUMNS,
    plan=None,
    table=AdvResource,
):
    """The ``Select`` this adapter emits for one corpus case."""
    if plan is None:
        plan = plan_from_golden(golden_case(case_id), planned_at or PLANNED_AT)
    return get_query(
        plan,
        table,
        ATTR_MAP if attr_map is None else attr_map,
        operator_override_fns=operator_override_fns,
        null_attribute_representation=null_attribute_representation,
        attribute_null_representation=attribute_null_representation,
        collection_columns=collection_columns,
    )


def _translated_or_refused(build):
    try:
        return build()
    except UnsupportedPlanError as exc:
        return f"refused: {exc}"


def _plan_carries_null_literal(node):
    """Whether any operand in the plan is a null literal, or a list holding one."""
    if not isinstance(node, dict):
        return False
    if "value" in node:
        value = node["value"]
        return value is None or (
            isinstance(value, list) and any(member is None for member in value)
        )
    expression = node.get("expression", node)
    return any(
        _plan_carries_null_literal(operand)
        for operand in expression.get("operands", [])
    )


class TestNullAttributeRepresentation:
    """The call-level null option; the plan cannot say which convention applies (#302)."""

    ACTION = "null/equals/null-literal-on-missing-attribute"

    def test_explicit_emits_an_is_null_filter(self):
        statement, _params = render(
            translate(
                self.ACTION,
                null_attribute_representation="explicit",
                attribute_null_representation=None,
            ),
            "sqlite",
        )
        assert "adversarial_resource.a_optional_string IS NULL" in statement

    def test_omitted_refuses_the_same_plan(self):
        # Here a NULL column means no attribute, which check() denies (#302).
        with pytest.raises(UnsupportedPlanError, match="null operand"):
            translate(
                self.ACTION,
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )

    def test_a_per_attribute_declaration_overrides_the_call_level_option(self):
        # `owner` is declared "explicit", so its null probe still translates (#308)...
        assert render(
            translate(
                "null/equals/null-literal", null_attribute_representation="omitted"
            ),
            "sqlite",
        ) == render(translate("null/equals/null-literal"), "sqlite")

        # ...and without the declaration it is refused.
        with pytest.raises(UnsupportedPlanError, match="null operand"):
            translate(
                "null/equals/null-literal",
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )


class TestOperatorOverrides:
    """Caller-supplied operator overrides: the corpus uses one fixed set."""

    def test_an_override_replaces_the_default_lowering_for_its_operator(self):
        # The README's example: `= ANY (...)` instead of `IN` on PostgreSQL.
        action = "null/in/missing-attribute-in-multi-element-list"
        attr_map = {
            "request.resource.attr.aOptionalString": AdvResource.a_optional_string
        }

        # This map lacks the attributes the corpus's null declarations name.
        def emitted(operator_override_fns):
            return render(
                translate(
                    action,
                    attr_map=attr_map,
                    operator_override_fns=operator_override_fns,
                    attribute_null_representation=None,
                ),
                "postgresql",
            )[0]

        assert " IN " in emitted(None)
        assert "= ANY (" in emitted({"in": lambda c, v: c == any_(v)})

    def test_an_unmapped_attribute_is_refused_rather_than_dropped(self):
        # Dropping it would change what the filter means.
        with pytest.raises(KeyError, match="Attribute does not exist"):
            translate(
                "string/equals/case-sensitive",
                attr_map={},
                attribute_null_representation=None,
            )

    # "Unsupported" in the ledger means unsupported with the corpus's overrides.
    # Each test below checks both halves: refused without the override, translated with it.

    def test_a_matches_override_admits_the_regex_the_corpus_refuses(self):
        # SQL regex engines aren't RE2, so there is no default. A caller may supply one.
        action = "regex/matches/anchored-prefix"
        with pytest.raises(
            UnsupportedPlanError, match="Unrecognised operator: matches"
        ):
            translate(action)

        statement, params = render(
            translate(
                action,
                operator_override_fns={
                    **OPERATOR_OVERRIDES,
                    "matches": lambda column, pattern: column.regexp_match(pattern),
                },
            ),
            "postgresql",
        )
        assert "adversarial_resource.a_string ~ %(a_string_1)s" in statement
        assert params["a_string_1"] == "^h"

    def test_an_index_override_still_serves_storage_nothing_declares(self):
        # For storage `collection_columns` can't describe (#227). Here a scalar column
        # stands in for a one-element list. Without a declaration or override, it's refused.
        action = "collection/index/first-element-of-string-list"
        undeclared = {
            "attr_map": {"request.resource.attr.tagNames": AdvResource.a_string},
            "attribute_null_representation": None,
            "collection_columns": None,
        }
        with pytest.raises(
            UnsupportedPlanError,
            match="Index storage shape is undeclared for 'request.resource.attr.tagNames'",
        ):
            translate(action, operator_override_fns=None, **undeclared)

        statement, params = render(
            translate(
                action,
                operator_override_fns={"index": lambda column, _position: column},
                **undeclared,
            ),
            "sqlite",
        )
        assert statement.endswith("WHERE adversarial_resource.a_string = ?")
        assert params == {"a_string_1": "public"}


class TestDeclaredCollectionStorage:
    """``collection_columns``, a caller argument the corpus cannot vary (#227)."""

    def test_a_declaration_takes_precedence_over_the_size_override(self):
        # The declaration names the attribute, so it beats the `size` override, which
        # would count the relation.
        declared, _ = render(
            translate("size/greater-than/collection-above-one"), "sqlite"
        )
        overridden, _ = render(
            translate(
                "size/greater-than/collection-above-one", collection_columns=None
            ),
            "sqlite",
        )

        assert "json_array_length(adversarial_resource.tags_json)" in declared
        assert "count(*)" not in declared
        assert "count(*)" in overridden

    @pytest.mark.parametrize(
        "action,rendered",
        [
            (
                "size/greater-than/collection-above-one",
                "jsonb_array_length(to_jsonb(adversarial_resource.tags_array))",
            ),
            (
                "collection/index/first-element-of-string-list",
                (
                    "(to_jsonb(adversarial_resource.tag_names_array) -> 0) "
                    "= to_jsonb(CAST(%(param_1)s AS TEXT))"
                ),
            ),
            (
                "collection/index/first-element-of-string-list-equals-null",
                (
                    "jsonb_typeof((to_jsonb(adversarial_resource.tag_names_array) -> 0)) "
                    "= 'null'"
                ),
            ),
        ],
    )
    def test_a_pg_array_is_read_by_position_through_to_jsonb(self, action, rendered):
        # Not `array[i + 1]`: the harness rebases arrays to start at 0, so assuming the
        # default lower bound would read the wrong element.
        statement, _ = render(
            translate(action, collection_columns=PG_ARRAY_COLLECTION_COLUMNS),
            "postgresql",
        )
        assert rendered in statement
        assert "[" not in statement.split(" WHERE ", 1)[1]

    def test_a_pg_array_answers_membership_through_to_jsonb(self):
        statement, _ = render(
            translate(
                "type-mismatch/has-intersection/resource-number-list-against-mixed-literal-list",
                collection_columns=PG_ARRAY_COLLECTION_COLUMNS,
            ),
            "postgresql",
        )
        assert (
            "jsonb_array_elements(to_jsonb(adversarial_resource.a_number_list_array))"
            in statement
        )

    def test_an_attr_map_entry_keeps_membership_off_the_declaration(self):
        # Membership uses the declaration only when `attr_map` doesn't map the attribute.
        # The corpus maps neither list, so the mapped side is caller-only.
        attr_map = {
            **ATTR_MAP,
            "request.resource.attr.aNumberList": AdvResource.a_number_list_json,
        }
        mapped, _ = render(
            translate(
                "membership/in/literal-in-resource-number-list", attr_map=attr_map
            ),
            "sqlite",
        )
        declared, _ = render(
            translate("membership/in/literal-in-resource-number-list"), "sqlite"
        )

        assert "json_each(adversarial_resource.a_number_list_json)" in declared
        assert "json_each" not in mapped

    def test_two_positions_do_not_share_a_cached_statement(self):
        # The position is inlined, so it must be in the cache key.
        first = translate("collection/index/first-element-of-string-list").whereclause
        second = translate("collection/index/negated-out-of-bounds").whereclause
        assert first._generate_cache_key() != second._generate_cache_key()

    def test_a_pg_array_does_not_render_on_sqlite(self):
        with pytest.raises(CompileError, match='"pgArray" requires a PostgreSQL array'):
            render(
                translate(
                    "size/greater-than/collection-above-one",
                    collection_columns=PG_ARRAY_COLLECTION_COLUMNS,
                ),
                "sqlite",
            )

    def test_a_dialect_it_was_not_written_for_is_refused_at_compile_time(self):
        # `get_query` doesn't know the dialect, so an unsupported one fails at compile time.
        from sqlalchemy.dialects import mysql

        with pytest.raises(CompileError, match="renders only on SQLite and PostgreSQL"):
            translate("collection/index/first-element-of-string-list").compile(
                dialect=mysql.dialect()
            )

    def test_str_of_a_query_still_renders_for_debugging(self):
        assert "cerbos_collection_size(" in str(
            translate("size/greater-than/collection-above-one")
        )

    def test_an_undeclared_collection_column_is_refused_rather_than_measured(self):
        # LENGTH() of a JSON column would measure its text.
        with pytest.raises(UnsupportedPlanError, match="needs its storage declared"):
            translate(
                "size/greater-than/collection-above-one",
                attr_map={"request.resource.attr.tags": AdvResource.tags_json},
                operator_override_fns=None,
                attribute_null_representation=None,
                collection_columns=None,
            )

    def test_a_declared_column_must_be_addressable_like_a_mapped_one(self):
        # A column on another table needs `table_mapping`, as in `attr_map`.
        with pytest.raises(TypeError, match="table_mapping"):
            translate(
                "size/greater-than/collection-above-one",
                collection_columns={
                    "request.resource.attr.tags": CollectionColumn(AdvTag.name, "json")
                },
            )

    def test_the_declaration_is_validated(self):
        with pytest.raises(ValueError, match="storage must be 'json' or 'pgArray'"):
            CollectionColumn(AdvResource.tags_json, "jsonb")
        with pytest.raises(TypeError, match="must be CollectionColumn"):
            translate(
                "size/greater-than/collection-above-one",
                collection_columns={
                    "request.resource.attr.tags": (AdvResource.tags_json, "json")
                },
            )


class TestTimestampLiterals:
    """Folded ``now()`` literals, refused because the PDP emits nanoseconds."""

    CASES: ClassVar[list[str]] = [
        "timestamp/less-than/relative-window",
        "timestamp/greater-than/relative-window-value-first",
    ]

    @pytest.mark.parametrize("case_id", CASES)
    def test_the_refusal_is_the_precision_and_not_the_shape(self, case_id):
        with pytest.raises(UnsupportedPlanError, match="precision"):
            translate(case_id)

        # At microsecond precision the same plan translates.
        statement, _params = render(
            translate(case_id, planned_at="2026-08-11T09:13:39.123456Z"), "sqlite"
        )
        assert "adversarial_resource.created_at" in statement

    def test_excess_fractional_digits_are_accepted_only_when_they_are_zero(self):
        # Trailing zero digits lose nothing; a non-zero one can't be stored.
        statement, params = render(
            translate(self.CASES[0], planned_at="2026-08-11T09:13:39.123456000Z"),
            "sqlite",
        )
        assert "adversarial_resource.created_at" in statement
        assert list(params.values()) == [
            datetime(2026, 8, 11, 9, 13, 39, 123456, tzinfo=timezone.utc)
        ]

    @pytest.mark.parametrize(
        "value",
        [
            "2024-01-01",
            "0000-01-01T00:00:00Z",
            "2024-02-30T00:00:00Z",
            "9999-12-31T23:00:00-02:00",
        ],
    )
    def test_an_instant_the_adapter_cannot_carry_fails_closed(self, value):
        # Lenient parsing would compare against the wrong instant and over-grant.
        with pytest.raises(
            UnsupportedPlanError, match="RFC-3339|precision|instant range|offset"
        ):
            translate(self.CASES[0], planned_at=value)


#: A zero divisor recorded as the integer ``0``, whose sign JSON has lost.
INTEGER_ZERO_DIVISOR = "comparison/greater-than/infinity-from-ternary"


class TestTransportDecoding:
    """The protobuf (gRPC) decoding path, fed the same golden plans (#321)."""

    @pytest.mark.parametrize(
        "case",
        [c for c in CURRENT if c["id"] != INTEGER_ZERO_DIVISOR],
        ids=lambda c: c["id"],
    )
    def test_the_protobuf_decoding_emits_the_same_sql(self, case):
        def build(plan):
            return _translated_or_refused(
                lambda: render(translate(case["id"], plan=plan), "sqlite")
            )

        assert build(grpc_plan_from_golden(case, PLANNED_AT)) == build(
            plan_from_golden(case, PLANNED_AT)
        )

    def test_a_double_negative_zero_divisor_translates_over_json(self):
        # `-0.0` stays a signed float through json.loads, so the infinity's sign is known.
        statement, _params = render(
            translate("arithmetic/divide/negative-zero-divisor"), "sqlite"
        )
        assert "adversarial_resource.a_number" in statement

    def test_an_integer_zero_divisor_is_refused_over_json_only(self):
        # The HTTP API renders -0.0 as `-0`, which json.loads makes int 0: sign lost (#312).
        with pytest.raises(UnsupportedPlanError, match="sign is indeterminate"):
            translate(INTEGER_ZERO_DIVISOR)

        # Protobuf turns it into +0.0, so the guard doesn't fire. This is a fixture
        # artefact, not evidence that gRPC supports this shape.
        statement, _params = render(
            translate(
                INTEGER_ZERO_DIVISOR,
                plan=grpc_plan_from_golden(golden_case(INTEGER_ZERO_DIVISOR)),
            ),
            "sqlite",
        )
        assert "adversarial_resource" in statement


class TestDeclarativeBaseModels:
    """SQLAlchemy 2.0 ``DeclarativeBase`` models, a separate path in ``get_query``."""

    @pytest.fixture(scope="class")
    def modern(self):
        try:
            from sqlalchemy.orm import DeclarativeBase, DeclarativeMeta
        except ImportError:
            pytest.skip("DeclarativeBase requires SQLAlchemy >= 2.0")

        class ModernBase(DeclarativeBase):
            pass

        twins = {
            legacy: type(
                f"Modern{legacy.__name__}",
                (ModernBase,),
                {"__table__": legacy.__table__},
            )
            for legacy in {
                value.class_
                for value in list(ATTR_MAP.values())
                + [declared.column for declared in COLLECTION_COLUMNS.values()]
                if isinstance(value, InstrumentedAttribute)
            }
        }

        def swap(value):
            if isinstance(value, InstrumentedAttribute):
                return getattr(twins[value.class_], value.key)
            return value

        table = twins[AdvResource]
        assert not isinstance(table, DeclarativeMeta)
        return {
            "table": table,
            "attr_map": {name: swap(value) for name, value in ATTR_MAP.items()},
            "collection_columns": {
                name: CollectionColumn(swap(declared.column), declared.storage)
                for name, declared in COLLECTION_COLUMNS.items()
            },
        }

    def test_every_case_emits_the_same_sql(self, modern):
        # The twins map the same tables, so the SQL must match.
        differing = [
            case["id"]
            for case in CURRENT
            if _translated_or_refused(
                lambda cid=case["id"]: render(translate(cid), "sqlite")
            )
            != _translated_or_refused(
                lambda cid=case["id"]: render(translate(cid, **modern), "sqlite")
            )
        ]
        assert differing == []


def test_every_null_carrying_plan_is_refused_under_omitted():
    # #302. The refusal keys off the null operand, not an operator list, which would
    # miss the null inside `hasIntersection(tagNames, ["public", null])`.
    null_carrying = [
        case["id"]
        for case in CURRENT
        if case["plan"]["kind"] == "KIND_CONDITIONAL"
        and _plan_carries_null_literal(case["plan"]["condition"])
    ]
    assert "null/has-intersection/literal-list-with-null-element" in null_carrying

    not_refused = []
    for case_id in null_carrying:
        try:
            translate(
                case_id,
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )
            not_refused.append(case_id)
        except UnsupportedPlanError as exc:
            if "null operand" not in str(exc):
                not_refused.append(f"{case_id} (refused for another reason: {exc})")
    assert not_refused == []


def test_no_case_binds_a_non_finite_number():
    # A bound NaN/Infinity can return the same rows as a folded one on PostgreSQL, so only
    # the parameters reveal it. The harness runs few cases on PostgreSQL, so check all here.
    offenders = []
    for case in CURRENT:
        try:
            query = translate(case["id"])
        except UnsupportedPlanError:
            continue
        for name in ("sqlite", "postgresql"):
            for key, value in render(query, name)[1].items():
                if isinstance(value, float) and not math.isfinite(value):
                    offenders.append(f"{case['id']} ({name}) {key}={value}")
    assert offenders == []


def test_refusals_keep_the_types_they_raised_before():
    # Refusals used to raise ValueError (one raised TypeError), so existing handlers
    # must still catch them.
    with pytest.raises(UnsupportedPlanError) as refused:
        translate("regex/matches/anchored-prefix")
    assert isinstance(refused.value, ValueError)

    with pytest.raises(UnsupportedPlanError, match="operator override") as refused:
        translate("comparison/equals/whole-list-literal")
    assert isinstance(refused.value, TypeError)
