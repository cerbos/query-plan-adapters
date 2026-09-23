"""Translator unit test: what this adapter can be asked without a store.

Offline -- no PDP, no container, no database. The rows a recorded plan returns are
``test_adversarial_conformance.py``'s job, replayed from ``conformance/golden/``; nothing
here re-asserts them. What lives here is what that harness structurally cannot vary or
see: the caller-supplied options (the null representation, operator overrides, declared
collection storage, the SDK transport and the model style), the precision rule for a
folded ``now()``, and the parameters a statement binds on PostgreSQL, which executes only
the cases that read a declared collection.

The plans are read from the golden files, not written: see
`ADR 0010 <../../docs/adr/0010-conformance-replays-recorded-pdp-decisions.md>`_.
The hand-built plans in ``test_query.py`` cover shapes no policy can produce.
"""

import math
from datetime import datetime, timezone

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

from cerbos_sqlalchemy import CollectionColumn, UnsupportedPlanError, get_query
from sqlalchemy import any_
from sqlalchemy.exc import CompileError
from sqlalchemy.orm.attributes import InstrumentedAttribute

CURRENT = golden_cases(PDP_TAGS[0])

#: One substitution for ``__NOW_MINUS_24H__`` per run, so two translations of a case agree.
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
    """The ``Select`` this adapter emits for one corpus case under the corpus mapping."""
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
    """Whether any operand anywhere in the plan is a literal null, or a list holding one."""
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
    """The call-level null representation, which the corpus mapping fixes at one value.

    ``aOptionalString == null`` produces the same ``eq(attr, null)`` node whichever
    convention the caller uses, so the adapter has to be told (#302). The corpus mapping
    declares ``aOptionalString`` omitted per attribute; these vary the call-level option
    with that declaration removed.
    """

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
        # A NULL column then sends no attribute, so check() denies on a missing-attribute
        # error while the filter above returns exactly those rows (#302).
        with pytest.raises(UnsupportedPlanError, match="null operand"):
            translate(
                self.ACTION,
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )

    def test_a_per_attribute_declaration_overrides_the_call_level_option(self):
        # #308. `owner` declares "explicit" in the shared map, so
        # `null/equals/null-literal` — which probes it — must still translate under a
        # call-level "omitted"...
        assert render(
            translate(
                "null/equals/null-literal", null_attribute_representation="omitted"
            ),
            "sqlite",
        ) == render(translate("null/equals/null-literal"), "sqlite")

        # ...and stripping the declaration must reject the same action under the same option,
        # so the override above is doing work rather than being quietly equivalent.
        with pytest.raises(UnsupportedPlanError, match="null operand"):
            translate(
                "null/equals/null-literal",
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )


class TestOperatorOverrides:
    """The override mechanism itself, which no policy shape can vary.

    The corpus drives one set of overrides -- the collection macros the adapter has no
    portable translation for. A *different* set is not a hostile CEL shape, it is a different
    call, so it is asserted here.
    """

    def test_an_override_replaces_the_default_lowering_for_its_operator(self):
        # The README's own example: PostgreSQL users preferring `= ANY (...)` to `IN`.
        action = "null/in/missing-attribute-in-multi-element-list"
        attr_map = {
            "request.resource.attr.aOptionalString": AdvResource.a_optional_string
        }

        # The map holds only the one attribute the action reaches, so the corpus's
        # per-attribute NULL declarations — which name attributes this map does not carry —
        # go with it.
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
        # Dropping it would emit a filter that answers a different question from the policy.
        with pytest.raises(KeyError, match="Attribute does not exist"):
            translate(
                "string/equals/case-sensitive",
                attr_map={},
                attribute_null_representation=None,
            )

    # The ledger classifies a case against ONE mapping, so "unsupported" there means "this
    # adapter refuses it with these overrides", not "no caller can translate it". Each test
    # below asserts both halves: the refusal without the override, and what it buys.

    def test_a_matches_override_admits_the_regex_the_corpus_refuses(self):
        # SQL dialect regex engines do not guarantee CEL/RE2 semantics, so the adapter has no
        # default lowering. An application whose database is known to agree may supply one.
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
        # The corpus reads this index through its `collection_columns` declaration (#227).
        # Storage of any other shape still has the override: here a scalar column standing for
        # a single-element list. Without either, the undeclared storage is refused by name
        # rather than read as though it were ordered.
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
    """``collection_columns``, which the corpus structurally cannot vary (#227).

    The corpus mapping declares its collections as JSON documents. A second storage shape, a
    missing declaration, a declaration the adapter must refuse and the precedence the
    declaration takes are properties of the caller's argument, not of a plan, so they are
    asserted here. The harness's PostgreSQL legs execute both storage shapes.
    """

    def test_a_declaration_takes_precedence_over_the_size_override(self):
        # The corpus's overrides include `size`, which counts a relation. The declaration names
        # the attribute and the override only the operator, so the declaration wins -- and this
        # is the assertion that says the harness's size shapes run through the declared storage
        # at all, rather than through the relation count they used before it existed.
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
                "(to_jsonb(adversarial_resource.tag_names_array) -> 0) "
                "= to_jsonb(CAST(%(param_1)s AS TEXT))",
            ),
            (
                "collection/index/first-element-of-string-list-equals-null",
                "jsonb_typeof((to_jsonb(adversarial_resource.tag_names_array) -> 0)) "
                "= 'null'",
            ),
        ],
    )
    def test_a_pg_array_is_read_by_position_through_to_jsonb(self, action, rendered):
        # `to_jsonb`, never `array[i + 1]`: the harness rebases every array to start at 0, so an
        # adapter that assumed PostgreSQL's default lower bound would read the wrong element.
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
        # Membership reads the declaration only for an attribute `attr_map` does not map, so
        # declaring storage for `size()` and `index` never moves a relation marker's membership.
        # The corpus maps neither number nor boolean list, so the mapped side is caller-only.
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
        # The position is inline SQL rather than a bind, so it has to reach the statement
        # cache key -- or `tagNames[1]` would be served the SQL compiled for `tagNames[0]`.
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
        # The SQL is chosen per dialect at compile time, because `get_query` is never told the
        # dialect. One it has no rendering for fails there, rather than getting another's SQL.
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
        # LENGTH() of a JSON column is the length of its text: a number, and the wrong one.
        with pytest.raises(UnsupportedPlanError, match="needs its storage declared"):
            translate(
                "size/greater-than/collection-above-one",
                attr_map={"request.resource.attr.tags": AdvResource.tags_json},
                operator_override_fns=None,
                attribute_null_representation=None,
                collection_columns=None,
            )

    def test_a_declared_column_must_be_addressable_like_a_mapped_one(self):
        # A column on another table needs a `table_mapping` join, exactly as an `attr_map`
        # entry does; the declaration is not a way around that validation.
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
    """The one operand a golden file cannot pin, and why the harness substitutes nanoseconds.

    The corpus records the folded ``now() - duration("24h")`` as ``__NOW_MINUS_24H__``, so a
    reader has to choose a value, and here that choice is load-bearing. The PDP emits
    NANOSECOND precision, which is the entire reason the ledger lists both relative-window
    cases as unsupported. A tidy microsecond substitution would translate cleanly and quietly
    contradict the ledger.
    """

    CASES = [
        "timestamp/less-than/relative-window",
        "timestamp/greater-than/relative-window-value-first",
    ]

    @pytest.mark.parametrize("case_id", CASES)
    def test_the_refusal_is_the_precision_and_not_the_shape(self, case_id):
        with pytest.raises(UnsupportedPlanError, match="precision"):
            translate(case_id)

        # The same plan at microsecond precision translates. Both directions matter: the
        # refusal is real, and it is a property of the instant rather than of the shape.
        statement, _params = render(
            translate(case_id, planned_at="2026-08-11T09:13:39.123456Z"), "sqlite"
        )
        assert "adversarial_resource.created_at" in statement

    def test_excess_fractional_digits_are_accepted_only_when_they_are_zero(self):
        # CEL's instant range is exact to the microsecond and no further, so trailing zeroes
        # are information-free and a non-zero digit is a value the column cannot hold.
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
        # Each is refused rather than coerced: a datetime parsed leniently would compare
        # against the column as some OTHER instant, which is a filter returning rows the PDP
        # denies rather than an error the caller can see.
        with pytest.raises(
            UnsupportedPlanError, match="RFC-3339|precision|instant range|offset"
        ):
            translate(self.CASES[0], planned_at=value)


#: A constant zero denominator recorded as the INTEGER ``0``, which a JSON plan cannot sign.
INTEGER_ZERO_DIVISOR = "comparison/greater-than/infinity-from-ternary"


class TestTransportDecoding:
    """``get_query`` accepts both SDK clients' responses; the harness replays HTTP-shaped plans.

    Re-encoding each golden plan into the gRPC client's protobuf response keeps the
    ``MessageToDict`` arm executed. It cannot stand in for a real gRPC frame: JSON can lose
    what the two transports disagree about (cerbos/query-plan-adapters#321).
    """

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
        # The golden records `-0.0`, which json.loads keeps as a signed float, so the adapter
        # knows which infinity CEL produced.
        statement, _params = render(
            translate("arithmetic/divide/negative-zero-divisor"), "sqlite"
        )
        assert "adversarial_resource.a_number" in statement

    def test_an_integer_zero_divisor_is_refused_over_json_only(self):
        # Cerbos's HTTP API renders -0.0 as `-0`, which json.loads returns as the INT 0, so an
        # integer zero cannot say whether CEL produced +Infinity or -Infinity (#312).
        with pytest.raises(UnsupportedPlanError, match="sign is indeterminate"):
            translate(INTEGER_ZERO_DIVISOR)

        # Re-encoded into protobuf, the same value widens to a POSITIVE double, so the guard
        # does not fire. That is not evidence that gRPC makes this shape supportable: the
        # sign was decided by the JSON, not by the transport.
        statement, _params = render(
            translate(
                INTEGER_ZERO_DIVISOR,
                plan=grpc_plan_from_golden(golden_case(INTEGER_ZERO_DIVISOR)),
            ),
            "sqlite",
        )
        assert "adversarial_resource" in statement


class TestDeclarativeBaseModels:
    """The SQLAlchemy 2.0 ``DeclarativeBase`` arm of ``GenericTable``.

    Its metaclass sits outside ``DeclarativeMeta``, so it is a different code path in
    ``get_query``. The twins map the SAME tables, so every case must emit the same SQL.
    """

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
        differing = [
            case["id"]
            for case in CURRENT
            if _translated_or_refused(lambda: render(translate(case["id"]), "sqlite"))
            != _translated_or_refused(
                lambda: render(translate(case["id"], **modern), "sqlite")
            )
        ]
        assert differing == []


def test_every_null_carrying_plan_is_refused_under_omitted():
    # #302. Under a call-level "omitted" with no per-attribute declarations, every plan
    # carrying a null literal must be refused. The rejection keys off the null OPERAND, not an
    # operator list: `hasIntersection(tagNames, ["public", null])` carries one in its value
    # list, and an allowlist of eq/ne/in would miss it.
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
    # PostgreSQL parses 'NaN' and 'Infinity' as double precision inputs and every comparison
    # against them is false -- the same rows a folded translation returns -- so only the
    # parameter list tells them apart. The harness executes PostgreSQL only for the cases that
    # read a declared collection, so this is stated here over every case and both dialects.
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
    # `UnsupportedPlanError` is a `ValueError`, which every translation refusal raised before
    # it existed; the one that raised `TypeError` (a relation marker no override consumes)
    # is both, so no existing handler stops catching it.
    with pytest.raises(UnsupportedPlanError) as refused:
        translate("regex/matches/anchored-prefix")
    assert isinstance(refused.value, ValueError)

    with pytest.raises(UnsupportedPlanError, match="operator override") as refused:
        translate("comparison/equals/whole-list-literal")
    assert isinstance(refused.value, TypeError)
