# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Translator unit test: the SQL this adapter emits for every corpus action. Offline.

Plans come from ``conformance/wire-fixtures/`` (ADR 0006). Expected SQL lives in
``golden/expectations.json``; rewrite it with ``pdm run golden:update`` and review the diff.
Refusals and their messages come from ``conformance/actions.json``. Every wire fixture must
be either a golden entry or a pinned throw, so a new corpus action fails here until handled.
"""

import json
import math
import os
import re
from datetime import datetime, timezone
from typing import ClassVar

import pytest
from corpus import (
    ADAPTER,
    ATTR_MAP,
    ATTRIBUTE_NULL_REPRESENTATION,
    COLLECTION_COLUMNS,
    GOLDEN_DIALECTS,
    GOLDEN_FILE,
    GOLDEN_REGENERATE_COMMAND,
    GOLDEN_SQLALCHEMY_MAJOR,
    INSTALLED_SQLALCHEMY_MAJOR,
    OPERATOR_OVERRIDES,
    PG_ARRAY_COLLECTION_COLUMNS,
    AdvResource,
    AdvTag,
    classify_actions_for_adapter,
    declared_columns,
    grpc_plan_from_wire_fixture,
    json_parameter,
    null_representation_throws,
    parse_actions_file,
    plan_from_wire_fixture,
    read_corpus_json,
    read_golden_expectations,
    render,
    require_message,
    statement_from,
    statement_preamble,
    where_clause,
    wire_fixture_actions,
    write_golden_expectations,
)
from sqlalchemy import any_, exists, literal, select
from sqlalchemy.exc import CompileError

from cerbos_sqlalchemy import CollectionColumn, get_query

ACTIONS_FILE = parse_actions_file(read_corpus_json("actions.json"))

# Actions this adapter must refuse, with their pinned messages. They get no golden entry:
# the message already lives in `actions.json`.
THROWING_ACTIONS = classify_actions_for_adapter(ACTIONS_FILE, ADAPTER).throwing_actions
THROWING = {action for action, _ in THROWING_ACTIONS}

# Not in THROWING: `null-eq-missing` translates under the default option and has a golden
# entry. Only the "omitted" option refuses it (see TestNullAttributeRepresentation).
NULL_REPRESENTATION_OMITTED = null_representation_throws(ACTIONS_FILE, ADAPTER)


def translate(
    action,
    *,
    planned_at=None,
    attr_map=None,
    operator_override_fns=OPERATOR_OVERRIDES,
    null_attribute_representation="explicit",
    attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
    collection_columns=COLLECTION_COLUMNS,
    plan=None,
):
    """The ``Select`` this adapter emits for one corpus action."""
    if plan is None:
        plan = (
            plan_from_wire_fixture(action)
            if planned_at is None
            else plan_from_wire_fixture(action, planned_at)
        )
    return get_query(
        plan,
        AdvResource,
        ATTR_MAP if attr_map is None else attr_map,
        operator_override_fns=operator_override_fns,
        null_attribute_representation=null_attribute_representation,
        attribute_null_representation=attribute_null_representation,
        collection_columns=collection_columns,
    )


def expectation_for(action):
    """The translator output for one action, in the golden file's shape.

    Both dialects must bind the same parameters, so the file records one parameter map.
    """
    query = translate(action)
    clauses = {}
    parameters = None
    for name in GOLDEN_DIALECTS:
        statement, raw = render(query, name)
        encoded = {
            key: json_parameter(f"{action} ({name}) {key}", value)
            for key, value in raw.items()
        }
        if parameters is None:
            parameters = encoded
        elif encoded != parameters:
            raise AssertionError(
                f"{action} binds different parameters per dialect: {parameters} vs {encoded}"
            )
        clauses[name] = where_clause(statement)
    return {"where": clauses, "params": parameters}


# -- the golden expectations ------------------------------------------------
#
# `pdm run golden:update` sets GOLDEN_UPDATE=1 and rewrites the file. CI never sets it,
# so a change in emitted SQL always fails CI until the file is regenerated.

if os.environ.get("GOLDEN_UPDATE") == "1":
    write_golden_expectations(
        {
            # Throwing actions get no entry. A misclassified one then fails either the
            # throw test or regeneration, instead of being recorded.
            action: expectation_for(action)
            for action in wire_fixture_actions()
            if action not in THROWING
        }
    )

RECORDED = read_golden_expectations()
RECORDED_ACTIONS = list(RECORDED)

#: The actions whose emitted statement carries no WHERE clause at all.
UNCONDITIONAL_ACTIONS = [
    action
    for action in RECORDED_ACTIONS
    if all(
        clause is None for clause in RECORDED[action]["expectation"]["where"].values()
    )
]
CONDITIONAL_ACTIONS = [a for a in RECORDED_ACTIONS if a not in UNCONDITIONAL_ACTIONS]


def recorded_statement(action, dialect_name):
    """The statement the asset pins for one action, reassembled around the preamble."""
    return statement_from(RECORDED[action]["expectation"]["where"][dialect_name])


#: Every emitted ``(statement, parameters)``, compiled once per action and dialect.
#: The rules below read these, not the golden file, so they hold on both SQLAlchemy majors.
EMITTED = {
    action: {name: render(translate(action), name) for name in GOLDEN_DIALECTS}
    for action in RECORDED_ACTIONS
}


def emitted_statement(action, dialect_name):
    return EMITTED[action][dialect_name][0]


def emitted_parameters():
    """``(action, dialect, key, value)`` for every parameter the corpus binds."""
    for action, per_dialect in EMITTED.items():
        for name, (_statement, params) in per_dialect.items():
            for key, value in params.items():
                yield action, name, key, value


#: Actions SQLAlchemy 1.4 compiles differently from 2.x, from the same expression tree.
#: 2.x parenthesises ``(a || b) = ?`` and adds SQLite's ``+ 0.0`` to float division.
#: The adversarial suite checks their rows on both majors. Asserted in both directions.
RENDERING_DIFFERS_ON_SQLALCHEMY_14 = (
    "arith-div",
    "arith-div-frac",
    "concat-f2f",
    "cr-contains",
    "cr-div-other-column",
    "cr-div-then-add",
    "cr-div-then-add-ne",
    "cr-div-zero",
    "cr-div-zero-eq-neg",
    "cr-div-zero-ne",
    "cr-endswith",
    "cr-startswith",
    "cr-startswith-concat",
    "f2f-contains",
    "f2f-endswith",
    "f2f-startswith",
    "id-concat",
    "id-concat-vf",
    "not-concat-unsolvable",
    "not-concat-unsolvable-ne",
    "not-contains",
    "not-startswith",
    "p-lambda-f2f-like",
)


class TestCorpusShapes:
    @pytest.mark.parametrize("action", RECORDED_ACTIONS)
    def test_emits_the_golden_expectation(self, action):
        emitted = expectation_for(action)
        recorded = RECORDED[action]["expectation"]

        if (
            INSTALLED_SQLALCHEMY_MAJOR != GOLDEN_SQLALCHEMY_MAJOR
            and action in RENDERING_DIFFERS_ON_SQLALCHEMY_14
        ):
            # Asserted, not skipped, so an action that stops diverging fails.
            assert emitted != recorded
            return

        # Plain `==` first for a readable diff.
        assert emitted == recorded
        # JSON second: `==` treats 3 and 3.0 as equal, and int vs float matters here (`-0`).
        assert json.dumps(emitted, sort_keys=True) == json.dumps(
            recorded, sort_keys=True
        )

    # Match the message, or any unrelated error would pass (#326).
    @pytest.mark.parametrize("action,message", THROWING_ACTIONS)
    def test_is_refused_with_the_message_actions_json_pins(self, action, message):
        with pytest.raises((ValueError, KeyError, TypeError), match=re.escape(message)):
            translate(action)

    def test_throwing_action_with_no_pinned_message_fails_classification(self):
        # A throwing action without a pinned message must fail, not assert a bare raise.
        for absent in (None, "", 42):
            with pytest.raises(AssertionError, match="pins no throw message"):
                require_message("synthetic-entry", absent)

    def test_every_corpus_action_is_accounted_for_here_exactly_once(self):
        classified = sorted(
            RECORDED_ACTIONS + [action for action, _ in THROWING_ACTIONS]
        )

        # Total: every fixture is either a golden entry or a pinned throw.
        assert classified == wire_fixture_actions()
        # Disjoint: no action is both.
        assert classified == sorted(set(classified))
        # Sorted, so diffs stay readable.
        assert RECORDED_ACTIONS == sorted(RECORDED_ACTIONS)

        # Tripwires. Bump deliberately.
        assert {
            "conditional": len(CONDITIONAL_ACTIONS),
            "unconditional": len(UNCONDITIONAL_ACTIONS),
            "throwing": len(THROWING_ACTIONS),
        } == {"conditional": 264, "unconditional": 3, "throwing": 57}

    def test_the_asset_declares_the_compiler_that_wrote_it(self):
        # The other major needs this header to know it should assert the divergence list.
        with open(GOLDEN_FILE, encoding="utf-8") as f:
            assert json.load(f)["sqlalchemy"] == GOLDEN_SQLALCHEMY_MAJOR
        assert INSTALLED_SQLALCHEMY_MAJOR in ("1.4", "2.x")

    @pytest.mark.skipif(
        INSTALLED_SQLALCHEMY_MAJOR == GOLDEN_SQLALCHEMY_MAJOR,
        reason="the divergence set is empty on the major the asset was generated under",
    )
    def test_only_the_pinned_shapes_render_differently_on_the_other_major(self):
        # Stops the divergence list from growing silently.
        differing = sorted(
            action
            for action in RECORDED_ACTIONS
            if expectation_for(action) != RECORDED[action]["expectation"]
        )
        assert differing == sorted(RENDERING_DIFFERS_ON_SQLALCHEMY_14)

    def test_every_shape_the_compilers_disagree_on_is_still_proved_by_the_oracle(self):
        # The divergence list is safe only because the oracle still checks these actions'
        # rows on both majors.
        oracle = set(classify_actions_for_adapter(ACTIONS_FILE, ADAPTER).oracle_actions)
        assert [
            action
            for action in RENDERING_DIFFERS_ON_SQLALCHEMY_14
            if action not in oracle
        ] == []

    def test_the_unconditional_action_is_the_planner_fold_the_corpus_declares(self):
        # `p-has` is a known planner divergence folded to ALWAYS_ALLOWED. Pinning the list
        # catches a translation that silently stops emitting a filter.
        assert UNCONDITIONAL_ACTIONS == ["p-has", "pv-empty-all", "pv-empty-not-exists"]
        assert "p-has" in ACTIONS_FILE.skipped_divergences(ADAPTER)


class TestWhatTheEmittedStatementContains:
    """Rules over every emitted statement that a careless regeneration can't hide.

    Each reads the live output, not the golden file, and has an anti-vacuity check.
    """

    def test_every_statement_is_the_corpus_select_plus_a_where_clause(self):
        # Recording only the WHERE clause is lossless only while this holds. It also
        # catches an unexpected join.
        preamble = statement_preamble()
        for action in RECORDED_ACTIONS:
            for name in GOLDEN_DIALECTS:
                assert emitted_statement(action, name).startswith(preamble), action
        assert preamble.endswith("FROM adversarial_resource")
        assert len(RECORDED_ACTIONS) > 0

    @pytest.mark.skipif(
        INSTALLED_SQLALCHEMY_MAJOR != GOLDEN_SQLALCHEMY_MAJOR,
        reason="the asset's bytes are one compiler's; the divergence set is asserted instead",
    )
    def test_a_recorded_where_clause_reassembles_into_the_statement_that_produced_it(
        self,
    ):
        # Each entry must reassemble into exactly the emitted statement.
        for action in RECORDED_ACTIONS:
            for name in GOLDEN_DIALECTS:
                assert recorded_statement(action, name) == emitted_statement(
                    action, name
                ), f"{action} ({name})"

    def test_the_resource_table_is_named_in_exactly_one_from_clause(self):
        # A subquery that lost its correlation names the outer table in its own FROM
        # and silently cross-joins every row.
        offenders = [
            f"{action} ({name})"
            for action in RECORDED_ACTIONS
            for name in GOLDEN_DIALECTS
            if _from_clauses_naming_the_resource(emitted_statement(action, name)) != 1
        ]
        assert offenders == []
        # Anti-vacuity: the corpus still emits subqueries...
        for action in ("w1-all-chain", "rel-bool-hop2", "exists-on-empty"):
            assert "(SELECT" in emitted_statement(action, "sqlite"), action
        # ...and the detector catches a real uncorrelated subquery, which renders as a
        # comma-joined FROM list.
        uncorrelated = select(AdvResource).where(
            exists(
                select(literal(1))
                .where(AdvTag.resource_id == AdvResource.id)
                .correlate(None)
            )
        )
        statement, _params = render(uncorrelated, "sqlite")
        assert "FROM adversarial_tag, adversarial_resource" in statement
        assert _from_clauses_naming_the_resource(statement) == 2

    def test_every_like_carries_an_escape_clause(self):
        # Without ESCAPE, the escaped `%` and `_` would be misread (#258/#259).
        unescaped = []
        with_like = 0
        for action in CONDITIONAL_ACTIONS:
            for name in GOLDEN_DIALECTS:
                statement = emitted_statement(action, name)
                likes = statement.count(" LIKE ")
                if not likes:
                    continue
                with_like += 1
                if likes != statement.count(" ESCAPE "):
                    unescaped.append((action, name))
        assert unescaped == []
        # Anti-vacuity.
        assert with_like > 0

    def test_every_qualified_identifier_names_a_column_the_schema_declares(self):
        # Catches a mapping that points at a column the schema doesn't declare.
        declared = set(declared_columns())
        stray = set()
        for action in RECORDED_ACTIONS:
            for name in GOLDEN_DIALECTS:
                for identifier in re.findall(
                    r"\badversarial_\w+\.\w+\b", emitted_statement(action, name)
                ):
                    if identifier not in declared:
                        stray.add(f"{action}: {identifier}")
        assert sorted(stray) == []
        assert len(declared) > 0

    def test_no_action_binds_a_non_finite_number(self):
        # A bound NaN/Infinity can return the same rows as a folded one on PostgreSQL,
        # so only the parameters reveal it.
        offenders = [
            f"{action} ({name}) {key}={value}"
            for action, name, key, value in emitted_parameters()
            if isinstance(value, float) and not math.isfinite(value)
        ]
        assert offenders == []

    def test_the_corpus_still_drives_the_folds_that_rule_polices(self):
        # Anti-vacuity for the rule above. `cr-div-neg-zero` and `nan-ord-inf` are absent
        # because this adapter refuses them.
        for action in (
            "nan-ord-le",
            "nan-ord-ternary",
            "nan-ord-ternary-vf",
            "cr-div-zero",
            "cr-div-other-column",
        ):
            assert action in CONDITIONAL_ACTIONS

    def test_the_actions_that_bind_a_datetime_the_asset_records_as_a_string(self):
        # The golden file stores a datetime as ISO-8601 text, so pin which actions bind one
        # and check no string parameter looks like an instant.
        instants = {
            action
            for action, _name, _key, value in emitted_parameters()
            if isinstance(value, datetime)
        }
        ambiguous = [
            f"{action} ({name}) {key}={value}"
            for action, name, key, value in emitted_parameters()
            if isinstance(value, str) and _looks_like_an_instant(value)
        ]
        assert sorted(instants) == ["ts-eq", "ts-eq-offset", "ts-ne"]
        assert ambiguous == []


def _from_clauses_naming_the_resource(statement):
    """How many of a statement's FROM lists name the resource table.

    Parses comma-joined lists, which is how an uncorrelated subquery pulls the table in.
    """
    return sum(
        "adversarial_resource" in clause.split(", ")
        for clause in re.findall(r"FROM ([a-z_]+(?:, [a-z_]+)*)", statement)
    )


def _null_omitted_message(action):
    """The message ``actions.json`` pins for one ``nullRepresentationOmitted`` action."""
    return next(
        message
        for candidate, _reason, message in NULL_REPRESENTATION_OMITTED
        if candidate == action
    )


def _looks_like_an_instant(value):
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


class TestNullAttributeRepresentation:
    """``null_attribute_representation``: the planner emits the same plan for both options.

    So the caller must say which one applies, and the SQL depends only on that choice.
    """

    ACTION = "null-eq-missing"
    MESSAGE = _null_omitted_message(ACTION)

    def test_explicit_emits_an_is_null_filter(self):
        statement, _params = render(
            translate(self.ACTION, null_attribute_representation="explicit"), "sqlite"
        )
        assert "adversarial_resource.a_optional_string IS NULL" in statement

    def test_omitted_refuses_the_same_plan(self):
        # Here a NULL column means no attribute, which check() denies (#302).
        with pytest.raises(ValueError, match=re.escape(self.MESSAGE)):
            translate(self.ACTION, null_attribute_representation="omitted")

    def test_a_per_attribute_declaration_overrides_the_call_level_option(self):
        # `owner` is declared "explicit", so `null-eq` still translates (#308)...
        assert render(
            translate("null-eq", null_attribute_representation="omitted"), "sqlite"
        ) == render(translate("null-eq"), "sqlite")

        # ...and without the declaration it is refused.
        with pytest.raises(ValueError, match="null operand"):
            translate(
                "null-eq",
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )


class TestOperatorOverrides:
    """Caller-supplied operator overrides, which the corpus cannot vary.

    The corpus uses one fixed override set; another set is a different call.
    """

    def test_an_override_replaces_the_default_lowering_for_its_operator(self):
        # The README's example: `= ANY (...)` instead of `IN` on PostgreSQL.
        action = "p-in-null-multi"
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
            translate("cs-eq", attr_map={}, attribute_null_representation=None)

    # "Unsupported" in actions.json means unsupported with the corpus's overrides.
    # The README documents caller overrides for these; each test checks both halves.

    def test_a_matches_override_admits_the_regex_the_corpus_refuses(self):
        # SQL regex engines aren't RE2, so there is no default. A caller may supply one.
        action = "p-matches"
        with pytest.raises(ValueError, match="Unrecognised operator: matches"):
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
        action = "index-scalar-list"
        undeclared = {
            "attr_map": {"request.resource.attr.tagNames": AdvResource.a_string},
            "attribute_null_representation": None,
            "collection_columns": None,
        }
        with pytest.raises(
            ValueError,
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
    """``collection_columns``, a caller argument the corpus cannot vary (#227).

    The corpus declares JSON storage. Other shapes and invalid declarations are tested here;
    the harness's PostgreSQL leg executes the ``pgArray`` renderings.
    """

    def test_a_declaration_takes_precedence_over_the_size_override(self):
        # The declaration names the attribute, so it beats the `size` override, which
        # would count the relation.
        declared, _ = render(translate("size-threshold"), "sqlite")
        overridden, _ = render(
            translate("size-threshold", collection_columns=None), "sqlite"
        )

        assert "json_array_length(adversarial_resource.tags_json)" in declared
        assert "count(*)" not in declared
        assert "count(*)" in overridden

    @pytest.mark.parametrize(
        "action,rendered",
        [
            (
                "size-threshold",
                "jsonb_array_length(to_jsonb(adversarial_resource.tags_array))",
            ),
            (
                "index-scalar-list",
                (
                    "(to_jsonb(adversarial_resource.tag_names_array) -> 0) "
                    "= to_jsonb(CAST(%(param_1)s AS TEXT))"
                ),
            ),
            (
                "index-scalar-list-null",
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
        assert "[" not in where_clause(statement)

    def test_a_pg_array_answers_membership_through_to_jsonb(self):
        statement, _ = render(
            translate(
                "hasint-number-list-vs-string",
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
        attr_map = {
            **ATTR_MAP,
            "request.resource.attr.aNumberList": AdvResource.a_number_list_json,
        }
        mapped, _ = render(translate("in-number-list", attr_map=attr_map), "sqlite")
        declared, _ = render(translate("in-number-list"), "sqlite")

        assert "json_each(adversarial_resource.a_number_list_json)" in declared
        assert "json_each" not in mapped

    def test_two_positions_do_not_share_a_cached_statement(self):
        # The position is inlined, so it must be in the cache key.
        first = translate("index-scalar-list").whereclause
        second = translate("index-not-oob").whereclause
        assert first._generate_cache_key() != second._generate_cache_key()

    def test_a_pg_array_does_not_render_on_sqlite(self):
        with pytest.raises(CompileError, match='"pgArray" requires a PostgreSQL array'):
            render(
                translate(
                    "size-threshold", collection_columns=PG_ARRAY_COLLECTION_COLUMNS
                ),
                "sqlite",
            )

    def test_a_dialect_it_was_not_written_for_is_refused_at_compile_time(self):
        # `get_query` doesn't know the dialect, so an unsupported one fails at compile time.
        from sqlalchemy.dialects import mysql

        with pytest.raises(CompileError, match="renders only on SQLite and PostgreSQL"):
            translate("index-scalar-list").compile(dialect=mysql.dialect())

    def test_str_of_a_query_still_renders_for_debugging(self):
        assert "cerbos_collection_size(" in str(translate("size-threshold"))

    def test_an_undeclared_collection_column_is_refused_rather_than_measured(self):
        # LENGTH() of a JSON column would measure its text.
        with pytest.raises(ValueError, match="needs its storage declared"):
            translate(
                "size-threshold",
                attr_map={"request.resource.attr.tags": AdvResource.tags_json},
                operator_override_fns=None,
                attribute_null_representation=None,
                collection_columns=None,
            )

    def test_a_declared_column_must_be_addressable_like_a_mapped_one(self):
        # A column on another table needs `table_mapping`, as in `attr_map`.
        with pytest.raises(TypeError, match="table_mapping"):
            translate(
                "size-threshold",
                collection_columns={
                    "request.resource.attr.tags": CollectionColumn(AdvTag.name, "json")
                },
            )

    def test_the_declaration_is_validated(self):
        with pytest.raises(ValueError, match="storage must be 'json' or 'pgArray'"):
            CollectionColumn(AdvResource.tags_json, "jsonb")
        with pytest.raises(TypeError, match="must be CollectionColumn"):
            translate(
                "size-threshold",
                collection_columns={
                    "request.resource.attr.tags": (AdvResource.tags_json, "json")
                },
            )


class TestTimestampLiterals:
    """Timestamp literals, and why ``PLANNED_AT`` has nanosecond precision.

    The PDP emits nanoseconds, which this adapter refuses; that is why ``ts-window`` and
    ``ts-vf`` are unsupported. A coarser placeholder would hide the refusal.
    """

    @pytest.mark.parametrize("action", ["ts-window", "ts-vf"])
    def test_the_refusal_is_the_precision_and_not_the_shape(self, action):
        message = next(
            pinned for candidate, pinned in THROWING_ACTIONS if candidate == action
        )
        with pytest.raises(ValueError, match=re.escape(message)):
            translate(action)

        # At microsecond precision the same plan translates.
        statement, _params = render(
            translate(action, planned_at="2026-08-11T09:13:39.123456Z"), "sqlite"
        )
        assert "adversarial_resource.created_at" in statement

    def test_excess_fractional_digits_are_accepted_only_when_they_are_zero(self):
        # Trailing zero digits lose nothing; a non-zero one can't be stored.
        statement, params = render(
            translate("ts-window", planned_at="2026-08-11T09:13:39.123456000Z"),
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
        with pytest.raises(ValueError, match="RFC-3339|precision|instant range|offset"):
            translate("ts-window", planned_at=value)


class TestTransportDecoding:
    """The protobuf (gRPC) decoding path, fed the same HTTP wire fixtures.

    JSON fixtures have already lost what the transports disagree on (the sign of ``-0``),
    so this pins the disagreement. Resolving it needs a real gRPC PDP (#321).
    """

    #: Every corpus action whose translation the two decodings agree on completely.
    AGREEING: ClassVar[list[str]] = [
        action
        for action in wire_fixture_actions()
        if action not in {"cr-div-neg-zero", "nan-ord-inf"}
    ]

    @pytest.mark.parametrize("action", AGREEING)
    def test_the_protobuf_decoding_emits_the_same_sql(self, action):
        def build(plan):
            try:
                return render(translate(action, plan=plan), "sqlite")
            except (ValueError, KeyError, TypeError) as exc:
                return f"{type(exc).__name__}: {exc}"

        assert build(grpc_plan_from_wire_fixture(action)) == build(
            plan_from_wire_fixture(action)
        )

    @pytest.mark.parametrize("action", ["cr-div-neg-zero", "nan-ord-inf"])
    def test_a_json_fixture_cannot_carry_the_sign_of_a_zero_into_protobuf(self, action):
        # Both divide by `-0.0`. `json.loads("-0")` gives int 0, so the sign is lost and
        # the HTTP path refuses (#312).
        with pytest.raises(ValueError, match="sign is indeterminate"):
            translate(action, plan=plan_from_wire_fixture(action))

        # Protobuf turns it into +0.0, so the guard doesn't fire. This is a fixture
        # artefact, not evidence that gRPC supports these shapes (#321).
        statement, _params = render(
            translate(action, plan=grpc_plan_from_wire_fixture(action)), "sqlite"
        )
        assert "adversarial_resource" in statement


class TestTheGoldenAsset:
    def test_names_a_command_this_package_actually_defines(self):
        # The golden file names its regenerate command; make sure it exists.
        runner, run, script = GOLDEN_REGENERATE_COMMAND.split(" ")
        assert (runner, run) == ("pdm", "run")
        assert script in _pdm_script_names()


def _pdm_script_names():
    """The keys of ``[tool.pdm.scripts]``.

    Scanned by hand because ``tomllib`` needs Python 3.11 and this package supports 3.8.
    """
    manifest = os.path.join(os.path.dirname(__file__), "..", "pyproject.toml")
    names = []
    inside = False
    with open(manifest, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped.startswith("["):
                inside = stripped == "[tool.pdm.scripts]"
                continue
            if inside and "=" in stripped:
                names.append(stripped.split("=", 1)[0].strip().strip('"'))
    return names
