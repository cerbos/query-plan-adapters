"""Translator unit test: for every action in the shared ``../conformance/`` corpus, the SQL
this adapter emits. Offline — no Cerbos sidecar, no container, no database.

This adapter had no test of this kind at all. Its per-adapter suite planned every shape
against a live PDP loaded with ``/policies/``, executed the query against three seeded rows
and compared the result with a hardcoded count — the weakest oracle any adapter had, and one
that never asserted the emitted filter anywhere. Three of the four assertions that suite
braided together are somebody else's job now, and this file makes only the fourth:

===================================================  ==========================================
assertion                                            who owns it
===================================================  ==========================================
the plan the PDP produces for a policy               ``conformance/wire-fixtures/``, replanned
                                                     and diffed by the ``Conformance Corpus``
                                                     workflow
which shapes this adapter must refuse, and with      ``conformance/actions.json`` — read below,
what message                                         never restated
the rows a filter returns                            ``test_adversarial_conformance.py``,
                                                     against real SQLite with ``check()`` as
                                                     the oracle
**the SQL this adapter emits for a plan**            **here**
===================================================  ==========================================

**The plans are read, not written.** A hand-built plan is a *belief* about what the planner
emits, and this repository keeps golden fixtures because that belief has been wrong before.
See `ADR 0006 <../../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md>`_.
The hand-built plans that remain in ``test_query.py`` are there for shapes no policy can
produce — malformed operands, a mapper the corpus does not use — which is the one thing a
fixture cannot supply.

**The expectations are data, not literals.** The SQL this adapter is pinned to emit lives in
``golden/expectations.json``, a **golden expectation** file this adapter owns — never under
``conformance/``, where every adapter workflow triggers and one adapter re-pinning one
statement would re-run all the others. It is regenerated with ``pdm run golden:update`` and
reviewed as a diff, exactly like the wire fixtures it is asserted against. See
`ADR 0007 <../../docs/adr/0007-adapters-share-data-not-code.md>`_ and the "Golden
expectations" section of ``conformance/README.md``.

**What a pinned statement buys over the harness.** The harness proves the query returns the
right rows *against the rows it seeds*. Two different queries can agree on all 21 of them and
disagree on the row a consumer has, so a rewrite that quietly changes the emitted SQL passes
there and shows up here as a diff a reviewer reads. It is also the only place PostgreSQL —
reasoned about all through ``query.py`` and executed by nothing in this repository — is
rendered at all.

**Adding a corpus action fails this file.** Every wire fixture must be accounted for here
exactly once — a golden expectation or a throw carrying the message ``actions.json`` pins —
and the completeness guard below is what makes a new action land as a failure rather than as
silence.
"""

import json
import math
import os
import re
from datetime import datetime, timezone

import pytest
from corpus import (
    ADAPTER,
    ATTR_MAP,
    ATTRIBUTE_NULL_REPRESENTATION,
    GOLDEN_DIALECTS,
    GOLDEN_FILE,
    GOLDEN_REGENERATE_COMMAND,
    GOLDEN_SQLALCHEMY_MAJOR,
    INSTALLED_SQLALCHEMY_MAJOR,
    OPERATOR_OVERRIDES,
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

from cerbos_sqlalchemy import get_query
from sqlalchemy import any_, exists, literal, select

ACTIONS_FILE = parse_actions_file(read_corpus_json("actions.json"))

# The shapes `actions.json` says this adapter must refuse, each with the message it must
# refuse them with. Identical to the classification `test_adversarial_conformance.py` asserts
# against a live PDP; asserting it here as well is what lets the completeness guard below be
# total, and it costs a millisecond rather than a container.
#
# A throwing action needs no golden expectation of its own: the message is already corpus
# data, pinned once in `actions.json` and read by every adapter. Writing it into this
# adapter's asset too would create two places to change one string with nothing to say which
# is authoritative.
THROWING_ACTIONS = classify_actions_for_adapter(ACTIONS_FILE, ADAPTER).throwing_actions
THROWING = {action for action, _ in THROWING_ACTIONS}

# `nullRepresentationOmitted` is NOT in that list: under the default representation this
# adapter translates `null-eq-missing` into an `IS NULL` filter, so it carries a golden entry
# like any other action. Its refusal is a property of the flipped option, asserted on its own
# below.
NULL_REPRESENTATION_OMITTED = null_representation_throws(ACTIONS_FILE, ADAPTER)


def translate(
    action,
    *,
    planned_at=None,
    attr_map=None,
    operator_override_fns=OPERATOR_OVERRIDES,
    null_attribute_representation="explicit",
    attribute_null_representation=ATTRIBUTE_NULL_REPRESENTATION,
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
    )


def expectation_for(action):
    """The whole translator output for one corpus action, in the shape the golden file records.

    The two dialects are checked to bind the SAME parameters before either is recorded, so the
    asset carries one parameter map rather than two copies of it. That is not a space saving
    dressed up as a rule: a dialect that started binding differently would be a translation
    difference the SQL text alone might not show, and it fails regeneration here rather than
    arriving as an unexplained diff.
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
# `pdm run golden:update` rewrites the file from what the translator emits today and preserves
# every `note`. That is the same deliberate act as regenerating the wire fixtures, and the
# safety is identical: the diff is what a reviewer reads. CI never sets the variable, so a
# translator change that moves the emitted SQL fails there whatever anyone ran locally.

if os.environ.get("GOLDEN_UPDATE") == "1":
    write_golden_expectations(
        {
            # A throwing action gets no entry: its message is corpus data. Skipping it here is
            # also what keeps regeneration from papering over a misclassification — an action
            # moved into `adapterUnsupported` that this adapter still translates fails the
            # throw suite, and one moved out of it that this adapter still refuses fails
            # regeneration itself.
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


#: Every emitted statement and its bound parameters, compiled once per action per dialect.
#:
#: The rules below read these rather than the pinned bytes, so each holds on both majors CI
#: runs. The asset is a snapshot of what one compiler produced; a rule is about what the
#: adapter emits, and only one of those two is the same on 1.4 and 2.x.
#:
#: Parameters are kept alongside the statements rather than recompiled per rule: the loop is
#: the same shape every time (translate, compile both dialects, walk the result) and running
#: it once means a rule reads a scan rather than restating it.
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


#: Corpus actions SQLAlchemy 1.4's compiler renders differently from 2.x's, from the SAME
#: expression tree — which is why they are pinned as a list rather than as a second asset.
#:
#: Two compiler changes account for every one of them. 2.x parenthesises a concatenation used
#: as a comparison operand (``(a || b) = ?`` where 1.4 emits ``a || b = ?``), and 2.x adds
#: SQLite's ``+ 0.0`` float-division coercion around a ``nullif`` denominator. Neither is a
#: translation decision: `test_adversarial_conformance.py` runs the same corpus against a real
#: PDP on both majors, and every one of these actions is an oracle comparison there.
#:
#: The list is asserted in BOTH directions below, so an action that stops diverging fails just
#: as loudly as one that starts.
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
            # Asserted, not skipped: an action that stops diverging has to fail here so the
            # list above shrinks deliberately rather than rotting into a permanent exemption.
            assert emitted != recorded
            return

        # The readable comparison first, so a failure prints the clause that moved...
        assert emitted == recorded
        # ...and the encoded one second, because `==` cannot tell 3 from 3.0 in Python and the
        # int/float distinction is exactly what the HTTP transport's `-0` hazard turns on.
        assert json.dumps(emitted, sort_keys=True) == json.dumps(
            recorded, sort_keys=True
        )

    # The message, not just the raise: a mapper typo or an unrelated validation satisfies a
    # bare `pytest.raises` just as well as the limitation the corpus documents (#326). The
    # harness makes the same assertion against a live PDP; here it costs a millisecond, which
    # is what lets the completeness guard below be total.
    @pytest.mark.parametrize("action,message", THROWING_ACTIONS)
    def test_is_refused_with_the_message_actions_json_pins(self, action, message):
        with pytest.raises((ValueError, KeyError, TypeError), match=re.escape(message)):
            translate(action)

    def test_throwing_action_with_no_pinned_message_fails_classification(self):
        # Adding a throwing action without pinning its message must fail this suite rather
        # than silently degrade the throw assertions to a bare "it raised" (#326).
        for absent in (None, "", 42):
            with pytest.raises(AssertionError, match="pins no throw message"):
                require_message("synthetic-entry", absent)

    def test_every_corpus_action_is_accounted_for_here_exactly_once(self):
        classified = sorted(
            RECORDED_ACTIONS + [action for action, _ in THROWING_ACTIONS]
        )

        # Total: a corpus action with no golden expectation and no pinned throw lands as a
        # failure rather than as silence. This is the assertion that makes the asset
        # self-maintaining — adding a hostile shape to the corpus forces someone to look at
        # the SQL this adapter emits for it, and `golden:update` refuses to invent one for a
        # shape that raises.
        assert classified == wire_fixture_actions()
        # Disjoint: an action carrying a golden expectation AND declared unsupported would
        # satisfy the union above while asserting two contradictory things.
        assert classified == sorted(set(classified))
        # The asset is written sorted, so a translator change reads as the list of shapes it
        # moved.
        assert RECORDED_ACTIONS == sorted(RECORDED_ACTIONS)

        # Tripwires. Bump them deliberately: a count that moves without anyone noticing is how
        # a shape gets dropped from an asset nobody reads end to end.
        assert {
            "conditional": len(CONDITIONAL_ACTIONS),
            "unconditional": len(UNCONDITIONAL_ACTIONS),
            "throwing": len(THROWING_ACTIONS),
        } == {"conditional": 179, "unconditional": 1, "throwing": 19}

    def test_the_asset_declares_the_compiler_that_wrote_it(self):
        # The asset is one compiler's rendering of the adapter's expression trees, and the two
        # SQLAlchemy majors CI runs do not agree on all of them. Recording which one wrote it
        # is what lets the other major assert a pinned divergence set instead of failing on 21
        # shapes for a reason that has nothing to do with translation.
        with open(GOLDEN_FILE, encoding="utf-8") as f:
            assert json.load(f)["sqlalchemy"] == GOLDEN_SQLALCHEMY_MAJOR
        assert INSTALLED_SQLALCHEMY_MAJOR in ("1.4", "2.x")

    @pytest.mark.skipif(
        INSTALLED_SQLALCHEMY_MAJOR == GOLDEN_SQLALCHEMY_MAJOR,
        reason="the divergence set is empty on the major the asset was generated under",
    )
    def test_only_the_pinned_shapes_render_differently_on_the_other_major(self):
        # The per-action test above allows each listed shape to differ; this is what stops the
        # list growing by accident. A NEW divergence is a compiler change worth knowing about,
        # and it lands here rather than silently widening an exemption.
        differing = sorted(
            action
            for action in RECORDED_ACTIONS
            if expectation_for(action) != RECORDED[action]["expectation"]
        )
        assert differing == sorted(RENDERING_DIFFERS_ON_SQLALCHEMY_14)

    def test_every_shape_the_compilers_disagree_on_is_still_proved_by_the_oracle(self):
        # The reason the divergence list is allowed to be a list rather than a second pinned
        # asset: an entry on it is a shape whose ROWS the harness proves against `check()` on
        # both majors, so what the bytes do not cover, the oracle does. An action that left the
        # oracle set while staying on this list would break that argument silently, which is
        # why it is asserted rather than asserted in a comment. Runs on both majors, since the
        # claim is about the list rather than about either compiler.
        oracle = set(classify_actions_for_adapter(ACTIONS_FILE, ADAPTER).oracle_actions)
        assert [
            action
            for action in RENDERING_DIFFERS_ON_SQLALCHEMY_14
            if action not in oracle
        ] == []

    def test_the_unconditional_action_is_the_planner_fold_the_corpus_declares(self):
        # `p-has` is the corpus's one `knownDivergences` entry: the planner folds `has()` on a
        # missing attribute to ALWAYS_ALLOWED while `check()` denies those rows. The adapter
        # must translate that faithfully — an unfiltered SELECT — and this is the assertion
        # that says the empty WHERE above belongs to that shape rather than to a translation
        # that quietly stopped emitting a filter.
        assert UNCONDITIONAL_ACTIONS == ["p-has"]
        assert "p-has" in ACTIONS_FILE.skipped_divergences(ADAPTER)


class TestWhatTheEmittedStatementContains:
    """The properties a regenerated asset must not silently accept.

    Pinned bytes do not survive ``pdm run golden:update`` being run and committed unread;
    rules do. So each of these is stated over every translated corpus action rather than over
    a chosen shape, and each carries an anti-vacuity assertion.

    They read what the translator emits RIGHT NOW rather than what the asset pins, so they
    hold on both SQLAlchemy majors CI runs -- the asset is one compiler's snapshot, a rule is
    about the adapter.
    """

    def test_every_statement_is_the_corpus_select_plus_a_where_clause(self):
        # The asset records only what follows WHERE, which is lossless exactly while this
        # holds. `where_clause()` raises on a statement that does not start with the preamble,
        # so this is also what would catch a `table_mapping` join appearing where none was
        # asked for.
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
        # The other half of "recording only the WHERE is lossless": every entry reassembles
        # into exactly the statement the adapter emitted, preamble included. Without this the
        # asset could be a faithful record of something the adapter never built.
        for action in RECORDED_ACTIONS:
            for name in GOLDEN_DIALECTS:
                assert recorded_statement(action, name) == emitted_statement(
                    action, name
                ), f"{action} ({name})"

    def test_the_resource_table_is_named_in_exactly_one_from_clause(self):
        # A correlated subquery that lost its correlation lists the outer table in its OWN
        # FROM and then compares against every row of it — silent wrongness, and the class of
        # bug the harness escalates SQLAlchemy's cartesian-product warning to an error for.
        # Here the same property is static, over every action rather than over the ones a
        # warning happened to fire on.
        offenders = [
            f"{action} ({name})"
            for action in RECORDED_ACTIONS
            for name in GOLDEN_DIALECTS
            if _from_clauses_naming_the_resource(emitted_statement(action, name)) != 1
        ]
        assert offenders == []
        # Anti-vacuity, in two parts because the rule needs both to say anything.
        #
        # The corpus must still emit subqueries at all: these three are the shapes that do —
        # a chained collection macro, a scalar read through two to-one hops, and a direct
        # EXISTS.
        for action in ("w1-all-chain", "rel-bool-hop2", "exists-on-empty"):
            assert "(SELECT" in emitted_statement(action, "sqlite"), action
        # And the detector must recognise the thing it is looking for. An uncorrelated
        # subquery renders the outer table into a comma-joined FROM list rather than a second
        # `FROM adversarial_resource`, so a naive substring count reads 1 for both the correct
        # and the broken statement and the rule guards nothing. This is the broken rendering,
        # built here rather than hoped for.
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
        # LIKE metacharacters in a needle are the corpus's founding bug class (#258/#259): an
        # unescaped `%` in a value turns an equality into a wildcard match and returns rows
        # the PDP denies. The adapter escapes them and declares the escape character, and a
        # LIKE that reached the database without the ESCAPE clause would read those
        # backslashes as literal text.
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
        # Anti-vacuity: satisfied by a corpus that emits no LIKE at all.
        assert with_like > 0

    def test_every_qualified_identifier_names_a_column_the_schema_declares(self):
        # An identifier the schema does not carry is a mapping that would fail at execution
        # time — or worse, resolve against a column that happens to exist. The harness cannot
        # catch the second: it seeds the same schema this maps against.
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
        # PostgreSQL parses 'NaN' and 'Infinity' as double precision inputs and every
        # comparison against them is false — the same rows a folded translation returns — so
        # an executed leg agrees either way and only the parameter list distinguishes them.
        # The harness pins this for three actions against the PostgreSQL compiler; stating it
        # over the whole corpus is what makes a NEW non-finite fold visible.
        offenders = [
            f"{action} ({name}) {key}={value}"
            for action, name, key, value in emitted_parameters()
            if isinstance(value, float) and not math.isfinite(value)
        ]
        assert offenders == []

    def test_the_corpus_still_drives_the_folds_that_rule_polices(self):
        # Anti-vacuity for the rule above: it is satisfied by a corpus with no non-finite
        # arithmetic in it at all. These are the actions that produce one and still translate;
        # `cr-div-neg-zero` and `nan-ord-inf` are deliberately absent, because a CONSTANT zero
        # denominator is refused outright on this adapter (see the transport tests below).
        for action in (
            "nan-ord-le",
            "nan-ord-ternary",
            "nan-ord-ternary-vf",
            "cr-div-zero",
            "cr-div-other-column",
        ):
            assert action in CONDITIONAL_ACTIONS

    def test_the_actions_that_bind_a_datetime_the_asset_records_as_a_string(self):
        # JSON has no instant, so `json_parameter` normalises a bound `datetime` to its
        # ISO-8601 spelling. Pinning the list here is what stops that normalisation from
        # hiding a change — and the second half is what keeps the encoding unambiguous: a
        # STRING parameter that happened to be ISO-8601 would read back indistinguishably
        # from an instant, and the corpus does carry ISO-8601 strings (`createdBy`).
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

    A FROM list is one or more bare table names, comma-separated — which is exactly how an
    uncorrelated subquery pulls the outer table in. Matching the list rather than the string
    ``FROM adversarial_resource`` is what makes the difference visible.
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
    """The corpus's ``nullRepresentationOmitted`` probe, which has no store in it at all.

    ``null-eq-missing`` compares ``aOptionalString == null``, and the planner emits the same
    ``eq(attr, null)`` node whichever convention the caller uses — so the adapter has to be
    told, and what it does when it is told is a pure translator property. The harness asserts
    the same pair against a live PDP *and* proves the over-grant with real rows; here it costs
    a millisecond and pins the SQL each option produces.
    """

    ACTION = "null-eq-missing"
    MESSAGE = _null_omitted_message(ACTION)

    def test_explicit_emits_an_is_null_filter(self):
        statement, _params = render(
            translate(self.ACTION, null_attribute_representation="explicit"), "sqlite"
        )
        assert "adversarial_resource.a_optional_string IS NULL" in statement

    def test_omitted_refuses_the_same_plan(self):
        # A NULL column then sends no attribute, so check() denies on a missing-attribute
        # error while the filter above returns exactly those rows (#302).
        with pytest.raises(ValueError, match=re.escape(self.MESSAGE)):
            translate(self.ACTION, null_attribute_representation="omitted")

    def test_a_per_attribute_declaration_overrides_the_call_level_option(self):
        # #308. `owner` declares "explicit" in the shared map, so `null-eq` — which probes it
        # — must still translate under a call-level "omitted"...
        assert render(
            translate("null-eq", null_attribute_representation="omitted"), "sqlite"
        ) == render(translate("null-eq"), "sqlite")

        # ...and stripping the declaration must reject the same action under the same option,
        # so the override above is doing work rather than being quietly equivalent.
        with pytest.raises(ValueError, match="null operand"):
            translate(
                "null-eq",
                null_attribute_representation="omitted",
                attribute_null_representation=None,
            )


class TestOperatorOverrides:
    """The override mechanism itself, which no policy shape can vary.

    The corpus drives one set of overrides — the collection macros the adapter has no
    portable translation for — and every golden expectation above is the SQL that set
    produces. A *different* set is not a hostile CEL shape, it is a different call, so this
    is where the coverage the retired shared-policy suite had that is genuinely not a corpus
    action lives.
    """

    def test_an_override_replaces_the_default_lowering_for_its_operator(self):
        # The README's own example: PostgreSQL users preferring `= ANY (...)` to `IN`. The
        # plan is read from a fixture rather than planned live, so what varies between the
        # two halves is the override and nothing else.
        action = "p-in-null-multi"
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
            translate("cs-eq", attr_map={}, attribute_null_representation=None)

    # The two tests below are the coverage the retired suite had that the corpus genuinely
    # cannot carry, and the distinction is worth stating once. `actions.json` classifies a
    # shape against ONE mapping — the corpus's — so "unsupported" there means "this adapter
    # refuses it with these overrides", not "no caller can translate it". Both shapes are
    # documented in the README as caller-supplied, so an assertion that the documented
    # override actually works is not a corpus question. Each asserts BOTH halves: the refusal
    # the corpus pins, and the translation the declaration buys.

    def test_a_matches_override_admits_the_regex_the_corpus_refuses(self):
        # `p-matches` is `expectedUnsupported` because SQL dialect regex engines do not
        # guarantee CEL/RE2 semantics, so the adapter has no default lowering. An application
        # whose database translation is known to be equivalent may supply one — the README
        # says so — and this is what says that path still works.
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

    def test_an_index_override_admits_the_positional_read_the_corpus_refuses(self):
        # `index-scalar-list` is `adapterUnsupported` because row order in a SQL relation is
        # not defined — but the refusal the corpus pins comes from the MAPPING, not the
        # operator: `tagNames` is a relation marker, `index` is not in the corpus's override
        # map, so the reference is not override-owned and `get_query`'s pre-validation refuses
        # it before the walk. An application whose storage makes a positional read meaningful
        # supplies both halves — here a scalar column standing for a single-element list,
        # which is the shape the retired suite used — and the same plan translates.
        action = "index-scalar-list"
        with pytest.raises(TypeError, match="must be handled by an operator override"):
            translate(action)

        statement, params = render(
            translate(
                action,
                attr_map={"request.resource.attr.tagNames": AdvResource.a_string},
                operator_override_fns={"index": lambda column, _position: column},
                attribute_null_representation=None,
            ),
            "sqlite",
        )
        assert statement.endswith("WHERE adversarial_resource.a_string = ?")
        assert params == {"a_string_1": "public"}


class TestTimestampLiterals:
    """The one operand a wire fixture cannot pin, and why this adapter's reader chooses a
    nanosecond instant.

    ``regenerate-wire-fixtures.sh`` rewrites ``ts-window``'s folded ``now() - duration("24h")``
    literal to a placeholder, because it differs on every capture — so reading the fixture back
    means choosing a value, and here that choice is load-bearing. The PDP emits NANOSECOND
    precision, which is the entire reason ``actions.json`` classifies ``ts-window`` and
    ``ts-vf`` as ``adapterUnsupported`` for this adapter. A tidy millisecond substitution in
    ``corpus.py`` would translate cleanly and quietly contradict the corpus, so the throw suite
    above would be asserting a limitation that does not exist.

    These are the assertions that make ``PLANNED_AT`` a decision rather than an accident.
    """

    @pytest.mark.parametrize("action", ["ts-window", "ts-vf"])
    def test_the_refusal_is_the_precision_and_not_the_shape(self, action):
        message = next(
            pinned for candidate, pinned in THROWING_ACTIONS if candidate == action
        )
        with pytest.raises(ValueError, match=re.escape(message)):
            translate(action)

        # The same plan at microsecond precision translates. Both directions matter: the
        # refusal is real, and it is a property of the instant rather than of the shape.
        statement, _params = render(
            translate(action, planned_at="2026-08-11T09:13:39.123456Z"), "sqlite"
        )
        assert "adversarial_resource.created_at" in statement

    def test_excess_fractional_digits_are_accepted_only_when_they_are_zero(self):
        # CEL's instant range is exact to the microsecond and no further, so trailing zeroes
        # are information-free and a non-zero digit is a value the column cannot hold.
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
        # Each is refused rather than coerced: a datetime parsed leniently would compare
        # against the column as some OTHER instant, which is a filter returning rows the PDP
        # denies rather than an error the caller can see.
        with pytest.raises(ValueError, match="RFC-3339|precision|instant range|offset"):
            translate("ts-window", planned_at=value)


class TestTransportDecoding:
    """``get_query`` accepts both SDK clients' responses, and only one of them has coverage now.

    The retired suite parametrised every one of its tests over the HTTP and gRPC clients, so
    it was the only thing in this adapter that reached the protobuf arm — ``MessageToDict``
    rather than ``to_dict()``. Wire fixtures are HTTP response bodies, so the suite above is
    HTTP-shaped by construction; decoding the same fixture into the protobuf response keeps
    that arm executed.

    What it proves, and what it does not, is the sharp part. The fixture is JSON, and JSON is
    where the information the two transports disagree about is already lost — so this pins the
    disagreement rather than resolving it. Re-triaging the two classifications that turn on it
    still needs a real gRPC PDP (cerbos/query-plan-adapters#321).
    """

    #: Every corpus action whose translation the two decodings agree on completely.
    AGREEING = [
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
        # Both actions divide by a literal `-0.0`. The planner ships the sign — the wire
        # operand is `-0` — but `json.loads("-0")` returns the **int** 0, so by the time the
        # fixture is a Python object the sign bit is gone. That is precisely why the HTTP arm
        # refuses these two shapes: the adapter's guard keys on the operand still being an
        # int, and an int zero cannot say whether CEL produced +Infinity or -Infinity (#312).
        with pytest.raises(ValueError, match="sign is indeterminate"):
            translate(action, plan=plan_from_wire_fixture(action))

        # Re-encoding that same value into protobuf widens it to a double — a POSITIVE one,
        # because the sign was lost two steps earlier — so the guard does not fire and the
        # adapter emits a filter for the +Infinity reading. Over a real gRPC transport the
        # sign survives and the same absence of a raise would be correct; from a fixture it is
        # not. Pinning it here is what stops "the protobuf arm translates it" from being read
        # as evidence that gRPC makes these shapes supportable (#321).
        statement, _params = render(
            translate(action, plan=grpc_plan_from_wire_fixture(action)), "sqlite"
        )
        assert "adversarial_resource" in statement


class TestTheGoldenAsset:
    def test_names_a_command_this_package_actually_defines(self):
        # The asset carries the command that rewrites it, so a reader who opens the file after
        # a failing assertion is told how to look at the difference. That is only useful while
        # the command exists.
        runner, run, script = GOLDEN_REGENERATE_COMMAND.split(" ")
        assert (runner, run) == ("pdm", "run")
        assert script in _pdm_script_names()


def _pdm_script_names():
    """The keys of ``[tool.pdm.scripts]``, read without a TOML parser.

    ``tomllib`` landed in 3.11 and this package supports 3.8, so a stdlib parse is not
    available on every version CI runs. The section scan below is enough for the one question
    asked of it — does the advertised command exist — and adding a TOML dependency to assert
    it would cost more than it proves.
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
