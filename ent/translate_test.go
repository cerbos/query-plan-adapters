// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
)

// Unit tests over hand-built plans, complementing the adversarial suite: the corpus proves
// semantics against a real PDP but only ever feeds the adapter plans the planner actually emits, so
// it cannot say what happens to a malformed or hostile one. Everything here runs without Docker.
//
// The translator this file exercises is vendored byte-for-byte into the pgx module as well, so the
// invariants below are deliberately kept in step with pgx/translate_test.go — same names, same
// section order, same shapes — and `conformance/scripts/validate-corpus.sh` fails if the two
// vendored trees drift. Two copies of the same code need two copies of the same proof: a fix landed
// in one tree and not the other is otherwise caught only when a corpus action happens to exercise
// it, and none of the hostile shapes here come off a real planner wire at all
// (cerbos/query-plan-adapters#319).

// -- plan builders -------------------------------------------------------------------------------

type operand = enginev1.PlanResourcesFilter_Expression_Operand

func val(t *testing.T, v any) *operand {
	t.Helper()
	pb, err := structpb.NewValue(v)
	require.NoError(t, err)
	return &operand{Node: &enginev1.PlanResourcesFilter_Expression_Operand_Value{Value: pb}}
}

func variable(name string) *operand {
	return &operand{Node: &enginev1.PlanResourcesFilter_Expression_Operand_Variable{Variable: name}}
}

func expr(op string, operands ...*operand) *operand {
	return &operand{Node: &enginev1.PlanResourcesFilter_Expression_Operand_Expression{
		Expression: &enginev1.PlanResourcesFilter_Expression{Operator: op, Operands: operands},
	}}
}

func conditional(cond *operand) *responsev1.PlanResourcesResponse {
	return &responsev1.PlanResourcesResponse{
		Filter: &enginev1.PlanResourcesFilter{
			Kind:      enginev1.PlanResourcesFilter_KIND_CONDITIONAL,
			Condition: cond,
		},
	}
}

func tagRelation(filter ...cerbosent.Restriction) *cerbosent.Relation {
	return &cerbosent.Relation{
		Table:          "tag",
		SourceColumn:   "id",
		TargetColumn:   "resource_id",
		Field:          &cerbosent.Entry{Column: "name"},
		Fields:         map[string]cerbosent.Entry{"name": {Column: "name"}},
		SubqueryFilter: filter,
	}
}

func testMapper() cerbosent.Mapper {
	return cerbosent.MapperMap{
		"request.resource.attr.name":      {Column: "name"},
		"request.resource.attr.count":     {Column: "count"},
		"request.resource.attr.owner":     {Column: "owner"},
		"request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbosent.ValueTimestamp},
		"request.resource.attr.tags":      {Relation: tagRelation()},
	}
}

func translate(t *testing.T, cond *operand, opts ...cerbosent.Option) (cerbosent.Result, error) {
	t.Helper()
	return cerbosent.Translate(conditional(cond), "resource", testMapper(), opts...)
}

// renderWhere renders a predicate through ent's builder and returns just the WHERE fragment.
//
// pgx's Result carries that fragment directly, so returning it here rather than the whole SELECT is
// what keeps the two suites' assertions readable side by side. It also checks selector.Err(): the
// predicate is lazy, so this is the pass that actually runs it, and ent reports a failure there by
// collecting it on the selector rather than by returning it — see TestSelectorReportsPredicateErrors
// in render_test.go.
func renderWhere(t *testing.T, d string, predicate *entsql.Predicate) (string, []any) {
	t.Helper()

	selector := entsql.Dialect(d).Select("id").From(entsql.Table("resource"))
	selector.Where(predicate)
	query, args := selector.Query()
	require.NoError(t, selector.Err(), "building the query: %s", query)

	_, where, found := strings.Cut(query, " WHERE ")
	require.True(t, found, "rendered query has no WHERE clause: %s", query)
	return where, args
}

// whereFor translates a plan against a mapper and renders it for one dialect.
func whereFor(t *testing.T, d string, mapper cerbosent.Mapper, cond *operand, opts ...cerbosent.Option) (string, []any) {
	t.Helper()

	result, err := cerbosent.Translate(conditional(cond), "resource", mapper, append(opts, cerbosent.WithDialect(d))...)
	require.NoError(t, err)
	require.Equal(t, cerbosent.KindConditional, result.Kind)
	return renderWhere(t, d, result.Predicate)
}

// translateWith is whereFor on the default dialect, which is what most assertions want.
func translateWith(t *testing.T, mapper cerbosent.Mapper, cond *operand) (string, []any) {
	t.Helper()
	return whereFor(t, dialect.SQLite, mapper, cond)
}

// tagsExists is `R.attr.tags.exists(t, t.name == "x")`.
func tagsExists(t *testing.T) *operand {
	t.Helper()
	return expr("exists", variable("request.resource.attr.tags"),
		expr("lambda", expr("eq", variable("t.name"), val(t, "x")), variable("t")))
}

// -- malformed plans must error, never panic -----------------------------------------------------

// A plan reaches this adapter over the wire. A panic in a library that sits on the authorization
// path takes the caller's process down, so every shape below must come back as an error.
func TestMalformedPlansReturnErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cond *operand
		name string
	}{
		{name: "nil condition", cond: nil},
		{name: "nil operand inside expression", cond: expr("eq", nil, nil)},
		{name: "empty expression", cond: expr("")},
		{name: "unknown operator", cond: expr("frobnicate", variable("request.resource.attr.name"))},
		{name: "and with no operands", cond: expr("and")},
		{name: "not with two operands", cond: expr("not", variable("request.resource.attr.name"), variable("request.resource.attr.count"))},
		{name: "eq with one operand", cond: expr("eq", variable("request.resource.attr.name"))},
		{name: "eq with three operands", cond: expr("eq", variable("request.resource.attr.name"), variable("request.resource.attr.count"), variable("request.resource.attr.owner"))},
		{name: "if with two operands", cond: expr("if", variable("request.resource.attr.name"), variable("request.resource.attr.count"))},
		{name: "exists with one operand", cond: expr("exists", variable("request.resource.attr.tags"))},
		{name: "exists with a non-lambda second operand", cond: expr("exists", variable("request.resource.attr.tags"), variable("request.resource.attr.name"))},
		{name: "lambda with no variable", cond: expr("exists", variable("request.resource.attr.tags"), expr("lambda", expr("eq", variable("t.name"), val(t, "x"))))},
		{name: "lambda outside a macro", cond: expr("lambda", val(t, true), variable("t"))},
		{name: "size with no operands", cond: expr("gt", expr("size"), val(t, 1))},
		{name: "hierarchy with three operands", cond: expr("ancestorOf", expr("hierarchy", variable("request.resource.attr.name"), val(t, "."), val(t, ".")), expr("hierarchy", val(t, "a")))},
		{name: "non-boolean constant as a predicate", cond: val(t, "not a bool")},
		{name: "unmapped attribute", cond: expr("eq", variable("request.resource.attr.nope"), val(t, "x"))},
		{name: "relation used as a scalar", cond: expr("eq", variable("request.resource.attr.tags"), val(t, "x"))},
		{name: "column used as a collection", cond: expr("exists", variable("request.resource.attr.name"), expr("lambda", val(t, true), variable("t")))},
		{name: "macro over a literal that is not a list", cond: expr("exists", val(t, "scalar"), expr("lambda", val(t, true), variable("t")))},
		{name: "exists_one over a literal list", cond: expr("exists_one", val(t, []any{"a", "b"}), expr("lambda", val(t, true), variable("t")))},
		{name: "lambda field missing from the element", cond: expr("exists", val(t, []any{map[string]any{"other": 1}}), expr("lambda", expr("eq", variable("t.name"), val(t, "x")), variable("t")))},
		{name: "hierarchy operands with different delimiters", cond: expr("ancestorOf", expr("hierarchy", val(t, "a"), val(t, ".")), expr("hierarchy", val(t, "a.b"), val(t, ":")))},
		{name: "hierarchy operator without hierarchy operands", cond: expr("ancestorOf", val(t, "a"), val(t, "a.b"))},
		{name: "empty hierarchy delimiter", cond: expr("ancestorOf", expr("hierarchy", val(t, "a"), val(t, "")), expr("hierarchy", val(t, "a.b"), val(t, "")))},
		{name: "invalid timestamp literal", cond: expr("gt", expr("timestamp", val(t, "not-a-timestamp")), val(t, 1))},
		{name: "timestamp over an untyped column", cond: expr("gt", expr("timestamp", variable("request.resource.attr.name")), val(t, 1))},
		{name: "regex", cond: expr("matches", variable("request.resource.attr.name"), val(t, ".*"))},
		{name: "filter outside size", cond: expr("filter", variable("request.resource.attr.tags"), expr("lambda", val(t, true), variable("t")))},
		{name: "hasIntersection between two stored collections", cond: expr("hasIntersection", variable("request.resource.attr.tags"), variable("request.resource.attr.tags"))},
		{name: "modulus by zero", cond: expr("eq", expr("mod", val(t, 4), val(t, 0)), val(t, 0))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// The assertion is as much that this line does not panic as that it errors.
			_, err := translate(t, tc.cond)
			require.Error(t, err)
			require.ErrorIs(t, err, cerbosent.ErrUnsupported)
		})
	}
}

// TestNilConditionIsNotAnAllow pins the fail-closed direction of a filter with no condition: it
// must never widen into "no filter", which would return every row.
func TestNilConditionIsNotAnAllow(t *testing.T) {
	t.Parallel()

	result, err := cerbosent.Translate(&responsev1.PlanResourcesResponse{}, "resource", testMapper())
	require.NoError(t, err)
	require.Equal(t, cerbosent.KindAlwaysDenied, result.Kind)
	require.Nil(t, result.Predicate)
}

func TestPlanKinds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		kind enginev1.PlanResourcesFilter_Kind
		want cerbosent.PlanKind
	}{
		{name: "denied", kind: enginev1.PlanResourcesFilter_KIND_ALWAYS_DENIED, want: cerbosent.KindAlwaysDenied},
		{name: "allowed", kind: enginev1.PlanResourcesFilter_KIND_ALWAYS_ALLOWED, want: cerbosent.KindAlwaysAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plan := &responsev1.PlanResourcesResponse{Filter: &enginev1.PlanResourcesFilter{Kind: tc.kind}}
			result, err := cerbosent.Translate(plan, "resource", testMapper())
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Kind)
			require.Nil(t, result.Predicate, "only KindConditional carries a predicate")
		})
	}
}

// TestUnrecognisedFilterKindIsRejected covers the remaining wire value: an unset kind is neither of
// the constants above and must not be read as one of them.
func TestUnrecognisedFilterKindIsRejected(t *testing.T) {
	t.Parallel()

	plan := &responsev1.PlanResourcesResponse{
		Filter: &enginev1.PlanResourcesFilter{Kind: enginev1.PlanResourcesFilter_KIND_UNSPECIFIED},
	}
	_, err := cerbosent.Translate(plan, "resource", testMapper())
	require.ErrorContains(t, err, "unrecognised filter kind")
}

// -- emitted SQL ---------------------------------------------------------------------------------

// TestNoPlanDataReachesSQLText is the injection guard. Every value in the plan below is chosen to
// be SQL syntax if it were ever interpolated rather than bound.
func TestNoPlanDataReachesSQLText(t *testing.T) {
	t.Parallel()

	hostile := `'; DROP TABLE resource; --`
	for _, d := range []string{dialect.SQLite, dialect.Postgres, dialect.MySQL} {
		t.Run(d, func(t *testing.T) {
			t.Parallel()

			query, args := whereFor(t, d, testMapper(), expr("or",
				expr("eq", variable("request.resource.attr.name"), val(t, hostile)),
				expr("contains", variable("request.resource.attr.owner"), val(t, hostile)),
				expr("in", variable("request.resource.attr.name"), val(t, []any{hostile, `" OR 1=1 --`})),
			))

			require.NotContains(t, query, "DROP TABLE")
			require.NotContains(t, query, "--")
			require.NotContains(t, query, "'")
			require.Contains(t, args, hostile)
		})
	}
}

// TestValueFirstComparisonsMirror pins the operand-order rule that shipped as the same bug to two
// adapters (cerbos/query-plan-adapters#257): the planner preserves policy source order, so
// `3 <= R.attr.count` arrives value-first and must become `count >= 3`, not `count <= 3`.
func TestValueFirstComparisonsMirror(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ operator, want string }{
		{operator: "lt", want: ">"},
		{operator: "le", want: ">="},
		{operator: "gt", want: "<"},
		{operator: "ge", want: "<="},
	} {
		t.Run(tc.operator, func(t *testing.T) {
			t.Parallel()

			query, args := translateWith(t, testMapper(), expr(tc.operator, val(t, 3), variable("request.resource.attr.count")))
			require.Equal(t, "(`resource`.`count` "+tc.want+" ?)", query)
			require.Equal(t, []any{float64(3)}, args)
		})
	}
}

// TestSymmetricComparisonsNormaliseToColumnFirst covers the other half of the rule: eq/ne/in mean
// the same thing either way round, so they normalise rather than mirror.
func TestSymmetricComparisonsNormaliseToColumnFirst(t *testing.T) {
	t.Parallel()

	query, _ := translateWith(t, testMapper(), expr("eq", val(t, "x"), variable("request.resource.attr.name")))
	require.Equal(t, "(`resource`.`name` = ?)", query)
}

// TestOperatorSymbols pins the two lookup tables the renderer spells operators through.
//
// They are the kind of thing nothing else catches: a `+` written where `-` belongs, or `<` where
// `<=` belongs, is valid SQL that quietly returns a different row set, and the corpus only notices
// if some action happens to straddle the boundary the wrong symbol moves. Every arm is asserted so
// there is no operator whose spelling is taken on trust.
func TestOperatorSymbols(t *testing.T) {
	t.Parallel()

	t.Run("comparisons", func(t *testing.T) {
		t.Parallel()

		for operator, symbol := range map[string]string{
			"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">=",
		} {
			query, _ := translateWith(t, testMapper(), expr(operator, variable("request.resource.attr.count"), val(t, 2)))
			require.Equal(t, "(`resource`.`count` "+symbol+" ?)", query, operator)
		}
	})

	t.Run("arithmetic", func(t *testing.T) {
		t.Parallel()

		// A column dividend keeps `div` and `mod` from folding to a constant, and the division
		// shapes wrap the arithmetic in the guards that keep a zero divisor UNKNOWN — so these
		// assert the operator appears rather than pinning the whole surrounding CASE.
		for operator, symbol := range map[string]string{
			"add": "+", "sub": "-", "mult": "*", "div": "/", "mod": "%",
		} {
			query, _ := translateWith(t, testMapper(), expr("gt",
				expr(operator, variable("request.resource.attr.count"), val(t, 2)), val(t, 1)))
			require.Contains(t, query, " "+symbol+" ", operator+": "+query)
		}
	})
}

// TestReceiverSensitiveOperatorsKeepWireOrder is the reason eq/ne/in are normalised by name rather
// than by "put the column first": swapping `"const".contains(col)` would silently exchange the
// haystack and the needle.
func TestReceiverSensitiveOperatorsKeepWireOrder(t *testing.T) {
	t.Parallel()

	query, args := translateWith(t, testMapper(), expr("contains", val(t, "haystack"), variable("request.resource.attr.name")))
	require.True(t, strings.HasPrefix(query, "(? LIKE"), "the constant is the receiver: %s", query)
	require.Equal(t, "haystack", args[0])
}

// TestLikeMetacharactersAreEscaped pins that policy data cannot act as a wildcard.
func TestLikeMetacharactersAreEscaped(t *testing.T) {
	t.Parallel()

	query, args := translateWith(t, testMapper(), expr("startsWith", variable("request.resource.attr.name"), val(t, `100%_a[b\`)))
	require.Equal(t, `100\%\_a\[b\\%`, args[0])
	require.Contains(t, query, "ESCAPE")
}

// TestNullComparisonBecomesIsNull pins the default (explicit-null) representation.
func TestNullComparisonBecomesIsNull(t *testing.T) {
	t.Parallel()

	query, args := translateWith(t, testMapper(), expr("eq", variable("request.resource.attr.owner"), val(t, nil)))
	require.Equal(t, "(`resource`.`owner` IS NULL)", query)
	require.Empty(t, args)
}

// TestNullOperandsRejectedUnderOmitted pins the rejection, and that it matches on the operand
// rather than an allowlist of operators — a null carried in a value LIST must be caught too.
func TestNullOperandsRejectedUnderOmitted(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		cond *operand
		name string
	}{
		{name: "eq", cond: expr("eq", variable("request.resource.attr.owner"), val(t, nil))},
		{name: "ne", cond: expr("ne", variable("request.resource.attr.owner"), val(t, nil))},
		{name: "in list containing null", cond: expr("in", variable("request.resource.attr.owner"), val(t, []any{"a", nil}))},
		{name: "nested under not", cond: expr("not", expr("eq", variable("request.resource.attr.owner"), val(t, nil)))},
		{name: "nested under and", cond: expr("and",
			expr("eq", variable("request.resource.attr.name"), val(t, "x")),
			expr("eq", variable("request.resource.attr.owner"), val(t, nil)),
		)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := translate(t, tc.cond, cerbosent.WithNullRepresentation(cerbosent.NullOmitted))
			require.ErrorIs(t, err, cerbosent.ErrUnsupported)
			require.Contains(t, err.Error(), "null operand")

			// The same plan translates fine under the default representation, so the rejection is
			// the option talking rather than an unrelated failure.
			_, err = translate(t, tc.cond)
			require.NoError(t, err)
		})
	}
}

// TestPredicateComposesAfterTheCallersOwnArguments is the ent counterpart of pgx's placeholder
// offset. pgx hands back a text fragment and has to be told where the caller's numbering left off;
// ent hands back a predicate the builder renumbers itself. Either way the hazard is the same — a
// filter appended to a query that already binds arguments must not reuse the caller's placeholders
// — so it is pinned on the dialect where the numbering is visible.
func TestPredicateComposesAfterTheCallersOwnArguments(t *testing.T) {
	t.Parallel()

	result, err := cerbosent.Translate(
		conditional(expr("eq", variable("request.resource.attr.name"), val(t, "plan"))),
		"resource", testMapper(), cerbosent.WithDialect(dialect.Postgres))
	require.NoError(t, err)

	selector := entsql.Dialect(dialect.Postgres).Select("id").From(entsql.Table("resource"))
	selector.Where(entsql.And(entsql.EQ("tenant", "caller"), result.Predicate))
	query, args := selector.Query()
	require.NoError(t, selector.Err())

	require.Contains(t, query, `"tenant" = $1`)
	require.Contains(t, query, `"name" = $2`)
	require.Equal(t, []any{"caller", "plan"}, args)
}

// TestIdentifiersAreQuoted pins that a mapped column is quoted for the dialect in use rather than
// pasted in bare — the qualifier and the column name both, since the qualified form is what makes
// the predicate composable inside a correlated subquery.
func TestIdentifiersAreQuoted(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ dialect, want string }{
		{dialect: dialect.SQLite, want: "(`resource`.`odd column` = ?)"},
		{dialect: dialect.MySQL, want: "(`resource`.`odd column` = ?)"},
		{dialect: dialect.Postgres, want: `("resource"."odd column" = $1::text)`},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			t.Parallel()

			mapper := cerbosent.MapperMap{"request.resource.attr.name": {Column: "odd column"}}
			query, _ := whereFor(t, tc.dialect, mapper,
				expr("eq", variable("request.resource.attr.name"), val(t, "x")))
			require.Equal(t, tc.want, query)
		})
	}
}

// TestMapperColumnCarryingTheDialectQuoteIsNotRequoted records a documented divergence from pgx
// rather than an invariant shared with it. pgx quotes defensively, doubling an embedded quote; ent
// delegates quoting to sql.Builder.Ident, which treats a name already containing the dialect's
// quote character as pre-quoted and passes it through verbatim.
//
// Neither is an injection boundary — a mapper is caller-supplied, and no plan data reaches an
// identifier (TestNoPlanDataReachesSQLText) — so this is pinned as the behaviour a caller has to
// know about, and stated as a hazard in the README, not silently relied upon. It is what makes
// "name your columns with ordinary identifiers" part of this adapter's mapping contract.
func TestMapperColumnCarryingTheDialectQuoteIsNotRequoted(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ dialect, column, want string }{
		{dialect: dialect.SQLite, column: "we`ird", want: "(`resource`.we`ird = ?)"},
		{dialect: dialect.MySQL, column: "we`ird", want: "(`resource`.we`ird = ?)"},
		{dialect: dialect.Postgres, column: `we"ird`, want: `("resource".we"ird = $1::text)`},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			t.Parallel()

			mapper := cerbosent.MapperMap{"request.resource.attr.name": {Column: tc.column}}
			query, _ := whereFor(t, tc.dialect, mapper,
				expr("eq", variable("request.resource.attr.name"), val(t, "x")))
			require.Equal(t, tc.want, query)
		})
	}
}

// TestTranslateRejectsMissingArguments covers the API preconditions.
func TestTranslateRejectsMissingArguments(t *testing.T) {
	t.Parallel()

	cond := expr("eq", variable("request.resource.attr.name"), val(t, "x"))

	_, err := cerbosent.Translate(nil, "resource", testMapper())
	require.Error(t, err)

	_, err = cerbosent.Translate(conditional(cond), "", testMapper())
	require.Error(t, err)

	_, err = cerbosent.Translate(conditional(cond), "resource", nil)
	require.Error(t, err)
}

// TestRootTableCannotShadowGeneratedAliases pins the guard against a resource table named like a
// generated subquery alias. Without it the inner alias would shadow the outer table and the
// correlation would silently compare the subquery's row against itself.
func TestRootTableCannotShadowGeneratedAliases(t *testing.T) {
	t.Parallel()

	cond := tagsExists(t)

	_, err := cerbosent.Translate(conditional(cond), "cerbos_rel_1", testMapper())
	require.ErrorIs(t, err, cerbosent.ErrUnsupported)
	require.Contains(t, err.Error(), "reserved for generated subquery aliases")

	// Any other table name still works, so the guard is narrow.
	_, err = cerbosent.Translate(conditional(cond), "resource", testMapper())
	require.NoError(t, err)
}

// -- regressions from the adversarial review ------------------------------------------------------

// TestModulusRejectsNonIntegerOperands pins a panic fix: `mod(4, 0.5)` passed the zero-divisor
// check, then truncated the divisor to zero and panicked with an integer divide by zero.
func TestModulusRejectsNonIntegerOperands(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		l, r any
	}{
		{name: "fractional divisor", l: 4, r: 0.5},
		{name: "fractional dividend", l: 4.5, r: 2},
		{name: "zero divisor", l: 4, r: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := translate(t, expr("eq", expr("mod", val(t, tc.l), val(t, tc.r)), val(t, 0)))
			require.ErrorIs(t, err, cerbosent.ErrUnsupported)
		})
	}
}

// TestTypedNilOperandsAreRejected pins the other panic fix: a oneof wrapper can be a typed nil,
// which the type switch still matches and which was then dereferenced.
func TestTypedNilOperandsAreRejected(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		cond *operand
		name string
	}{
		{name: "expression", cond: &operand{Node: (*enginev1.PlanResourcesFilter_Expression_Operand_Expression)(nil)}},
		{name: "variable", cond: &operand{Node: (*enginev1.PlanResourcesFilter_Expression_Operand_Variable)(nil)}},
		{name: "value", cond: &operand{Node: (*enginev1.PlanResourcesFilter_Expression_Operand_Value)(nil)}},
		{name: "unset value", cond: &operand{Node: &enginev1.PlanResourcesFilter_Expression_Operand_Value{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := translate(t, tc.cond)
			require.ErrorIs(t, err, cerbosent.ErrUnsupported)
		})
	}
}

// TestNestedNullsRejectedUnderOmitted pins that the NullOmitted scan sees a null nested inside a
// literal collection. Lambda substitution extracts it long after the scan runs, so a shallow check
// let `IS NULL` through and returned exactly the rows the PDP denies.
func TestNestedNullsRejectedUnderOmitted(t *testing.T) {
	t.Parallel()

	cond := expr("exists",
		val(t, []any{map[string]any{"v": nil}}),
		expr("lambda", expr("eq", variable("x.v"), variable("request.resource.attr.owner")), variable("x")))

	_, err := translate(t, cond, cerbosent.WithNullRepresentation(cerbosent.NullOmitted))
	require.ErrorIs(t, err, cerbosent.ErrUnsupported)
	require.Contains(t, err.Error(), "null operand")
}

// TestRelationMembershipRespectsNullRepresentation pins that null-safe equality is used only under
// the explicit convention. Under NullOmitted a NULL column carries no attribute, so CEL errors and
// denies; treating it as a definite non-match would make `!(x in coll)` true for exactly those rows.
//
// The operator is spelled three different ways across the dialects ent supports, so the assertion
// runs on all of them: a dialect whose null-safe spelling is wrong silently falls back to plain
// equality, which is the bug this test exists to catch.
func TestRelationMembershipRespectsNullRepresentation(t *testing.T) {
	t.Parallel()

	cond := expr("in", variable("request.resource.attr.owner"), variable("request.resource.attr.tags"))

	for _, tc := range []struct{ dialect, nullSafe, plain string }{
		{dialect: dialect.SQLite, nullSafe: "`cerbos_rel_1`.`name` IS `resource`.`owner`", plain: "`cerbos_rel_1`.`name` = `resource`.`owner`"},
		{dialect: dialect.Postgres, nullSafe: `"cerbos_rel_1"."name" IS NOT DISTINCT FROM "resource"."owner"`, plain: `"cerbos_rel_1"."name" = "resource"."owner"`},
		{dialect: dialect.MySQL, nullSafe: "`cerbos_rel_1`.`name` <=> `resource`.`owner`", plain: "`cerbos_rel_1`.`name` = `resource`.`owner`"},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			t.Parallel()

			explicit, _ := whereFor(t, tc.dialect, testMapper(), cond)
			require.Contains(t, explicit, tc.nullSafe)

			omitted, _ := whereFor(t, tc.dialect, testMapper(), cond,
				cerbosent.WithNullRepresentation(cerbosent.NullOmitted))
			require.NotContains(t, omitted, tc.nullSafe)
			require.Contains(t, omitted, tc.plain)
		})
	}
}

// TestNumericCastsAreRejected pins the fail-closed answer to CEL's int()/double()
// (cerbos/query-plan-adapters#311).
//
// The adapter used to render `CAST(trunc(...))`, which is exactly right for a numeric column —
// int(1.9) is 1 to CEL while PostgreSQL's plain float-to-bigint cast rounds to 2, and MySQL needs
// TRUNCATE() to say the same thing. It is wrong for a string one: CEL reads a WHOLE string or
// raises, and an error denies the row, while SQL reads whatever numeric prefix parses. Nothing in
// the plan says which kind of column the operand is, so the corpus actions cast-int-string /
// cast-double-string cannot be told apart from cast-int-double at translation time and the whole
// family fails closed. Re-enabling the numeric direction needs a caller-declared numeric ValueType,
// the way timestamp() already works — and the integer render path was removed with the rest of it,
// so re-enabling means writing it again rather than reviving an untested branch (#319).
func TestNumericCastsAreRejected(t *testing.T) {
	t.Parallel()

	for _, operator := range []string{"int", "double"} {
		t.Run(operator, func(t *testing.T) {
			t.Parallel()

			_, err := translate(t, expr("eq", expr(operator, variable("request.resource.attr.count")), val(t, 2)))
			require.ErrorIs(t, err, cerbosent.ErrUnsupported)
			require.ErrorContains(t, err, "cannot be lowered to SQL CAST")
		})
	}
}

// TestMapperQualifierCannotShadowGeneratedAliases covers the other half of the alias guard: the
// collision can come from an Entry's own qualifier, not just the resource table.
func TestMapperQualifierCannotShadowGeneratedAliases(t *testing.T) {
	t.Parallel()

	mapper := cerbosent.MapperMap{
		"request.resource.attr.name": {Column: "name", Qualifier: "cerbos_rel_1"},
	}
	_, err := cerbosent.Translate(
		conditional(expr("eq", variable("request.resource.attr.name"), val(t, "x"))),
		"resource", mapper)
	require.ErrorIs(t, err, cerbosent.ErrUnsupported)
}

// -- dialect coverage ----------------------------------------------------------------------------

// Everything in this section is ent-specific: pgx renders for one engine, while this adapter's
// renderer spells the same tree three ways. The adversarial suite proves each spelling against a
// real server, but only for the shapes the corpus happens to plan and only when Docker is
// available. These pin the divergences themselves, so a wrong spelling fails in a second.

// TestDialectSpellings covers each construct the renderer spells per dialect. A wrong spelling is
// not a syntax error on every engine — `||` is valid MySQL (it means OR) and `LENGTH` is valid
// everywhere (it counts bytes on MySQL) — so the shapes that would still run, wrongly, are the
// point.
func TestDialectSpellings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		want map[string]string
		cond *operand
		name string
	}{
		{
			// CEL reads a whole string; MySQL spells the text target `char`.
			name: "string() cast",
			cond: expr("eq", expr("string", variable("request.resource.attr.count")), val(t, "2")),
			want: map[string]string{
				dialect.SQLite:   "CAST(`resource`.`count` AS text)",
				dialect.Postgres: `CAST("resource"."count" AS text)`,
				dialect.MySQL:    "CAST(`resource`.`count` AS char)",
			},
		},
		{
			// The float cast guards the division shapes. PostgreSQL's `real` is single precision and
			// would silently round a CEL double, which is why `double precision` is spelled out.
			name: "float cast",
			cond: expr("gt", expr("div", variable("request.resource.attr.count"), val(t, 2)), val(t, 1)),
			want: map[string]string{
				dialect.SQLite:   "CAST(`resource`.`count` AS real)",
				dialect.Postgres: `CAST("resource"."count" AS double precision)`,
				dialect.MySQL:    "CAST(`resource`.`count` AS double)",
			},
		},
		{
			// CEL's size() counts code points. MySQL's LENGTH() counts bytes — "héllo🚀" is 6 to CEL
			// and 10 to MySQL — so it needs CHAR_LENGTH.
			name: "size() over a string",
			cond: expr("gt", expr("size", variable("request.resource.attr.name")), val(t, 2)),
			want: map[string]string{
				dialect.SQLite:   "length(`resource`.`name`)",
				dialect.Postgres: `length("resource"."name")`,
				dialect.MySQL:    "char_length(`resource`.`name`)",
			},
		},
		{
			// A dynamic LIKE pattern concatenates. MySQL reads `||` as logical OR outside
			// PIPES_AS_CONCAT, which would collapse the pattern to a boolean and match nothing;
			// its CONCAT() propagates NULL where PostgreSQL's skips NULLs, so each engine gets the
			// spelling that keeps a missing attribute UNKNOWN.
			name: "concat in a dynamic LIKE pattern",
			cond: expr("contains", variable("request.resource.attr.name"), variable("request.resource.attr.owner")),
			want: map[string]string{
				dialect.SQLite:   "LIKE (? || replace(",
				dialect.Postgres: `LIKE ($1::text || replace(`,
				dialect.MySQL:    "LIKE CONCAT(?, replace(",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Len(t, tc.want, 3, "every case must state all three dialects")
			for d, want := range tc.want {
				query, _ := whereFor(t, d, testMapper(), tc.cond)
				require.Contains(t, query, want, "%s: %s", d, query)
			}
		})
	}
}

// TestPostgresBindsTypedParameters pins the `::type` annotations. PostgreSQL infers an untyped `$n`
// from the context it appears in, and in `CAST(col AS double precision) / $1` there is nothing to
// infer from — it falls back to text and the query dies with "operator does not exist". The plan is
// the only thing that knows a literal's type, so it is stated. SQLite and MySQL infer from the
// bound value and must carry no annotation, or the SQL is invalid there.
func TestPostgresBindsTypedParameters(t *testing.T) {
	t.Parallel()

	cond := expr("and",
		expr("eq", variable("request.resource.attr.name"), val(t, "x")),
		expr("gt", variable("request.resource.attr.count"), val(t, 2)),
	)

	postgres, _ := whereFor(t, dialect.Postgres, testMapper(), cond)
	require.Contains(t, postgres, "$1::text")
	require.Contains(t, postgres, "$2::double precision")

	for _, d := range []string{dialect.SQLite, dialect.MySQL} {
		query, _ := whereFor(t, d, testMapper(), cond)
		require.NotContains(t, query, "::", "%s infers from the bound value: %s", d, query)
	}
}

// TestRestrictionValuesAreTypedForPostgres covers the other source of bound values: a caller's
// Restriction carries `any`, so the parameter-type table has to handle the Go types a plan never
// produces. CEL numbers are always doubles, so an int or a bool reaches bindValue only from here.
func TestRestrictionValuesAreTypedForPostgres(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		value any
		want  string
	}{
		{value: 42, want: "$1::bigint"},
		{value: true, want: "$1::boolean"},
		{value: "main", want: "$1::text"},
		{value: 1.5, want: "$1::double precision"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()

			mapper := cerbosent.MapperMap{
				"request.resource.attr.tags": {Relation: tagRelation(
					cerbosent.Restriction{Column: "kind", Value: tc.value})},
			}
			query, args := whereFor(t, dialect.Postgres, mapper, tagsExists(t))
			require.Contains(t, query, tc.want)
			require.Contains(t, args, tc.value)
		})
	}
}

// TestTimestampsAreBoundForTheDialect pins the SQLite timestamp convention. SQLite has no temporal
// type, so an instant is stored and compared as text, and lexicographic order only agrees with
// chronological order when every value is fixed width and in the same zone — Go's RFC3339Nano trims
// trailing zeros from the fraction and would order "…:05.5Z" after "…:05.12Z". PostgreSQL and MySQL
// have real temporal types and take the time.Time itself.
func TestTimestampsAreBoundForTheDialect(t *testing.T) {
	t.Parallel()

	instant := time.Date(2024, time.June, 1, 0, 0, 0, 500000000, time.UTC)
	cond := expr("gt", variable("request.resource.attr.createdAt"),
		expr("timestamp", val(t, instant.Format(time.RFC3339Nano))))

	sqlite, args := whereFor(t, dialect.SQLite, testMapper(), cond)
	require.Equal(t, "(`resource`.`created_at` > ?)", sqlite)
	require.Equal(t, []any{"2024-06-01T00:00:00.500000000Z"}, args,
		"a fixed-width UTC layout is what makes text comparison chronological")
	require.Equal(t, len("2006-01-02T15:04:05.000000000Z"), len(args[0].(string)))

	for _, d := range []string{dialect.Postgres, dialect.MySQL} {
		_, args := whereFor(t, d, testMapper(), cond)
		require.Equal(t, []any{instant}, args, "%s has a real temporal type", d)
	}
}

// TestBooleanConstantsAvoidKeywords pins the tautology spelling. TRUE and FALSE are not portable
// keywords across every engine ent targets, and these constants are how a folded macro and an
// always-true filter reach the query at all, so they cannot be dialect-specific.
func TestBooleanConstantsAvoidKeywords(t *testing.T) {
	t.Parallel()

	// A macro over an empty literal collection folds to its identity: `exists` is false, `all` true.
	for _, tc := range []struct{ operator, want string }{
		{operator: "exists", want: "1 = 0"},
		{operator: "all", want: "1 = 1"},
	} {
		t.Run(tc.operator, func(t *testing.T) {
			t.Parallel()

			cond := expr(tc.operator, val(t, []any{}), expr("lambda", val(t, true), variable("t")))
			for _, d := range []string{dialect.SQLite, dialect.Postgres, dialect.MySQL} {
				query, _ := whereFor(t, d, testMapper(), cond)
				require.Equal(t, tc.want, query)
			}
		})
	}
}

// -- mapping hazards: the caller-declared store-side predicate -------------------------------------

// TestSubqueryFilterNarrowsEveryShapeBuiltOnTheRelation is the class 1 mapping-hazard contract
// (README, "Mapping hazards"). This adapter is handed a table name and two column names, so
// nothing the application applies to its own reads of that table reaches the generated subquery.
// Relation.SubqueryFilter closes that gap (cerbos/query-plan-adapters#323) — and it has to reach
// every subquery the translator builds over the relation, not just the one the author of the
// feature happened to look at. A shape that misses it silently reverts to reading the table bare.
func TestSubqueryFilterNarrowsEveryShapeBuiltOnTheRelation(t *testing.T) {
	t.Parallel()

	visible := cerbosent.Restriction{Column: "deleted_at", Op: cerbosent.RestrictIsNull}
	mapperFor := func(rel *cerbosent.Relation) cerbosent.Mapper {
		return cerbosent.MapperMap{
			"request.resource.attr.name": {Column: "name"},
			"request.resource.attr.tags": {Relation: rel},
		}
	}

	cases := []struct {
		cond *operand
		name string
	}{
		{name: "exists", cond: tagsExists(t)},
		{name: "negated exists", cond: expr("not", tagsExists(t))},
		{name: "all", cond: expr("all", variable("request.resource.attr.tags"),
			expr("lambda", expr("eq", variable("t.name"), val(t, "x")), variable("t")))},
		{name: "except", cond: expr("except", variable("request.resource.attr.tags"),
			expr("lambda", expr("eq", variable("t.name"), val(t, "x")), variable("t")))},
		{name: "exists_one", cond: expr("exists_one", variable("request.resource.attr.tags"),
			expr("lambda", expr("eq", variable("t.name"), val(t, "x")), variable("t")))},
		{name: "count", cond: expr("gt", expr("size", variable("request.resource.attr.tags")), val(t, 2))},
		{name: "membership", cond: expr("in", val(t, "x"), variable("request.resource.attr.tags"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bare, _ := translateWith(t, mapperFor(tagRelation()), tc.cond)
			require.NotContains(t, bare, "deleted_at",
				"undeclared must emit exactly what it emitted before the field existed")

			subqueries := strings.Count(bare, "FROM `tag`")
			require.NotZero(t, subqueries, "sanity: this shape must build a subquery at all")

			// A shape typically renders the relation more than once — a true witness and an
			// UNKNOWN witness, or a count beside a guard. Counting the restriction against the
			// number of subqueries catches one that picks it up only in some of them.
			declared, _ := translateWith(t, mapperFor(tagRelation(visible)), tc.cond)
			require.Equal(t, subqueries, strings.Count(declared, "`deleted_at` IS NULL"),
				"every subquery over the relation must carry the declared restriction")
		})
	}
}

// TestSubqueryFilterRestrictsIntermediateHops covers the flattened chain. A hop is read bare too,
// and it is the to-ONE parent whose absence must deny (#309) — a row the application hides is,
// for that purpose, absent, so the hop guard has to agree with the element subquery.
func TestSubqueryFilterRestrictsIntermediateHops(t *testing.T) {
	t.Parallel()

	mapper := cerbosent.MapperMap{
		"request.resource.attr.chain": {Relation: &cerbosent.Relation{
			Table:        "sub_category",
			SourceColumn: "id",
			TargetColumn: "category_id",
			Fields:       map[string]cerbosent.Entry{"name": {Column: "name"}},
			Via: []cerbosent.Hop{{
				Table:          "category",
				ChildColumn:    "category_id",
				JoinColumn:     "id",
				SubqueryFilter: []cerbosent.Restriction{{Column: "kind", Value: "main"}},
			}},
		}},
	}
	cond := expr("not", expr("exists", variable("request.resource.attr.chain"),
		expr("lambda", expr("eq", variable("s.name"), val(t, "x")), variable("s"))))

	query, _ := translateWith(t, mapper, cond)

	// The negated macro renders the element subquery twice (true and UNKNOWN witnesses) and the
	// hop-existence guard once, and all three read `category`. The guard is the one that matters:
	// without the restriction it answers "the parent exists" for a parent the application hides,
	// which is what turns the #309 fix back into an over-grant.
	require.Equal(t, 3, strings.Count(query, "`category` AS "))
	require.Equal(t, 3, strings.Count(query, "`kind` = "),
		"the hop guard must restrict the hop the same way the element subquery does")
}

// TestSubqueryFilterMembershipEdges pins the two list identities. `IN ()` is a syntax error, so
// the empty cases have to fold to constants rather than reach the renderer.
func TestSubqueryFilterMembershipEdges(t *testing.T) {
	t.Parallel()

	mapperFor := func(rel *cerbosent.Relation) cerbosent.Mapper {
		return cerbosent.MapperMap{"request.resource.attr.tags": {Relation: rel}}
	}
	cond := tagsExists(t)

	// ent's builder spells the boolean constants `1 = 0` / `1 = 1`; they lead the subquery's
	// WHERE because the restrictions are prepended to the correlation conjunction.
	empty, _ := translateWith(t, mapperFor(tagRelation(
		cerbosent.Restriction{Column: "kind", Op: cerbosent.RestrictIn})), cond)
	require.NotContains(t, empty, "IN ()")
	require.Contains(t, empty, "WHERE (1 = 0 AND", "membership in an empty list hides every row")

	emptyNot, _ := translateWith(t, mapperFor(tagRelation(
		cerbosent.Restriction{Column: "kind", Op: cerbosent.RestrictNotIn})), cond)
	require.NotContains(t, emptyNot, "IN ()")
	require.Contains(t, emptyNot, "WHERE (1 = 1 AND", "non-membership in an empty list hides none")

	listed, args := translateWith(t, mapperFor(tagRelation(
		cerbosent.Restriction{Column: "kind", Op: cerbosent.RestrictIn, Values: []any{"a", "b"}})), cond)
	require.Contains(t, listed, "`kind` IN (")
	require.NotContains(t, listed, "'a'", "the declared values are bound, never interpolated")
	require.Contains(t, args, "a")
	require.Contains(t, args, "b")
}

// TestRestrictionMismatchFailsClosed pins the safety property that makes Restriction's
// Value/Values pairing safe to leave unenforced: getting it wrong hides rows, never exposes them.
// A sum type with a constructor would be tidier, but this is the property that actually matters
// for an authorization filter, so it is asserted rather than assumed.
func TestRestrictionMismatchFailsClosed(t *testing.T) {
	t.Parallel()

	mapperFor := func(rel *cerbosent.Relation) cerbosent.Mapper {
		return cerbosent.MapperMap{"request.resource.attr.tags": {Relation: rel}}
	}
	cond := tagsExists(t)

	// Values supplied where Op reads Value: the comparison renders against NULL, which is
	// UNKNOWN, so the subquery matches nothing.
	valuesOnEq, args := translateWith(t, mapperFor(tagRelation(
		cerbosent.Restriction{Column: "kind", Op: cerbosent.RestrictEq, Values: []any{"a"}})), cond)
	require.Contains(t, valuesOnEq, "`kind` = NULL")
	require.NotContains(t, args, "a")

	// Value supplied where Op reads Values: the empty list folds to FALSE.
	valueOnIn, args := translateWith(t, mapperFor(tagRelation(
		cerbosent.Restriction{Column: "kind", Op: cerbosent.RestrictIn, Value: "a"})), cond)
	require.Contains(t, valueOnIn, "WHERE (1 = 0 AND")
	require.NotContains(t, args, "a")
}
