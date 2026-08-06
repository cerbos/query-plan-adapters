// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx_test

import (
	"strings"
	"testing"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	cerbospgx "github.com/cerbos/query-plan-adapters/pgx"
)

// Unit tests over hand-built plans. These complement the adversarial suite rather than duplicating
// it: the corpus proves semantics against a real PDP but only ever feeds the adapter plans the
// planner actually emits, so it cannot say what happens to a malformed or hostile one. Everything
// here runs without Docker.

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

func testMapper() cerbospgx.Mapper {
	return cerbospgx.MapperMap{
		"request.resource.attr.name":  {Column: "name"},
		"request.resource.attr.count": {Column: "count"},
		"request.resource.attr.owner": {Column: "owner"},
		"request.resource.attr.tags":  {Relation: &cerbospgx.Relation{Table: "tag", SourceColumn: "id", TargetColumn: "resource_id", Field: &cerbospgx.Entry{Column: "name"}, Fields: map[string]cerbospgx.Entry{"name": {Column: "name"}}}},
	}
}

func translate(t *testing.T, cond *operand, opts ...cerbospgx.Option) (cerbospgx.Result, error) {
	t.Helper()
	return cerbospgx.Translate(conditional(cond), "resource", testMapper(), opts...)
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
			require.ErrorIs(t, err, cerbospgx.ErrUnsupported)
		})
	}
}

// TestNilConditionIsNotAnAllow pins the fail-closed direction of a filter with no condition: it
// must never widen into "no filter", which would return every row.
func TestNilConditionIsNotAnAllow(t *testing.T) {
	t.Parallel()

	result, err := cerbospgx.Translate(&responsev1.PlanResourcesResponse{}, "resource", testMapper())
	require.NoError(t, err)
	require.Equal(t, cerbospgx.KindAlwaysDenied, result.Kind)
}

func TestPlanKinds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		kind enginev1.PlanResourcesFilter_Kind
		want cerbospgx.PlanKind
	}{
		{name: "denied", kind: enginev1.PlanResourcesFilter_KIND_ALWAYS_DENIED, want: cerbospgx.KindAlwaysDenied},
		{name: "allowed", kind: enginev1.PlanResourcesFilter_KIND_ALWAYS_ALLOWED, want: cerbospgx.KindAlwaysAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plan := &responsev1.PlanResourcesResponse{Filter: &enginev1.PlanResourcesFilter{Kind: tc.kind}}
			result, err := cerbospgx.Translate(plan, "resource", testMapper())
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Kind)
		})
	}
}

// -- emitted SQL ---------------------------------------------------------------------------------

// TestNoPlanDataReachesSQLText is the injection guard. Every value in the plan below is chosen to
// be SQL syntax if it were ever interpolated rather than bound.
func TestNoPlanDataReachesSQLText(t *testing.T) {
	t.Parallel()

	hostile := `'; DROP TABLE resource; --`
	result, err := translate(t, expr("or",
		expr("eq", variable("request.resource.attr.name"), val(t, hostile)),
		expr("contains", variable("request.resource.attr.owner"), val(t, hostile)),
		expr("in", variable("request.resource.attr.name"), val(t, []any{hostile, `" OR 1=1 --`})),
	))
	require.NoError(t, err)

	require.NotContains(t, result.Where, "DROP TABLE")
	require.NotContains(t, result.Where, "--")
	require.NotContains(t, result.Where, "'")
	require.Contains(t, result.Args, hostile)
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

			result, err := translate(t, expr(tc.operator, val(t, 3), variable("request.resource.attr.count")))
			require.NoError(t, err)
			require.Equal(t, `("resource"."count" `+tc.want+` $1::double precision)`, result.Where)
			require.Equal(t, []any{float64(3)}, result.Args)
		})
	}
}

// TestSymmetricComparisonsNormaliseToColumnFirst covers the other half of the rule: eq/ne/in mean
// the same thing either way round, so they normalise rather than mirror.
func TestSymmetricComparisonsNormaliseToColumnFirst(t *testing.T) {
	t.Parallel()

	result, err := translate(t, expr("eq", val(t, "x"), variable("request.resource.attr.name")))
	require.NoError(t, err)
	require.Equal(t, `("resource"."name" = $1::text)`, result.Where)
}

// TestReceiverSensitiveOperatorsKeepWireOrder is the reason eq/ne/in are normalised by name rather
// than by "put the column first": swapping `"const".contains(col)` would silently exchange the
// haystack and the needle.
func TestReceiverSensitiveOperatorsKeepWireOrder(t *testing.T) {
	t.Parallel()

	result, err := translate(t, expr("contains", val(t, "haystack"), variable("request.resource.attr.name")))
	require.NoError(t, err)
	require.Contains(t, result.Where, "$1::text LIKE")
	require.Equal(t, "haystack", result.Args[0])
}

// TestLikeMetacharactersAreEscaped pins that policy data cannot act as a wildcard.
func TestLikeMetacharactersAreEscaped(t *testing.T) {
	t.Parallel()

	result, err := translate(t, expr("startsWith", variable("request.resource.attr.name"), val(t, `100%_a[b\`)))
	require.NoError(t, err)
	require.Equal(t, `100\%\_a\[b\\%`, result.Args[0])
	require.Contains(t, result.Where, "ESCAPE")
}

// TestNullComparisonBecomesIsNull pins the default (explicit-null) representation.
func TestNullComparisonBecomesIsNull(t *testing.T) {
	t.Parallel()

	result, err := translate(t, expr("eq", variable("request.resource.attr.owner"), val(t, nil)))
	require.NoError(t, err)
	require.Equal(t, `("resource"."owner" IS NULL)`, result.Where)
	require.Empty(t, result.Args)
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

			_, err := translate(t, tc.cond, cerbospgx.WithNullRepresentation(cerbospgx.NullOmitted))
			require.ErrorIs(t, err, cerbospgx.ErrUnsupported)
			require.Contains(t, err.Error(), "null operand")

			// The same plan translates fine under the default representation, so the rejection is
			// the option talking rather than an unrelated failure.
			_, err = translate(t, tc.cond)
			require.NoError(t, err)
		})
	}
}

// TestPlaceholderOffset pins that the fragment can be appended to a query that already binds
// arguments — numbering has to start after the caller's, not at $1.
func TestPlaceholderOffset(t *testing.T) {
	t.Parallel()

	result, err := translate(t,
		expr("and",
			expr("eq", variable("request.resource.attr.name"), val(t, "a")),
			expr("eq", variable("request.resource.attr.owner"), val(t, "b")),
		),
		cerbospgx.WithPlaceholderOffset(2))
	require.NoError(t, err)
	require.Contains(t, result.Where, "$3::text")
	require.Contains(t, result.Where, "$4::text")
	require.NotContains(t, result.Where, "$1")
	require.Len(t, result.Args, 2)
}

// TestIdentifiersAreQuoted pins that a mapper naming a column with a quote in it cannot break out
// of the identifier. Mapper contents are caller-supplied rather than plan-supplied, so this is
// defence in depth rather than an injection boundary.
func TestIdentifiersAreQuoted(t *testing.T) {
	t.Parallel()

	mapper := cerbospgx.MapperMap{
		`request.resource.attr.name`: {Column: `we"ird`},
	}
	result, err := cerbospgx.Translate(
		conditional(expr("eq", variable("request.resource.attr.name"), val(t, "x"))),
		"resource", mapper)
	require.NoError(t, err)
	require.Contains(t, result.Where, `"we""ird"`)
	require.Equal(t, 1, strings.Count(result.Where, `"we""ird"`))
}

// TestTranslateRejectsMissingArguments covers the API preconditions.
func TestTranslateRejectsMissingArguments(t *testing.T) {
	t.Parallel()

	cond := expr("eq", variable("request.resource.attr.name"), val(t, "x"))

	_, err := cerbospgx.Translate(nil, "resource", testMapper())
	require.Error(t, err)

	_, err = cerbospgx.Translate(conditional(cond), "", testMapper())
	require.Error(t, err)

	_, err = cerbospgx.Translate(conditional(cond), "resource", nil)
	require.Error(t, err)
}

// TestRootTableCannotShadowGeneratedAliases pins the guard against a resource table named like a
// generated subquery alias. Without it the inner alias would shadow the outer table and the
// correlation would silently compare the subquery's row against itself.
func TestRootTableCannotShadowGeneratedAliases(t *testing.T) {
	t.Parallel()

	cond := expr("exists", variable("request.resource.attr.tags"),
		expr("lambda", expr("eq", variable("t.name"), val(t, "x")), variable("t")))

	_, err := cerbospgx.Translate(conditional(cond), "cerbos_rel_1", testMapper())
	require.ErrorIs(t, err, cerbospgx.ErrUnsupported)
	require.Contains(t, err.Error(), "reserved for generated subquery aliases")

	// Any other table name still works, so the guard is narrow.
	_, err = cerbospgx.Translate(conditional(cond), "resource", testMapper())
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
			require.ErrorIs(t, err, cerbospgx.ErrUnsupported)
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
			require.ErrorIs(t, err, cerbospgx.ErrUnsupported)
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

	_, err := translate(t, cond, cerbospgx.WithNullRepresentation(cerbospgx.NullOmitted))
	require.ErrorIs(t, err, cerbospgx.ErrUnsupported)
	require.Contains(t, err.Error(), "null operand")
}

// TestRelationMembershipRespectsNullRepresentation pins that null-safe equality is used only under
// the explicit convention. Under NullOmitted a NULL column carries no attribute, so CEL errors and
// denies; treating it as a definite non-match would make `!(x in coll)` true for exactly those rows.
func TestRelationMembershipRespectsNullRepresentation(t *testing.T) {
	t.Parallel()

	cond := expr("in", variable("request.resource.attr.owner"), variable("request.resource.attr.tags"))

	explicit, err := translate(t, cond)
	require.NoError(t, err)
	require.Contains(t, explicit.Where, "IS NOT DISTINCT FROM")

	omitted, err := translate(t, cond, cerbospgx.WithNullRepresentation(cerbospgx.NullOmitted))
	require.NoError(t, err)
	require.NotContains(t, omitted.Where, "IS NOT DISTINCT FROM")
	require.Contains(t, omitted.Where, `"cerbos_rel_1"."name" = "resource"."owner"`)
}

// TestIntCastTruncatesTowardZero pins CEL conversion semantics: int(1.9) is 1, but PostgreSQL's
// float-to-bigint cast rounds to 2, which over-grants on an equality threshold.
func TestIntCastTruncatesTowardZero(t *testing.T) {
	t.Parallel()

	result, err := translate(t, expr("eq", expr("int", variable("request.resource.attr.count")), val(t, 2)))
	require.NoError(t, err)
	require.Contains(t, result.Where, "CAST(trunc(")
}

// TestMapperQualifierCannotShadowGeneratedAliases covers the other half of the alias guard: the
// collision can come from an Entry's own qualifier, not just the resource table.
func TestMapperQualifierCannotShadowGeneratedAliases(t *testing.T) {
	t.Parallel()

	mapper := cerbospgx.MapperMap{
		"request.resource.attr.name": {Column: "name", Qualifier: "cerbos_rel_1"},
	}
	_, err := cerbospgx.Translate(
		conditional(expr("eq", variable("request.resource.attr.name"), val(t, "x"))),
		"resource", mapper)
	require.ErrorIs(t, err, cerbospgx.ErrUnsupported)
}
