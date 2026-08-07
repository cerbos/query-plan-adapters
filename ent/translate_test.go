// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"strings"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
)

// Unit tests over hand-built plans, complementing the adversarial suite: the corpus proves
// semantics against a real PDP but only ever feeds the adapter plans the planner actually emits.
// Everything here runs without Docker.

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

// translateWith translates a plan and renders the resulting predicate to SQL and bound args.
func translateWith(t *testing.T, mapper cerbosent.Mapper, cond *operand) (string, []any) {
	t.Helper()
	result, err := cerbosent.Translate(conditional(cond), "resource", mapper)
	require.NoError(t, err)
	require.Equal(t, cerbosent.KindConditional, result.Kind)

	selector := entsql.Dialect(dialect.SQLite).Select("id").From(entsql.Table("resource"))
	selector.Where(result.Predicate)
	query, args := selector.Query()
	return query, args
}

// tagsExists is `R.attr.tags.exists(t, t.name == "x")`.
func tagsExists(t *testing.T) *operand {
	t.Helper()
	return expr("exists", variable("request.resource.attr.tags"),
		expr("lambda", expr("eq", variable("t.name"), val(t, "x")), variable("t")))
}

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
