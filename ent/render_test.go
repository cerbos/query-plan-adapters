// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent

import (
	"testing"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/stretchr/testify/require"

	"github.com/cerbos/query-plan-adapters/ent/internal/queryplan"
)

// The renderer's error path is the one thing translate_test.go cannot reach through the public API.
// Every node the translator emits renders, and the two nodes that do not — an Expr type the switch
// does not know, a Call naming a function the dialect table does not spell — are unconstructible
// from a plan: nothing outside the internal package can add an Expr type, and every FuncName the
// translator uses is handled. So these live here, in the package's own test binary, where a node
// the walk never produces can be built by hand.
//
// They are not decoration. render() writes the tree twice — once into a throwaway builder so the
// failure surfaces from Translate, once inside the lazy predicate — and ent's sql.Builder.Wrap
// silently discards errors recorded on the nested builder it hands the callback. Everything the
// renderer emits is parenthesised, so an error raised more than one level down is exactly the case
// that would be lost. No Docker required.

// unrenderable is a Call whose FuncName has no dialect spelling. FuncName is an exported string
// type, so this is buildable here while being something the translator never emits.
var unrenderable = queryplan.Call{Name: queryplan.FuncName("notAFunction")}

// TestRenderRejectsUnrenderableNodesBeforeReturning pins the probe pass: the failure has to come
// back from render (and so from Translate) rather than from the caller's query builder, where it
// would read as a malformed statement rather than a refusal to translate.
func TestRenderRejectsUnrenderableNodesBeforeReturning(t *testing.T) {
	t.Parallel()

	for _, d := range []string{dialect.SQLite, dialect.Postgres, dialect.MySQL} {
		t.Run(d, func(t *testing.T) {
			t.Parallel()

			predicate, err := render(unrenderable, d)
			require.Error(t, err)
			require.ErrorContains(t, err, "notAFunction")
			require.Nil(t, predicate, "a predicate that cannot render must not be handed back")
		})
	}
}

// TestUnknownCastTargetIsRejected pins that the cast spelling table fails closed. Only the two
// targets the translator emits have a spelling, and CastType is an exported string type, so a third
// is buildable here. Falling through to the float spelling would compare against a value the policy
// never named — a wrong filter where an error is the whole contract.
func TestUnknownCastTargetIsRejected(t *testing.T) {
	t.Parallel()

	cast := queryplan.Cast{X: queryplan.Column{Name: "count"}, To: queryplan.CastType("decimal")}

	for _, d := range []string{dialect.SQLite, dialect.Postgres, dialect.MySQL} {
		t.Run(d, func(t *testing.T) {
			t.Parallel()

			_, err := render(cast, d)
			require.ErrorContains(t, err, `cannot render cast to "decimal"`)
		})
	}
}

// TestRenderErrorsEscapeNestedParentheses is the assertion that makes the probe pass trustworthy.
// sql.Builder.Wrap gives the callback a fresh builder and copies back only its string and args, so
// an error recorded inside a Wrap is dropped. render's helper therefore has to carry the error out
// of the closure and return it instead of calling AddError on the builder it was handed. Nesting
// the bad node under a comparison under a negation puts it two Wraps deep, where the difference
// between the two designs is the difference between a refusal and a silently truncated query.
func TestRenderErrorsEscapeNestedParentheses(t *testing.T) {
	t.Parallel()

	nested := queryplan.Not{X: queryplan.Cmp{
		Op: queryplan.OpEq,
		L:  queryplan.Column{Qualifier: "resource", Name: "name"},
		R:  unrenderable,
	}}

	_, err := render(nested, dialect.SQLite)
	require.ErrorContains(t, err, "notAFunction")
}

// TestSelectorReportsPredicateErrors pins the contract the adversarial harness depends on when it
// checks selector.Err() after Query() (cerbos/query-plan-adapters#319).
//
// The predicate is lazy, so its second write pass runs inside the caller's Query(). ent reports a
// failure there by collecting it on the selector rather than by returning it, and Query() still
// hands back the SQL the failed pass left behind — which parses, so it executes, and a truncated
// WHERE clause returns rows nothing authorised. Only Err() distinguishes the two, which is why the
// harness has to read it; this test fails if a future ent stops propagating and that check goes
// quiet.
func TestSelectorReportsPredicateErrors(t *testing.T) {
	t.Parallel()

	// The predicate render installs, over a tree that fails on the write pass. Built through
	// predicateFor rather than restated here, so the test cannot drift from what render does.
	predicate := predicateFor(unrenderable)

	selector := sql.Dialect(dialect.SQLite).Select("id").From(sql.Table("resource"))
	selector.Where(predicate)

	query, _ := selector.Query()
	require.Contains(t, query, "SELECT", "Query() hands back runnable SQL regardless")
	require.ErrorContains(t, selector.Err(), "notAFunction",
		"a predicate failure must be readable from the selector, or the harness check is vacuous")
}
