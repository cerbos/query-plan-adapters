// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cerbos/query-plan-adapters/pgx/internal/queryplan"
)

// The renderer's error path is the one thing translate_test.go cannot reach through the public API.
// Every node the translator emits renders, and the nodes that do not — an Expr type the switch does
// not know, a Call naming a function with no spelling, a Cast to a target with no spelling — are
// unconstructible from a plan: nothing outside the internal package can add an Expr type, and every
// FuncName and CastType the translator uses is handled. So these live here, in the package's own
// test binary, where a node the walk never produces can be built by hand. The ent module carries the
// same file for the same reason. No Docker required.

func renderExpr(t *testing.T, e queryplan.Expr) error {
	t.Helper()

	_, _, err := render(e, 0)
	return err
}

// TestUnrenderableFunctionIsRejected pins that a function with no spelling is refused rather than
// emitted as something else.
func TestUnrenderableFunctionIsRejected(t *testing.T) {
	t.Parallel()

	err := renderExpr(t, queryplan.Call{Name: queryplan.FuncName("notAFunction")})
	require.ErrorContains(t, err, "notAFunction")
}

// TestRenderErrorsEscapeNestedNodes pins that a failure raised deep in the tree reaches the caller
// rather than being swallowed by an enclosing node. Everything the renderer emits is parenthesised,
// so an error more than one level down is the case that would be lost.
func TestRenderErrorsEscapeNestedNodes(t *testing.T) {
	t.Parallel()

	nested := queryplan.Not{X: queryplan.Cmp{
		Op: queryplan.OpEq,
		L:  queryplan.Column{Qualifier: "resource", Name: "name"},
		R:  queryplan.Call{Name: queryplan.FuncName("notAFunction")},
	}}

	require.ErrorContains(t, renderExpr(t, nested), "notAFunction")
}

// TestUnknownCastTargetIsRejected pins that the cast spelling table fails closed. Only the two
// targets the translator emits have a spelling, and CastType is an exported string type, so a third
// is buildable here. Falling through to the float spelling would compare against a value the policy
// never named — a wrong filter where an error is the whole contract.
func TestUnknownCastTargetIsRejected(t *testing.T) {
	t.Parallel()

	cast := queryplan.Cast{X: queryplan.Column{Name: "count"}, To: queryplan.CastType("decimal")}
	require.ErrorContains(t, renderExpr(t, cast), `cannot render cast to "decimal"`)
}
