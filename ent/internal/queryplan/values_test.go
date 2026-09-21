// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// A symbolic operand added without an asExpr arm must never become a bound parameter.
type futureSymbolicValue struct{}

func (futureSymbolicValue) isSymbolicValue() {}

func TestUnloweredSymbolicValueIsRejected(t *testing.T) {
	t.Parallel()
	result, err := asExpr(futureSymbolicValue{})
	require.ErrorContains(t, err, "symbolic value queryplan.futureSymbolicValue has no SQL representation")
	require.Nil(t, result)
}

// The corpus's nan-ord-* and not-nan-ord-le actions prove the row semantics. This pins the
// lowered constant without involving a database renderer, including the distinction from NULL.
func TestNaNOrderingFoldsToFalse(t *testing.T) {
	t.Parallel()
	for _, op := range []CmpOp{OpLt, OpLe, OpGt, OpGe} {
		result, err := compareLeaf(op, ieeeConst{v: math.NaN()}, float64(2))
		require.NoError(t, err)
		require.Equal(t, BoolConst{V: false}, result)
		result, err = compareLeaf(op, float64(2), ieeeConst{v: math.NaN()})
		require.NoError(t, err)
		require.Equal(t, BoolConst{V: false}, result)
	}
}

// A boolean context must infer the NULL type. PostgreSQL independently resolves an all-NULL
// CASE to text, so adding a presence guard here makes the corpus's not-nan-order-string fail.
func TestMixedTypeOrderingHasNoRedundantNullGuard(t *testing.T) {
	t.Parallel()
	result, err := applyComparison(OpLt, float64(1), Column{Name: "text", Type: ValueString})
	require.NoError(t, err)
	require.Equal(t, Lit{V: nil}, result)
}
