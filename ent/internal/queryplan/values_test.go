// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
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
