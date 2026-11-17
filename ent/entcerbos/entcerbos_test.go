// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package entcerbos_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cerbos/query-plan-adapters/ent/entcerbos"
	"github.com/cerbos/query-plan-adapters/ent/internal/conformanceschema"
)

// The conformance harness maps through the generated CerbosSchema; loading the same schema
// types at runtime must give the same source. ENTCERBOS_UPDATE=1 rewrites it.
func TestGeneratedSchemaMatchesLoad(t *testing.T) {
	schema, err := entcerbos.Load(conformanceschema.All()...)
	require.NoError(t, err)
	src, err := entcerbos.Source("conformanceschema", schema)
	require.NoError(t, err)

	const generated = "../internal/conformanceschema/cerbos_schema.go"
	if os.Getenv("ENTCERBOS_UPDATE") == "1" {
		require.NoError(t, os.WriteFile(generated, src, 0o600))
	}
	want, err := os.ReadFile(generated)
	require.NoError(t, err)
	require.Equal(t, string(want), string(src))
}
