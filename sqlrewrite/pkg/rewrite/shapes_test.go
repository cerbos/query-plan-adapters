// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

//go:build tests

package rewrite_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	"github.com/stretchr/testify/require"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/rewrite"
)

// SQL-shape suite. The conformance suite proves the filter on the one statement
// `SELECT id FROM adversarial_resource`; this one proves where the filter lands in the statements
// callers actually write. Each shape is a template in which {R} is the protected table and {R:a}
// the protected table aliased a. The rewritten statement runs against the seeded store, and the
// oracle is the same template with every {R} replaced by a derived table holding only the
// golden's recorded allowed ids, so it never goes through the rewriter or the translator.

type shape struct {
	name   string
	sql    string
	params []any
}

var shapes = []shape{
	{name: "bare", sql: "SELECT id FROM {R}"},
	{name: "alias-and-caller-param", sql: "SELECT r.id FROM {R:r} WHERE r.id <> {P1}", params: []any{"a1"}},
	{name: "inner-join", sql: "SELECT r.id, t.tag_id FROM {R:r} JOIN adversarial_tag t ON t.resource_id = r.id"},
	{name: "left-join-onto-protected", sql: "SELECT p.id, r.id FROM adversarial_parent p LEFT JOIN {R:r} ON r.id = p.resource_id"},
	{name: "cte", sql: "WITH x AS (SELECT id FROM {R}) SELECT id FROM x"},
	{name: "in-subquery", sql: "SELECT tag_id FROM adversarial_tag WHERE resource_id IN (SELECT id FROM {R})"},
	{name: "correlated-exists", sql: "SELECT t.tag_id FROM adversarial_tag t WHERE EXISTS (SELECT 1 FROM {R:r} WHERE r.id = t.resource_id)"},
	{name: "scalar-count", sql: "SELECT (SELECT count(*) FROM {R}) AS n"},
	{name: "union-all", sql: "SELECT id FROM {R} WHERE created_by = {P1} UNION ALL SELECT id FROM {R}", params: []any{"user1"}},
	{name: "self-join", sql: "SELECT a.id, b.id FROM {R:a} JOIN {R:b} ON a.id = b.id"},
	{name: "group-by", sql: "SELECT created_by, count(*) FROM {R} GROUP BY created_by"},
	{name: "derived-table", sql: "SELECT d.id FROM (SELECT id FROM {R}) d"},
	{name: "order-limit", sql: "SELECT id FROM {R} ORDER BY id LIMIT 5"},
	{name: "params-either-side", sql: "SELECT r.id FROM adversarial_tag t JOIN {R:r} ON r.id = t.resource_id AND t.tag_id <> {P1} WHERE r.id <> {P2}", params: []any{"zz", "a2"}},
}

// expand writes a template as the statement a caller sends (oracle == nil) or as the oracle.
func expand(tmpl string, d rewrite.Dialect, oracle []string) string {
	placeholder := func(i int) string {
		if d == rewrite.DialectPostgres {
			return fmt.Sprintf("$%d", i)
		}
		return "?"
	}
	for i := 1; i <= 2; i++ {
		tmpl = strings.ReplaceAll(tmpl, fmt.Sprintf("{P%d}", i), placeholder(i))
	}

	allowed := "SELECT * FROM " + resourceTable + " WHERE 1 = 0"
	if len(oracle) > 0 {
		quoted := make([]string, len(oracle))
		for i, id := range oracle {
			quoted[i] = "'" + id + "'"
		}
		allowed = "SELECT * FROM " + resourceTable + " WHERE id IN (" + strings.Join(quoted, ", ") + ")"
	}

	var b strings.Builder
	for {
		start := strings.Index(tmpl, "{R")
		if start < 0 {
			b.WriteString(tmpl)
			return b.String()
		}
		end := start + strings.Index(tmpl[start:], "}")
		alias := strings.TrimPrefix(tmpl[start+2:end], ":")
		b.WriteString(tmpl[:start])
		switch {
		case oracle == nil && alias == "":
			b.WriteString(resourceTable)
		case oracle == nil:
			b.WriteString(resourceTable + " " + alias)
		default:
			if alias == "" {
				alias = resourceTable
			}
			b.WriteString("(" + allowed + ") " + alias)
		}
		tmpl = tmpl[end+1:]
	}
}

func (s *store) rows(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%w\nSQL: %s", err, query)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []string{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				vals[i] = string(b)
			}
		}
		out = append(out, fmt.Sprint(vals...))
	}
	slices.Sort(out)
	return out, rows.Err()
}

func TestSQLShapes(t *testing.T) {
	corpus := loadCorpus(t)
	tag := corpus.Tags[len(corpus.Tags)-1]

	for _, tgt := range targets() {
		t.Run(tgt.name, func(t *testing.T) {
			rw, err := rewrite.New(loadConfig(t, tgt.dialect))
			require.NoError(t, err)
			s := openStore(t, tgt, corpus)

			for _, golden := range corpus.Goldens[tag] {
				if _, listed := corpus.Ledger[golden.ID]; listed || golden.Skipped() || golden.Tier == "adversarial" {
					continue
				}
				plan, err := golden.PlanResponse(time.Now())
				require.NoError(t, err)

				t.Run(golden.ID, func(t *testing.T) {
					for _, sh := range shapes {
						res, err := rw.Rewrite(t.Context(), rewrite.Request{
							SQL:    expand(sh.sql, tgt.dialect, nil),
							Params: sh.params,
							Plans:  map[string]*responsev1.PlanResourcesResponse{"conformance": plan},
						})
						require.NoError(t, err, sh.name)
						got, err := s.rows(t.Context(), res.SQL, res.Params...)
						require.NoError(t, err, sh.name)

						want, err := s.rows(t.Context(), expand(sh.sql, tgt.dialect, golden.Allowed), sh.params...)
						require.NoError(t, err, sh.name)
						require.Equal(t, want, got, "%s\nSQL: %s", sh.name, res.SQL)
					}
				})
			}
		})
	}
}

// Every statement here reads the protected table in a way the rewriter cannot filter, or misuses
// the request, so it must be refused with the stated code and nothing executed.
func TestSQLShapeRefusals(t *testing.T) {
	corpus := loadCorpus(t)
	tag := corpus.Tags[len(corpus.Tags)-1]
	var plan *responsev1.PlanResourcesResponse
	for _, golden := range corpus.Goldens[tag] {
		if golden.ID == "string/equals/case-sensitive" {
			p, err := golden.PlanResponse(time.Now())
			require.NoError(t, err)
			plan = p
		}
	}
	require.NotNil(t, plan)

	type refusal struct {
		name    string
		sql     string
		params  []any
		code    rewrite.Code
		only    rewrite.Dialect
		noPlans bool
	}
	cases := []refusal{
		{name: "delete", sql: "DELETE FROM {R}", code: rewrite.CodeUnsupportedSQL},
		{name: "update", sql: "UPDATE {R} SET scope = 'x'", code: rewrite.CodeUnsupportedSQL},
		{name: "two-statements", sql: "SELECT id FROM {R}; SELECT id FROM {R}", code: rewrite.CodeUnsupportedSQL},
		{name: "cte-shadows-protected", sql: "WITH " + resourceTable + " AS (SELECT 'x' AS id) SELECT id FROM " + resourceTable, code: rewrite.CodeAmbiguousReference},
		{name: "reserved-alias", sql: "SELECT cerbos_resource.id FROM {R:cerbos_resource}", code: rewrite.CodeUnsupportedSQL},
		{name: "reserved-marker", sql: "SELECT id AS cerbos_filter_0_ FROM {R}", code: rewrite.CodeUnsupportedSQL},
		{name: "too-few-params", sql: "SELECT id FROM {R} WHERE id = {P1}", code: rewrite.CodeInvalidRequest},
		{name: "no-plans", sql: "SELECT id FROM {R}", code: rewrite.CodeInvalidRequest, noPlans: true},
		{name: "unused-plan", sql: "SELECT tag_id FROM adversarial_tag", code: rewrite.CodeUnusedPlan},
		{name: "query-to-xml", sql: "SELECT query_to_xml('SELECT * FROM " + resourceTable + "', true, true, '')", code: rewrite.CodeUnsupportedSQL, only: rewrite.DialectPostgres},
		{name: "select-into", sql: "SELECT id INTO copy FROM {R}", code: rewrite.CodeUnsupportedSQL, only: rewrite.DialectPostgres},
		{name: "in-table", sql: "SELECT tag_id FROM adversarial_tag WHERE resource_id IN " + resourceTable, code: rewrite.CodeUnsupportedSQL, only: rewrite.DialectSQLite},
	}

	for _, d := range []rewrite.Dialect{rewrite.DialectSQLite, rewrite.DialectPostgres, rewrite.DialectMySQL} {
		rw, err := rewrite.New(loadConfig(t, d))
		require.NoError(t, err)
		for _, c := range cases {
			if c.only != "" && c.only != d {
				continue
			}
			t.Run(string(d)+"/"+c.name, func(t *testing.T) {
				plans := map[string]*responsev1.PlanResourcesResponse{"conformance": plan}
				if c.noPlans {
					plans = nil
				}
				res, err := rw.Rewrite(t.Context(), rewrite.Request{SQL: expand(c.sql, d, nil), Params: c.params, Plans: plans})
				require.ErrorIs(t, err, rewrite.ErrRefused, "rewritten to %v", res)
				var refusal *rewrite.RefusalError
				require.True(t, errors.As(err, &refusal))
				require.Equal(t, c.code, refusal.Code, refusal.Message)
			})
		}
	}
}
