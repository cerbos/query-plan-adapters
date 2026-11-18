package domains_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/encoding/protojson"

	cerbospgx "github.com/cerbos/query-plan-adapters/pgx"
	"github.com/cerbos/query-plan-adapters/pgx/domains"
)

type golden struct {
	Plan          json.RawMessage `json:"plan"`
	PlanError     any             `json:"planError"`
	PDP           string          `json:"pdp"`
	PrincipalName string          `json:"principalName"`
	Action        string          `json:"action"`
	Allowed       []string        `json:"allowed"`
}

type result struct {
	Domain    string   `json:"domain"`
	PDP       string   `json:"pdp"`
	Principal string   `json:"principal"`
	Action    string   `json:"action"`
	Verdict   string   `json:"verdict"`
	Returned  []string `json:"returned"`
	Allowed   []string `json:"allowed"`
	Detail    string   `json:"detail"`
}

func TestDomains(t *testing.T) {
	root := repoRoot(t)
	image, err := os.ReadFile(filepath.Join(root, "pgx", "POSTGRES_IMAGE"))
	require.NoError(t, err)
	c, err := postgres.Run(t.Context(), strings.TrimSpace(string(image)),
		postgres.WithDatabase("domains"), postgres.WithUsername("domains"), postgres.WithPassword("domains"),
		testcontainers.WithEnv(map[string]string{"POSTGRES_INITDB_ARGS": "--locale-provider=icu --icu-locale=en-US"}),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(2*time.Minute)),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, c)
	dsn, err := c.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	results := []result{}
	for _, d := range domains.All {
		script, err := os.ReadFile(filepath.Join(root, "domains", d.Name, "generated", "sql", "postgres.sql"))
		require.NoError(t, err)
		_, err = pool.Exec(t.Context(), string(script))
		require.NoError(t, err, d.Name)
		results = append(results, replay(t, root, d, pool)...)
	}
	writeResults(t, root, "postgres", results)
}

func replay(t *testing.T, root string, d domains.Domain, pool *pgxpool.Pool) []result {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "domains", d.Name, "golden", "*", "*", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	var out []result
	for _, f := range files {
		var g golden
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &g))
		allowed := sorted(g.Allowed)
		r := result{Domain: d.Name, PDP: g.PDP, Principal: g.PrincipalName, Action: g.Action, Allowed: allowed, Returned: []string{}}
		returned, unmapped, sqlText, err := run(t.Context(), d, pool, g)
		r.Verdict, r.Detail = verdict(allowed, returned, err)
		if returned != nil {
			r.Returned = returned
		}
		if len(unmapped) > 0 {
			r.Detail = "unmapped " + strings.Join(unmapped, ", ") + ": " + r.Detail
		}
		if r.Verdict == "over-grant" || r.Verdict == "under-grant" {
			r.Detail = sqlText
		}
		out = append(out, r)
	}
	return out
}

func run(ctx context.Context, d domains.Domain, pool *pgxpool.Pool, g golden) ([]string, []string, string, error) {
	if g.PlanError != nil {
		return nil, nil, "", fmt.Errorf("plan error: %v", g.PlanError)
	}
	filter := &enginev1.PlanResourcesFilter{}
	if err := protojson.Unmarshal(g.Plan, filter); err != nil {
		return nil, nil, "", fmt.Errorf("decoding plan: %w", err)
	}
	var unmapped []string
	recording := cerbospgx.MapperFunc(func(ref string) (cerbospgx.Entry, bool) {
		e, ok := d.Mapper.Resolve(ref)
		if !ok && strings.HasPrefix(ref, "request.resource.") && !slices.Contains(unmapped, ref) {
			unmapped = append(unmapped, ref)
		}
		return e, ok
	})
	res, err := cerbospgx.Translate(&responsev1.PlanResourcesResponse{Filter: filter}, d.Table, recording,
		cerbospgx.WithNullRepresentation(d.Nulls))
	if err != nil {
		return nil, unmapped, "", err
	}
	query := fmt.Sprintf(`SELECT %q FROM %q WHERE %s`, d.ID, d.Table, res.Where)
	sqlText := fmt.Sprintf("%s %v", query, res.Args)
	rows, err := pool.Query(ctx, query, res.Args...)
	if err != nil {
		return nil, unmapped, sqlText, fmt.Errorf("query: %w; SQL: %s", err, sqlText)
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, unmapped, sqlText, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, unmapped, sqlText, fmt.Errorf("query: %w; SQL: %s", err, sqlText)
	}
	return sorted(ids), unmapped, sqlText, nil
}

func verdict(allowed, returned []string, err error) (string, string) {
	switch {
	case errors.Is(err, cerbospgx.ErrUnsupported):
		return "refused", err.Error()
	case err != nil:
		return "error", err.Error()
	case slices.ContainsFunc(returned, func(id string) bool { return !slices.Contains(allowed, id) }):
		return "over-grant", ""
	case slices.Equal(returned, allowed):
		return "pass", ""
	case len(slices.Compact(slices.Clone(returned))) != len(returned):
		return "error", "duplicate ids"
	default:
		return "under-grant", ""
	}
}

func sorted(ids []string) []string {
	out := append([]string{}, ids...)
	slices.Sort(out)
	return out
}

func writeResults(t *testing.T, root, storeName string, results []result) {
	t.Helper()
	slices.SortFunc(results, func(a, b result) int {
		return strings.Compare(a.Domain+a.PDP+a.Principal+a.Action, b.Domain+b.PDP+b.Principal+b.Action)
	})
	counts := map[string]int{}
	for _, r := range results {
		counts[r.Verdict]++
	}
	t.Logf("%s: %v", storeName, counts)
	raw, err := json.MarshalIndent(map[string]any{"adapter": "pgx", "store": storeName, "results": results}, "", "  ")
	require.NoError(t, err)
	dir := filepath.Join(root, "domains", "results", "pgx")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, storeName+".json"), append(raw, '\n'), 0o644))
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "domains", "kit")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "no domains/kit above the working directory")
		dir = parent
	}
}
