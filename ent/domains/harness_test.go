package domains_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/encoding/protojson"
	_ "modernc.org/sqlite"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/domains"
)

// Run one store leg at a time: DOMAIN_STORES=sqlite go test ./... (default: all three).

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

type store struct {
	open    func(t *testing.T, domain string) *sql.DB
	name    string
	dialect string
}

func TestDomains(t *testing.T) {
	root := repoRoot(t)
	wanted := os.Getenv("DOMAIN_STORES")
	for _, s := range []store{
		{name: "sqlite", dialect: dialect.SQLite, open: sqliteOpener()},
		{name: "postgres", dialect: dialect.Postgres, open: postgresOpener()},
		{name: "mysql", dialect: dialect.MySQL, open: mysqlOpener()},
	} {
		if wanted != "" && !slices.Contains(strings.Split(wanted, ","), s.name) {
			continue
		}
		t.Run(s.name, func(t *testing.T) {
			results := []result{}
			for _, d := range domains.All {
				db := s.open(t, d.Name)
				load(t, root, d.Name, s.dialect, db)
				results = append(results, replay(t, root, d, s.dialect, db)...)
			}
			writeResults(t, root, s.name, results)
		})
	}
}

func replay(t *testing.T, root string, d domains.Domain, dia string, db *sql.DB) []result {
	t.Helper()
	mapper, err := d.Mapper()
	require.NoError(t, err, d.Name)
	files, err := filepath.Glob(filepath.Join(root, "domains", d.Name, "golden", "*", "*", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	drv := entsql.OpenDB(dia, db)
	var out []result
	for _, f := range files {
		var g golden
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &g))
		allowed := sorted(g.Allowed)
		r := result{Domain: d.Name, PDP: g.PDP, Principal: g.PrincipalName, Action: g.Action, Allowed: allowed, Returned: []string{}}
		returned, unmapped, sqlText, err := run(t.Context(), d, mapper, dia, drv, g)
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

func run(ctx context.Context, d domains.Domain, mapper cerbosent.Mapper, dia string, drv *entsql.Driver, g golden) ([]string, []string, string, error) {
	if g.PlanError != nil {
		return nil, nil, "", fmt.Errorf("plan error: %v", g.PlanError)
	}
	filter := &enginev1.PlanResourcesFilter{}
	if err := protojson.Unmarshal(g.Plan, filter); err != nil {
		return nil, nil, "", fmt.Errorf("decoding plan: %w", err)
	}
	var unmapped []string
	recording := cerbosent.MapperFunc(func(ref string) (cerbosent.Entry, bool) {
		e, ok := mapper.Resolve(ref)
		if !ok && strings.HasPrefix(ref, "request.resource.") && !slices.Contains(unmapped, ref) {
			unmapped = append(unmapped, ref)
		}
		return e, ok
	})
	res, err := cerbosent.Translate(&responsev1.PlanResourcesResponse{Filter: filter}, d.Table, recording,
		cerbosent.WithDialect(dia), cerbosent.WithNullRepresentation(d.Nulls))
	if err != nil {
		return nil, unmapped, "", err
	}
	sel := entsql.Dialect(dia).Select("*").From(entsql.Table(d.Table)).Where(res.Predicate)
	query, args := sel.Query()
	sqlText := fmt.Sprintf("%s %v", query, args)
	ids, err := d.IDs(ctx, drv, res.Predicate)
	if err != nil {
		return nil, unmapped, sqlText, fmt.Errorf("query: %w; SQL: %s", err, sqlText)
	}
	return sorted(ids), unmapped, sqlText, nil
}

func verdict(allowed, returned []string, err error) (string, string) {
	switch {
	case errors.Is(err, cerbosent.ErrUnsupported):
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
	raw, err := json.MarshalIndent(map[string]any{"adapter": "ent", "store": storeName, "results": results}, "", "  ")
	require.NoError(t, err)
	dir := filepath.Join(root, "domains", "results", "ent")
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

// -- loading ----------------------------------------------------------------------------------

func statements(t *testing.T, root, domain, dia string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "domains", domain, "generated", "sql", dia+".sql"))
	require.NoError(t, err)
	var out []string
	for stmt := range strings.SplitSeq(string(raw), ";\n") {
		var lines []string
		for line := range strings.SplitSeq(stmt, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				lines = append(lines, line)
			}
		}
		if s := strings.TrimSpace(strings.Join(lines, "\n")); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// load runs the generated SQL on PostgreSQL and MySQL. On SQLite it runs only the DDL and inserts
// rows.json itself, writing timestamps in the adapter's fixed-width layout.
func load(t *testing.T, root, domain, dia string, db *sql.DB) {
	t.Helper()
	ctx := t.Context()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	if dia == dialect.MySQL {
		_, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "SET FOREIGN_KEY_CHECKS=1") //nolint:errcheck
	}
	file := map[string]string{dialect.SQLite: "sqlite", dialect.Postgres: "postgres", dialect.MySQL: "mysql"}[dia]
	for _, stmt := range statements(t, root, domain, file) {
		if dia == dialect.SQLite && strings.HasPrefix(stmt, "INSERT") {
			continue
		}
		_, err := conn.ExecContext(ctx, stmt)
		require.NoError(t, err, "%s: %s", domain, stmt)
	}
	if dia != dialect.SQLite {
		return
	}
	var rows struct {
		Tables []struct {
			Name    string `json:"name"`
			Columns []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"columns"`
			Rows []map[string]any `json:"rows"`
		} `json:"tables"`
	}
	raw, err := os.ReadFile(filepath.Join(root, "domains", domain, "generated", "rows.json"))
	require.NoError(t, err)
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&rows))
	for _, tbl := range rows.Tables {
		for _, row := range tbl.Rows {
			var cols []string
			var vals []any
			for _, c := range tbl.Columns {
				cols = append(cols, c.Name)
				vals = append(vals, sqliteValue(t, c.Type, row[c.Name]))
			}
			q, args := entsql.Dialect(dialect.SQLite).Insert(tbl.Name).Columns(cols...).Values(vals...).Query()
			_, err := conn.ExecContext(ctx, q, args...)
			require.NoError(t, err, "%s: %s", domain, tbl.Name)
		}
	}
}

func sqliteValue(t *testing.T, typ string, v any) any {
	t.Helper()
	if v == nil {
		return nil
	}
	switch typ {
	case "timestamp":
		ts, err := time.Parse(time.RFC3339Nano, v.(string))
		require.NoError(t, err)
		return ts.UTC().Format(cerbosent.SQLiteTimestampLayout)
	case "json", "string[]":
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		return string(raw)
	case "int", "bigint":
		n, err := v.(json.Number).Int64()
		require.NoError(t, err)
		return n
	case "float", "decimal":
		n, err := v.(json.Number).Float64()
		require.NoError(t, err)
		return n
	}
	return v
}

// -- stores -----------------------------------------------------------------------------------

func sqliteOpener() func(t *testing.T, domain string) *sql.DB {
	return func(t *testing.T, domain string) *sql.DB {
		t.Helper()
		db, err := sql.Open("sqlite", "file:"+domain+"?mode=memory&cache=shared&_pragma=case_sensitive_like(1)&_pragma=foreign_keys(1)")
		require.NoError(t, err)
		db.SetMaxIdleConns(4)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.PingContext(t.Context()))
		return db
	}
}

func postgresOpener() func(t *testing.T, domain string) *sql.DB {
	var db *sql.DB
	return func(t *testing.T, _ string) *sql.DB {
		t.Helper()
		if db != nil {
			return db
		}
		c, err := postgres.Run(context.Background(), "postgres:17-alpine",
			postgres.WithDatabase("domains"), postgres.WithUsername("domains"), postgres.WithPassword("domains"),
			testcontainers.WithEnv(map[string]string{"POSTGRES_INITDB_ARGS": "--locale-provider=icu --icu-locale=en-US"}),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(2*time.Minute)),
		)
		require.NoError(t, err)
		testcontainers.CleanupContainer(t, c)
		dsn, err := c.ConnectionString(t.Context(), "sslmode=disable")
		require.NoError(t, err)
		db, err = sql.Open("pgx", dsn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
}

// mysqlCollation is the default collation of a domain's database where the server default
// cannot hold the dataset: government's agency codes "DSS" and "dss" collide under
// utf8mb4_0900_ai_ci, so its keys need a case-sensitive collation.
var mysqlCollation = map[string]string{"government": "utf8mb4_0900_bin"}

func mysqlOpener() func(t *testing.T, domain string) *sql.DB {
	var admin *sql.DB
	var dsn string
	return func(t *testing.T, domain string) *sql.DB {
		t.Helper()
		if admin == nil {
			c, err := mysql.Run(context.Background(), "mysql:8.4",
				mysql.WithDatabase("domains"), mysql.WithUsername("root"), mysql.WithPassword("domains"))
			require.NoError(t, err)
			testcontainers.CleanupContainer(t, c)
			dsn, err = c.ConnectionString(t.Context(), "parseTime=true")
			require.NoError(t, err)
			admin, err = sql.Open("mysql", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { _ = admin.Close() })
			var pingErr error
			for range 30 {
				if pingErr = admin.PingContext(t.Context()); pingErr == nil {
					break
				}
				time.Sleep(time.Second)
			}
			require.NoError(t, pingErr)
		}
		create := "CREATE DATABASE `" + domain + "`"
		if c, ok := mysqlCollation[domain]; ok {
			create += " COLLATE " + c
		}
		_, err := admin.ExecContext(t.Context(), create)
		require.NoError(t, err)
		db, err := sql.Open("mysql", strings.Replace(dsn, "/domains?", "/"+domain+"?", 1))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
}
