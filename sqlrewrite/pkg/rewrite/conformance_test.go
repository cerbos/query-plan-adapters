// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

//go:build tests

package rewrite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	_ "modernc.org/sqlite"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"

	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/config"
	"github.com/cerbos/query-plan-adapters/sqlrewrite/pkg/rewrite"
)

// Conformance suite (../../../conformance/README.md, "The harness contract").
//
// The corpus dataset is seeded into SQLite, PostgreSQL and MySQL. For each pinned PDP and each
// golden file, `SELECT id FROM adversarial_resource` is sent through the rewriter with the recorded
// plan, the rewritten statement is executed, and the returned ids are compared with the recorded
// check() decisions. The schema and seeding are ent's; the mapping is testdata/conformance.yaml.

const (
	postgresImage = "postgres:17-alpine@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b"
	mysqlImage    = "mysql:8.4@sha256:76f0358884cb92d9a086d24e5671c38048d3fef40f2588703d73eb55392d6b69"
)

const (
	resourceTable    = "adversarial_resource"
	tagTable         = "adversarial_tag"
	categoryTable    = "adversarial_category"
	subCategoryTable = "adversarial_sub_category"
	labelTable       = "adversarial_label"
	parentTable      = "adversarial_parent"
	innerTable       = "adversarial_inner"
	numberElemTable  = "adversarial_number_elem"
	boolElemTable    = "adversarial_bool_elem"
)

type target struct {
	open       func(t *testing.T) *sql.DB
	name       string
	dialect    rewrite.Dialect
	entDialect string
	ddl        string
}

// SQLREWRITE_TEST_STORES limits the run to a comma-separated subset of sqlite, postgres and mysql.
func targets() []target {
	all := []target{
		{name: "sqlite", dialect: rewrite.DialectSQLite, entDialect: dialect.SQLite, ddl: sqliteDDL, open: openSQLite},
		{name: "postgres", dialect: rewrite.DialectPostgres, entDialect: dialect.Postgres, ddl: postgresDDL, open: openPostgres},
		{name: "mysql", dialect: rewrite.DialectMySQL, entDialect: dialect.MySQL, ddl: mysqlDDL, open: openMySQL},
	}
	only, ok := os.LookupEnv("SQLREWRITE_TEST_STORES")
	if !ok {
		return all
	}
	want := strings.Split(only, ",")
	return slices.DeleteFunc(all, func(tg target) bool { return !slices.Contains(want, tg.name) })
}

const sqliteDSN = "file:adversarial?mode=memory&cache=shared" +
	"&_pragma=case_sensitive_like(1)&_pragma=foreign_keys(1)"

const sqliteDDL = `
CREATE TABLE adversarial_resource (
	id                 text PRIMARY KEY,
	a_bool             integer,
	a_string           text,
	a_number           integer,
	a_double           real,
	a_optional_string  text,
	created_by         text    NOT NULL,
	scope              text,
	created_at         text,
	updated_at         text
);
CREATE TABLE adversarial_tag (
	pk           integer PRIMARY KEY AUTOINCREMENT,
	tag_id       text NOT NULL,
	name         text,
	position     integer NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_category (
	id           text PRIMARY KEY,
	name         text NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_sub_category (
	id           text PRIMARY KEY,
	name         text NOT NULL,
	category_id  text NOT NULL REFERENCES adversarial_category(id)
);
CREATE TABLE adversarial_label (
	id               text PRIMARY KEY,
	name             text,
	sub_category_id  text NOT NULL REFERENCES adversarial_sub_category(id)
);
CREATE TABLE adversarial_parent (
	id                 text PRIMARY KEY,
	a_bool             integer,
	a_string           text,
	a_number           integer,
	a_optional_string  text,
	resource_id        text    NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_inner (
	id                 text PRIMARY KEY,
	a_bool             integer,
	a_string           text,
	a_number           integer,
	a_optional_string  text,
	parent_id          text    NOT NULL UNIQUE REFERENCES adversarial_parent(id)
);
CREATE TABLE adversarial_number_elem (
	pk           integer PRIMARY KEY AUTOINCREMENT,
	value        real,
	position     integer NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_bool_elem (
	pk           integer PRIMARY KEY AUTOINCREMENT,
	value        integer,
	position     integer NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
`

// The PostgreSQL schema uses native boolean and timestamptz columns, so it exercises the typed
// path the SQLite schema cannot: on SQLite a timestamp is text compared lexicographically, here it
// is a real instant.
const postgresDDL = `
CREATE TABLE adversarial_resource (
	id                 text PRIMARY KEY,
	a_bool             boolean,
	a_string           text,
	a_number           bigint,
	a_double           double precision,
	a_optional_string  text,
	created_by         text             NOT NULL,
	scope              text,
	created_at         timestamptz,
	updated_at         timestamptz
);
CREATE TABLE adversarial_tag (
	pk           bigserial PRIMARY KEY,
	tag_id       text NOT NULL,
	name         text,
	position     integer NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_category (
	id           text PRIMARY KEY,
	name         text NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_sub_category (
	id           text PRIMARY KEY,
	name         text NOT NULL,
	category_id  text NOT NULL REFERENCES adversarial_category(id)
);
CREATE TABLE adversarial_label (
	id               text PRIMARY KEY,
	name             text,
	sub_category_id  text NOT NULL REFERENCES adversarial_sub_category(id)
);
CREATE TABLE adversarial_parent (
	id                 text   PRIMARY KEY,
	a_bool             boolean,
	a_string           text,
	a_number           bigint,
	a_optional_string  text,
	resource_id        text    NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_inner (
	id                 text   PRIMARY KEY,
	a_bool             boolean,
	a_string           text,
	a_number           bigint,
	a_optional_string  text,
	parent_id          text    NOT NULL UNIQUE REFERENCES adversarial_parent(id)
);
CREATE TABLE adversarial_number_elem (
	pk           bigserial PRIMARY KEY,
	value        double precision,
	position     integer NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_bool_elem (
	pk           bigserial PRIMARY KEY,
	value        boolean,
	position     integer NOT NULL,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
`

// The MySQL schema pins the byte-exact NO PAD collation utf8mb4_0900_bin on every string
// column. The default utf8mb4_0900_ai_ci ignores case and accents, over-granting on
// `string/equals/case-sensitive` ("One" would match "one"), `string/equals/non-ascii-literal`
// and hierarchy prefixes (#310). The older utf8mb4_bin ignores trailing spaces ("a" would match
// "a "). utf8mb4_0900_as_cs is case- and accent-sensitive but still ignores a default-ignorable
// code point such as U+00AD, so seed h6's "o\u00ADne" would match "one" (#474). CEL
// distinguishes all four, so the collation is part of the policy contract here (#436).
//
// DATETIME(6) is microsecond-resolution, the same caveat PostgreSQL's timestamptz carries:
// the corpus's a5 seed holds microsecond precision and no finer.
const mysqlDDL = `
CREATE TABLE adversarial_resource (
	id                 varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	a_bool             boolean,
	a_string           varchar(255) COLLATE utf8mb4_0900_bin,
	a_number           bigint,
	a_double           double,
	a_optional_string  varchar(255) COLLATE utf8mb4_0900_bin,
	created_by         varchar(64) COLLATE utf8mb4_0900_bin NOT NULL,
	scope              varchar(255) COLLATE utf8mb4_0900_bin,
	created_at         datetime(6),
	updated_at         datetime(6)
);
CREATE TABLE adversarial_tag (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	tag_id       varchar(64) COLLATE utf8mb4_0900_bin NOT NULL,
	name         varchar(255) COLLATE utf8mb4_0900_bin,
	position     int NOT NULL,
	resource_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_category (
	id           varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	name         varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
	resource_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_sub_category (
	id           varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	name         varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
	category_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_category(id)
);
CREATE TABLE adversarial_label (
	id               varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	name             varchar(255) COLLATE utf8mb4_0900_bin,
	sub_category_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_sub_category(id)
);
CREATE TABLE adversarial_parent (
	id                 varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	a_bool             boolean,
	a_string           varchar(255) COLLATE utf8mb4_0900_bin,
	a_number           bigint,
	a_optional_string  varchar(255) COLLATE utf8mb4_0900_bin,
	resource_id        varchar(64) COLLATE utf8mb4_0900_bin NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_inner (
	id                 varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	a_bool             boolean,
	a_string           varchar(255) COLLATE utf8mb4_0900_bin,
	a_number           bigint,
	a_optional_string  varchar(255) COLLATE utf8mb4_0900_bin,
	parent_id          varchar(64) COLLATE utf8mb4_0900_bin NOT NULL UNIQUE REFERENCES adversarial_parent(id)
);
CREATE TABLE adversarial_number_elem (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	value        double,
	position     int NOT NULL,
	resource_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_bool_elem (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	value        boolean,
	position     int NOT NULL,
	resource_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_resource(id)
);
`

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", sqliteDSN)
	require.NoError(t, err, "opening SQLite")
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	return db
}

// SQLREWRITE_TEST_MYSQL_DSN and SQLREWRITE_TEST_POSTGRES_DSN point a leg at an existing, empty
// database instead of starting a container.
func openMySQL(t *testing.T) *sql.DB {
	t.Helper()

	dsn, ok := os.LookupEnv("SQLREWRITE_TEST_MYSQL_DSN")
	if !ok {
		container, err := mysql.Run(t.Context(),
			mysqlImage,
			mysql.WithDatabase("conformance"),
			mysql.WithUsername("conformance"),
			mysql.WithPassword("conformance"),
		)
		require.NoError(t, err, "starting MySQL")
		testcontainers.CleanupContainer(t, container)

		dsn, err = container.ConnectionString(t.Context(), "parseTime=true")
		require.NoError(t, err)
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "opening MySQL")
	t.Cleanup(func() { _ = db.Close() })

	var pingErr error
	for range 30 {
		if pingErr = db.PingContext(t.Context()); pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, pingErr, "waiting for MySQL to accept connections")
	return db
}

// follow the column's collation. PostgreSQL collations are deterministic, so = is byte-exact under
// any of them, but a linguistic one orders case, accents and punctuation below the letter: under
// glibc's en_US.utf8 'One' > 'a' is TRUE, which over-grants
// comparison/greater-than/string-code-point-order (cerbos/query-plan-adapters#489). "C" orders by
// byte, which for UTF-8 is code point order.
//
// Stated rather than inherited: the Alpine image reports en_US.utf8 without it, and orders by byte
// only because musl's strcoll does. The same database on a glibc image, or on a managed service,
// orders linguistically.
//
// ADAPTER_TEST_POSTGRES_INITDB_ARGS overrides it, so the over-grant can be reproduced rather than
// taken on trust: "--locale-provider=icu --icu-locale=en-US" gives the pinned image ICU's
// linguistic order, which musl cannot, and fails both string-code-point-order cases. A measurement
// escape hatch, not a CI leg.
func postgresInitdbArgs() string {
	if args, ok := os.LookupEnv("ADAPTER_TEST_POSTGRES_INITDB_ARGS"); ok {
		return args
	}
	return "--lc-collate=C"
}

func openPostgres(t *testing.T) *sql.DB {
	t.Helper()

	dsn, ok := os.LookupEnv("SQLREWRITE_TEST_POSTGRES_DSN")
	if !ok {
		container, err := postgres.Run(t.Context(),
			postgresImage,
			postgres.WithDatabase("conformance"),
			postgres.WithUsername("conformance"),
			postgres.WithPassword("conformance"),
			testcontainers.WithEnv(map[string]string{"POSTGRES_INITDB_ARGS": postgresInitdbArgs()}),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(2*time.Minute),
			),
		)
		require.NoError(t, err, "starting PostgreSQL")
		testcontainers.CleanupContainer(t, container)

		dsn, err = container.ConnectionString(t.Context(), "sslmode=disable")
		require.NoError(t, err)
	}

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err, "opening PostgreSQL")
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func loadConfig(t *testing.T, d rewrite.Dialect) rewrite.Config {
	t.Helper()

	p, err := config.Load(config.Flags{Path: "testdata/conformance.yaml", Set: []string{"rewrite.dialect=" + string(d)}})
	require.NoError(t, err)
	var conf rewrite.Config
	require.NoError(t, p.GetSection(&conf))
	return conf
}

// -- the stores ---------------------------------------------------------------------------------

// store is one opened and seeded database.
type store struct {
	db     *sql.DB
	target target
}

// openStore opens tgt, creates the schema and loads the corpus dataset into it.
func openStore(t *testing.T, tgt target, corpus *Corpus) *store {
	t.Helper()

	s := &store{db: tgt.open(t), target: tgt}
	// A database named by DSN may hold a previous run's tables.
	for _, table := range []string{
		labelTable, subCategoryTable, categoryTable, innerTable, parentTable,
		tagTable, numberElemTable, boolElemTable, resourceTable,
	} {
		_, err := s.db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+table)
		require.NoError(t, err, "dropping %s", table)
	}
	// Statement by statement rather than one multi-statement Exec: the MySQL driver rejects
	// batched DDL unless multiStatements is on, and splitting keeps every dialect on the same
	// path.
	for stmt := range strings.SplitSeq(tgt.ddl, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err := s.db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, "creating %s schema", tgt.name)
	}
	s.seed(t, corpus)
	return s
}

// exec builds a statement through ent's builder so placeholders match the dialect, then runs it.
func (s *store) exec(t *testing.T, table string, columns []string, values ...any) {
	t.Helper()

	query, args := entsql.Dialect(s.target.entDialect).
		Insert(table).Columns(columns...).Values(values...).Query()
	_, err := s.db.ExecContext(t.Context(), query, args...)
	require.NoError(t, err, "seeding %s", table)
}

func (s *store) seed(t *testing.T, corpus *Corpus) {
	t.Helper()

	for _, seed := range corpus.Seeds {
		derived := corpus.derived(seed)
		s.exec(t, resourceTable,
			[]string{
				"id", "a_bool", "a_string", "a_number", "a_double",
				"a_optional_string", "created_by", "scope", "created_at", "updated_at",
			},
			seed.ID, seed.ABool, seed.AString, seed.ANumber, nullableFloat(derived.ADouble),
			nullableString(seed.AOptionalString), derived.CreatedBy,
			nullableString(derived.Scope), s.storedTimestamp(t, derived.CreatedAt),
			s.storedTimestamp(t, derived.UpdatedAt))

		// The to-one chain, one owned row per level. A seed with no parent gets no row at all,
		// which is what makes the absent-parent hazard reachable through a SCALAR rather than
		// only through mainCategory's collection.
		if parentSeed := corpus.parentSeedOf(&seed); parentSeed != nil {
			s.exec(t, parentTable,
				[]string{"id", "a_bool", "a_string", "a_number", "a_optional_string", "resource_id"},
				parentID(seed), parentSeed.ABool, parentSeed.AString, parentSeed.ANumber,
				nullableString(parentSeed.AOptionalString), seed.ID)

			if inner := corpus.parentSeedOf(parentSeed); inner != nil {
				s.exec(t, innerTable,
					[]string{"id", "a_bool", "a_string", "a_number", "a_optional_string", "parent_id"},
					innerID(seed), inner.ABool, inner.AString, inner.ANumber,
					nullableString(inner.AOptionalString), parentID(seed))
			}
		}

		// Each list keeps its order in a position column, which is what `index` reads.
		for i, tag := range seed.Tags {
			s.exec(t, tagTable, []string{"tag_id", "name", "position", "resource_id"},
				tag.ID, nullableString(tag.Name), i, seed.ID)
		}
		for i, element := range seed.ANumberList {
			s.exec(t, numberElemTable, []string{"value", "position", "resource_id"}, nullableFloat(element), i, seed.ID)
		}
		for i, element := range seed.ABoolList {
			s.exec(t, boolElemTable, []string{"value", "position", "resource_id"}, nullableBool(element), i, seed.ID)
		}

		catID := categoryID(seed)
		if len(seed.SubCategoryNames) > 0 {
			s.exec(t, categoryTable, []string{"id", "name", "resource_id"}, catID, "business", seed.ID)
		}
		for i, subName := range seed.SubCategoryNames {
			subID := subCategoryID(seed, i)
			s.exec(t, subCategoryTable, []string{"id", "name", "category_id"}, subID, subName, catID)

			for j, label := range derived.Labels {
				s.exec(t, labelTable, []string{"id", "name", "sub_category_id"},
					fmt.Sprintf("%s-label%d", subID, j), nullableString(label), subID)
			}
		}
	}
}

// storedTimestamp writes the derived createdAt in whatever form the dialect compares correctly.
//
// PostgreSQL has a real instant type and takes the time.Time directly. SQLite stores text and
// compares it lexicographically, so it gets the adapter's documented fixed-width layout — the same
// one the adapter binds its own timestamp parameters in.
func (s *store) storedTimestamp(t *testing.T, raw *string) any {
	t.Helper()

	if raw == nil {
		return nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, *raw)
	require.NoError(t, err, "parsing derived timestamp")

	if s.target.dialect == rewrite.DialectSQLite {
		return parsed.UTC().Format(rewrite.SQLiteTimestampLayout)
	}
	return parsed.UTC()
}

func nullableString(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableFloat(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableBool(v *bool) any {
	if v == nil {
		return nil
	}
	return *v
}

func (s *store) query(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing the rewritten statement: %w\nSQL: %s", err, query)
	}
	defer rows.Close()

	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, rows.Err()
}

// filteredIDs sends the caller's statement through the rewriter with the golden's plan and runs the
// result. An unmapped_attribute refusal is a harness defect, not the rewriter's refusal: an
// attribute this mapping forgot would otherwise satisfy an `unsupported` ledger entry vacuously.
func (s *store) filteredIDs(ctx context.Context, rw *rewrite.Rewriter, golden Golden) ([]string, error) {
	plan, err := golden.PlanResponse(time.Now())
	if err != nil {
		return nil, err
	}

	res, err := rw.Rewrite(ctx, rewrite.Request{
		SQL:   "SELECT id FROM " + resourceTable,
		Plans: map[string]*responsev1.PlanResourcesResponse{"conformance": plan},
	})
	if refusal := (*rewrite.RefusalError)(nil); errors.As(err, &refusal) && refusal.Code == rewrite.CodeUnmappedAttribute {
		return nil, fmt.Errorf("a harness defect, not a refusal: %w", err)
	}
	if err != nil {
		return nil, err
	}
	return s.query(ctx, res.SQL, res.Params...)
}

func TestAdversarialConformance(t *testing.T) {
	corpus := loadCorpus(t)

	for _, tgt := range targets() {
		t.Run(tgt.name, func(t *testing.T) {
			rw, err := rewrite.New(loadConfig(t, tgt.dialect))
			require.NoError(t, err)
			s := openStore(t, tgt, corpus)
			var fuzzResults []FuzzVerdict
			defer func() { writeFuzzResults(t, "sqlrewrite", tgt.name, fuzzResults) }()
			for _, tag := range corpus.Tags {
				t.Run(tag, func(t *testing.T) {
					passed, total := map[string]int{}, map[string]int{}
					for _, golden := range corpus.Goldens[tag] {
						total[golden.Tier]++
						entry, listed := corpus.Ledger[golden.ID]
						listed = listed && entry.AppliesTo(tag)

						ok := t.Run(golden.ID, func(t *testing.T) {
							if golden.Skipped() {
								t.Skip("plannerDivergence: the recorded plan and check() disagree under this PDP")
							}
							ids, err := s.filteredIDs(t.Context(), rw, golden)
							if fuzzDir != "" {
								v := fuzzVerdict(golden, ids, err, rewrite.ErrRefused)
								fuzzResults = append(fuzzResults, v)
								require.True(t, v.OK(), "%+v", v)
								return
							}
							switch {
							case !listed:
								require.NoError(t, err)
								require.Equal(t, sortedCopy(golden.Allowed), ids)
							case entry.Status == "unsupported":
								require.ErrorIs(t, err, rewrite.ErrRefused,
									"ledger says unsupported (%s): the rewrite must refuse", entry.Reason)
							default:
								require.NoError(t, err)
								require.NotEqual(t, sortedCopy(golden.Allowed), ids,
									"ledger says divergent (%s), but the result matches: remove the entry", entry.Issue)
							}
						})
						if ok && !listed && !golden.Skipped() {
							passed[golden.Tier]++
						}
					}
					for _, tier := range []string{"core", "extended", "adversarial"} {
						t.Logf("%s, PDP %s, %s: %d / %d passed", tgt.name, tag, tier, passed[tier], total[tier])
					}
				})
			}
		})
	}
}

func sortedCopy(ids []string) []string {
	out := append([]string{}, ids...)
	slices.Sort(out)
	return out
}
