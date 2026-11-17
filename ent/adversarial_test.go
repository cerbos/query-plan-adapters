// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"context"
	"database/sql"
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

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/internal/conformanceschema"
)

// Conformance suite (../conformance/README.md, "The harness contract").
//
// The corpus dataset is seeded into each database this adapter claims through WithDialect —
// SQLite, PostgreSQL and MySQL. Then, for each pinned PDP and each golden file, the recorded plan
// is translated by this adapter and executed, and the returned ids are compared with the check()
// decisions recorded next to it. conformance-ledger.json lists the cases this adapter cannot pass,
// and holds for every dialect. No PDP is started: its answers are recorded. This file owns only the
// SQL half — the schemas, the mapping and the seeding; corpus_test.go reads the corpus.

const (
	postgresImage = "postgres:17-alpine"
	mysqlImage    = "mysql:8.4"
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

// -- dialect targets ---------------------------------------------------------------------------
//
// The suite runs end to end against every dialect this adapter claims to support. Ent's builder
// owns quoting and placeholders, but the adapter still makes three dialect-sensitive choices of
// its own — cast spellings, null-safe equality, and timestamp binding — and a dialect the harness
// does not exercise is a dialect this adapter does not actually cover.

// target is one database the whole corpus is replayed against.
type target struct {
	open    func(t *testing.T) *sql.DB
	name    string
	dialect string
	ddl     string
}

func targets() []target {
	return []target{
		{name: "sqlite", dialect: dialect.SQLite, ddl: sqliteDDL, open: openSQLite},
		{name: "postgres", dialect: dialect.Postgres, ddl: postgresDDL, open: openPostgres},
		{name: "mysql", dialect: dialect.MySQL, ddl: mysqlDDL, open: openMySQL},
	}
}

// CEL string matching is case-sensitive; SQLite's LIKE is case-insensitive for ASCII by default,
// which would over-grant on the `string/equals/case-sensitive` and `hierarchy/*` cases. Foreign
// keys are on so the seeded relation graph is genuinely referentially valid.
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

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", sqliteDSN)
	require.NoError(t, err, "opening SQLite")
	t.Cleanup(func() { _ = db.Close() })

	// The shared-cache in-memory database lives only as long as a connection is held, and a
	// pooled connection closing would drop the schema mid-run.
	db.SetMaxOpenConns(1)
	return db
}

// The MySQL schema names no collation, so every string column takes the server default,
// utf8mb4_0900_ai_ci, which folds case and accents: the corpus proves the adapter compares
// strings byte for byte itself (#474).
//
// DATETIME(6) is microsecond-resolution, the same caveat PostgreSQL's timestamptz carries:
// the corpus's a5 seed holds microsecond precision and no finer.
const mysqlDDL = `
CREATE TABLE adversarial_resource (
	id                 varchar(64) PRIMARY KEY,
	a_bool             boolean,
	a_string           varchar(255),
	a_number           bigint,
	a_double           double,
	a_optional_string  varchar(255),
	created_by         varchar(64) NOT NULL,
	scope              varchar(255),
	created_at         datetime(6),
	updated_at         datetime(6)
);
CREATE TABLE adversarial_tag (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	tag_id       varchar(64) NOT NULL,
	name         varchar(255),
	position     int NOT NULL,
	resource_id  varchar(64) NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_category (
	id           varchar(64) PRIMARY KEY,
	name         varchar(255) NOT NULL,
	resource_id  varchar(64) NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_sub_category (
	id           varchar(64) PRIMARY KEY,
	name         varchar(255) NOT NULL,
	category_id  varchar(64) NOT NULL REFERENCES adversarial_category(id)
);
CREATE TABLE adversarial_label (
	id               varchar(64) PRIMARY KEY,
	name             varchar(255),
	sub_category_id  varchar(64) NOT NULL REFERENCES adversarial_sub_category(id)
);
CREATE TABLE adversarial_parent (
	id                 varchar(64) PRIMARY KEY,
	a_bool             boolean,
	a_string           varchar(255),
	a_number           bigint,
	a_optional_string  varchar(255),
	resource_id        varchar(64) NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_inner (
	id                 varchar(64) PRIMARY KEY,
	a_bool             boolean,
	a_string           varchar(255),
	a_number           bigint,
	a_optional_string  varchar(255),
	parent_id          varchar(64) NOT NULL UNIQUE REFERENCES adversarial_parent(id)
);
CREATE TABLE adversarial_number_elem (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	value        double,
	position     int NOT NULL,
	resource_id  varchar(64) NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_bool_elem (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	value        boolean,
	position     int NOT NULL,
	resource_id  varchar(64) NOT NULL REFERENCES adversarial_resource(id)
);
`

func openMySQL(t *testing.T) *sql.DB {
	t.Helper()

	container, err := mysql.Run(t.Context(),
		mysqlImage,
		mysql.WithDatabase("conformance"),
		mysql.WithUsername("conformance"),
		mysql.WithPassword("conformance"),
	)
	require.NoError(t, err, "starting MySQL")
	testcontainers.CleanupContainer(t, container)

	dsn, err := container.ConnectionString(t.Context(), "parseTime=true")
	require.NoError(t, err)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "opening MySQL")
	t.Cleanup(func() { _ = db.Close() })

	// The module reports readiness from the server log, which MySQL emits once during
	// initialisation and again when it actually accepts connections — the first sighting can
	// hand back a socket that closes mid-DDL ("invalid connection"). Ping until it holds.
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

// postgresInitdbArgs initialises the PostgreSQL leg under ICU's linguistic en-US order, as a
// managed service's default does, so the corpus proves the adapter orders strings by code point
// itself (#489). ADAPTER_TEST_POSTGRES_INITDB_ARGS overrides it.
func postgresInitdbArgs() string {
	if args, ok := os.LookupEnv("ADAPTER_TEST_POSTGRES_INITDB_ARGS"); ok {
		return args
	}
	return "--locale-provider=icu --icu-locale=en-US"
}

func openPostgres(t *testing.T) *sql.DB {
	t.Helper()

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

	dsn, err := container.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err, "opening PostgreSQL")
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// buildMapper wires the corpus's attribute references onto the schema above, through the ent
// schema types in internal/conformanceschema: every relation's cardinality, tables and join
// columns, and every column's value type, come from ent.
//
// `owner` and `tagNames` deliberately alias columns that `aOptionalString` and `tags[].name`
// already cover: the corpus sends those two as EXPLICIT nulls while the originals are omitted when
// NULL, and CEL membership distinguishes null from missing. Mapping both is what lets a single
// schema exercise both conventions.
//
// Every attribute resources.json omits when its column is NULL is declared NullConventionOmitted,
// as is every parent.* hop, whose absent level is omitted too. A null literal against one is then
// a missing-attribute error CEL denies, so it is refused rather than rendered as IS NULL (#528).
// `owner` and `coOwner` alias `aOptionalString` and `scope` under the explicit convention.
func buildMapper(tb testing.TB) cerbosent.Mapper {
	tb.Helper()

	const omitted, explicit = cerbosent.NullConventionOmitted, cerbosent.NullConventionExplicit
	named := map[string]cerbosent.SchemaEntry{"name": {Column: "name"}}
	labels := cerbosent.SchemaEntry{Edge: "labels", Element: &cerbosent.SchemaEntry{Column: "name"}, Fields: named}
	subCategoryFields := map[string]cerbosent.SchemaEntry{"name": {Column: "name"}, "labels": labels}
	tagFields := map[string]cerbosent.SchemaEntry{"id": {Column: "tag_id"}, "name": {Column: "name"}}
	elements := func(edge string) cerbosent.SchemaEntry {
		return cerbosent.SchemaEntry{Edge: edge, Position: "position", Element: &cerbosent.SchemaEntry{Column: "value", NullConvention: explicit}}
	}

	entries := cerbosent.SchemaMap{
		"request.resource.id":                   {Column: "id"},
		"request.resource.attr.aBool":           {Column: "a_bool", NullConvention: omitted},
		"request.resource.attr.aString":         {Column: "a_string", NullConvention: omitted},
		"request.resource.attr.aNumber":         {Column: "a_number", NullConvention: omitted},
		"request.resource.attr.aDouble":         {Column: "a_double", NullConvention: omitted},
		"request.resource.attr.aOptionalString": {Column: "a_optional_string", NullConvention: omitted},
		"request.resource.attr.createdBy":       {Column: "created_by"},
		"request.resource.attr.owner":           {Column: "a_optional_string", NullConvention: explicit},
		"request.resource.attr.coOwner":         {Column: "scope", NullConvention: explicit},
		"request.resource.attr.scope":           {Column: "scope", NullConvention: omitted},
		"request.resource.attr.createdAt":       {Column: "created_at", NullConvention: omitted},
		"request.resource.attr.updatedAt":       {Column: "updated_at", NullConvention: omitted},
		// obj.inner is not a real nested column: it mirrors aString, as in the other harnesses.
		"request.resource.attr.obj.inner": {Column: "a_string", NullConvention: omitted},

		"request.resource.attr.tags": {Edge: "tags", Position: "position", Element: &cerbosent.SchemaEntry{Column: "name"}, Fields: tagFields},
		"request.resource.attr.tagNames": {
			Edge: "tags", Position: "position", Fields: tagFields,
			Element: &cerbosent.SchemaEntry{Column: "name", NullConvention: explicit},
		},
		"request.resource.attr.categories": {
			Edge:   "categories",
			Fields: map[string]cerbosent.SchemaEntry{"name": {Column: "name"}, "subCategories": {Edge: "sub_categories", Element: &cerbosent.SchemaEntry{Column: "name"}, Fields: subCategoryFields}},
		},
		"request.resource.attr.aNumberList": elements("number_elems"),
		"request.resource.attr.aBoolList":   elements("bool_elems"),

		// mainCategory.* flattens the two-hop chain from the root.
		"request.resource.attr.mainCategory.subCategories": {Edge: "categories.sub_categories", Element: &cerbosent.SchemaEntry{Column: "name"}, Fields: subCategoryFields},
		"request.resource.attr.mainCategory.subNames":      {Edge: "categories.sub_categories", Element: &cerbosent.SchemaEntry{Column: "name"}, Fields: subCategoryFields},

		// `parent` itself is the to-one hop, not a column: declared so a macro over it
		// (`collection/exists/map-keys`) meets the translator's own refusal.
		"request.resource.attr.parent": {Edge: "parent"},
	}
	for _, level := range []struct{ attr, edge string }{{"parent", "parent"}, {"parent.inner", "parent.inner"}} {
		for attr, col := range map[string]string{"aBool": "a_bool", "aString": "a_string", "aNumber": "a_number", "aOptionalString": "a_optional_string"} {
			entries["request.resource.attr."+level.attr+"."+attr] = cerbosent.SchemaEntry{Edge: level.edge, Column: col, NullConvention: omitted}
		}
	}

	mapper, err := conformanceschema.CerbosSchema.Mapper("Resource", entries)
	require.NoError(tb, err)
	return mapper
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

	query, args := entsql.Dialect(s.target.dialect).
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

	if s.target.dialect == dialect.SQLite {
		return parsed.UTC().Format(cerbosent.SQLiteTimestampLayout)
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

// filteredIDs translates the golden's plan for this store's dialect and executes it, returning the
// ids the predicate selects, sorted. A translation error is returned as is, so the caller can tell a
// refusal from anything else.
func (s *store) filteredIDs(ctx context.Context, mapper cerbosent.Mapper, golden Golden) ([]string, error) {
	plan, err := golden.PlanResponse(time.Now())
	if err != nil {
		return nil, err
	}

	// Translate wraps every mapper miss in ErrUnsupported, so a resource attribute this harness
	// forgot to map would satisfy an `unsupported` ledger entry vacuously. A miss on a
	// `request.resource.*` reference is therefore a harness defect, reported as such rather than as
	// the adapter's refusal. (Lambda-local names such as `t.name` also reach the root mapper, as a
	// probe before the element scope answers them, and a `request.principal.*` reference the
	// planner left unresolved has no column to map, so only `request.resource.*` is checked.)
	var unmapped []string
	recording := cerbosent.MapperFunc(func(reference string) (cerbosent.Entry, bool) {
		entry, ok := mapper.Resolve(reference)
		if !ok && strings.HasPrefix(reference, "request.resource.") {
			unmapped = append(unmapped, reference)
		}
		return entry, ok
	})

	// Every column the dataset omits when NULL is declared NullConventionOmitted in the mapping. The
	// call-level option covers what is left, list elements and principal-side literals, which the
	// dataset sends as values; the adapter's own default is pinned in translate_test.go.
	result, err := cerbosent.Translate(plan, resourceTable, recording,
		cerbosent.WithDialect(s.target.dialect), cerbosent.WithNullRepresentation(cerbosent.NullExplicit))
	if len(unmapped) > 0 {
		return nil, fmt.Errorf("the harness mapping has no entry for %s (a harness defect, not a refusal)", strings.Join(unmapped, ", "))
	}
	if err != nil {
		return nil, err
	}
	// The outer FROM holds only the resource table — every relation is reached through a
	// correlated subquery — so an unqualified `id` is unambiguous here. The predicate is applied
	// without switching on Kind, as a caller that skips the switch would: every kind's Predicate
	// must return exactly the rows the PDP allows on its own (ADR 0011).
	selector := entsql.Dialect(s.target.dialect).Select("id").From(entsql.Table(resourceTable)).
		Where(result.Predicate)

	// Query() runs the predicate's closure — the second write pass. Translate already surfaced a
	// render error from its own probe pass, so a failure here is one only this pass can reach, and
	// the builder collects it on the selector rather than returning it. Ignoring it would execute
	// whatever partial SQL the failed pass left behind: a truncated WHERE clause still parses
	// (cerbos/query-plan-adapters#319).
	query, args := selector.Query()
	if err := selector.Err(); err != nil {
		return nil, fmt.Errorf("building the translated query: %w\nSQL: %s", err, query)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing the translated predicate: %w\nSQL: %s", err, query)
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

// -- the suite ----------------------------------------------------------------------------------

func TestAdversarialConformance(t *testing.T) {
	mapper := buildMapper(t)

	for _, tgt := range targets() {
		t.Run(tgt.name, func(t *testing.T) {
			corpus := loadCorpus(t, tgt.name)
			s := openStore(t, tgt, corpus)
			var fuzzResults []FuzzVerdict
			defer func() { writeFuzzResults(t, "ent", tgt.name, fuzzResults) }()
			for _, tag := range corpus.Tags {
				t.Run(tag, func(t *testing.T) {
					// passed[tier] and total[tier] feed the summary logged at the end.
					passed, total := map[string]int{}, map[string]int{}
					for _, golden := range corpus.Goldens[tag] {
						total[golden.Tier]++
						entry, listed := corpus.Ledger[golden.ID]
						listed = listed && entry.AppliesTo(tag)

						ok := t.Run(golden.ID, func(t *testing.T) {
							if golden.Skipped() {
								t.Skip("plannerDivergence: the recorded plan and check() disagree under this PDP")
							}
							ids, err := s.filteredIDs(t.Context(), mapper, golden)
							if fuzzDir != "" {
								v := fuzzVerdict(golden, ids, err, cerbosent.ErrUnsupported)
								fuzzResults = append(fuzzResults, v)
								require.True(t, v.OK(), "%+v", v)
								return
							}
							switch {
							case !listed:
								require.NoError(t, err)
								require.Equal(t, sortedCopy(golden.Allowed), ids)
							case entry.Status == "unsupported":
								require.ErrorIs(t, err, cerbosent.ErrUnsupported,
									"ledger says unsupported (%s): translation must refuse", entry.Reason)
							default: // divergent
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
