// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"

	"github.com/cerbos/cerbos-sdk-go/cerbos"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	_ "modernc.org/sqlite"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
)

// Adversarial differential suite.
//
// Every action in the shared ../conformance/ corpus is planned against a REAL Cerbos PDP pinned to
// conformance/CERBOS_VERSION and loaded with conformance/policies/adversarial.yaml, translated by
// this adapter, and executed against seeded SQLite rows — then the filtered id set is compared
// against an oracle computed by calling check() for each row with attributes mirroring that row
// exactly.
//
// There are no hand-written expectations. This file owns only the SQLite-specific half: the
// schema, the seeding, and the attribute mapping. SQLite is the dialect exercised here because it
// is ent's default and the one the original helper module targeted; the adapter renders through
// ent's own builder, so PostgreSQL and MySQL differ only in the cast spellings covered by
// WithDialect.

// Database container images, pinned by tag AND digest. A tag is mutable, so a tag-only pin
// records an intent rather than a build; the adversarial suite is a differential whose
// divergences are dialect behaviour, so "which build was this proved against" has to be
// answerable from the repository alone. conformance/scripts/validate-corpus.sh asserts every
// service image reference in the repository carries both halves.
const (
	postgresImage = "postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
	mysqlImage    = "mysql:8.4@sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb"
)

const (
	adapterName = "ent"

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
// which would over-grant on the `cs-eq` and `hier-*` probes. Foreign keys are on so the seeded
// relation graph is genuinely referentially valid.
const sqliteDSN = "file:adversarial?mode=memory&cache=shared" +
	"&_pragma=case_sensitive_like(1)&_pragma=foreign_keys(1)"

const sqliteDDL = `
CREATE TABLE adversarial_resource (
	id                 text PRIMARY KEY,
	a_bool             integer NOT NULL,
	a_string           text    NOT NULL,
	a_number           integer NOT NULL,
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
	a_bool             integer NOT NULL,
	a_string           text    NOT NULL,
	a_number           integer NOT NULL,
	a_optional_string  text,
	resource_id        text    NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_inner (
	id                 text PRIMARY KEY,
	a_bool             integer NOT NULL,
	a_string           text    NOT NULL,
	a_number           integer NOT NULL,
	a_optional_string  text,
	parent_id          text    NOT NULL UNIQUE REFERENCES adversarial_parent(id)
);
CREATE TABLE adversarial_number_elem (
	pk           integer PRIMARY KEY AUTOINCREMENT,
	value        real,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_bool_elem (
	pk           integer PRIMARY KEY AUTOINCREMENT,
	value        integer,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
`

// The PostgreSQL schema uses native boolean and timestamptz columns, so it exercises the typed
// path the SQLite schema cannot: on SQLite a timestamp is text compared lexicographically, here it
// is a real instant.
const postgresDDL = `
CREATE TABLE adversarial_resource (
	id                 text PRIMARY KEY,
	a_bool             boolean          NOT NULL,
	a_string           text             NOT NULL,
	a_number           bigint           NOT NULL,
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
	a_bool             boolean NOT NULL,
	a_string           text    NOT NULL,
	a_number           bigint  NOT NULL,
	a_optional_string  text,
	resource_id        text    NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_inner (
	id                 text   PRIMARY KEY,
	a_bool             boolean NOT NULL,
	a_string           text    NOT NULL,
	a_number           bigint  NOT NULL,
	a_optional_string  text,
	parent_id          text    NOT NULL UNIQUE REFERENCES adversarial_parent(id)
);
CREATE TABLE adversarial_number_elem (
	pk           bigserial PRIMARY KEY,
	value        double precision,
	resource_id  text NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_bool_elem (
	pk           bigserial PRIMARY KEY,
	value        boolean,
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

// The MySQL schema pins the byte-exact NO PAD collation utf8mb4_0900_bin on every string
// column. The default utf8mb4_0900_ai_ci ignores case and accents, over-granting on
// `cs-eq` ("One" would match "one"), `unicode-eq` and hierarchy prefixes (#310). The older
// utf8mb4_bin ignores trailing spaces ("a" would match "a "). utf8mb4_0900_as_cs is case-
// and accent-sensitive but still ignores a default-ignorable code point such as U+00AD, so
// seed h6's "o\u00ADne" would match "one" (#474). CEL distinguishes all four, so the
// collation is part of the policy contract here (#436).
//
// DATETIME(6) is microsecond-resolution, the same caveat PostgreSQL's timestamptz carries:
// the corpus's a5 seed holds microsecond precision and no finer.
const mysqlDDL = `
CREATE TABLE adversarial_resource (
	id                 varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	a_bool             boolean          NOT NULL,
	a_string           varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
	a_number           bigint           NOT NULL,
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
	a_bool             boolean NOT NULL,
	a_string           varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
	a_number           bigint  NOT NULL,
	a_optional_string  varchar(255) COLLATE utf8mb4_0900_bin,
	resource_id        varchar(64) COLLATE utf8mb4_0900_bin NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_inner (
	id                 varchar(64) COLLATE utf8mb4_0900_bin PRIMARY KEY,
	a_bool             boolean NOT NULL,
	a_string           varchar(255) COLLATE utf8mb4_0900_bin NOT NULL,
	a_number           bigint  NOT NULL,
	a_optional_string  varchar(255) COLLATE utf8mb4_0900_bin,
	parent_id          varchar(64) COLLATE utf8mb4_0900_bin NOT NULL UNIQUE REFERENCES adversarial_parent(id)
);
CREATE TABLE adversarial_number_elem (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	value        double,
	resource_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_resource(id)
);
CREATE TABLE adversarial_bool_elem (
	pk           bigint AUTO_INCREMENT PRIMARY KEY,
	value        boolean,
	resource_id  varchar(64) COLLATE utf8mb4_0900_bin NOT NULL REFERENCES adversarial_resource(id)
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

func openPostgres(t *testing.T) *sql.DB {
	t.Helper()

	container, err := postgres.Run(t.Context(),
		postgresImage,
		postgres.WithDatabase("conformance"),
		postgres.WithUsername("conformance"),
		postgres.WithPassword("conformance"),
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

// buildMapper wires the corpus's attribute references onto the schema above.
//
// `owner` and `tagNames` deliberately alias columns that `aOptionalString` and `tags[].name`
// already cover: the corpus sends those two as EXPLICIT nulls while the originals are omitted when
// NULL, and CEL membership distinguishes null from missing. Mapping both is what lets a single
// schema exercise both conventions.
func buildMapper() cerbosent.Mapper {
	tags := &cerbosent.Relation{
		Table:        tagTable,
		SourceColumn: "id", TargetColumn: "resource_id",
		Field: &cerbosent.Entry{Column: "name", ValueType: cerbosent.ValueString},
		Fields: map[string]cerbosent.Entry{
			"id": {Column: "tag_id"},
			// Declared, like every other string column: `t.name == 0` is false in CEL, and an
			// undeclared column hands the number to the engine, which coerces (MySQL reads each
			// non-numeric name as 0) or fails to execute (PostgreSQL has no text = double).
			"name": {Column: "name", ValueType: cerbosent.ValueString},
		},
	}

	tagNames := *tags
	tagNames.Field = &cerbosent.Entry{Column: "name", ValueType: cerbosent.ValueString, NullConvention: cerbosent.NullConventionExplicit}

	labels := &cerbosent.Relation{
		Table:        labelTable,
		SourceColumn: "id", TargetColumn: "sub_category_id",
		Field:  &cerbosent.Entry{Column: "name"},
		Fields: map[string]cerbosent.Entry{"name": {Column: "name"}},
	}

	subCategories := &cerbosent.Relation{
		Table:        subCategoryTable,
		SourceColumn: "id", TargetColumn: "category_id",
		Field: &cerbosent.Entry{Column: "name"},
		Fields: map[string]cerbosent.Entry{
			"name":   {Column: "name"},
			"labels": {Relation: labels},
		},
	}

	categories := &cerbosent.Relation{
		Table:        categoryTable,
		SourceColumn: "id", TargetColumn: "resource_id",
		Fields: map[string]cerbosent.Entry{
			"name":          {Column: "name"},
			"subCategories": {Relation: subCategories},
		},
	}

	// mainCategory.* flattens the two-hop chain from the root: the subquery joins through the
	// intermediate category table while only the resource row correlates outwards.
	mainSub := &cerbosent.Relation{
		Table:        subCategoryTable,
		Via:          []cerbosent.Hop{{Table: categoryTable, ChildColumn: "category_id", JoinColumn: "id"}},
		SourceColumn: "id", TargetColumn: "resource_id",
		Field: &cerbosent.Entry{Column: "name"},
		Fields: map[string]cerbosent.Entry{
			"name":   {Column: "name"},
			"labels": {Relation: labels},
		},
	}

	// The two levels of the corpus's real to-one chain. The resource owns at most one parent
	// (`resource_id` is UNIQUE) and a parent at most one inner (`parent_id` is UNIQUE), so each
	// correlation matches at most one row.
	parentRel := &cerbosent.Relation{
		Table:        parentTable,
		SourceColumn: "id", TargetColumn: "resource_id",
	}
	innerRel := &cerbosent.Relation{
		Table:        innerTable,
		Via:          []cerbosent.Hop{{Table: parentTable, ChildColumn: "parent_id", JoinColumn: "id"}},
		SourceColumn: "id", TargetColumn: "resource_id",
	}

	return cerbosent.MapperMap{
		// The primary key, reached as `request.resource.id` rather than through `attr` (the
		// `id-*` actions). An adapter that resolves references by stripping a
		// `request.resource.attr.` prefix never sees this name.
		"request.resource.id": {Column: "id"},
		// Declared boolean so `string()` over it spells CEL's "true"/"false" through a CASE
		// rather than a CAST: SQLite and MySQL store a boolean as 1/0 and render "1" where CEL
		// and PostgreSQL render "true", and nothing in the plan names a column's type.
		"request.resource.attr.aBool": {Column: "a_bool", ValueType: cerbosent.ValueBool},
		// Declared string so CEL's `+` between two columns resolves to concatenation:
		// the operator is overloaded and the plan carries no operand types, so an
		// undeclared pair fails closed rather than emitting a numeric `+`.
		"request.resource.attr.aString":         {Column: "a_string", ValueType: cerbosent.ValueString},
		"request.resource.attr.aNumber":         {Column: "a_number", ValueType: cerbosent.ValueNumber},
		"request.resource.attr.aDouble":         {Column: "a_double", ValueType: cerbosent.ValueNumber},
		"request.resource.attr.aOptionalString": {Column: "a_optional_string", ValueType: cerbosent.ValueString, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.createdBy":       {Column: "created_by"},
		// `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under
		// the OTHER null convention: the oracle sends a real null attribute for them rather than
		// omitting it. Declaring that here is what makes the equality family definite for these
		// two attributes and leaves it untouched for every other mapping.
		"request.resource.attr.owner":     {Column: "a_optional_string", NullConvention: cerbosent.NullConventionExplicit},
		"request.resource.attr.coOwner":   {Column: "scope", NullConvention: cerbosent.NullConventionExplicit},
		"request.resource.attr.scope":     {Column: "scope"},
		"request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbosent.ValueTimestamp},
		"request.resource.attr.updatedAt": {Column: "updated_at", ValueType: cerbosent.ValueTimestamp},
		// obj.inner is not a real nested column — it mirrors aString, the same trick the
		// spring-data and prisma reference harnesses use for the p-struct probe.
		"request.resource.attr.obj.inner": {Column: "a_string"},

		"request.resource.attr.tags":       {Relation: tags},
		"request.resource.attr.tagNames":   {Relation: &tagNames},
		"request.resource.attr.categories": {Relation: categories},

		// The two homogeneous scalar lists, one element per row of a related table, the way
		// tagNames is stored. Declaring the element's type is what lets a literal of another
		// type (`"2" in aNumberList`) be answered false as CEL answers it, rather than handed to
		// an engine that coerces '2' onto a numeric column. A null element is a real null
		// member, as the corpus spells it, so the elements take the explicit-null convention.
		"request.resource.attr.aNumberList": {Relation: elementList(numberElemTable, cerbosent.Entry{ValueType: cerbosent.ValueNumber})},
		"request.resource.attr.aBoolList":   {Relation: elementList(boolElemTable, cerbosent.Entry{ValueType: cerbosent.ValueBool})},

		"request.resource.attr.mainCategory.subCategories": {Relation: mainSub},
		"request.resource.attr.mainCategory.subNames":      {Relation: mainSub},

		// The corpus's one REAL to-one chain (the `rel-*` actions). `ScalarRelation` reads one
		// column of the joined row as a correlated scalar subquery; both levels' foreign keys are
		// UNIQUE, which is the to-ONE claim the field's doc comment says the caller is making.
		// `parent.inner` reaches two tables out, so it names the inner table and joins THROUGH
		// the parent with a Hop — the same Via vocabulary mainCategory.subCategories uses.
		"request.resource.attr.parent.aBool":                 {ScalarRelation: parentRel, Column: "a_bool"},
		"request.resource.attr.parent.aString":               {ScalarRelation: parentRel, Column: "a_string"},
		"request.resource.attr.parent.aNumber":               {ScalarRelation: parentRel, Column: "a_number"},
		"request.resource.attr.parent.aOptionalString":       {ScalarRelation: parentRel, Column: "a_optional_string"},
		"request.resource.attr.parent.inner.aBool":           {ScalarRelation: innerRel, Column: "a_bool"},
		"request.resource.attr.parent.inner.aString":         {ScalarRelation: innerRel, Column: "a_string"},
		"request.resource.attr.parent.inner.aNumber":         {ScalarRelation: innerRel, Column: "a_number"},
		"request.resource.attr.parent.inner.aOptionalString": {ScalarRelation: innerRel, Column: "a_optional_string"},
	}
}

// -- fixtures ---------------------------------------------------------------------------------

// suite holds what every dialect run shares: the corpus and one Cerbos PDP. Planning and checking
// are dialect-independent, so starting a second PDP per target would only slow the run down.
type suite struct {
	client *cerbos.GRPCClient
	corpus *Corpus
}

type harness struct {
	db     *sql.DB
	client *cerbos.GRPCClient
	corpus *Corpus
	mapper cerbosent.Mapper
	target target
}

func setupSuite(t *testing.T) *suite {
	t.Helper()

	corpus := loadCorpus(t, adapterName)
	strictEvaluation, modeSet := os.LookupEnv("ADAPTER_TEST_STRICT_EVALUATION")
	if !modeSet {
		strictEvaluation = "false"
	}
	require.Contains(t, []string{"false", "true"}, strictEvaluation,
		"ADAPTER_TEST_STRICT_EVALUATION must be false or true")

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        corpus.CerbosImage,
			ExposedPorts: []string{"3593/tcp"},
			Cmd: []string{
				"server", "--set=storage.disk.directory=/policies",
				"--set=engine.strictEvaluation=" + strictEvaluation,
			},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      corpus.Dir + "/policies",
				ContainerFilePath: "/policies",
				FileMode:          0o755,
			}},
			WaitingFor: wait.ForLog("Starting gRPC server").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err, "starting Cerbos")
	testcontainers.CleanupContainer(t, container)

	endpoint, err := container.PortEndpoint(t.Context(), "3593/tcp", "")
	require.NoError(t, err)

	client, err := cerbos.New(endpoint, cerbos.WithPlaintext())
	require.NoError(t, err, "connecting to Cerbos")

	return &suite{client: client, corpus: corpus}
}

func (s *suite) harnessFor(t *testing.T, tgt target) *harness {
	t.Helper()

	db := tgt.open(t)
	// Statement by statement rather than one multi-statement Exec: the MySQL driver rejects
	// batched DDL unless multiStatements is on, and splitting keeps every dialect on the same
	// path.
	for _, stmt := range strings.Split(tgt.ddl, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, "creating %s schema", tgt.name)
	}

	h := &harness{db: db, client: s.client, corpus: s.corpus, mapper: buildMapper(), target: tgt}
	h.seed(t)
	return h
}

// exec builds a statement through ent's builder so placeholders match the dialect, then runs it.
func (h *harness) exec(t *testing.T, table string, columns []string, values ...any) {
	t.Helper()

	query, args := entsql.Dialect(h.target.dialect).
		Insert(table).Columns(columns...).Values(values...).Query()
	_, err := h.db.ExecContext(t.Context(), query, args...)
	require.NoError(t, err, "seeding %s", table)
}

func (h *harness) seed(t *testing.T) {
	t.Helper()

	for _, seed := range h.corpus.Seeds.Seeds {
		h.exec(t, resourceTable,
			[]string{
				"id", "a_bool", "a_string", "a_number", "a_double",
				"a_optional_string", "created_by", "scope", "created_at", "updated_at",
			},
			seed.ID, seed.ABool, seed.AString, seed.ANumber, nullableFloat(h.corpus.aDouble(seed)),
			nullableString(seed.AOptionalString), h.corpus.createdBy(seed),
			nullableString(h.corpus.scopeOf(seed)), h.storedTimestamp(t, h.corpus.createdAt(seed)),
			h.storedTimestamp(t, h.corpus.updatedAt(seed)))

		// The to-one chain, one owned row per level. A seed with no parent gets no row at all,
		// which is what makes the absent-parent hazard reachable through a SCALAR rather than
		// only through mainCategory's collection.
		if parentSeed := h.corpus.parentSeedOf(&seed); parentSeed != nil {
			h.exec(t, parentTable,
				[]string{"id", "a_bool", "a_string", "a_number", "a_optional_string", "resource_id"},
				parentID(seed), parentSeed.ABool, parentSeed.AString, parentSeed.ANumber,
				nullableString(parentSeed.AOptionalString), seed.ID)

			if inner := h.corpus.parentSeedOf(parentSeed); inner != nil {
				h.exec(t, innerTable,
					[]string{"id", "a_bool", "a_string", "a_number", "a_optional_string", "parent_id"},
					innerID(seed), inner.ABool, inner.AString, inner.ANumber,
					nullableString(inner.AOptionalString), parentID(seed))
			}
		}

		for _, tag := range seed.Tags {
			h.exec(t, tagTable, []string{"tag_id", "name", "resource_id"},
				tag.ID, nullableString(tag.Name), seed.ID)
		}

		// A relation has no row order, which is why `index` over these lists stays refused;
		// membership and hasIntersection are position-blind and need none.
		for _, element := range seed.ANumberList {
			h.exec(t, numberElemTable, []string{"value", "resource_id"}, nullableFloat(element), seed.ID)
		}
		for _, element := range seed.ABoolList {
			h.exec(t, boolElemTable, []string{"value", "resource_id"}, nullableBool(element), seed.ID)
		}

		for i, subName := range seed.SubCategoryNames {
			catID, subID := categoryID(seed, i), subCategoryID(seed, i)
			h.exec(t, categoryTable, []string{"id", "name", "resource_id"}, catID, "business", seed.ID)
			h.exec(t, subCategoryTable, []string{"id", "name", "category_id"}, subID, subName, catID)

			for j, label := range h.corpus.labelsOf(seed) {
				h.exec(t, labelTable, []string{"id", "name", "sub_category_id"},
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
func (h *harness) storedTimestamp(t *testing.T, raw *string) any {
	t.Helper()

	if raw == nil {
		return nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, *raw)
	require.NoError(t, err, "parsing derived timestamp")

	if h.target.dialect == dialect.SQLite {
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

// elementList maps a scalar list stored one element per row of table.
// element.ValueType declares the element type; the column and the explicit-null convention are
// the same for both lists.
func elementList(table string, element cerbosent.Entry) *cerbosent.Relation {
	element.Column = "value"
	element.NullConvention = cerbosent.NullConventionExplicit
	return &cerbosent.Relation{
		Table:        table,
		SourceColumn: "id", TargetColumn: "resource_id",
		Field: &element,
	}
}

// -- the two sides of the differential ---------------------------------------------------------

func (h *harness) principal() *cerbos.Principal {
	p := h.corpus.Seeds.Principal
	return cerbos.NewPrincipal(p.ID, p.Roles...).WithAttributes(p.Attr)
}

// checkResource builds Cerbos attributes mirroring exactly what the seeded row holds.
//
// A DB NULL is a MISSING attribute by default: CEL raises a missing-attribute error, which Cerbos
// treats as a deny — the same three-valued logic SQL applies when a NULL participates in a
// comparison. `owner` and `tagNames` are the deliberate exceptions, sent as explicit nulls.
func (h *harness) checkResource(seed Seed) *cerbos.Resource {
	tags := make([]any, 0, len(seed.Tags))
	tagNames := make([]any, 0, len(seed.Tags))
	for _, tag := range seed.Tags {
		element := map[string]any{"id": tag.ID}
		if tag.Name != nil {
			element["name"] = *tag.Name
		}
		tags = append(tags, element)

		if tag.Name == nil {
			tagNames = append(tagNames, nil)
		} else {
			tagNames = append(tagNames, *tag.Name)
		}
	}

	labels := make([]any, 0)
	for _, label := range h.corpus.labelsOf(seed) {
		if label == nil {
			labels = append(labels, map[string]any{})
		} else {
			labels = append(labels, map[string]any{"name": *label})
		}
	}

	categories := make([]any, 0, len(seed.SubCategoryNames))
	for _, subName := range seed.SubCategoryNames {
		categories = append(categories, map[string]any{
			"name":          "business",
			"subCategories": []any{map[string]any{"name": subName, "labels": labels}},
		})
	}

	attr := map[string]any{
		"aBool":      seed.ABool,
		"aString":    seed.AString,
		"aNumber":    seed.ANumber,
		"createdBy":  h.corpus.createdBy(seed),
		"obj":        map[string]any{"inner": seed.AString},
		"tags":       tags,
		"tagNames":   tagNames,
		"categories": categories,
		// Sent verbatim, null elements included, and stored one element per row of a related
		// table. Membership and hasIntersection over them are position-blind and compared. A
		// positional read is `index`, which has no case in the vendored translator (a relation has
		// no row order to read position 0 from), so those actions fail closed before any mapping
		// is consulted; the degeneracy guard still proves each one a live, discriminating probe.
		"aNumberList": scalarList(seed.ANumberList),
		"aBoolList":   scalarList(seed.ABoolList),
	}

	// Explicit null: `owner` aliases the same column but is sent as a real null attribute.
	if seed.AOptionalString != nil {
		attr["owner"] = *seed.AOptionalString
		attr["aOptionalString"] = *seed.AOptionalString
	} else {
		attr["owner"] = nil
	}

	// `coOwner` is the explicit-null alias of the `scope` column, the second half of
	// `null-value-f2f`: `scope` itself is omitted when NULL (below), so the corpus carries the
	// same column under both conventions and the field-to-field probe has two explicit nulls.
	if s := h.corpus.scopeOf(seed); s != nil {
		attr["coOwner"] = *s
	} else {
		attr["coOwner"] = nil
	}

	if d := h.corpus.aDouble(seed); d != nil {
		attr["aDouble"] = *d
	}
	if s := h.corpus.scopeOf(seed); s != nil {
		attr["scope"] = *s
	}
	if ts := h.corpus.createdAt(seed); ts != nil {
		attr["createdAt"] = *ts
	}
	if ts := h.corpus.updatedAt(seed); ts != nil {
		attr["updatedAt"] = *ts
	}

	// mainCategory mirrors the row's category graph as ONE nested object. Rows without a
	// category get NO attribute — a CEL missing-attribute error (deny), matching the adapter's
	// empty join chain excluding the row.
	if len(seed.SubCategoryNames) > 0 {
		subs := make([]any, 0, len(seed.SubCategoryNames))
		names := make([]any, 0, len(seed.SubCategoryNames))
		for _, subName := range seed.SubCategoryNames {
			subs = append(subs, map[string]any{"name": subName})
			names = append(names, subName)
		}
		attr["mainCategory"] = map[string]any{
			"name": "business", "subCategories": subs, "subNames": names,
		}
	}

	// The real to-one chain, mirroring the seeded rows exactly. A row with no parent sends NO
	// `parent` attribute — a CEL missing-path error (deny) — matching a join that finds nothing;
	// the same holds one level down for `parent.inner`.
	if parentSeed := h.corpus.parentSeedOf(&seed); parentSeed != nil {
		parent := relationAttr(parentSeed)
		if inner := h.corpus.parentSeedOf(parentSeed); inner != nil {
			parent["inner"] = relationAttr(inner)
		}
		attr["parent"] = parent
	}

	return cerbos.NewResource(h.corpus.Seeds.ResourceKind, seed.ID).WithAttributes(attr)
}

func (h *harness) oracleAllowedIDs(t *testing.T, action string) []string {
	t.Helper()

	var allowed []string
	for _, seed := range h.corpus.Seeds.Seeds {
		ok, err := h.client.IsAllowed(t.Context(), h.principal(), h.checkResource(seed), action)
		require.NoError(t, err, "check() for %s/%s", action, seed.ID)
		if ok {
			allowed = append(allowed, seed.ID)
		}
	}
	sort.Strings(allowed)
	return allowed
}

// allSeedIDs is every seeded id, sorted — what an unfiltered query returns.
func (h *harness) allSeedIDs() []string {
	ids := make([]string, 0, len(h.corpus.Seeds.Seeds))
	for _, seed := range h.corpus.Seeds.Seeds {
		ids = append(ids, seed.ID)
	}
	sort.Strings(ids)
	return ids
}

// requireOracleShape is the degeneracy guard for one oracle-compared action, asserted on the oracle
// the comparison already computed. A trivial oracle lets the comparison pass vacuously: an empty one
// still catches an over-grant, but a total one cannot, because an adapter returning every row agrees
// with it. So every compared action must be non-empty and non-total, unless the corpus declares it
// degenerate by construction in actions.json's `degenerateOracles` — and then it must be exactly
// that, so an entry whose oracle starts discriminating cannot stay an exemption
// (cerbos/query-plan-adapters#490).
func (h *harness) requireOracleShape(t *testing.T, action string, allowed []string) {
	t.Helper()

	switch h.corpus.DegenerateOracles[action] {
	case "empty":
		require.Empty(t, allowed, "%s: degenerateOracles declares an empty oracle", action)
	case "total":
		require.Equal(t, h.allSeedIDs(), allowed, "%s: degenerateOracles declares a total oracle", action)
	default:
		require.NotEmpty(t, allowed,
			"%s: oracle allows nothing, so the differential cannot fail; restore its discriminating "+
				"seed or declare it in conformance/actions.json degenerateOracles", action)
		require.Less(t, len(allowed), len(h.corpus.Seeds.Seeds),
			"%s: oracle allows every seed, so an adapter returning every row passes; restore its "+
				"discriminating seed or declare it in conformance/actions.json degenerateOracles", action)
	}
}

// adapterFilteredIDs plans, translates and executes, returning the ids the predicate selects.
func (h *harness) adapterFilteredIDs(t *testing.T, action string, opts ...cerbosent.Option) ([]string, error) {
	t.Helper()

	plan, err := h.client.PlanResources(t.Context(), h.principal(),
		cerbos.NewResource(h.corpus.Seeds.ResourceKind, ""), action)
	require.NoError(t, err, "planning %s", action)

	opts = append(opts, cerbosent.WithDialect(h.target.dialect))
	result, err := cerbosent.Translate(plan.PlanResourcesResponse, resourceTable, h.mapper, opts...)
	if err != nil {
		return nil, err
	}
	if result.Kind == cerbosent.KindAlwaysDenied {
		return nil, nil
	}

	// The outer FROM holds only the resource table — every relation is reached through a
	// correlated subquery — so an unqualified `id` is unambiguous here.
	selector := entsql.Dialect(h.target.dialect).
		Select("id").
		From(entsql.Table(resourceTable))
	if result.Kind == cerbosent.KindConditional {
		selector.Where(result.Predicate)
	}

	// Query() runs the predicate's closure — the second write pass. Translate already surfaced a
	// render error from its own probe pass, so a failure here is one only this pass can reach, and
	// the builder reports it by collecting it on the selector rather than by returning it. Ignoring
	// it would execute whatever partial SQL the failed pass left behind: a truncated WHERE clause
	// still parses, so the run would compare the wrong row set against the oracle instead of
	// failing (cerbos/query-plan-adapters#319).
	query, args := selector.Query()
	require.NoError(t, selector.Err(), "building the translated query for %s\nSQL: %s", action, query)

	rows, err := h.db.QueryContext(t.Context(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing translated predicate for %s: %w\nSQL: %s", action, err, query)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Strings(ids)
	return ids, nil
}

// -- the suite ----------------------------------------------------------------------------------

func TestAdversarialConformance(t *testing.T) {
	s := setupSuite(t)

	for _, tgt := range targets() {
		t.Run(tgt.name, func(t *testing.T) {
			runConformance(t, s.harnessFor(t, tgt))
		})
	}
}

func runConformance(t *testing.T, h *harness) {
	t.Helper()

	t.Run("manifest classifies every action exactly once", func(t *testing.T) {
		seen := map[string]int{}
		for _, action := range h.corpus.AllClassifiedActions() {
			seen[action]++
		}
		for action, count := range seen {
			require.Equal(t, 1, count, "action %q classified %d times", action, count)
		}
		// Corpus-size tripwire: bump deliberately when the corpus grows, so a new hostile shape
		// cannot slip past this adapter unnoticed.
		require.Len(t, seen, 324, "corpus size changed; triage the new action(s) before bumping")
		require.Len(t, h.corpus.Seeds.Seeds, 29, "seed count changed")
		// Throwing-count tripwire: each of these carries a pinned message, so a shape gained or
		// lost has to be re-triaged here rather than joining the throw suite unnoticed.
		require.Len(t, h.corpus.ThrowingActions, 63, "throwing action count changed")
	})

	t.Run("oracle", func(t *testing.T) {
		for _, action := range h.corpus.OracleActions {
			if h.corpus.SkippedActions[action] {
				continue
			}
			t.Run(action, func(t *testing.T) {
				expected := h.oracleAllowedIDs(t, action)
				h.requireOracleShape(t, action, expected)
				actual, err := h.adapterFilteredIDs(t, action)
				require.NoError(t, err, "translating %s", action)
				require.Equal(t, expected, actual,
					"filtered ids diverge from the check() oracle for %s", action)
			})
		}
	})

	t.Run("unsupported shapes fail loudly", func(t *testing.T) {
		for _, entry := range h.corpus.ThrowingActions {
			t.Run(entry.Action, func(t *testing.T) {
				// Translate directly rather than through adapterFilteredIDs: the plan is
				// fetched with its own error check and no query executes, so the database
				// rejecting a wrongly emitted predicate cannot masquerade as the adapter
				// refusing to translate.
				plan, err := h.client.PlanResources(t.Context(), h.principal(),
					cerbos.NewResource(h.corpus.Seeds.ResourceKind, ""), entry.Action)
				require.NoError(t, err, "planning %s", entry.Action)

				_, err = cerbosent.Translate(plan.PlanResourcesResponse, resourceTable, h.mapper,
					cerbosent.WithDialect(h.target.dialect))
				require.Error(t, err,
					"%s must fail translation rather than emit a predicate (%s)", entry.Action, entry.Reason)
				require.ErrorIs(t, err, cerbosent.ErrUnsupported,
					"%s must be refused as unsupported, not fail incidentally", entry.Action)
				// ErrUnsupported pins the family; the corpus message pins the mechanism. Without
				// it a mapper typo or an unrelated validation wrapped in the same sentinel would
				// satisfy this case as well as the documented limitation
				// (cerbos/query-plan-adapters#326).
				require.ErrorContains(t, err, entry.Message,
					"%s must be refused for the mechanism actions.json declares", entry.Action)
			})
		}
	})

	// #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
	// refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — check()
	// cannot evaluate a non-boolean conjunction — so it is declared in degenerateOracles,
	// and the throw suite above, on its own, would say nothing about whether refusing it is
	// REQUIRED.
	//
	// This is that argument. The other conjunct is `R.attr.aBool`, which this adapter certainly
	// can express and which `root-bare-bool` spells on its own; an adapter that dropped the
	// conjunct it could not translate would emit exactly that predicate and return every row it
	// selects, all of which the PDP denies for this action.
	t.Run("filter-as-conjunct must be refused because dropping its untranslatable half over-grants", func(t *testing.T) {
		require.Empty(t, h.oracleAllowedIDs(t, "filter-as-conjunct"),
			"check() must deny every seed: a filter() in boolean position is not evaluable")

		survivingHalf, err := h.adapterFilteredIDs(t, "root-bare-bool")
		require.NoError(t, err, "the surviving conjunct must translate on its own")
		require.NotEmpty(t, survivingHalf,
			"root-bare-bool must return rows, else dropping the other conjunct would cost nothing")
		require.Less(t, len(survivingHalf), len(h.corpus.Seeds.Seeds),
			"root-bare-bool must not return every seed")

		_, err = h.adapterFilteredIDs(t, "filter-as-conjunct")
		require.Error(t, err, "filter-as-conjunct must be refused rather than translated")
		// The pinned message, like every other throwing action: a bare "it errored" is satisfied
		// by a mapper typo, and this shape used to fail at EXECUTION rather than translation.
		for _, entry := range h.corpus.ThrowingActions {
			if entry.Action == "filter-as-conjunct" {
				require.ErrorContains(t, err, entry.Message,
					"filter-as-conjunct must be refused for the mechanism actions.json declares")
			}
		}
	})

	t.Run("null representation omitted is rejected", func(t *testing.T) {
		for _, entry := range h.corpus.NullOmittedActions {
			t.Run(entry.Action, func(t *testing.T) {
				_, err := h.adapterFilteredIDs(t, entry.Action,
					cerbosent.WithNullRepresentation(cerbosent.NullOmitted))
				require.Error(t, err, "%s must be rejected under the omitted representation", entry.Action)
				// The rejection must be the null-operand check talking, not an incidental failure:
				// a mapper typo satisfying this assertion would leave the representation guard
				// proving nothing (cerbos/query-plan-adapters#326).
				require.ErrorContains(t, err, entry.Message,
					"%s must be rejected by the null-operand check, not incidentally", entry.Action)

				// Anti-vacuity: pin WHY the rejection is required. Under the default explicit
				// representation this adapter emits IS NULL and returns rows the PDP denies, so
				// the rejection is load-bearing rather than incidental.
				explicitHarness := *h
				explicitHarness.mapper = cerbosent.MapperFunc(func(ref string) (cerbosent.Entry, bool) {
					mapped, ok := h.mapper.Resolve(ref)
					if ref == "request.resource.attr.aOptionalString" {
						mapped.NullConvention = cerbosent.NullConventionExplicit
					}
					return mapped, ok
				})
				overGranted, err := explicitHarness.adapterFilteredIDs(t, entry.Action)
				require.NoError(t, err, "the explicit representation must still translate %s", entry.Action)
				require.NotEmpty(t, overGranted,
					"%s must return rows under the explicit representation, else the rejection proves nothing",
					entry.Action)
				require.Empty(t, h.oracleAllowedIDs(t, entry.Action),
					"%s: check() must deny every seed under the omitted convention", entry.Action)
			})
		}
	})

	// The has() planner fold is a known divergence, so it is excluded from the oracle run above
	// and nothing else in this suite touches it — the action would be exercised on neither side.
	// Pin the over-grant itself: the plan folds to ALWAYS_ALLOWED while check() denies the seeds
	// whose attribute is missing, so this adapter returns every row. When the planner stops
	// folding, this fails and prompts re-inclusion in the oracle run
	// (cerbos/query-plan-adapters#324).
	t.Run("pins the upstream has() planner over-grant", func(t *testing.T) {
		const action = "p-has"
		require.True(t, h.corpus.SkippedActions[action],
			"%s must stay registered as a known divergence for this adapter", action)

		plan, err := h.client.PlanResources(t.Context(), h.principal(),
			cerbos.NewResource(h.corpus.Seeds.ResourceKind, ""), action)
		require.NoError(t, err, "planning %s", action)
		require.Equal(t, enginev1.PlanResourcesFilter_KIND_ALWAYS_ALLOWED,
			plan.PlanResourcesResponse.GetFilter().GetKind(),
			"%s must remain the documented planner over-grant", action)

		oracle := h.oracleAllowedIDs(t, action)
		require.NotEmpty(t, oracle, "%s: check() must still allow the seeds that hold the attribute", action)
		require.Less(t, len(oracle), len(h.corpus.Seeds.Seeds),
			"%s: check() must still deny the seeds whose attribute is missing", action)
		require.Contains(t, oracle, "a1", "%s: a1 holds aOptionalString", action)

		filtered, err := h.adapterFilteredIDs(t, action)
		require.NoError(t, err, "translating %s", action)
		require.Equal(t, h.allSeedIDs(), filtered,
			"%s: the folded plan makes this adapter return every row", action)
	})

	// The to-one relation carries no corpus action yet — this is the expand half of
	// cerbos/query-plan-adapters#372's expand-contract — so nothing else in this suite would
	// notice a seeder that stored no chain at all, or one that attached every parent to the wrong
	// resource. Read the two hops back through a real join rather than counting rows: a count
	// cannot tell an inner row carrying the corpus's values from one carrying the root's own
	// columns, which is exactly the flat-column-alias failure this relation exists to make
	// visible.
	t.Run("the seeded to-one chain matches the corpus relation", func(t *testing.T) {
		want := map[string][2]*string{}
		withParent, withInner := 0, 0
		for i := range h.corpus.Seeds.Seeds {
			seed := h.corpus.Seeds.Seeds[i]
			var parent, inner *string
			if p := h.corpus.parentSeedOf(&seed); p != nil {
				withParent++
				parent = &p.AString
				if in := h.corpus.parentSeedOf(p); in != nil {
					withInner++
					inner = &in.AString
				}
			}
			want[seed.ID] = [2]*string{parent, inner}
		}
		require.NotZero(t, withParent, "no seed has a parent")
		require.NotZero(t, withInner, "no seed reaches parent.inner")
		require.Less(t, withParent, len(h.corpus.Seeds.Seeds), "every seed has a parent")

		resources := entsql.Table(resourceTable).As("r")
		parents := entsql.Table(parentTable).As("p")
		inners := entsql.Table(innerTable).As("i")
		query, args := entsql.Dialect(h.target.dialect).
			Select(resources.C("id"), parents.C("a_string"), inners.C("a_string")).
			From(resources).
			LeftJoin(parents).On(resources.C("id"), parents.C("resource_id")).
			LeftJoin(inners).On(parents.C("id"), inners.C("parent_id")).
			Query()
		rows, err := h.db.QueryContext(t.Context(), query, args...)
		require.NoError(t, err, "reading the seeded chain\nSQL: %s", query)
		defer rows.Close()

		got := map[string][2]*string{}
		for rows.Next() {
			var id string
			var parent, inner *string
			require.NoError(t, rows.Scan(&id, &parent, &inner))
			got[id] = [2]*string{parent, inner}
		}
		require.NoError(t, rows.Err())
		require.Equal(t, want, got)
	})

	t.Run("degeneracy guard", func(t *testing.T) {
		// The oracle subtest above asserts a non-empty, non-total oracle for EVERY action it
		// compares (requireOracleShape), minus the corpus's degenerateOracles allowlist. A
		// representative sample used to stand here, and left the actions it did not name free to go
		// degenerate unnoticed (cerbos/query-plan-adapters#490).
		//
		// The allowlist is asserted whether or not this adapter compares the action: it is a
		// property of the PDP and the corpus, so an entry this adapter refuses still has to hold.
		for _, entry := range h.corpus.Actions.DegenerateOracles {
			t.Run(entry.Oracle+" oracle/"+entry.Action, func(t *testing.T) {
				h.requireOracleShape(t, entry.Action, h.oracleAllowedIDs(t, entry.Action))
			})
		}

		// int() over a numeric column is unsupported for every adapter but convex, so there is no
		// comparison behind it here: it stays as a PDP/policy liveness probe for the cast group.
		// Asserting the complement keeps the split honest — a shape this adapter gains support for
		// must leave this list, because the oracle sweep then covers it.
		// The constructed hierarchy path has no comparison behind it here either, because `list`
		// has no translator case at all.
		// #387 adds three more groups with no comparison behind them: modulo (reached through the
		// int() cast that gives `%` an integer operand), the positional read of a scalar list, and
		// list equality over a map() projection, which reaches a plain value position where a held
		// collection has no scalar meaning.
		livenessOnly := []string{
			// #396: refusals retain live, discriminating oracle probes.
			"regex-final-newline", "regex-eq-true", "regex-lookahead",
			"index-negative", "index-fractional", "index-not-oob",
			"cast-not-int", "cast-not-double", "cast-not-timestamp",
			"cast-int-double", "hier-list-id",
			"arith-mod", "index-scalar-list", "map-eq-list",
			// Index errors and explicit-null elements must stay distinguishable under negation.
			"index-scalar-list-not-eq", "index-scalar-list-null",
			// The same positional read over number and boolean elements, refused by the same
			// missing `index` case: both polarities, and the two cross-type probes CEL answers
			// false for every row where a JSON-as-SQL reading answers true for b4 or c1.
			"index-number-list", "index-number-list-not-eq",
			"index-bool-list", "index-bool-list-not-eq",
			"index-bool-list-vs-number", "index-number-list-vs-bool",
			// An empty hierarchy delimiter is refused before the prefix LIKE is built, and a regex
			// with a top-level alternation is a matches(), never translated here.
			"hier-empty-delim", "matches-alt",
			// #414: the observed classification of every new discriminating action.
			"regex-digit",
			"regex-case",
			"regex-posix",
			"regex-unanchored",
			"regex-dot",
			"regex-alternation",
			"regex-grouped",
			"regex-brace",
			"regex-repetition",
			"regex-optional-operators",
			"pv-except",
			"except-size",
			"except-eq",
			"pv-structs",
			"pv-exists-one",
			"pv-filter",
			"pv-map",
			"hier-overlaps-list-prefix",
			"div-by-division",
			"temporal-raw-eq",
			"eq-list",
			"ne-list",
		}

		oracleCompared := h.corpus.OracleComparedActions()
		for _, action := range livenessOnly {
			t.Run(action, func(t *testing.T) {
				require.False(t, oracleCompared[action],
					"%s is now oracle-compared: remove it from the liveness probes, the oracle sweep covers it", action)
				require.NotContains(t, h.corpus.DegenerateOracles, action,
					"%s cannot prove liveness: degenerateOracles declares its oracle trivial", action)
				h.requireOracleShape(t, action, h.oracleAllowedIDs(t, action))
			})
		}
	})
}
