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
)

// Conformance suite (../conformance/README.md, "The harness contract").
//
// The corpus dataset is seeded into each database this adapter claims through WithDialect —
// SQLite, PostgreSQL and MySQL. Then, for each pinned PDP and each golden file, the recorded plan
// is translated by this adapter and executed, and the returned ids are compared with the check()
// decisions recorded next to it. conformance-ledger.json lists the cases this adapter cannot pass,
// and holds for every dialect. No PDP is started: its answers are recorded. This file owns only the
// SQL half — the schemas, the mapping and the seeding; corpus_test.go reads the corpus.

// Database container images, pinned by tag AND digest. A tag is mutable, so a tag-only pin
// records an intent rather than a build; the conformance suite is a differential whose
// divergences are dialect behaviour, so "which build was this proved against" has to be
// answerable from the repository alone. conformance/scripts/validate-corpus.sh asserts every
// service image reference in the repository carries both halves.
const (
	postgresImage = "postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
	mysqlImage    = "mysql:8.4@sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb"
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

// postgresInitdbArgs is how the PostgreSQL leg's database is initialised: --lc-collate=C, a
// byte-order collation. CEL orders strings by code point, and <, <=, > and >= on a text column
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
		// `identifier/*` cases). An adapter that resolves references by stripping a
		// `request.resource.attr.` prefix never sees this name.
		"request.resource.id": {Column: "id"},
		// Declared boolean so `string()` over it spells CEL's "true"/"false" through a CASE
		// rather than a CAST: SQLite and MySQL store a boolean as 1/0 and render "1" where CEL
		// and PostgreSQL render "true", and nothing in the plan names a column's type.
		//
		// Every attribute resources.json omits when its column is NULL is declared
		// NullConventionOmitted: aBool, aString and aNumber (NULL on j3, j1, j2), aOptionalString,
		// aDouble, scope, createdAt, updatedAt, obj.inner and every parent.* hop. A null literal
		// against one is then a missing-attribute error CEL denies, so it is refused rather than
		// rendered as IS NULL (#528).
		"request.resource.attr.aBool": {Column: "a_bool", ValueType: cerbosent.ValueBool, NullConvention: cerbosent.NullConventionOmitted},
		// Declared string so CEL's `+` between two columns resolves to concatenation:
		// the operator is overloaded and the plan carries no operand types, so an
		// undeclared pair fails closed rather than emitting a numeric `+`.
		"request.resource.attr.aString":         {Column: "a_string", ValueType: cerbosent.ValueString, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.aNumber":         {Column: "a_number", ValueType: cerbosent.ValueNumber, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.aDouble":         {Column: "a_double", ValueType: cerbosent.ValueNumber, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.aOptionalString": {Column: "a_optional_string", ValueType: cerbosent.ValueString, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.createdBy":       {Column: "created_by"},
		// `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under
		// the OTHER null convention: the corpus sends a real null attribute for them rather than
		// omitting it. Declaring that here is what makes the equality family definite for these
		// two attributes and leaves it untouched for every other mapping.
		"request.resource.attr.owner":     {Column: "a_optional_string", NullConvention: cerbosent.NullConventionExplicit},
		"request.resource.attr.coOwner":   {Column: "scope", NullConvention: cerbosent.NullConventionExplicit},
		"request.resource.attr.scope":     {Column: "scope", NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbosent.ValueTimestamp, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.updatedAt": {Column: "updated_at", ValueType: cerbosent.ValueTimestamp, NullConvention: cerbosent.NullConventionOmitted},
		// obj.inner is not a real nested column — it mirrors aString, the same trick the
		// other harnesses use for the `obj.inner` cases.
		"request.resource.attr.obj.inner": {Column: "a_string", NullConvention: cerbosent.NullConventionOmitted},

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

		// The corpus's one REAL to-one chain (the `relation/*` cases). `ScalarRelation` reads one
		// column of the joined row as a correlated scalar subquery; both levels' foreign keys are
		// UNIQUE, which is the to-ONE claim the field's doc comment says the caller is making.
		// `parent.inner` reaches two tables out, so it names the inner table and joins THROUGH
		// the parent with a Hop — the same Via vocabulary mainCategory.subCategories uses.
		// A hop's NULL column, like an absent level, is omitted from the resource, so every hop
		// is declared NullConventionOmitted.
		// The two aBool hops are declared boolean, like the root aBool, because that is what they
		// hold, and an undeclared one keeps the plain CAST that SQLite and MySQL render as 1/0.
		// `parent` itself is the to-one hop, not a column: a CEL map whose keys are the joined
		// row's non-NULL columns. Declared so a macro over it (`collection/exists/map-keys`) meets
		// the translator's own refusal rather than a harness defect.
		"request.resource.attr.parent":                       {ScalarRelation: parentRel},
		"request.resource.attr.parent.aBool":                 {ScalarRelation: parentRel, Column: "a_bool", ValueType: cerbosent.ValueBool, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.parent.aString":               {ScalarRelation: parentRel, Column: "a_string", NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.parent.aNumber":               {ScalarRelation: parentRel, Column: "a_number", NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.parent.aOptionalString":       {ScalarRelation: parentRel, Column: "a_optional_string", NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.parent.inner.aBool":           {ScalarRelation: innerRel, Column: "a_bool", ValueType: cerbosent.ValueBool, NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.parent.inner.aString":         {ScalarRelation: innerRel, Column: "a_string", NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.parent.inner.aNumber":         {ScalarRelation: innerRel, Column: "a_number", NullConvention: cerbosent.NullConventionOmitted},
		"request.resource.attr.parent.inner.aOptionalString": {ScalarRelation: innerRel, Column: "a_optional_string", NullConvention: cerbosent.NullConventionOmitted},
	}
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

		for _, tag := range seed.Tags {
			s.exec(t, tagTable, []string{"tag_id", "name", "resource_id"},
				tag.ID, nullableString(tag.Name), seed.ID)
		}

		// A relation has no row order, which is why `index` over these lists stays refused;
		// membership and hasIntersection are position-blind and need none.
		for _, element := range seed.ANumberList {
			s.exec(t, numberElemTable, []string{"value", "resource_id"}, nullableFloat(element), seed.ID)
		}
		for _, element := range seed.ABoolList {
			s.exec(t, boolElemTable, []string{"value", "resource_id"}, nullableBool(element), seed.ID)
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
	// `request.*` reference is therefore a harness defect, reported as such rather than as the
	// adapter's refusal. (Lambda-local names such as `t.name` also reach the root mapper, as a
	// probe before the element scope answers them, so only `request.*` is checked.)
	var unmapped []string
	recording := cerbosent.MapperFunc(func(reference string) (cerbosent.Entry, bool) {
		entry, ok := mapper.Resolve(reference)
		if !ok && strings.HasPrefix(reference, "request.") {
			unmapped = append(unmapped, reference)
		}
		return entry, ok
	})

	result, err := cerbosent.Translate(plan, resourceTable, recording, cerbosent.WithDialect(s.target.dialect))
	if len(unmapped) > 0 {
		return nil, fmt.Errorf("the harness mapping has no entry for %s (a harness defect, not a refusal)", strings.Join(unmapped, ", "))
	}
	if err != nil {
		return nil, err
	}
	if result.Kind == cerbosent.KindAlwaysDenied {
		return []string{}, nil
	}

	// The outer FROM holds only the resource table — every relation is reached through a
	// correlated subquery — so an unqualified `id` is unambiguous here.
	selector := entsql.Dialect(s.target.dialect).Select("id").From(entsql.Table(resourceTable))
	if result.Kind == cerbosent.KindConditional {
		selector.Where(result.Predicate)
	}

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
	corpus := loadCorpus(t)
	mapper := buildMapper()

	for _, tgt := range targets() {
		t.Run(tgt.name, func(t *testing.T) {
			s := openStore(t, tgt, corpus)
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
