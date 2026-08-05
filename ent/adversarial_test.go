// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/cerbos/cerbos-sdk-go/cerbos"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	_ "modernc.org/sqlite"

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

const (
	adapterName = "ent"

	resourceTable    = "adversarial_resource"
	tagTable         = "adversarial_tag"
	categoryTable    = "adversarial_category"
	subCategoryTable = "adversarial_sub_category"
	labelTable       = "adversarial_label"
)

// CEL string matching is case-sensitive; SQLite's LIKE is case-insensitive for ASCII by default,
// which would over-grant on the `cs-eq` and `hier-*` probes. Foreign keys are on so the seeded
// relation graph is genuinely referentially valid.
const sqliteDSN = "file:adversarial?mode=memory&cache=shared" +
	"&_pragma=case_sensitive_like(1)&_pragma=foreign_keys(1)"

const schemaDDL = `
CREATE TABLE adversarial_resource (
	id                 text PRIMARY KEY,
	a_bool             integer NOT NULL,
	a_string           text    NOT NULL,
	a_number           integer NOT NULL,
	a_double           real,
	a_optional_string  text,
	created_by         text    NOT NULL,
	scope              text,
	created_at         text
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
`

// buildMapper wires the corpus's attribute references onto the schema above.
//
// `owner` and `tagNames` deliberately alias columns that `aOptionalString` and `tags[].name`
// already cover: the corpus sends those two as EXPLICIT nulls while the originals are omitted when
// NULL, and CEL membership distinguishes null from missing. Mapping both is what lets a single
// schema exercise both conventions.
func buildMapper() cerbosent.Mapper {
	tags := &cerbosent.Relation{
		Kind: cerbosent.RelationMany, Table: tagTable,
		SourceColumn: "id", TargetColumn: "resource_id",
		Field: &cerbosent.Entry{Column: "name"},
		Fields: map[string]cerbosent.Entry{
			"id":   {Column: "tag_id"},
			"name": {Column: "name"},
		},
	}

	labels := &cerbosent.Relation{
		Kind: cerbosent.RelationMany, Table: labelTable,
		SourceColumn: "id", TargetColumn: "sub_category_id",
		Field:  &cerbosent.Entry{Column: "name"},
		Fields: map[string]cerbosent.Entry{"name": {Column: "name"}},
	}

	subCategories := &cerbosent.Relation{
		Kind: cerbosent.RelationMany, Table: subCategoryTable,
		SourceColumn: "id", TargetColumn: "category_id",
		Field: &cerbosent.Entry{Column: "name"},
		Fields: map[string]cerbosent.Entry{
			"name":   {Column: "name"},
			"labels": {Relation: labels},
		},
	}

	categories := &cerbosent.Relation{
		Kind: cerbosent.RelationMany, Table: categoryTable,
		SourceColumn: "id", TargetColumn: "resource_id",
		Fields: map[string]cerbosent.Entry{
			"name":          {Column: "name"},
			"subCategories": {Relation: subCategories},
		},
	}

	// mainCategory.* flattens the two-hop chain from the root: the subquery joins through the
	// intermediate category table while only the resource row correlates outwards.
	mainSub := &cerbosent.Relation{
		Kind: cerbosent.RelationMany, Table: subCategoryTable,
		Via:          []cerbosent.Hop{{Table: categoryTable, ChildColumn: "category_id", JoinColumn: "id"}},
		SourceColumn: "id", TargetColumn: "resource_id",
		Field: &cerbosent.Entry{Column: "name"},
		Fields: map[string]cerbosent.Entry{
			"name":   {Column: "name"},
			"labels": {Relation: labels},
		},
	}

	return cerbosent.MapperMap{
		"request.resource.attr.aBool":           {Column: "a_bool"},
		"request.resource.attr.aString":         {Column: "a_string"},
		"request.resource.attr.aNumber":         {Column: "a_number"},
		"request.resource.attr.aDouble":         {Column: "a_double"},
		"request.resource.attr.aOptionalString": {Column: "a_optional_string"},
		"request.resource.attr.createdBy":       {Column: "created_by"},
		"request.resource.attr.owner":           {Column: "a_optional_string"},
		"request.resource.attr.scope":           {Column: "scope"},
		"request.resource.attr.createdAt":       {Column: "created_at", ValueType: cerbosent.ValueTimestamp},
		// obj.inner is not a real nested column — it mirrors aString, the same trick the
		// spring-data and prisma reference harnesses use for the p-struct probe.
		"request.resource.attr.obj.inner": {Column: "a_string"},

		"request.resource.attr.tags":       {Relation: tags},
		"request.resource.attr.tagNames":   {Relation: tags, ScalarCollection: true},
		"request.resource.attr.categories": {Relation: categories},

		"request.resource.attr.mainCategory.subCategories": {Relation: mainSub},
		"request.resource.attr.mainCategory.subNames":      {Relation: mainSub, ScalarCollection: true},
	}
}

// -- fixtures ---------------------------------------------------------------------------------

type harness struct {
	db     *sql.DB
	client *cerbos.GRPCClient
	corpus *Corpus
	mapper cerbosent.Mapper
}

func setup(t *testing.T) *harness {
	t.Helper()

	corpus := loadCorpus(t, adapterName)
	ctx := t.Context()

	db, err := sql.Open("sqlite", sqliteDSN)
	require.NoError(t, err, "opening SQLite")
	t.Cleanup(func() { _ = db.Close() })

	// The shared-cache in-memory database lives only as long as a connection is held, and a
	// pooled connection closing would drop the schema mid-run.
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(ctx, schemaDDL)
	require.NoError(t, err, "creating schema")
	seedDatabase(t, db, corpus)

	cerbosContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/cerbos/cerbos:" + corpus.CerbosVersion,
			ExposedPorts: []string{"3593/tcp"},
			Cmd:          []string{"server", "--set=storage.disk.directory=/policies"},
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
	testcontainers.CleanupContainer(t, cerbosContainer)

	endpoint, err := cerbosContainer.PortEndpoint(ctx, "3593/tcp", "")
	require.NoError(t, err)

	client, err := cerbos.New(endpoint, cerbos.WithPlaintext())
	require.NoError(t, err, "connecting to Cerbos")

	return &harness{db: db, client: client, corpus: corpus, mapper: buildMapper()}
}

func seedDatabase(t *testing.T, db *sql.DB, corpus *Corpus) {
	t.Helper()

	exec := func(query string, args ...any) {
		_, err := db.ExecContext(t.Context(), query, args...)
		require.NoError(t, err, "seeding: %s", query)
	}

	for _, seed := range corpus.Seeds.Seeds {
		// Timestamps are stored in the adapter's documented SQLite layout: SQLite compares text
		// lexicographically, so a variable-width fraction would order instants wrongly.
		var created any
		if raw := createdAt(seed); raw != nil {
			parsed, err := time.Parse(time.RFC3339Nano, *raw)
			require.NoError(t, err, "parsing derived createdAt for %s", seed.ID)
			created = parsed.UTC().Format(cerbosent.SQLiteTimestampLayout)
		}

		exec(`INSERT INTO adversarial_resource
			(id, a_bool, a_string, a_number, a_double, a_optional_string, created_by, scope, created_at)
			VALUES (?,?,?,?,?,?,?,?,?)`,
			seed.ID, seed.ABool, seed.AString, seed.ANumber, nullableFloat(aDouble(seed)),
			nullableString(seed.AOptionalString), createdBy(seed), nullableString(scopeOf(seed)), created)

		for _, tag := range seed.Tags {
			exec(`INSERT INTO adversarial_tag (tag_id, name, resource_id) VALUES (?,?,?)`,
				tag.ID, nullableString(tag.Name), seed.ID)
		}

		for i, subName := range seed.SubCategoryNames {
			catID, subID := categoryID(seed, i), subCategoryID(seed, i)
			exec(`INSERT INTO adversarial_category (id, name, resource_id) VALUES (?,?,?)`,
				catID, "business", seed.ID)
			exec(`INSERT INTO adversarial_sub_category (id, name, category_id) VALUES (?,?,?)`,
				subID, subName, catID)

			for j, label := range labelsOf(seed) {
				exec(`INSERT INTO adversarial_label (id, name, sub_category_id) VALUES (?,?,?)`,
					fmt.Sprintf("%s-label%d", subID, j), nullableString(label), subID)
			}
		}
	}
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
	for _, label := range labelsOf(seed) {
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
		"createdBy":  createdBy(seed),
		"obj":        map[string]any{"inner": seed.AString},
		"tags":       tags,
		"tagNames":   tagNames,
		"categories": categories,
	}

	// Explicit null: `owner` aliases the same column but is sent as a real null attribute.
	if seed.AOptionalString != nil {
		attr["owner"] = *seed.AOptionalString
		attr["aOptionalString"] = *seed.AOptionalString
	} else {
		attr["owner"] = nil
	}

	if d := aDouble(seed); d != nil {
		attr["aDouble"] = *d
	}
	if s := scopeOf(seed); s != nil {
		attr["scope"] = *s
	}
	if ts := createdAt(seed); ts != nil {
		attr["createdAt"] = *ts
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

// adapterFilteredIDs plans, translates and executes, returning the ids the predicate selects.
func (h *harness) adapterFilteredIDs(t *testing.T, action string, opts ...cerbosent.Option) ([]string, error) {
	t.Helper()

	plan, err := h.client.PlanResources(t.Context(), h.principal(),
		cerbos.NewResource(h.corpus.Seeds.ResourceKind, ""), action)
	require.NoError(t, err, "planning %s", action)

	opts = append(opts, cerbosent.WithDialect(dialect.SQLite))
	result, err := cerbosent.Translate(plan.PlanResourcesResponse, resourceTable, h.mapper, opts...)
	if err != nil {
		return nil, err
	}
	if result.Kind == cerbosent.KindAlwaysDenied {
		return nil, nil
	}

	// The outer FROM holds only the resource table — every relation is reached through a
	// correlated subquery — so an unqualified `id` is unambiguous here.
	selector := entsql.Dialect(dialect.SQLite).
		Select("id").
		From(entsql.Table(resourceTable))
	if result.Kind == cerbosent.KindConditional {
		selector.Where(result.Predicate)
	}

	query, args := selector.Query()
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
	h := setup(t)

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
		require.Len(t, seen, 127, "corpus size changed; triage the new action(s) before bumping")
		require.Len(t, h.corpus.Seeds.Seeds, 20, "seed count changed")
	})

	t.Run("oracle", func(t *testing.T) {
		for _, action := range h.corpus.OracleActions {
			if h.corpus.SkippedActions[action] {
				continue
			}
			t.Run(action, func(t *testing.T) {
				expected := h.oracleAllowedIDs(t, action)
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
				_, err := h.adapterFilteredIDs(t, entry.Action)
				require.Error(t, err,
					"%s must fail translation rather than emit a predicate (%s)", entry.Action, entry.Reason)
			})
		}
	})

	t.Run("null representation omitted is rejected", func(t *testing.T) {
		for _, entry := range h.corpus.NullOmittedActions {
			t.Run(entry.Action, func(t *testing.T) {
				_, err := h.adapterFilteredIDs(t, entry.Action,
					cerbosent.WithNullRepresentation(cerbosent.NullOmitted))
				require.Error(t, err, "%s must be rejected under the omitted representation", entry.Action)

				// Anti-vacuity: pin WHY the rejection is required. Under the default explicit
				// representation this adapter emits IS NULL and returns rows the PDP denies, so
				// the rejection is load-bearing rather than incidental.
				overGranted, err := h.adapterFilteredIDs(t, entry.Action)
				require.NoError(t, err, "the explicit representation must still translate %s", entry.Action)
				require.NotEmpty(t, overGranted,
					"%s must return rows under the explicit representation, else the rejection proves nothing",
					entry.Action)
				require.Empty(t, h.oracleAllowedIDs(t, entry.Action),
					"%s: check() must deny every seed under the omitted convention", entry.Action)
			})
		}
	})

	t.Run("degeneracy guard", func(t *testing.T) {
		// The comparison above can pass vacuously if the oracle itself is trivial. Assert that a
		// representative spread of actions has an oracle that is neither empty nor the full seed
		// set — without this, a silently broken PDP connection would still pass every case.
		representative := []string{
			"vf-le", "in-single", "like-percent", "exists-on-empty", "not-exists",
			"nary-and", "field-to-field", "ternary-cmp", "arith-add", "size-threshold",
			"hier-ancestor-cf", "pv-exists", "in-null-elem-mixed", "null-eq", "cs-eq",
		}
		total := len(h.corpus.Seeds.Seeds)
		for _, action := range representative {
			t.Run(action, func(t *testing.T) {
				allowed := h.oracleAllowedIDs(t, action)
				require.NotEmpty(t, allowed, "%s: oracle allows nothing", action)
				require.Less(t, len(allowed), total, "%s: oracle allows every seed", action)
			})
		}
	})
}
