// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cerbos/cerbos-sdk-go/cerbos"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	cerbospgx "github.com/cerbos/query-plan-adapters/pgx"
)

// Adversarial differential suite.
//
// Every action in the shared ../conformance/ corpus is planned against a REAL Cerbos PDP pinned to
// conformance/CERBOS_VERSION and loaded with conformance/policies/adversarial.yaml, translated by
// this adapter, and executed against seeded PostgreSQL rows — then the filtered id set is compared
// against an oracle computed by calling check() for each row with attributes mirroring that row
// exactly.
//
// There are no hand-written expectations. If this adapter's filter semantics diverge from Cerbos's
// own evaluation for any row, the mismatch surfaces mechanically. This file owns only the
// PostgreSQL-specific half: the schema, the seeding, and the attribute mapping.

const (
	adapterName = "pgx"

	resourceTable    = "adversarial_resource"
	tagTable         = "adversarial_tag"
	categoryTable    = "adversarial_category"
	subCategoryTable = "adversarial_sub_category"
	labelTable       = "adversarial_label"
)

const schemaDDL = `
CREATE TABLE adversarial_resource (
	id                 text PRIMARY KEY,
	a_bool             boolean          NOT NULL,
	a_string           text             NOT NULL,
	a_number           bigint           NOT NULL,
	a_double           double precision,
	a_optional_string  text,
	created_by         text             NOT NULL,
	scope              text,
	created_at         timestamptz
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
`

// mapper wires the corpus's attribute references onto the schema above.
//
// `owner` and `tagNames` deliberately alias columns that `aOptionalString` and `tags[].name`
// already cover: the corpus sends those two as EXPLICIT nulls while the originals are omitted when
// NULL, and CEL membership distinguishes null from missing. Mapping both is what lets a single
// schema exercise both conventions.
func buildMapper() cerbospgx.Mapper {
	tagFields := map[string]cerbospgx.Entry{
		"id":   {Column: "tag_id"},
		"name": {Column: "name"},
	}
	tags := &cerbospgx.Relation{
		Table:        tagTable,
		SourceColumn: "id", TargetColumn: "resource_id",
		Field:  &cerbospgx.Entry{Column: "name"},
		Fields: tagFields,
	}

	labels := &cerbospgx.Relation{
		Table:        labelTable,
		SourceColumn: "id", TargetColumn: "sub_category_id",
		Field:  &cerbospgx.Entry{Column: "name"},
		Fields: map[string]cerbospgx.Entry{"name": {Column: "name"}},
	}

	subCategories := &cerbospgx.Relation{
		Table:        subCategoryTable,
		SourceColumn: "id", TargetColumn: "category_id",
		Field: &cerbospgx.Entry{Column: "name"},
		Fields: map[string]cerbospgx.Entry{
			"name":   {Column: "name"},
			"labels": {Relation: labels},
		},
	}

	categories := &cerbospgx.Relation{
		Table:        categoryTable,
		SourceColumn: "id", TargetColumn: "resource_id",
		Fields: map[string]cerbospgx.Entry{
			"name":          {Column: "name"},
			"subCategories": {Relation: subCategories},
		},
	}

	// mainCategory.* flattens the two-hop chain from the root: the subquery joins through the
	// intermediate category table while only the resource row correlates outwards.
	mainChain := []cerbospgx.Hop{{
		Table: categoryTable, ChildColumn: "category_id", JoinColumn: "id",
	}}
	mainSub := &cerbospgx.Relation{
		Table:        subCategoryTable,
		Via:          mainChain,
		SourceColumn: "id", TargetColumn: "resource_id",
		Field: &cerbospgx.Entry{Column: "name"},
		Fields: map[string]cerbospgx.Entry{
			"name":   {Column: "name"},
			"labels": {Relation: labels},
		},
	}

	return cerbospgx.MapperMap{
		"request.resource.attr.aBool":           {Column: "a_bool"},
		"request.resource.attr.aString":         {Column: "a_string"},
		"request.resource.attr.aNumber":         {Column: "a_number"},
		"request.resource.attr.aDouble":         {Column: "a_double"},
		"request.resource.attr.aOptionalString": {Column: "a_optional_string"},
		"request.resource.attr.createdBy":       {Column: "created_by"},
		"request.resource.attr.owner":           {Column: "a_optional_string"},
		"request.resource.attr.scope":           {Column: "scope"},
		"request.resource.attr.createdAt":       {Column: "created_at", ValueType: cerbospgx.ValueTimestamp},
		// obj.inner is not a real nested column — it mirrors aString, the same trick the
		// spring-data and prisma reference harnesses use for the p-struct probe.
		"request.resource.attr.obj.inner": {Column: "a_string"},

		"request.resource.attr.tags":     {Relation: tags},
		"request.resource.attr.tagNames": {Relation: tags},

		"request.resource.attr.categories": {Relation: categories},

		"request.resource.attr.mainCategory.subCategories": {Relation: mainSub},
		"request.resource.attr.mainCategory.subNames":      {Relation: mainSub},
	}
}

// -- fixtures ---------------------------------------------------------------------------------

type harness struct {
	pool   *pgxpool.Pool
	client *cerbos.GRPCClient
	corpus *Corpus
	mapper cerbospgx.Mapper
}

func setup(t *testing.T) *harness {
	t.Helper()

	corpus := loadCorpus(t, adapterName)
	ctx := t.Context()

	pgContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("conformance"),
		postgres.WithUsername("conformance"),
		postgres.WithPassword("conformance"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err, "starting PostgreSQL")
	testcontainers.CleanupContainer(t, pgContainer)

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, schemaDDL)
	require.NoError(t, err, "creating schema")
	seedDatabase(t, ctx, pool, corpus)

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

	return &harness{pool: pool, client: client, corpus: corpus, mapper: buildMapper()}
}

func seedDatabase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, corpus *Corpus) {
	t.Helper()

	for _, seed := range corpus.Seeds.Seeds {
		var created *time.Time
		if raw := createdAt(seed); raw != nil {
			parsed, err := time.Parse(time.RFC3339Nano, *raw)
			require.NoError(t, err, "parsing derived createdAt for %s", seed.ID)
			created = &parsed
		}

		_, err := pool.Exec(ctx, `
			INSERT INTO adversarial_resource
				(id, a_bool, a_string, a_number, a_double, a_optional_string, created_by, scope, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			seed.ID, seed.ABool, seed.AString, seed.ANumber, aDouble(seed),
			seed.AOptionalString, createdBy(seed), scopeOf(seed), created)
		require.NoError(t, err, "seeding resource %s", seed.ID)

		for _, tag := range seed.Tags {
			_, err := pool.Exec(ctx,
				`INSERT INTO adversarial_tag (tag_id, name, resource_id) VALUES ($1,$2,$3)`,
				tag.ID, tag.Name, seed.ID)
			require.NoError(t, err, "seeding tag %s", tag.ID)
		}

		for i, subName := range seed.SubCategoryNames {
			catID, subID := categoryID(seed, i), subCategoryID(seed, i)

			_, err := pool.Exec(ctx,
				`INSERT INTO adversarial_category (id, name, resource_id) VALUES ($1,$2,$3)`,
				catID, "business", seed.ID)
			require.NoError(t, err, "seeding category %s", catID)

			_, err = pool.Exec(ctx,
				`INSERT INTO adversarial_sub_category (id, name, category_id) VALUES ($1,$2,$3)`,
				subID, subName, catID)
			require.NoError(t, err, "seeding sub-category %s", subID)

			for j, label := range labelsOf(seed) {
				_, err := pool.Exec(ctx,
					`INSERT INTO adversarial_label (id, name, sub_category_id) VALUES ($1,$2,$3)`,
					fmt.Sprintf("%s-label%d", subID, j), label, subID)
				require.NoError(t, err, "seeding label for %s", subID)
			}
		}
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
			"name": "business",
			"subCategories": []any{
				map[string]any{"name": subName, "labels": labels},
			},
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

// oracleAllowedIds asks the PDP itself, row by row.
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

// adapterFilteredIDs plans, translates and executes, returning the ids the filter selects.
func (h *harness) adapterFilteredIDs(t *testing.T, action string, opts ...cerbospgx.Option) ([]string, error) {
	t.Helper()

	plan, err := h.client.PlanResources(t.Context(), h.principal(),
		cerbos.NewResource(h.corpus.Seeds.ResourceKind, ""), action)
	require.NoError(t, err, "planning %s", action)

	result, err := cerbospgx.Translate(plan.PlanResourcesResponse, resourceTable, h.mapper, opts...)
	if err != nil {
		return nil, err
	}

	query := `SELECT id FROM ` + resourceTable
	switch result.Kind {
	case cerbospgx.KindAlwaysDenied:
		return nil, nil
	case cerbospgx.KindAlwaysAllowed:
	case cerbospgx.KindConditional:
		query += " WHERE " + result.Where
	}

	rows, err := h.pool.Query(t.Context(), query, result.Args...)
	if err != nil {
		return nil, fmt.Errorf("executing translated filter for %s: %w\nSQL: %s", action, err, query)
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
		require.Len(t, seen, 143, "corpus size changed; triage the new action(s) before bumping")
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
				// Translate directly rather than through adapterFilteredIDs: the plan is
				// fetched with its own error check and no query executes, so Postgres
				// rejecting a wrongly emitted filter cannot masquerade as the adapter
				// refusing to translate.
				plan, err := h.client.PlanResources(t.Context(), h.principal(),
					cerbos.NewResource(h.corpus.Seeds.ResourceKind, ""), entry.Action)
				require.NoError(t, err, "planning %s", entry.Action)

				_, err = cerbospgx.Translate(plan.PlanResourcesResponse, resourceTable, h.mapper)
				require.Error(t, err,
					"%s must fail translation rather than emit a filter (%s)", entry.Action, entry.Reason)
				require.ErrorIs(t, err, cerbospgx.ErrUnsupported,
					"%s must be refused as unsupported, not fail incidentally", entry.Action)
			})
		}
	})

	t.Run("null representation omitted is rejected", func(t *testing.T) {
		for _, entry := range h.corpus.NullOmittedActions {
			t.Run(entry.Action, func(t *testing.T) {
				_, err := h.adapterFilteredIDs(t, entry.Action,
					cerbospgx.WithNullRepresentation(cerbospgx.NullOmitted))
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
		// w1-size-zero-chain, w1-not-size-chain, cast-int-string and cast-double-string are
		// deliberately absent: their oracles are empty by CONSTRUCTION (no seed holds a to-one
		// parent with zero children; every seed's aString raises in int()/double()), so they
		// cannot satisfy this guard. cast-int-double stands in for the cast group.
		representative := []string{
			"vf-le", "in-single", "like-percent", "exists-on-empty", "not-exists",
			"nary-and", "field-to-field", "ternary-cmp", "arith-add", "size-threshold",
			"hier-ancestor-cf", "pv-exists", "in-null-elem-mixed", "null-eq", "cs-eq",
			"w1-all-chain", "w1-not-exists-chain", "w1-size-nonneg-chain",
			"w1-not-in-chain", "w1-not-hasint-chain",
			"cr-div-neg-zero", "cr-div-other-column", "cr-div-then-add", "cr-div-then-add-ne",
			"cast-int-double",
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
