// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"

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

// The database container image, pinned by tag AND digest. A tag is mutable, so a tag-only pin
// records an intent rather than a build; the adversarial suite is a differential whose
// divergences are dialect behaviour, so "which build was this proved against" has to be
// answerable from the repository alone. conformance/scripts/validate-corpus.sh asserts every
// service image reference in the repository carries both halves.
//
// It lives in a file rather than in this constant because example/run.sh needs the same reference
// and cannot read a Go constant, which is the same argument as langchain-chromadb/CHROMA_IMAGE and
// mongoose/MONGO_IMAGE. A literal in each would be two copies, and validate-corpus.sh can only hold
// one digest per TAG — nothing holds two tags equal, so moving this suite off 17-alpine would leave
// the example behind, still pinned to a real build and still green. The `_IMAGE` suffix is what that
// script's file scan looks for.
const postgresImageFile = "POSTGRES_IMAGE"

func postgresImage(tb testing.TB) string {
	tb.Helper()

	ref, err := os.ReadFile(postgresImageFile)
	if err != nil {
		tb.Fatalf("reading %s: %v", postgresImageFile, err)
	}
	return strings.TrimSpace(string(ref))
}

const (
	adapterName = "pgx"

	resourceTable    = "adversarial_resource"
	tagTable         = "adversarial_tag"
	categoryTable    = "adversarial_category"
	subCategoryTable = "adversarial_sub_category"
	labelTable       = "adversarial_label"
	parentTable      = "adversarial_parent"
	innerTable       = "adversarial_inner"
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

CREATE TABLE adversarial_parent (
	id                 text    PRIMARY KEY,
	a_bool             boolean NOT NULL,
	a_string           text    NOT NULL,
	a_number           bigint  NOT NULL,
	a_optional_string  text,
	resource_id        text    NOT NULL UNIQUE REFERENCES adversarial_resource(id)
);

CREATE TABLE adversarial_inner (
	id                 text    PRIMARY KEY,
	a_bool             boolean NOT NULL,
	a_string           text    NOT NULL,
	a_number           bigint  NOT NULL,
	a_optional_string  text,
	parent_id          text    NOT NULL UNIQUE REFERENCES adversarial_parent(id)
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

	// The two levels of the corpus's real to-one chain. The resource owns at most one parent
	// (`resource_id` is UNIQUE) and a parent at most one inner (`parent_id` is UNIQUE), so each
	// correlation matches at most one row.
	parentRel := &cerbospgx.Relation{
		Table:        parentTable,
		SourceColumn: "id", TargetColumn: "resource_id",
	}
	innerRel := &cerbospgx.Relation{
		Table:        innerTable,
		Via:          []cerbospgx.Hop{{Table: parentTable, ChildColumn: "parent_id", JoinColumn: "id"}},
		SourceColumn: "id", TargetColumn: "resource_id",
	}

	return cerbospgx.MapperMap{
		// The primary key, reached as `request.resource.id` rather than through `attr` (the
		// `id-*` actions). An adapter that resolves references by stripping a
		// `request.resource.attr.` prefix never sees this name.
		"request.resource.id": {Column: "id"},
		// Declared boolean so `string()` over it fails closed: SQLite and MySQL store a
		// boolean as 1/0 and render "1" where CEL and PostgreSQL render "true", and nothing
		// in the plan names a column's type.
		"request.resource.attr.aBool": {Column: "a_bool", ValueType: cerbospgx.ValueBool},
		// Declared string so CEL's `+` between two columns resolves to concatenation:
		// the operator is overloaded and the plan carries no operand types, so an
		// undeclared pair fails closed rather than emitting a numeric `+`.
		"request.resource.attr.aString":         {Column: "a_string", ValueType: cerbospgx.ValueString},
		"request.resource.attr.aNumber":         {Column: "a_number"},
		"request.resource.attr.aDouble":         {Column: "a_double"},
		"request.resource.attr.aOptionalString": {Column: "a_optional_string", ValueType: cerbospgx.ValueString},
		"request.resource.attr.createdBy":       {Column: "created_by"},
		// `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under
		// the OTHER null convention: the oracle sends a real null attribute for them rather than
		// omitting it. Declaring that here is what makes the equality family definite for these
		// two attributes and leaves it untouched for every other mapping.
		"request.resource.attr.owner":     {Column: "a_optional_string", NullConvention: cerbospgx.NullConventionExplicit},
		"request.resource.attr.coOwner":   {Column: "scope", NullConvention: cerbospgx.NullConventionExplicit},
		"request.resource.attr.scope":     {Column: "scope"},
		"request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbospgx.ValueTimestamp},
		// obj.inner is not a real nested column — it mirrors aString, the same trick the
		// other harnesses use for the p-struct probe.
		"request.resource.attr.obj.inner": {Column: "a_string"},

		"request.resource.attr.tags":     {Relation: tags},
		"request.resource.attr.tagNames": {Relation: tags},

		"request.resource.attr.categories": {Relation: categories},

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
		postgresImage(t),
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
			Image:        corpus.CerbosImage,
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
		if raw := corpus.createdAt(seed); raw != nil {
			parsed, err := time.Parse(time.RFC3339Nano, *raw)
			require.NoError(t, err, "parsing derived createdAt for %s", seed.ID)
			created = &parsed
		}

		_, err := pool.Exec(ctx, `
			INSERT INTO adversarial_resource
				(id, a_bool, a_string, a_number, a_double, a_optional_string, created_by, scope, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			seed.ID, seed.ABool, seed.AString, seed.ANumber, corpus.aDouble(seed),
			seed.AOptionalString, corpus.createdBy(seed), corpus.scopeOf(seed), created)
		require.NoError(t, err, "seeding resource %s", seed.ID)

		// The to-one chain, one owned row per level. A seed with no parent gets no row at all,
		// which is what makes the absent-parent hazard reachable through a SCALAR rather than
		// only through mainCategory's collection.
		if parentSeed := corpus.parentSeedOf(&seed); parentSeed != nil {
			_, err := pool.Exec(ctx, `
				INSERT INTO adversarial_parent
					(id, a_bool, a_string, a_number, a_optional_string, resource_id)
				VALUES ($1,$2,$3,$4,$5,$6)`,
				parentID(seed), parentSeed.ABool, parentSeed.AString, parentSeed.ANumber,
				parentSeed.AOptionalString, seed.ID)
			require.NoError(t, err, "seeding parent for %s", seed.ID)

			if inner := corpus.parentSeedOf(parentSeed); inner != nil {
				_, err := pool.Exec(ctx, `
					INSERT INTO adversarial_inner
						(id, a_bool, a_string, a_number, a_optional_string, parent_id)
					VALUES ($1,$2,$3,$4,$5,$6)`,
					innerID(seed), inner.ABool, inner.AString, inner.ANumber,
					inner.AOptionalString, parentID(seed))
				require.NoError(t, err, "seeding inner for %s", seed.ID)
			}
		}

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

			for j, label := range corpus.labelsOf(seed) {
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
	p := h.corpus.CheckResources.Principal
	return cerbos.NewPrincipal(p.ID, p.Roles...).WithAttributes(p.Attr)
}

func (h *harness) checkResource(resource CheckResource) *cerbos.Resource {
	return cerbos.NewResource(resource.Kind, resource.ID).WithAttributes(resource.Attr)
}

func (h *harness) resourceKind() string {
	return h.corpus.CheckResources.Resources[0].Kind
}

// oracleAllowedIds asks the PDP itself, row by row.
func (h *harness) oracleAllowedIDs(t *testing.T, action string) []string {
	t.Helper()

	var allowed []string
	for _, resource := range h.corpus.CheckResources.Resources {
		ok, err := h.client.IsAllowed(t.Context(), h.principal(), h.checkResource(resource), action)
		require.NoError(t, err, "check() for %s/%s", action, resource.ID)
		if ok {
			allowed = append(allowed, resource.ID)
		}
	}
	sort.Strings(allowed)
	return allowed
}

// allSeedIDs is every seeded id, sorted — what an unfiltered query returns.
func (h *harness) allSeedIDs() []string {
	ids := make([]string, 0, len(h.corpus.CheckResources.Resources))
	for _, resource := range h.corpus.CheckResources.Resources {
		ids = append(ids, resource.ID)
	}
	sort.Strings(ids)
	return ids
}

// adapterFilteredIDs plans, translates and executes, returning the ids the filter selects.
func (h *harness) adapterFilteredIDs(t *testing.T, action string, opts ...cerbospgx.Option) ([]string, error) {
	t.Helper()

	plan, err := h.client.PlanResources(t.Context(), h.principal(),
		cerbos.NewResource(h.resourceKind(), ""), action)
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
		expectedActions := len(h.corpus.Catalog.Actions)
		if h.corpus.SelectedAction != "" {
			expectedActions = 1
		}
		require.Len(t, seen, expectedActions,
			"selected manifest outcomes must be classified exactly once")
		require.Len(t, h.corpus.Seeds.Seeds, len(h.corpus.CheckResources.Resources),
			"stored rows must match the canonical check resources")
	})

	t.Run("oracle", func(t *testing.T) {
		for _, action := range h.corpus.OracleActions {
			if h.corpus.UpstreamBlockedActions[action] {
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
					cerbos.NewResource(h.resourceKind(), ""), entry.Action)
				require.NoError(t, err, "planning %s", entry.Action)

				_, err = cerbospgx.Translate(plan.PlanResourcesResponse, resourceTable, h.mapper)
				require.Error(t, err,
					"%s must fail translation rather than emit a filter (%s)", entry.Action, entry.Reason)
				require.ErrorIs(t, err, cerbospgx.ErrUnsupported,
					"%s must be refused as unsupported, not fail incidentally", entry.Action)
				// ErrUnsupported pins the family; the corpus message pins the mechanism. Without
				// it a mapper typo or an unrelated validation wrapped in the same sentinel would
				// satisfy this case as well as the documented limitation
				// (cerbos/query-plan-adapters#326).
				require.ErrorContains(t, err, entry.Message,
					"%s must be refused for the mechanism adapterctl.json declares", entry.Action)
			})
		}
	})

	// #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
	// refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — check()
	// cannot evaluate a non-boolean conjunction — so it belongs to neither degeneracy-guard list,
	// and the throw suite above, on its own, would say nothing about whether refusing it is
	// REQUIRED.
	//
	// This is that argument. The other conjunct is `R.attr.aBool`, which this adapter certainly
	// can express and which `root-bare-bool` spells on its own; an adapter that dropped the
	// conjunct it could not translate would emit exactly that predicate and return every row it
	// selects, all of which the PDP denies for this action.
	t.Run("filter-as-conjunct must be refused because dropping its untranslatable half over-grants", func(t *testing.T) {
		if h.corpus.SelectedAction != "" && h.corpus.SelectedAction != "filter-as-conjunct" {
			t.Skip("another action was selected by ADAPTERCTL_ACTION")
		}
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
					"filter-as-conjunct must be refused for the mechanism adapterctl.json declares")
			}
		}
	})

	t.Run("representation-dependent outcome is rejected", func(t *testing.T) {
		for _, entry := range h.corpus.NullOmittedActions {
			t.Run(entry.Action, func(t *testing.T) {
				_, err := h.adapterFilteredIDs(t, entry.Action,
					cerbospgx.WithNullRepresentation(cerbospgx.NullOmitted))
				require.Error(t, err, "%s must be rejected under the omitted representation", entry.Action)
				// The rejection must be the null-operand check talking, not an incidental failure:
				// a mapper typo satisfying this assertion would leave the representation guard
				// proving nothing (cerbos/query-plan-adapters#326).
				require.ErrorContains(t, err, entry.Message,
					"%s must be rejected by the null-operand check, not incidentally", entry.Action)

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

	// The has() planner fold is upstream-blocked, so it is excluded from the oracle run above
	// and nothing else in this suite touches it — the action would be exercised on neither side.
	// Pin the over-grant itself: the plan folds to ALWAYS_ALLOWED while check() denies the seeds
	// whose attribute is missing, so this adapter returns every row. When the planner stops
	// folding, this fails and prompts re-inclusion in the oracle run
	// (cerbos/query-plan-adapters#324).
	t.Run("pins the upstream has() planner over-grant", func(t *testing.T) {
		const action = "p-has"
		if h.corpus.SelectedAction != "" && h.corpus.SelectedAction != action {
			t.Skip("another action was selected by ADAPTERCTL_ACTION")
		}
		require.True(t, h.corpus.UpstreamBlockedActions[action],
			"%s must stay registered as upstream-blocked for this adapter", action)

		plan, err := h.client.PlanResources(t.Context(), h.principal(),
			cerbos.NewResource(h.resourceKind(), ""), action)
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

		rows, err := h.pool.Query(t.Context(), `
			SELECT r.id, p.a_string, i.a_string
			FROM `+resourceTable+` r
			LEFT JOIN `+parentTable+` p ON p.resource_id = r.id
			LEFT JOIN `+innerTable+` i ON i.parent_id = p.id`)
		require.NoError(t, err, "reading the seeded chain")
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

	t.Run("catalog oracle expectations are live", func(t *testing.T) {
		total := len(h.corpus.CheckResources.Resources)
		for _, entry := range h.corpus.Catalog.Actions {
			if h.corpus.SelectedAction != "" && h.corpus.SelectedAction != entry.Name {
				continue
			}
			t.Run(entry.Name, func(t *testing.T) {
				allowed := h.oracleAllowedIDs(t, entry.Name)
				switch entry.OracleExpectation.Kind {
				case "proper-subset":
					require.NotEmpty(t, allowed, "%s: oracle allows nothing", entry.Name)
					require.Less(t, len(allowed), total, "%s: oracle allows every resource", entry.Name)
				case "empty":
					require.Empty(t, allowed, "%s: catalog declares an empty oracle", entry.Name)
				case "total":
					require.Len(t, allowed, total, "%s: catalog declares a total oracle", entry.Name)
				default:
					t.Fatalf("%s: unknown oracle expectation %q", entry.Name, entry.OracleExpectation.Kind)
				}
			})
		}
	})
}
