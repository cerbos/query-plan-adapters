// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	cerbospgx "github.com/cerbos/query-plan-adapters/pgx"
)

// Conformance suite (../conformance/README.md, "The harness contract").
//
// The corpus dataset is seeded into a real PostgreSQL. Then, for each pinned PDP and each golden
// file, the recorded plan is translated by this adapter and executed, and the returned ids are
// compared with the check() decisions recorded next to it. conformance-ledger.json lists the cases
// this adapter cannot pass. No PDP is started: its answers are recorded. This file owns only the
// PostgreSQL half — the schema, the mapping and the seeding; corpus_test.go reads the corpus.

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

// mapper wires the corpus's attribute references onto the schema above.
//
// `owner` and `tagNames` deliberately alias columns that `aOptionalString` and `tags[].name`
// already cover: the corpus sends those two as EXPLICIT nulls while the originals are omitted when
// NULL, and CEL membership distinguishes null from missing. Mapping both is what lets a single
// schema exercise both conventions.
func buildMapper() cerbospgx.Mapper {
	tagFields := map[string]cerbospgx.Entry{
		"id": {Column: "tag_id"},
		// Declared, like every other string column: `t.name == 0` is false in CEL, and an
		// undeclared column hands the number to PostgreSQL, which has no text = double precision
		// and fails to execute.
		"name": {Column: "name", ValueType: cerbospgx.ValueString},
	}
	tags := &cerbospgx.Relation{
		Table:        tagTable,
		SourceColumn: "id", TargetColumn: "resource_id",
		Field:  &cerbospgx.Entry{Column: "name", ValueType: cerbospgx.ValueString},
		Fields: tagFields,
	}

	tagNames := *tags
	tagNames.Field = &cerbospgx.Entry{Column: "name", ValueType: cerbospgx.ValueString, NullConvention: cerbospgx.NullConventionExplicit}

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
		// `identifier/*` cases). An adapter that resolves references by stripping a
		// `request.resource.attr.` prefix never sees this name.
		"request.resource.id": {Column: "id"},
		// Declared boolean so `string()` over it spells CEL's "true"/"false" through a CASE
		// rather than a CAST: SQLite and MySQL store a boolean as 1/0 and render "1" where CEL
		// and PostgreSQL render "true", and nothing in the plan names a column's type.
		"request.resource.attr.aBool": {Column: "a_bool", ValueType: cerbospgx.ValueBool},
		// Declared string so CEL's `+` between two columns resolves to concatenation:
		// the operator is overloaded and the plan carries no operand types, so an
		// undeclared pair fails closed rather than emitting a numeric `+`.
		"request.resource.attr.aString":         {Column: "a_string", ValueType: cerbospgx.ValueString},
		"request.resource.attr.aNumber":         {Column: "a_number", ValueType: cerbospgx.ValueNumber},
		"request.resource.attr.aDouble":         {Column: "a_double", ValueType: cerbospgx.ValueNumber},
		"request.resource.attr.aOptionalString": {Column: "a_optional_string", ValueType: cerbospgx.ValueString, NullConvention: cerbospgx.NullConventionOmitted},
		"request.resource.attr.createdBy":       {Column: "created_by"},
		// `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under
		// the OTHER null convention: the corpus sends a real null attribute for them rather than
		// omitting it. Declaring that here is what makes the equality family definite for these
		// two attributes and leaves it untouched for every other mapping.
		"request.resource.attr.owner":     {Column: "a_optional_string", NullConvention: cerbospgx.NullConventionExplicit},
		"request.resource.attr.coOwner":   {Column: "scope", NullConvention: cerbospgx.NullConventionExplicit},
		"request.resource.attr.scope":     {Column: "scope"},
		"request.resource.attr.createdAt": {Column: "created_at", ValueType: cerbospgx.ValueTimestamp},
		"request.resource.attr.updatedAt": {Column: "updated_at", ValueType: cerbospgx.ValueTimestamp},
		// obj.inner is not a real nested column — it mirrors aString, the same trick the
		// other harnesses use for the `obj.inner` cases.
		"request.resource.attr.obj.inner": {Column: "a_string"},

		"request.resource.attr.tags":     {Relation: tags},
		"request.resource.attr.tagNames": {Relation: &tagNames},

		"request.resource.attr.categories": {Relation: categories},

		// The two homogeneous scalar lists, one element per row of a related table, the way
		// tagNames is stored. Declaring the element's type is what lets a literal of another
		// type (`"2" in aNumberList`) be answered false as CEL answers it, rather than handed to
		// PostgreSQL, which reads an untyped '2' as the column's type and matches it.
		"request.resource.attr.aNumberList": {Relation: elementList(numberElemTable, cerbospgx.Entry{ValueType: cerbospgx.ValueNumber})},
		"request.resource.attr.aBoolList":   {Relation: elementList(boolElemTable, cerbospgx.Entry{ValueType: cerbospgx.ValueBool})},

		"request.resource.attr.mainCategory.subCategories": {Relation: mainSub},
		"request.resource.attr.mainCategory.subNames":      {Relation: mainSub},

		// The corpus's one REAL to-one chain (the `relation/*` cases). `ScalarRelation` reads one
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

// -- the store ----------------------------------------------------------------------------------

func startPostgres(t *testing.T, corpus *Corpus) *pgxpool.Pool {
	t.Helper()
	ctx := t.Context()

	container, err := postgres.Run(ctx,
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
	testcontainers.CleanupContainer(t, container)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, schemaDDL)
	require.NoError(t, err, "creating schema")
	seedDatabase(t, ctx, pool, corpus)
	return pool
}

// elementList maps a scalar list stored one element per row of table. element.ValueType declares
// the element type; the column and the explicit-null convention are the same for both lists.
func elementList(table string, element cerbospgx.Entry) *cerbospgx.Relation {
	element.Column = "value"
	element.NullConvention = cerbospgx.NullConventionExplicit
	return &cerbospgx.Relation{
		Table:        table,
		SourceColumn: "id", TargetColumn: "resource_id",
		Field: &element,
	}
}

func seedDatabase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, corpus *Corpus) {
	t.Helper()

	for _, seed := range corpus.Seeds {
		var created *time.Time
		if raw := corpus.derived(seed).CreatedAt; raw != nil {
			parsed, err := time.Parse(time.RFC3339Nano, *raw)
			require.NoError(t, err, "parsing derived createdAt for %s", seed.ID)
			created = &parsed
		}

		var updated *time.Time
		if raw := corpus.derived(seed).UpdatedAt; raw != nil {
			parsed, err := time.Parse(time.RFC3339Nano, *raw)
			require.NoError(t, err, "parsing derived updatedAt for %s", seed.ID)
			updated = &parsed
		}

		_, err := pool.Exec(ctx, `
			INSERT INTO adversarial_resource
				(id, a_bool, a_string, a_number, a_double, a_optional_string, created_by, scope, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			seed.ID, seed.ABool, seed.AString, seed.ANumber, corpus.derived(seed).ADouble,
			seed.AOptionalString, corpus.derived(seed).CreatedBy, corpus.derived(seed).Scope, created, updated)
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

		// A relation has no row order, which is why `index` over these lists stays refused;
		// membership and hasIntersection are position-blind and need none.
		for _, element := range seed.ANumberList {
			_, err := pool.Exec(ctx,
				`INSERT INTO adversarial_number_elem (value, resource_id) VALUES ($1,$2)`, element, seed.ID)
			require.NoError(t, err, "seeding aNumberList for %s", seed.ID)
		}
		for _, element := range seed.ABoolList {
			_, err := pool.Exec(ctx,
				`INSERT INTO adversarial_bool_elem (value, resource_id) VALUES ($1,$2)`, element, seed.ID)
			require.NoError(t, err, "seeding aBoolList for %s", seed.ID)
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

			for j, label := range corpus.derived(seed).Labels {
				_, err := pool.Exec(ctx,
					`INSERT INTO adversarial_label (id, name, sub_category_id) VALUES ($1,$2,$3)`,
					fmt.Sprintf("%s-label%d", subID, j), label, subID)
				require.NoError(t, err, "seeding label for %s", subID)
			}
		}
	}
}

// filteredIDs translates plan and executes it, returning the ids the filter selects, sorted. A
// translation error is returned as is, so the caller can tell a refusal from anything else.
func filteredIDs(ctx context.Context, pool *pgxpool.Pool, mapper cerbospgx.Mapper, golden Golden) ([]string, error) {
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
	recording := cerbospgx.MapperFunc(func(reference string) (cerbospgx.Entry, bool) {
		entry, ok := mapper.Resolve(reference)
		if !ok && strings.HasPrefix(reference, "request.") {
			unmapped = append(unmapped, reference)
		}
		return entry, ok
	})

	result, err := cerbospgx.Translate(plan, resourceTable, recording)
	if len(unmapped) > 0 {
		return nil, fmt.Errorf("the harness mapping has no entry for %s (a harness defect, not a refusal)", strings.Join(unmapped, ", "))
	}
	if err != nil {
		return nil, err
	}

	query := `SELECT id FROM ` + resourceTable
	switch result.Kind {
	case cerbospgx.KindAlwaysDenied:
		return []string{}, nil
	case cerbospgx.KindAlwaysAllowed:
	case cerbospgx.KindConditional:
		query += " WHERE " + result.Where
	}

	rows, err := pool.Query(ctx, query, result.Args...)
	if err != nil {
		return nil, fmt.Errorf("executing the translated filter: %w\nSQL: %s", err, query)
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
	pool := startPostgres(t, corpus)
	mapper := buildMapper()

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
					ids, err := filteredIDs(t.Context(), pool, mapper, golden)
					switch {
					case !listed:
						require.NoError(t, err)
						require.Equal(t, sortedCopy(golden.Allowed), ids)
					case entry.Status == "unsupported":
						require.ErrorIs(t, err, cerbospgx.ErrUnsupported,
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
				t.Logf("PDP %s, %s: %d / %d passed", tag, tier, passed[tier], total[tier])
			}
		})
	}
}

func sortedCopy(ids []string) []string {
	out := append([]string{}, ids...)
	slices.Sort(out)
	return out
}
