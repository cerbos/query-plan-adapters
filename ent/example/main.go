// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Example application for the Cerbos query plan adapter for ent, run against the shared demo
// domain in demo/.
//
// This is NOT a test of what the adapter translates — ../adversarial_test.go proves that against a
// hostile corpus with a live PDP as the oracle, and ../translate_test.go and ../render_test.go pin
// the SQL it emits. This proves the thing every suite in ../ structurally cannot: the usage shapes.
//
// A conformance harness runs one flat filtered query, and it runs it through a hand-built
// entsql.Selector against database/sql — no generated ent client is involved anywhere in ../. So
// the line every consumer of this adapter actually writes, handing Result.Predicate to a GENERATED
// query's Where(func(*sql.Selector)) escape hatch and composing it with generated predicates of
// their own, is executed here and nowhere else in this repository.
//
// It does NOT prove packaging. Go has no packaging step: this module resolves the adapter through a
// `replace` directive, which is not what a consumer does. See ent/example/README.md and the
// Consequences section of docs/adr/0002-examples-install-the-packed-artifact.md.
//
// Prints one JSON document to stdout; everything a human might want to read goes to stderr.
// demo/scripts/run-example.sh diffs that document against demo/cases.json.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/cerbos/cerbos-sdk-go/cerbos"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/example/ent"
	"github.com/cerbos/query-plan-adapters/ent/example/ent/document"

	_ "modernc.org/sqlite"
)

const (
	adapterName  = "ent"
	resourceKind = "document"

	// The shared corpus, relative to this directory: run.sh runs the program from ent/example, and
	// a Go binary has no equivalent of Python's __file__ to resolve against instead.
	seedsPath = "../../demo/seeds.json"
	casesPath = "../../demo/cases.json"

	// A file rather than an in-memory database, so a failing run leaves the seeded rows behind to
	// inspect. It is scratch state this directory owns and .gitignore excludes, and every run deletes
	// it and creates the schema again. Nothing here turns on WAL, so there is no sidecar file to
	// delete alongside it: the rollback journal this configuration uses is transient and SQLite
	// removes it itself.
	dbPath = "demo.db"
)

// seedPrincipal is one entry of demo/seeds.json's `principals`.
//
// Read rather than restated. Every live case looks its principal up here, so an unknown or stale
// principal fails during execution instead of silently using a local copy.
type seedPrincipal struct {
	ID    string   `json:"id"`
	Roles []string `json:"roles"`
}

// seedApplicationFilter is the predicate the APPLICATION owns, declared in the corpus and never in
// policy. ANDing it with the adapter's predicate is usage shape 5.
type seedApplicationFilter struct {
	Description string `json:"description"`
	Region      string `json:"region"`
	Archived    bool   `json:"archived"`
}

// seedDocument is one row of demo/seeds.json's `documents`.
type seedDocument struct {
	ID string `json:"id"`
	//nolint:tagliatelle // The corpus spells it ownerId; Go's ID suffix is not the JSON name.
	OwnerID  string `json:"ownerId"`
	Region   string `json:"region"`
	Public   bool   `json:"public"`
	Archived bool   `json:"archived"`
}

// seeds is demo/seeds.json. Every key the file carries is named, including the prose ones, because
// it is decoded with unknown fields rejected: adding a seed field means updating every example's
// schema (demo/README.md, "Changing the demo domain"), and this is what makes that loud here rather
// than silently dropping the new column out of the rows this program loads.
type seeds struct {
	Schema            string                `json:"$schema"` //nolint:tagliatelle // JSON Schema's reserved key.
	Description       string                `json:"description"`
	Principals        []seedPrincipal       `json:"principals"`
	Documents         []seedDocument        `json:"documents"`
	ApplicationFilter seedApplicationFilter `json:"applicationFilter"`
}

func loadSeeds() (*seeds, error) {
	raw, err := os.ReadFile(seedsPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", seedsPath, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	var parsed seeds
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", seedsPath, err)
	}
	return &parsed, nil
}

type demoPagination struct {
	PageSizes []int `json:"pageSizes"`
	PageSize  int   `json:"pageSize"`
}

type demoExpected struct {
	Kind string   `json:"kind"`
	IDs  []string `json:"ids"`
}

type demoCase struct {
	ID         string          `json:"id"`
	Operation  string          `json:"operation"`
	Principal  string          `json:"principal"`
	Action     string          `json:"action"`
	Pagination *demoPagination `json:"pagination"`
	Expected   demoExpected    `json:"expected"`
}

type demoCases struct {
	Cases         []demoCase `json:"cases"`
	SchemaVersion int        `json:"schemaVersion"`
}

func loadCases() (*demoCases, error) {
	raw, err := os.ReadFile(casesPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", casesPath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var parsed demoCases
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", casesPath, err)
	}
	if parsed.SchemaVersion != 1 {
		return nil, fmt.Errorf("%s uses schemaVersion %d, want 1", casesPath, parsed.SchemaVersion)
	}
	return &parsed, nil
}

// principal looks one principal up in the corpus by id.
func (s *seeds) principal(id string) (*cerbos.Principal, error) {
	for _, candidate := range s.Principals {
		if candidate.ID == id {
			return cerbos.NewPrincipal(candidate.ID, candidate.Roles...), nil
		}
	}
	return nil, fmt.Errorf("%s declares no principal %q", seedsPath, id)
}

// mapper resolves the plan's attribute references onto columns. Resolution is fail-closed, so
// without it the adapter has nothing to resolve `request.resource.attr.ownerId` to and returns an
// error — which is itself worth seeing in an example.
//
// The columns come from the GENERATED field constants rather than string literals, so renaming a
// field in ent/schema/document.go is a compile error here instead of an unmapped-reference failure
// at run time.
//
// `region` and `archived` are deliberately absent: no rule in demo/policies/document.yaml names
// them, and they exist so the application can own a predicate the policy never sees.
var mapper = cerbosent.MapperMap{
	"request.resource.attr.ownerId": {Column: document.FieldOwnerID},
	"request.resource.attr.public":  {Column: document.FieldIsPublic},
}

// shapeResult is one entry of demo/cases.json: the plan kind alongside the ids.
//
// The kind is what stops this program returning every row for `admin-view` without ever having
// reached the PDP. pageSize and pageSizes belong to usage shape 4 alone and are omitted elsewhere,
// because the shared runner diffs the whole document.
type shapeResult struct {
	Kind      string   `json:"kind"`
	IDs       []string `json:"ids"` //nolint:tagliatelle // The corpus spells it ids.
	PageSizes []int    `json:"pageSizes,omitempty"`
	PageSize  int      `json:"pageSize,omitempty"`
}

type output struct {
	Shapes  map[string]map[string]shapeResult `json:"shapes"`
	Adapter string                            `json:"adapter"`
}

// app is the example itself: a Cerbos client, an ent client, and the shared corpus.
type app struct {
	cerbos *cerbos.GRPCClient
	ent    *ent.Client
	seeds  *seeds
	cases  *demoCases
}

// shapes executes every usage shape, keyed the way demo/cases.json keys them.
func (a *app) shapes(ctx context.Context) (map[string]map[string]shapeResult, error) {
	shapes := map[string]map[string]shapeResult{}

	for _, demoCase := range a.cases.Cases {
		if demoCase.ID != demoCase.Operation+"/"+demoCase.Principal+"/"+demoCase.Action {
			return nil, fmt.Errorf("invalid demo case id %q", demoCase.ID)
		}
		var (
			result shapeResult
			err    error
		)
		switch demoCase.Operation {
		case "paginated":
			if demoCase.Pagination == nil {
				return nil, fmt.Errorf("%s: missing pagination", demoCase.ID)
			}
			result, err = a.paginated(ctx, demoCase.Principal, demoCase.Action, demoCase.Pagination.PageSize)
		case "composed":
			result, err = a.composed(ctx, demoCase.Principal, demoCase.Action)
		case "filtered", "alwaysAllowed", "alwaysDenied":
			result, err = a.filtered(ctx, demoCase.Principal, demoCase.Action)
		default:
			// Named rather than defaulted, so a mistyped shape fails here and says so. Falling
			// through to `filtered` would fail the shared diff instead, as a missing entry.
			return nil, fmt.Errorf("no exercise for operation %q", demoCase.Operation)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", demoCase.ID, err)
		}

		if shapes[demoCase.Operation] == nil {
			shapes[demoCase.Operation] = map[string]shapeResult{}
		}
		shapes[demoCase.Operation][demoCase.Principal+"/"+demoCase.Action] = result
	}

	return shapes, nil
}

// -- the five usage shapes ----------------------------------------------------------------------

// filtered is usage shapes 1, 2 and 3: a plain filtered list, where the adapter's predicate is the
// whole query. All three plan kinds come through here, and the caller does not choose which — the
// policy does.
func (a *app) filtered(ctx context.Context, principalID, action string) (shapeResult, error) {
	result, kind, err := a.translate(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}

	ids, err := a.idsUnderPlan(ctx, a.ent.Document.Query(), result)
	if err != nil {
		return shapeResult{}, err
	}
	return shapeResult{Kind: kind, IDs: ids}, nil
}

// paginated is usage shape 4: ent's own Limit/Offset applied on top of the adapter's predicate.
//
// Reported as page SIZES plus the sorted union of the ids, never as per-page order:
// demo/cases.json is shared by every example and several of the stores have no total order to
// paginate by. The Order is still required for the paging itself to be correct — without a total
// order, LIMIT/OFFSET may repeat or omit rows between pages — which is a separate concern from how
// the result is asserted.
func (a *app) paginated(ctx context.Context, principalID, action string, pageSize int) (shapeResult, error) {
	result, kind, err := a.translate(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}

	query, accessible, err := applyPlan(a.ent.Document.Query(), result)
	if err != nil {
		return shapeResult{}, err
	}

	pageSizes := []int{}
	ids := []string{}
	if accessible {
		for offset := 0; ; offset += pageSize {
			// Clone per page. Limit, Offset and Order mutate the query they are called on and
			// return it, so paging off one builder would accumulate a duplicate Order term every
			// iteration.
			page, err := query.Clone().
				Order(document.ByID()).
				Limit(pageSize).
				Offset(offset).
				IDs(ctx)
			if err != nil {
				return shapeResult{}, fmt.Errorf("reading page at offset %d: %w", offset, err)
			}
			if len(page) == 0 {
				break
			}

			pageSizes = append(pageSizes, len(page))
			ids = append(ids, page...)
			if len(page) < pageSize {
				break
			}
		}
	}

	slices.Sort(ids)
	return shapeResult{Kind: kind, PageSize: pageSize, PageSizes: pageSizes, IDs: ids}, nil
}

// composed is usage shape 5: the adapter's predicate ANDed with the application's own.
//
// The application's half is written in the GENERATED ent predicates a consumer would reach for,
// against the two columns no rule in demo/policies/document.yaml names, and it is applied BEFORE
// the plan is: an application composing its own filters does not know, and must not have to know,
// which plan kind the PDP is about to return.
//
// All three plan kinds go through here on purpose. KindAlwaysAllowed leaves nothing to AND with,
// and KindAlwaysDenied must not have its denial undone — see the comment on idsUnderPlan.
func (a *app) composed(ctx context.Context, principalID, action string) (shapeResult, error) {
	result, kind, err := a.translate(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}

	query := a.ent.Document.Query().Where(
		document.ArchivedEQ(a.seeds.ApplicationFilter.Archived),
		document.RegionEQ(a.seeds.ApplicationFilter.Region),
	)

	ids, err := a.idsUnderPlan(ctx, query, result)
	if err != nil {
		return shapeResult{}, err
	}
	return shapeResult{Kind: kind, IDs: ids}, nil
}

// -- plumbing -----------------------------------------------------------------------------------

// translate plans one action for one principal and lowers the plan into an ent predicate.
func (a *app) translate(ctx context.Context, principalID, action string) (cerbosent.Result, string, error) {
	principal, err := a.seeds.principal(principalID)
	if err != nil {
		return cerbosent.Result{}, "", err
	}

	plan, err := a.cerbos.PlanResources(ctx, principal, cerbos.NewResource(resourceKind, ""), action)
	if err != nil {
		return cerbosent.Result{}, "", fmt.Errorf("planning %s: %w", action, err)
	}

	kind := plan.PlanResourcesResponse.GetFilter().GetKind()
	if kind == enginev1.PlanResourcesFilter_KIND_UNSPECIFIED {
		return cerbosent.Result{}, "", errors.New("the PDP returned a plan with no kind — is $CERBOS_HOST a Cerbos PDP?")
	}

	// document.Table is the generated table name, so the predicate qualifies its columns with the
	// same table the query below reads. WithDialect must match the ent client's dialect: cast
	// spellings and timestamp storage differ between them.
	result, err := cerbosent.Translate(plan.PlanResourcesResponse, document.Table, mapper,
		cerbosent.WithDialect(dialect.SQLite))
	if err != nil {
		return cerbosent.Result{}, "", fmt.Errorf("translating the plan for %s: %w", action, err)
	}

	return result, kind.String(), nil
}

// applyPlan hands a translated plan to a generated ent query — the one line every consumer of this
// adapter writes, and the line no suite in ../ reaches.
//
// The bool is "any row may be accessible". A KindAlwaysDenied plan carries no predicate at all,
// because the adapter's contract for it is that the caller skips the query entirely.
func applyPlan(query *ent.DocumentQuery, result cerbosent.Result) (*ent.DocumentQuery, bool, error) {
	switch result.Kind {
	case cerbosent.KindAlwaysDenied:
		return nil, false, nil
	case cerbosent.KindAlwaysAllowed:
		return query, true, nil
	case cerbosent.KindConditional:
		return query.Where(func(s *entsql.Selector) { s.Where(result.Predicate) }), true, nil
	default:
		return nil, false, fmt.Errorf("the adapter returned an unrecognised plan kind %d", result.Kind)
	}
}

// idsUnderPlan applies a translated plan to a query, runs it, and returns the ids it selects,
// sorted.
//
// Sorted because demo/cases.json is: a SELECT with no ORDER BY has no defined row order, and
// SQLite returning insertion order is an implementation detail rather than a promise.
//
// A denied plan returns the empty set without executing anything, and that is the adapter's API
// rather than a shortcut this program takes: unlike an ORM whose adapter hands back a `WHERE false`
// query, there is no predicate to compose the application's own filter with. So in usage shape 5
// the application predicate cannot resurrect a denied row — it has nothing to widen.
func (a *app) idsUnderPlan(ctx context.Context, query *ent.DocumentQuery, result cerbosent.Result) ([]string, error) {
	query, accessible, err := applyPlan(query, result)
	if err != nil {
		return nil, err
	}
	if !accessible {
		return []string{}, nil
	}

	ids, err := query.IDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("executing the query: %w", err)
	}
	if ids == nil {
		ids = []string{}
	}
	slices.Sort(ids)
	return ids, nil
}

// seed creates the schema and loads the corpus rows, mapping each corpus attribute name onto the
// field the schema calls it.
func (a *app) seed(ctx context.Context) error {
	if err := a.ent.Schema.Create(ctx); err != nil {
		return fmt.Errorf("creating the schema: %w", err)
	}

	builders := make([]*ent.DocumentCreate, 0, len(a.seeds.Documents))
	for _, row := range a.seeds.Documents {
		builders = append(builders, a.ent.Document.Create().
			SetID(row.ID).
			SetOwnerID(row.OwnerID).
			SetIsPublic(row.Public).
			SetRegion(row.Region).
			SetArchived(row.Archived))
	}

	if err := a.ent.Document.CreateBulk(builders...).Exec(ctx); err != nil {
		return fmt.Errorf("seeding %d documents: %w", len(builders), err)
	}
	fmt.Fprintf(os.Stderr, "seeded %d documents\n", len(builders))
	return nil
}

// cerbosHost is the address the shared runner published the demo PDP on.
//
// There is deliberately no fallback. The obvious default — Cerbos's own 3592/3593 — is where every
// adapter's `cerbos run` test sidecar listens, so an unset CERBOS_HOST would not fail: it would
// quietly plan against whatever policies that sidecar serves. The live runner injects a
// non-default CERBOS_HOST, so successful case execution proves this value is actually used.
func cerbosHost() (string, error) {
	host := os.Getenv("CERBOS_HOST")
	if host == "" {
		return "", errors.New("CERBOS_HOST is not set — run this example through demo/scripts/run-example.sh " + adapterName)
	}
	return host, nil
}

func openStore() (*ent.Client, func() error, error) {
	if err := os.Remove(dbPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, fmt.Errorf("removing %s: %w", dbPath, err)
	}

	// modernc.org/sqlite registers itself as "sqlite", so the driver is opened here and handed to
	// ent rather than letting ent.Open look up its own default ("sqlite3", which is the cgo one).
	//
	// ent's migration refuses to run on SQLite with foreign keys off, and its error names mattn's
	// `_fk=1`; this driver spells the same thing `_pragma=foreign_keys(1)`.
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=foreign_keys(1)")
	if err != nil {
		return nil, nil, fmt.Errorf("opening %s: %w", dbPath, err)
	}

	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.SQLite, db)))
	return client, client.Close, nil
}

func run() error {
	host, err := cerbosHost()
	if err != nil {
		return err
	}

	corpus, err := loadSeeds()
	if err != nil {
		return err
	}
	cases, err := loadCases()
	if err != nil {
		return err
	}

	entClient, closeStore, err := openStore()
	if err != nil {
		return err
	}
	defer func() {
		if err := closeStore(); err != nil {
			fmt.Fprintf(os.Stderr, "closing the store: %v\n", err)
		}
	}()

	cerbosClient, err := cerbos.New(host, cerbos.WithPlaintext())
	if err != nil {
		return fmt.Errorf("connecting to the PDP at %s: %w", host, err)
	}

	application := &app{cerbos: cerbosClient, ent: entClient, seeds: corpus, cases: cases}

	ctx := context.Background()
	if err := application.seed(ctx); err != nil {
		return err
	}

	shapes, err := application.shapes(ctx)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output{Adapter: adapterName, Shapes: shapes})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s example: %v\n", adapterName, err)
		os.Exit(1)
	}
}
