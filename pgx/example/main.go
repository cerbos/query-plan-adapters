// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Example application for the Cerbos query plan adapter for pgx, run against the shared demo
// domain in demo/.
//
// This is NOT a test of what the adapter translates — ../adversarial_test.go proves that against a
// hostile corpus with a live PDP as the oracle, and ../translate_test.go and ../render_test.go pin
// the SQL it emits. This proves the thing every suite in ../ structurally cannot: the usage shapes.
//
// This adapter hands back SQL TEXT and a slice of arguments rather than an ORM object, so the usage
// shape that matters most here is composition — the application has to splice the fragment into a
// statement of its own without breaking PostgreSQL's ORDINAL parameter numbering. `$1` means "the
// first argument I send", not "the first argument in this fragment", so a fragment that numbers from
// $1 while the surrounding statement already binds two arguments is wrong in a way no type checker
// sees. Both directions are exercised here:
//
//   - usage shape 5 puts the application's predicate FIRST and shifts the fragment behind it with
//     WithPlaceholderOffset;
//   - usage shape 4 puts the fragment first and numbers the application's own LIMIT/OFFSET
//     parameters after it, which the adapter cannot help with — the application has to count
//     Result.Args.
//
// Every statement a usage shape builds goes through checkPlaceholders before it is executed, so a
// misnumbered composition fails by name instead of returning plausible rows. The two negative controls
// — assertOffsetIsLoadBearing and assertDenialCannotBeSpliced — go straight to the database instead,
// which is the point of them: the first executes a statement checkPlaceholders has just been required
// to REJECT, and the second is about syntax rather than numbering. Nothing else bypasses the check.
//
// It does NOT prove packaging. Go has no packaging step: this module resolves the adapter through a
// `replace` directive, which is not what a consumer does. See pgx/example/README.md and the
// Consequences section of docs/adr/0002-examples-install-the-packed-artifact.md.
//
// Prints one JSON document to stdout; everything a human might want to read goes to stderr.
// demo/scripts/run-example.sh diffs that document against demo/cases.json.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/cerbos/cerbos-sdk-go/cerbos"
	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cerbospgx "github.com/cerbos/query-plan-adapters/pgx"
)

const (
	adapterName  = "pgx"
	resourceKind = "document"

	// The shared corpus, relative to this directory: run.sh runs the program from pgx/example, and
	// a Go binary has no equivalent of Python's __file__ to resolve against instead.
	seedsPath = "../../demo/seeds.json"
	casesPath = "../../demo/cases.json"

	// The store run.sh starts, on 15432 rather than PostgreSQL's default 5432: the adapter's own
	// adversarial suite starts PostgreSQL containers of its own, and a demo server holding the
	// default port would leave one of the two reading the other's rows. run.sh publishes the other
	// half of this number.
	//
	// Hardcoded rather than read from the environment because an example takes no arguments and
	// reads nothing but $CERBOS_HOST (demo/README.md). The credentials are this throwaway
	// container's, and run.sh is where they are set.
	//
	//nolint:gosec // G101: the credentials of a throwaway container this repository starts itself.
	dsn = "postgres://cerbos_demo:cerbos_demo@127.0.0.1:15432/cerbos_demo?sslmode=disable"
)

// The application's own table. This program owns the DDL, so unlike the ent example there are no
// generated constants to name the columns with — the mapper and the application's own predicate use
// the constants below, and PostgreSQL is the thing that checks them: a column name that does not
// match this DDL is `column "…" does not exist` at query time rather than a silently wrong answer.
//
// `owner_id` and `is_public` are named unlike the Cerbos attributes they carry on purpose. A Cerbos
// attribute name is not a column name, which is what makes the mapper below necessary rather than
// decorative.
//
// DROP first so a re-run against a container left behind by an interrupted run starts from the same
// rows as a run against a fresh one.
const schemaDDL = `
DROP TABLE IF EXISTS document;
CREATE TABLE document (
	id         text    PRIMARY KEY,
	owner_id   text    NOT NULL,
	is_public  boolean NOT NULL,
	region     text    NOT NULL,
	archived   boolean NOT NULL
)`

const (
	table = "document"

	colID       = "id"
	colOwnerID  = "owner_id"
	colIsPublic = "is_public"
	colRegion   = "region"
	colArchived = "archived"

	// The SELECT every usage shape starts from; each appends its own clauses. Named so that the
	// interesting half of each statement — which is all of them, in this example — is what a reader
	// sees at the call site.
	idSelect = "SELECT " + colID + " FROM " + table
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
// policy. ANDing it with the adapter's fragment is usage shape 5.
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
// `region` and `archived` are deliberately absent: no rule in demo/policies/document.yaml names
// them, and they exist so the application can own a predicate the policy never sees.
var mapper = cerbospgx.MapperMap{
	"request.resource.attr.ownerId": {Column: colOwnerID},
	"request.resource.attr.public":  {Column: colIsPublic},
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

// app is the example itself: a Cerbos client, a connection pool, and the shared corpus.
type app struct {
	cerbos *cerbos.GRPCClient
	pool   *pgxpool.Pool
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

// filtered is usage shapes 1, 2 and 3: a plain filtered list, where the adapter's fragment is the
// whole `WHERE` clause. All three plan kinds come through here, and the caller does not choose
// which — the policy does.
func (a *app) filtered(ctx context.Context, principalID, action string) (shapeResult, error) {
	result, kind, err := a.translate(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}

	where, accessible, err := clause(result)
	if err != nil {
		return shapeResult{}, err
	}
	if !accessible {
		return shapeResult{Kind: kind, IDs: []string{}}, nil
	}

	// A conditional plan contributes a `WHERE`; an unconditional allow contributes nothing and the
	// statement is left unfiltered. `Where` carries no WHERE keyword of its own precisely so the
	// caller decides where it goes.
	stmt := idSelect
	if where != "" {
		stmt += " WHERE " + where
	}

	ids, err := a.selectIDs(ctx, stmt, result.Args)
	if err != nil {
		return shapeResult{}, err
	}
	return shapeResult{Kind: kind, IDs: ids}, nil
}

// paginated is usage shape 4: LIMIT and OFFSET applied on top of the adapter's fragment.
//
// This is the composition hazard in the direction the adapter cannot help with. The fragment goes
// first, so it keeps its own $1.. numbering, and the application's two pagination parameters have to
// be numbered AFTER it — which means counting Result.Args. There is no option for this side; get it
// wrong and the statement either fails to bind or pages by the wrong values.
//
// Reported as page SIZES plus the sorted union of the ids, never as per-page order:
// demo/cases.json is shared by every example and several of the stores have no total order to
// paginate by. The ORDER BY is still required for the paging itself to be correct — without a total
// order, LIMIT/OFFSET may repeat or omit rows between pages — which is a separate concern from how
// the result is asserted.
func (a *app) paginated(ctx context.Context, principalID, action string, pageSize int) (shapeResult, error) {
	result, kind, err := a.translate(ctx, principalID, action)
	if err != nil {
		return shapeResult{}, err
	}

	where, accessible, err := clause(result)
	if err != nil {
		return shapeResult{}, err
	}

	pageSizes := []int{}
	ids := []string{}
	if accessible {
		stmt := idSelect
		if where != "" {
			stmt += " WHERE " + where
		}
		// The application's parameters start one past the adapter's, whatever the plan bound.
		limitPlaceholder := len(result.Args) + 1
		offsetPlaceholder := limitPlaceholder + 1
		stmt += fmt.Sprintf(" ORDER BY %s LIMIT $%d OFFSET $%d", colID, limitPlaceholder, offsetPlaceholder)

		for offset := 0; ; offset += pageSize {
			args := append(slices.Clone(result.Args), pageSize, offset)
			page, err := a.selectIDs(ctx, stmt, args)
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

// composed is usage shape 5: the adapter's fragment ANDed with the application's own predicate.
//
// The application's half goes FIRST, over the two columns no rule in demo/policies/document.yaml
// names, and it is built BEFORE the plan is translated — because that is the honest order: an
// application composing its own filters does not know, and must not have to know, which plan kind
// the PDP is about to return. Putting it first is also what makes this shape sharp for this adapter:
// the application's parameters take $1 and $2, so the fragment MUST be numbered from $3, which is
// what WithPlaceholderOffset does. The offset renumbers the placeholders; keeping Result.Args in the
// matching position of the argument slice is still the caller's job.
//
// All three plan kinds go through here on purpose. KindAlwaysAllowed leaves nothing to AND with, and
// KindAlwaysDenied must not have its denial undone — see assertDenialCannotBeSpliced.
func (a *app) composed(ctx context.Context, principalID, action string) (shapeResult, error) {
	appWhere, appArgs := a.seeds.applicationPredicate()

	result, kind, err := a.translate(ctx, principalID, action, cerbospgx.WithPlaceholderOffset(len(appArgs)))
	if err != nil {
		return shapeResult{}, err
	}

	where, accessible, err := clause(result)
	if err != nil {
		return shapeResult{}, err
	}
	if !accessible {
		// No fragment to compose with, so no query runs. The application predicate cannot widen a
		// denial because there is nothing here for it to widen — and splicing the empty fragment in
		// anyway is refused by PostgreSQL rather than quietly returning the application's own rows.
		if err := a.assertDenialCannotBeSpliced(ctx, appWhere, appArgs, result); err != nil {
			return shapeResult{}, err
		}
		return shapeResult{Kind: kind, IDs: []string{}}, nil
	}

	stmt := idSelect + " WHERE " + appWhere
	args := appArgs
	if where != "" {
		stmt += " AND (" + where + ")"
		args = append(slices.Clone(appArgs), result.Args...)
	}

	ids, err := a.selectIDs(ctx, stmt, args)
	if err != nil {
		return shapeResult{}, err
	}

	if where != "" {
		if err := a.assertOffsetIsLoadBearing(ctx, principalID, action, appWhere, appArgs, ids); err != nil {
			return shapeResult{}, err
		}
	}

	return shapeResult{Kind: kind, IDs: ids}, nil
}

// applicationPredicate is the filter the APPLICATION owns: `applicationFilter` in demo/seeds.json,
// read from the corpus rather than restated, and never expressed in policy.
//
// Plain `$1`/`$2` with no cast suffixes, which is what an application's own query builder writes —
// PostgreSQL infers a parameter's type from the column it is compared with. The adapter's own
// parameters carry explicit casts because a plan operand can land somewhere with nothing to infer
// from; ../render.go's pgTypeSuffix says why. The two halves of the composed statement are
// visibly different in that respect, which is worth seeing side by side.
func (s *seeds) applicationPredicate() (string, []any) {
	where := fmt.Sprintf("(%s = $1 AND %s = $2)", colArchived, colRegion)
	return where, []any{s.ApplicationFilter.Archived, s.ApplicationFilter.Region}
}

// clause reduces a translated plan to the boolean expression the statement should filter on, plus
// whether any row is accessible at all. An empty expression with `accessible` true is an
// unconditional allow: there is no filter to apply.
//
// The three usage-shape functions go through here rather than testing Result.Kind themselves, and it
// is a switch with a default for one reason: an unrecognised kind must not fall through to "no
// expression", because that is an unconditional ALLOW. Failing closed on a kind this program does not
// know is the only safe direction for an authorization filter, and it is the shape the adapter's own
// README and the ent example both take.
func clause(result cerbospgx.Result) (string, bool, error) {
	switch result.Kind {
	case cerbospgx.KindAlwaysDenied:
		return "", false, nil
	case cerbospgx.KindAlwaysAllowed:
		return "", true, nil
	case cerbospgx.KindConditional:
		return result.Where, true, nil
	default:
		return "", false, fmt.Errorf("the adapter returned an unrecognised plan kind %d", result.Kind)
	}
}

// -- the composition guards ---------------------------------------------------------------------

// placeholderPattern matches a PostgreSQL positional parameter.
//
// Scanning finished SQL is safe HERE, and only here: the adapter binds every plan value as a
// parameter rather than interpolating it (../render.go), and this program's own fragments are
// constants, so no `$n` in these statements can be part of a string literal. That is also why the
// adapter numbers placeholders while writing them rather than rewriting them afterwards.
var placeholderPattern = regexp.MustCompile(`\$(\d+)`)

// checkPlaceholders asserts that a statement references exactly $1..$n for the n arguments it will
// be sent, with no gaps and no duplicates.
//
// A gap is as wrong as a duplicate. `$1, $2, $4` with three arguments means the fragment was shifted
// too far and every value after the gap is bound to the wrong placeholder.
//
// What this buys, honestly stated: PostgreSQL is not defenceless here. A misnumbered composition
// changes how many distinct placeholders the statement has, and pgx refuses to bind an argument
// count that disagrees with it — `expected 2 arguments, got 3` — while the server refuses a value
// whose type does not fit the placeholder's inferred type. So the mistake is loud with or without
// this function. What it adds is a failure that names the composition, before a round trip, instead
// of a driver message about argument counts that reads as a bug in the application's own SQL.
//
// The class NEITHER catches is arguments in the right number and the wrong ORDER: the offset
// renumbers the fragment's placeholders, and nothing checks that Result.Args ended up at the
// position the offset promised. Two same-typed parameters swapped is a silently wrong answer, and it
// is a caller-side mistake no adapter option can see. The demo domain cannot construct one — the
// application predicate binds a boolean and a text where the plan binds a text, so every permutation
// is a type error — which is why this is documented rather than asserted.
func checkPlaceholders(stmt string, args []any) error {
	seen := map[int]bool{}
	highest := 0
	for _, match := range placeholderPattern.FindAllStringSubmatch(stmt, -1) {
		n, err := strconv.Atoi(match[1])
		if err != nil {
			return fmt.Errorf("unreadable placeholder %q in %s", match[0], stmt)
		}
		seen[n] = true
		highest = max(highest, n)
	}

	switch {
	case highest == 0 && len(args) > 0:
		return fmt.Errorf(
			"statement references no placeholders but binds %d argument(s): %s", len(args), stmt)
	case highest != len(args):
		return fmt.Errorf(
			"statement references $1..$%d but binds %d argument(s) — a composed fragment is misnumbered: %s",
			highest, len(args), stmt)
	}
	for n := 1; n <= len(args); n++ {
		if !seen[n] {
			return fmt.Errorf(
				"statement binds %d argument(s) but never references $%d — a composed fragment is misnumbered: %s",
				len(args), n, stmt)
		}
	}
	return nil
}

// assertOffsetIsLoadBearing rebuilds one composed statement the way a consumer who forgot
// WithPlaceholderOffset would — fragment numbered from $1, arguments appended after the
// application's — and requires that it does not answer the same question.
//
// Without this the offset could be wrong, or unnecessary, and every shape would still pass: the
// composed ids would come back correct because nothing had proved the numbering mattered. So this is
// the negative control for checkPlaceholders, and it asserts two things. First, that the check
// REPORTS the misnumbering, rather than being a scan that happens to pass everything. Second, that
// executing it really does give a different answer — either PostgreSQL refuses it, or it returns
// some other set of rows.
//
// In this domain it is always the first: the application predicate binds two arguments and the plan
// binds one, so dropping the offset leaves three arguments for a statement with two placeholders and
// pgx will not send it. Which outcome happens is a property of the shapes involved rather than a
// promise — see checkPlaceholders for the case nothing catches — so this reports what it observed
// rather than requiring a particular failure.
func (a *app) assertOffsetIsLoadBearing(
	ctx context.Context, principalID, action, appWhere string, appArgs []any, correct []string,
) error {
	misnumbered, _, err := a.translate(ctx, principalID, action)
	if err != nil {
		return err
	}

	// Through clause() like every other read of a Result, and required to be conditional: this is the
	// same plan re-planned, so anything else means the two calls disagreed and there is no
	// misnumbering left to demonstrate.
	where, accessible, err := clause(misnumbered)
	if err != nil {
		return err
	}
	if !accessible || where == "" {
		return fmt.Errorf(
			"re-planning %s/%s for the negative control gave plan kind %d rather than a conditional one",
			principalID, action, misnumbered.Kind)
	}

	stmt := idSelect + " WHERE " + appWhere + " AND (" + where + ")"
	args := append(slices.Clone(appArgs), misnumbered.Args...)

	if err := checkPlaceholders(stmt, args); err == nil {
		return fmt.Errorf(
			"composing without WithPlaceholderOffset produced a statement checkPlaceholders accepts, "+
				"so that check is guarding nothing: %s", stmt)
	}

	// Deliberately past checkPlaceholders and straight to query: the statement it just rejected is the
	// thing being executed. Sorted before the comparison because `correct` came back sorted, and the
	// same id SET in another order is exactly the case this guard exists to rule out — comparing
	// unsorted against sorted would call it a difference and pass.
	ids, err := a.query(ctx, stmt, args)
	slices.Sort(ids)
	switch {
	case err != nil:
		fmt.Fprintf(os.Stderr, "    omitting WithPlaceholderOffset: rejected before any rows were read (%v)\n", err)
		return nil
	case slices.Equal(ids, correct):
		return fmt.Errorf(
			"composing without WithPlaceholderOffset returned the same rows as composing with it, "+
				"so usage shape 5 is not exercising the numbering at all: %s", stmt)
	default:
		fmt.Fprintf(os.Stderr, "    omitting WithPlaceholderOffset: silently returned [%s] instead of [%s]\n",
			joinIDs(ids), joinIDs(correct))
		return nil
	}
}

// assertDenialCannotBeSpliced pins what happens to a caller who ignores Kind on a denied plan.
//
// Translate returns Where and Args for KindConditional only; for a denial there is no fragment at
// all, and the caller's contract is to run no query. That is the same structural situation as the
// ent adapter — there is nothing for the application's predicate to be ANDed with, so it cannot
// resurrect a denied row — and it is worth pinning rather than asserting, because THIS adapter hands
// back a string and a string can be concatenated whatever it contains. So the question "what does
// the mistake do" has an answer here that it does not have for an ORM object, and the answer is that
// the empty fragment makes the statement unparseable: PostgreSQL refuses it, no rows come back, and
// the denial survives the mistake. An adapter that rendered `TRUE` for a denial would instead answer
// with the application's own rows, which is why this is checked and not assumed.
func (a *app) assertDenialCannotBeSpliced(
	ctx context.Context, appWhere string, appArgs []any, result cerbospgx.Result,
) error {
	if result.Where != "" || len(result.Args) != 0 {
		return fmt.Errorf(
			"a denied plan carried a filter (%q, %d argument(s)) — this adapter's contract is that it carries none",
			result.Where, len(result.Args))
	}

	// Straight to query, past checkPlaceholders: the numbering here is fine and the SYNTAX is what is
	// being tested, so there is nothing for that check to say.
	stmt := idSelect + " WHERE " + appWhere + " AND " + result.Where
	ids, err := a.query(ctx, stmt, appArgs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "    splicing a denied plan's empty filter: refused by PostgreSQL (%v)\n", err)
		return nil
	}
	return fmt.Errorf(
		"splicing a denied plan's empty filter into a statement returned [%s] rather than failing — "+
			"a caller ignoring Kind would see rows the PDP denies", joinIDs(ids))
}

// -- plumbing -----------------------------------------------------------------------------------

// translate plans one action for one principal and lowers the plan into a PostgreSQL fragment.
func (a *app) translate(
	ctx context.Context, principalID, action string, opts ...cerbospgx.Option,
) (cerbospgx.Result, string, error) {
	principal, err := a.seeds.principal(principalID)
	if err != nil {
		return cerbospgx.Result{}, "", err
	}

	plan, err := a.cerbos.PlanResources(ctx, principal, cerbos.NewResource(resourceKind, ""), action)
	if err != nil {
		return cerbospgx.Result{}, "", fmt.Errorf("planning %s: %w", action, err)
	}

	kind := plan.PlanResourcesResponse.GetFilter().GetKind()
	if kind == enginev1.PlanResourcesFilter_KIND_UNSPECIFIED {
		return cerbospgx.Result{}, "", errors.New("the PDP returned a plan with no kind — is $CERBOS_HOST a Cerbos PDP?")
	}

	// The table name is what the fragment qualifies its columns with, so it has to be the name the
	// statement reads — an alias in the FROM clause would leave the fragment naming a table the
	// query does not have.
	result, err := cerbospgx.Translate(plan.PlanResourcesResponse, table, mapper, opts...)
	if err != nil {
		return cerbospgx.Result{}, "", fmt.Errorf("translating the plan for %s: %w", action, err)
	}

	return result, kind.String(), nil
}

// selectIDs checks the statement's placeholder numbering, runs it, and returns the ids it selects,
// sorted.
//
// Sorted because demo/cases.json is: a SELECT whose ORDER BY does not cover every row it returns
// has no defined order, and PostgreSQL returning insertion order is an implementation detail rather
// than a promise.
func (a *app) selectIDs(ctx context.Context, stmt string, args []any) ([]string, error) {
	if err := checkPlaceholders(stmt, args); err != nil {
		return nil, err
	}

	ids, err := a.query(ctx, stmt, args)
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	return ids, nil
}

// query runs a statement and collects the single text column it selects. The one place this program
// talks to the database, so the SQL every shape built is logged from here.
func (a *app) query(ctx context.Context, stmt string, args []any) ([]string, error) {
	fmt.Fprintf(os.Stderr, "    %s\n      args: %v\n", stmt, args)

	rows, err := a.pool.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("executing the query: %w", err)
	}

	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("reading the rows: %w", err)
	}
	return ids, nil
}

// joinIDs renders an id set for a diagnostic, sorted so two sets are comparable by eye.
func joinIDs(ids []string) string {
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	return strings.Join(sorted, ",")
}

// seed creates the schema and loads the corpus rows, mapping each corpus attribute name onto the
// column the DDL calls it.
func (a *app) seed(ctx context.Context) error {
	if _, err := a.pool.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("creating the schema: %w", err)
	}

	insert := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES ($1, $2, $3, $4, $5)",
		table, colID, colOwnerID, colIsPublic, colRegion, colArchived)
	for _, row := range a.seeds.Documents {
		if _, err := a.pool.Exec(ctx, insert,
			row.ID, row.OwnerID, row.Public, row.Region, row.Archived); err != nil {
			return fmt.Errorf("seeding %s: %w", row.ID, err)
		}
	}

	fmt.Fprintf(os.Stderr, "seeded %d documents\n", len(a.seeds.Documents))
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

func openStore(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("opening the connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("reaching PostgreSQL — run this example through demo/scripts/run-example.sh %s: %w",
			adapterName, err)
	}
	return pool, nil
}

func run(ctx context.Context) error {
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

	pool, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	cerbosClient, err := cerbos.New(host, cerbos.WithPlaintext())
	if err != nil {
		return fmt.Errorf("connecting to the PDP at %s: %w", host, err)
	}

	application := &app{cerbos: cerbosClient, pool: pool, seeds: corpus, cases: cases}
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
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%s example: %v\n", adapterName, err)
		os.Exit(1)
	}
}
