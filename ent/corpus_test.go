// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	enginev1 "github.com/cerbos/cerbos/api/genpb/cerbos/engine/v1"
	responsev1 "github.com/cerbos/cerbos/api/genpb/cerbos/response/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// The shared conformance corpus, read from ../conformance/ at runtime: the dataset, the recorded
// golden plans and check() decisions for each pinned PDP, and this adapter's ledger. Nothing here is
// specific to one SQL dialect; adversarial_test.go owns the stores. See ../conformance/README.md,
// "The harness contract".

const (
	conformanceDirName = "conformance"
	ledgerFile         = "conformance-ledger.json"
	// nowMinus24h is the placeholder a golden records for the instant `now() - duration("24h")`
	// folds to. It is substituted before the plan is decoded.
	nowMinus24h = "__NOW_MINUS_24H__"
)

// -- the dataset -------------------------------------------------------------------------------

// Tag is one element of a seed's `tags` collection. A nil Name is a NULL column.
type Tag struct {
	Name *string `json:"name"`
	ID   string  `json:"id"`
}

// Seed is one row of conformance/seeds.json.
type Seed struct {
	AOptionalString *string `json:"aOptionalString"`
	// ParentSeedID names the seed whose scalars this row's to-one `parent` carries, or nil for a
	// row with no parent. See conformance/README.md, "The dataset".
	ParentSeedID     *string    `json:"parentSeedId"` //nolint:tagliatelle // The corpus spells it parentSeedId; Go's ID suffix is not the JSON name.
	ID               string     `json:"id"`
	AString          *string    `json:"aString"`
	Tags             []Tag      `json:"tags"`
	SubCategoryNames []string   `json:"subCategoryNames"`
	ANumberList      []*float64 `json:"aNumberList"`
	ABoolList        []*bool    `json:"aBoolList"`
	ANumber          *int       `json:"aNumber"`
	ABool            *bool      `json:"aBool"`
	// Requires names the store capabilities the row needs; a store that lacks one cannot hold it.
	Requires []string `json:"requires"`
}

// Derived is one seed's entry in conformance/derived-fields.json. A nil value is a NULL column; a nil
// element of Labels is a NULL label name.
type Derived struct {
	CreatedAt *string   `json:"createdAt"`
	UpdatedAt *string   `json:"updatedAt"`
	Scope     *string   `json:"scope"`
	ADouble   *float64  `json:"aDouble"`
	CreatedBy string    `json:"createdBy"`
	Labels    []*string `json:"labels"`
}

// -- the goldens and the ledger ----------------------------------------------------------------

// Golden is one conformance/golden/<tag>/<case id>.json.
type Golden struct {
	// PlannerDivergence is decoded as raw JSON: the contract skips a case whenever it is set, so
	// nothing inside it is consulted. The generator writes it into a tag's golden only when the
	// divergence applies to that tag.
	PlannerDivergence json.RawMessage `json:"plannerDivergence"`
	ID                string          `json:"id"`
	PDP               string          `json:"pdp"`
	Tier              string          `json:"tier"`
	Plan              json.RawMessage `json:"plan"`
	Allowed           []string        `json:"allowed"`
}

// Skipped reports whether the golden records a planner divergence, which the harness contract
// skips: the recorded plan and check() disagree, so no adapter can pass.
func (g Golden) Skipped() bool {
	return len(g.PlannerDivergence) > 0 && string(g.PlannerDivergence) != "null"
}

// PlanResponse decodes the recorded filter, substituting the relative-time placeholder.
func (g Golden) PlanResponse(now time.Time) (*responsev1.PlanResourcesResponse, error) {
	raw := strings.ReplaceAll(string(g.Plan), nowMinus24h, now.Add(-24*time.Hour).UTC().Format(time.RFC3339Nano))
	filter := &enginev1.PlanResourcesFilter{}
	if err := protojson.Unmarshal([]byte(raw), filter); err != nil {
		return nil, fmt.Errorf("decoding the plan of %s: %w", g.ID, err)
	}
	return &responsev1.PlanResourcesResponse{Filter: filter}, nil
}

// LedgerEntry is one case this adapter cannot pass. Status is "unsupported" or "divergent".
type LedgerEntry struct {
	Status string   `json:"status"`
	Reason string   `json:"reason"`
	Issue  string   `json:"issue"`
	PDP    []string `json:"pdp"`
}

// AppliesTo reports whether the entry covers this PDP tag.
func (e LedgerEntry) AppliesTo(tag string) bool {
	return len(e.PDP) == 0 || slices.Contains(e.PDP, tag)
}

// Corpus is everything the harness reads.
type Corpus struct {
	Derived map[string]Derived
	// Goldens maps each PDP tag in pdp-versions.json to its golden files, sorted by case id.
	Goldens map[string][]Golden
	Ledger  map[string]LedgerEntry
	Seeds   []Seed
	// Tags is the PDP tags, current first.
	Tags []string
}

func loadCorpus(tb testing.TB) *Corpus {
	tb.Helper()

	dir := findConformanceDir(tb)
	c := &Corpus{Goldens: map[string][]Golden{}}

	var seeds struct {
		Seeds []Seed `json:"seeds"`
	}
	readJSON(tb, filepath.Join(dir, "seeds.json"), &seeds)
	c.Seeds = seeds.Seeds

	var derived struct {
		Derived map[string]Derived `json:"derived"`
	}
	readJSON(tb, filepath.Join(dir, "derived-fields.json"), &derived)
	c.Derived = derived.Derived

	var ledger struct {
		Cases map[string]LedgerEntry `json:"cases"`
		// Lacks maps each store capability this adapter's store lacks to the reason.
		Lacks map[string]string `json:"lacks"`
	}
	readJSON(tb, ledgerFile, &ledger)
	c.Ledger = ledger.Cases

	// A seed requiring a capability this store lacks is a resource it cannot hold: it is not
	// loaded, and its id is struck from every golden's allowed set.
	var unheld []string
	c.Seeds = slices.DeleteFunc(c.Seeds, func(s Seed) bool {
		if slices.ContainsFunc(s.Requires, func(r string) bool { _, lacked := ledger.Lacks[r]; return lacked }) {
			unheld = append(unheld, s.ID)
			return true
		}
		return false
	})

	var versions map[string]struct {
		Tag string `json:"tag"`
	}
	readJSON(tb, filepath.Join(dir, "pdp-versions.json"), &versions)
	c.Tags = []string{versions["current"].Tag, versions["previous"].Tag}

	for _, tag := range c.Tags {
		root := filepath.Join(dir, "golden", tag)
		if fuzzDir != "" {
			root = filepath.Join(fuzzDir, tag)
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || filepath.Ext(path) != ".json" {
				return err
			}
			var golden Golden
			readJSON(tb, path, &golden)
			// The ledger is keyed by the id inside the file, so a file whose id does not match its
			// path could dodge the entry meant for it; one recorded under another PDP would be
			// compared with the wrong decisions.
			rel, _ := filepath.Rel(root, path)
			if want := strings.TrimSuffix(filepath.ToSlash(rel), ".json"); golden.ID != want || golden.PDP != tag {
				tb.Fatalf("%s: id %q, pdp %q; want id %q, pdp %q", path, golden.ID, golden.PDP, want, tag)
			}
			c.Goldens[tag] = append(c.Goldens[tag], golden)
			return nil
		})
		if err != nil || len(c.Goldens[tag]) == 0 {
			tb.Fatalf("reading the goldens for PDP %q under %s: %v", tag, root, err)
		}
		for i := range c.Goldens[tag] {
			g := &c.Goldens[tag][i]
			g.Allowed = slices.DeleteFunc(g.Allowed, func(id string) bool { return slices.Contains(unheld, id) })
		}
		slices.SortFunc(c.Goldens[tag], func(a, b Golden) int { return strings.Compare(a.ID, b.ID) })
	}

	// The stale-ledger guard: every entry must name a case some PDP still has a golden for. A fuzz
	// run is judged without the ledger, so its goldens cannot make an entry stale.
	for id, entry := range c.Ledger {
		if fuzzDir != "" {
			break
		}
		if entry.Status != "unsupported" && entry.Status != "divergent" {
			tb.Fatalf("%s: ledger entry %q has status %q, want unsupported or divergent", ledgerFile, id, entry.Status)
		}
		if entry.Reason == "" || (entry.Status == "divergent" && entry.Issue == "") {
			tb.Fatalf("%s: ledger entry %q needs a reason, and an issue when divergent", ledgerFile, id)
		}
		// An entry scoped only to tags that are no longer pinned never applies: it is stale.
		for _, tag := range entry.PDP {
			if !slices.Contains(c.Tags, tag) {
				tb.Fatalf("%s: ledger entry %q is scoped to PDP %q, which pdp-versions.json no longer pins", ledgerFile, id, tag)
			}
		}
		if !slices.ContainsFunc(c.Tags, func(tag string) bool {
			return slices.ContainsFunc(c.Goldens[tag], func(g Golden) bool { return g.ID == id })
		}) {
			tb.Fatalf("%s names %q, which has no golden file", ledgerFile, id)
		}
	}

	return c
}

// findConformanceDir walks up from the working directory to the repository's conformance/.
func findConformanceDir(tb testing.TB) string {
	tb.Helper()

	dir, err := os.Getwd()
	if err != nil {
		tb.Fatalf("resolving working directory: %v", err)
	}
	for {
		candidate := filepath.Join(dir, conformanceDirName)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			tb.Fatalf("could not find the %s directory above %s", conformanceDirName, dir)
		}
		dir = parent
	}
}

func readJSON(tb testing.TB, path string, out any) {
	tb.Helper()

	data, err := os.ReadFile(path) //nolint:gosec // corpus paths are derived from the repo layout
	if err != nil {
		tb.Fatalf("reading %s: %v", path, err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		tb.Fatalf("parsing %s: %v", path, err)
	}
}

// -- dataset helpers ---------------------------------------------------------------------------

func (c *Corpus) derived(s Seed) Derived {
	entry, ok := c.Derived[s.ID]
	if !ok {
		panic("no derived-fields.json entry for seed " + s.ID)
	}
	return entry
}

// parentSeedOf returns the seed one hop out along the to-one `parent` relation, or nil. Passing nil
// returns nil, so `parent.inner` is `parentSeedOf(parentSeedOf(seed))`.
func (c *Corpus) parentSeedOf(s *Seed) *Seed {
	if s == nil || s.ParentSeedID == nil {
		return nil
	}
	for i := range c.Seeds {
		if c.Seeds[i].ID == *s.ParentSeedID {
			return &c.Seeds[i]
		}
	}
	panic("seeds.json parentSeedId names no seed: " + *s.ParentSeedID)
}

// Each seed owns its own category graph and its own parent chain rows, so no filter can match
// through another row's data.
// A seed owns one category, holding every subcategory name.
func categoryID(s Seed) string           { return s.ID + "-cat" }
func subCategoryID(s Seed, i int) string { return fmt.Sprintf("%s-sub%d", s.ID, i) }
func parentID(s Seed) string             { return s.ID + "-parent" }
func innerID(s Seed) string              { return s.ID + "-parent-inner" }

// -- fuzz mode ----------------------------------------------------------------------------------

// fuzzDir is the directory a fuzz run was recorded into (conformance/README.md, "Fuzzing"). When
// it is set, loadCorpus reads the generated cases from it instead of the corpus, and the harness
// judges each case with fuzzVerdict instead of the ledger: a refusal is always acceptable, since the
// ledger has no entry for a shape nobody has written down yet.
var fuzzDir = os.Getenv("QPA_FUZZ_DIR")

// FuzzVerdict is one case's line in <fuzzDir>/results/<adapter>-<store>.json, which the
// generator's fuzz-report reads.
type FuzzVerdict struct {
	PDP      string   `json:"pdp"`
	ID       string   `json:"id"`
	Verdict  string   `json:"verdict"`
	Expected []string `json:"expected,omitempty"`
	Returned []string `json:"returned,omitempty"`
	Detail   string   `json:"detail,omitempty"`
}

// OK reports whether the verdict is a match or a refusal, the two the invariant allows.
func (v FuzzVerdict) OK() bool { return v.Verdict == "match" || v.Verdict == "refused" }

// fuzzVerdict judges what translating and running a fuzz case gave: refusal is an error wrapping
// the adapter's refusal sentinel.
func fuzzVerdict(golden Golden, returned []string, err error, refusal error) FuzzVerdict {
	v := FuzzVerdict{PDP: golden.PDP, ID: golden.ID}
	expected := sortedCopy(golden.Allowed)
	switch {
	case errors.Is(err, refusal):
		v.Verdict = "refused"
		return v
	case err != nil:
		v.Verdict, v.Detail = "error", err.Error()
		return v
	case slices.Equal(returned, expected):
		v.Verdict = "match"
		return v
	}
	v.Expected, v.Returned = expected, returned
	switch {
	case slices.ContainsFunc(returned, func(id string) bool { return !slices.Contains(expected, id) }):
		v.Verdict = "over-grant"
	case slices.ContainsFunc(expected, func(id string) bool { return !slices.Contains(returned, id) }):
		v.Verdict = "under-grant"
	default:
		v.Verdict, v.Detail = "error", "the right rows, but some of them repeated"
	}
	return v
}

// writeFuzzResults writes one store's verdicts; outside fuzz mode it does nothing.
func writeFuzzResults(tb testing.TB, adapter, store string, results []FuzzVerdict) {
	tb.Helper()
	if fuzzDir == "" {
		return
	}
	slices.SortFunc(results, func(a, b FuzzVerdict) int {
		return cmp.Or(strings.Compare(a.ID, b.ID), strings.Compare(a.PDP, b.PDP))
	})
	raw, err := json.MarshalIndent(map[string]any{"adapter": adapter, "store": store, "results": results}, "", "  ")
	if err != nil {
		tb.Fatalf("encoding the fuzz results: %v", err)
	}
	dir := filepath.Join(fuzzDir, "results")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("writing the fuzz results: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, adapter+"-"+store+".json"), append(raw, '\n'), 0o644); err != nil {
		tb.Fatalf("writing the fuzz results: %v", err)
	}
}
