// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx_test

import (
	"encoding/json"
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
// specific to PostgreSQL; adversarial_test.go owns the store. See ../conformance/README.md, "The
// harness contract".

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
	AString          string     `json:"aString"`
	Tags             []Tag      `json:"tags"`
	SubCategoryNames []string   `json:"subCategoryNames"`
	ANumberList      []*float64 `json:"aNumberList"`
	ABoolList        []*bool    `json:"aBoolList"`
	ANumber          int        `json:"aNumber"`
	ABool            bool       `json:"aBool"`
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
	// PlannerDivergence is decoded only for presence: the generator writes it into a tag's golden
	// only where it applies to that tag, so non-null is the whole skip rule.
	PlannerDivergence json.RawMessage `json:"plannerDivergence"`
	ID                string          `json:"id"`
	Tier              string          `json:"tier"`
	Plan              json.RawMessage `json:"plan"`
	Allowed           []string        `json:"allowed"`
}

// Skipped reports whether the golden records a planner divergence: the plan and check() disagree
// under this PDP, so no adapter can pass it (../conformance/README.md, "The harness contract").
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
	}
	readJSON(tb, ledgerFile, &ledger)
	c.Ledger = ledger.Cases

	var versions map[string]struct {
		Tag string `json:"tag"`
	}
	readJSON(tb, filepath.Join(dir, "pdp-versions.json"), &versions)
	c.Tags = []string{versions["current"].Tag, versions["previous"].Tag}

	for _, tag := range c.Tags {
		root := filepath.Join(dir, "golden", tag)
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || filepath.Ext(path) != ".json" {
				return err
			}
			var golden Golden
			readJSON(tb, path, &golden)
			c.Goldens[tag] = append(c.Goldens[tag], golden)
			return nil
		})
		if err != nil || len(c.Goldens[tag]) == 0 {
			tb.Fatalf("reading the goldens for PDP %q under %s: %v", tag, root, err)
		}
		slices.SortFunc(c.Goldens[tag], func(a, b Golden) int { return strings.Compare(a.ID, b.ID) })
	}

	// The stale-ledger guard: every entry must name a case some PDP still has a golden for.
	for id, entry := range c.Ledger {
		if entry.Status != "unsupported" && entry.Status != "divergent" {
			tb.Fatalf("%s: ledger entry %q has status %q, want unsupported or divergent", ledgerFile, id, entry.Status)
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
func categoryID(s Seed, i int) string    { return fmt.Sprintf("%s-cat%d", s.ID, i) }
func subCategoryID(s Seed, i int) string { return fmt.Sprintf("%s-sub%d", s.ID, i) }
func parentID(s Seed) string             { return s.ID + "-parent" }
func innerID(s Seed) string              { return s.ID + "-parent-inner" }
