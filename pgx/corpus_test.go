// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbospgx_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The shared adversarial corpus. Everything about what is tested — the hostile rows, the action
// list, the per-adapter classification, the pinned PDP version — is read from ../conformance/ at
// runtime rather than restated here. Copying any of it into this file would let this adapter
// silently drift from the contract the other adapters are held to.
//
// See ../conformance/README.md for the oracle recipe and the classification rules.

const conformanceDirName = "conformance"

// Tag is one element of a seed's `tags` collection. A nil Name is the hostile case: it must reach
// the database as a NULL column and check() as a MISSING element attribute.
type Tag struct {
	Name *string `json:"name"`
	ID   string  `json:"id"`
}

// Seed is one hostile row.
type Seed struct {
	AOptionalString  *string  `json:"aOptionalString"`
	ID               string   `json:"id"`
	AString          string   `json:"aString"`
	Tags             []Tag    `json:"tags"`
	SubCategoryNames []string `json:"subCategoryNames"`
	ANumber          int      `json:"aNumber"`
	ABool            bool     `json:"aBool"`
}

// Principal is the fixed principal every action is planned and checked with.
type Principal struct {
	Attr  map[string]any `json:"attr"`
	ID    string         `json:"id"`
	Roles []string       `json:"roles"`
}

// SeedsFile is conformance/seeds.json.
type SeedsFile struct {
	ResourceKind string    `json:"resourceKind"`
	Seeds        []Seed    `json:"seeds"`
	Principal    Principal `json:"principal"`
}

// UnsupportedShape is an entry in expectedUnsupported.
type UnsupportedShape struct {
	Action            string `json:"action"`
	Shape             string `json:"shape"`
	SpringDataMessage string `json:"springDataMessage"`
}

// AdapterEntry is an entry in adapterUnsupported / adapterSupportedExpected /
// nullRepresentationOmitted.
type AdapterEntry struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

// KnownDivergence is an action excluded from the oracle run for named adapters.
type KnownDivergence struct {
	Action   string   `json:"action"`
	Reason   string   `json:"reason"`
	Adapters []string `json:"adapters"`
}

// ActionsFile is conformance/actions.json.
//
// Every group is parsed explicitly. A field this struct does not name would be dropped silently,
// and a dropped group makes its actions vanish from the manifest assertion and from every
// parameterised case at once — the projection trap conformance/README.md warns about.
type ActionsFile struct {
	AdapterUnsupported        map[string][]AdapterEntry `json:"adapterUnsupported"`
	AdapterSupportedExpected  map[string][]AdapterEntry `json:"adapterSupportedExpected"`
	Conformance               []string                  `json:"conformance"`
	ExpectedUnsupported       []UnsupportedShape        `json:"expectedUnsupported"`
	NullRepresentationOmitted []AdapterEntry            `json:"nullRepresentationOmitted"`
	KnownDivergences          []KnownDivergence         `json:"knownDivergences"`
}

// Corpus is the parsed corpus plus this adapter's derived classification.
type Corpus struct {
	Dir           string
	CerbosVersion string
	Seeds         SeedsFile
	Actions       ActionsFile

	// OracleActions must match the check() oracle exactly.
	OracleActions []string
	// ThrowingActions must fail translation rather than emit a filter.
	ThrowingActions []AdapterEntry
	// NullOmittedActions are translated with the null representation flipped and must be rejected.
	NullOmittedActions []AdapterEntry
	// SkippedActions are known upstream divergences, excluded from the oracle run.
	SkippedActions map[string]bool
}

// loadCorpus reads the corpus and derives the classification for adapterName exactly as
// conformance/README.md prescribes:
//
//	oracleActions   = conformance - adapterUnsupported[me] + adapterSupportedExpected[me]
//	throwingActions = adapterUnsupported[me] + (expectedUnsupported - adapterSupportedExpected[me])
//	nullOmitted     = nullRepresentationOmitted
//	skipped         = knownDivergences where adapters contains me
func loadCorpus(tb testing.TB, adapterName string) *Corpus {
	tb.Helper()

	dir := findConformanceDir(tb)
	c := &Corpus{Dir: dir}

	readJSON(tb, filepath.Join(dir, "seeds.json"), &c.Seeds)
	readJSON(tb, filepath.Join(dir, "actions.json"), &c.Actions)

	version, err := os.ReadFile(filepath.Join(dir, "CERBOS_VERSION"))
	if err != nil {
		tb.Fatalf("reading CERBOS_VERSION: %v", err)
	}
	c.CerbosVersion = strings.TrimSpace(string(version))

	unsupported := c.Actions.AdapterUnsupported[adapterName]
	unsupportedSet := make(map[string]bool, len(unsupported))
	for _, entry := range unsupported {
		unsupportedSet[entry.Action] = true
	}

	supportedExpected := c.Actions.AdapterSupportedExpected[adapterName]
	supportedExpectedSet := make(map[string]bool, len(supportedExpected))
	for _, entry := range supportedExpected {
		supportedExpectedSet[entry.Action] = true
	}

	c.SkippedActions = make(map[string]bool)
	for _, divergence := range c.Actions.KnownDivergences {
		for _, adapter := range divergence.Adapters {
			if adapter == adapterName {
				c.SkippedActions[divergence.Action] = true
			}
		}
	}

	for _, action := range c.Actions.Conformance {
		if !unsupportedSet[action] {
			c.OracleActions = append(c.OracleActions, action)
		}
	}
	for _, entry := range supportedExpected {
		c.OracleActions = append(c.OracleActions, entry.Action)
	}

	c.ThrowingActions = append(c.ThrowingActions, unsupported...)
	for _, shape := range c.Actions.ExpectedUnsupported {
		if !supportedExpectedSet[shape.Action] {
			c.ThrowingActions = append(c.ThrowingActions, AdapterEntry{
				Action: shape.Action,
				Reason: shape.Shape,
			})
		}
	}

	c.NullOmittedActions = c.Actions.NullRepresentationOmitted

	return c
}

// AllClassifiedActions returns every action the corpus classifies, so the harness can assert that
// the groups it consumes cover the policy exactly once.
func (c *Corpus) AllClassifiedActions() []string {
	seen := make([]string, 0, len(c.Actions.Conformance)+len(c.Actions.ExpectedUnsupported))
	seen = append(seen, c.Actions.Conformance...)
	for _, s := range c.Actions.ExpectedUnsupported {
		seen = append(seen, s.Action)
	}
	for _, s := range c.Actions.NullRepresentationOmitted {
		seen = append(seen, s.Action)
	}
	for _, d := range c.Actions.KnownDivergences {
		seen = append(seen, d.Action)
	}
	return seen
}

// findConformanceDir walks up from the working directory so the harness works whether it is run
// from the module root or from a nested package.
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

// -- deterministic derived fields (conformance/README.md, "Deterministic derived fields") -------
//
// These are part of the shared contract, not adapter-specific fixtures: the same values feed both
// the seeded rows and the check() oracle, so an error here would make both sides agree for the
// wrong reason.

func createdBy(s Seed) string {
	if s.ANumber >= 2 {
		return "2024-06-01T00:00:00Z"
	}
	return "2026-06-01T00:00:00Z"
}

func aDouble(s Seed) *float64 {
	switch s.ID {
	case "a1":
		return ptr(-0.6)
	case "a2":
		return ptr(0.25)
	case "a3":
		return nil
	default:
		return ptr(float64(s.ANumber) + 0.3)
	}
}

func createdAt(s Seed) *string {
	fixed := map[string]*string{
		"a1": ptr("2020-03-15T10:30:00Z"),
		"a2": ptr("2037-01-01T00:00:00Z"),
		"a3": nil,
		"a4": ptr("2024-06-01T00:00:00Z"),
		"a5": ptr("2020-03-15T10:30:00.123456Z"),
	}
	if v, ok := fixed[s.ID]; ok {
		return v
	}
	if s.ANumber >= 2 {
		return ptr("2036-06-06T06:06:06Z")
	}
	return ptr("2021-05-05T05:05:05Z")
}

func scopeOf(s Seed) *string {
	scopes := map[string]string{
		"a1": "dept", "a2": "dept.eng", "a3": "dept.eng.platform",
		"a4": "dept.eng.platform.obs", "a5": "dept.engineering", "a6": "dept.sales",
		"a8": "", "a9": "50%", "b1": "50%:a_b:x", "b2": "50x:a_b:y", "b3": "50%:aXb:y",
		"b4": "50%:a_b", "b5": "dept.eng.platform2", "b6": "50%.a_b", "c1": "Dept.Eng",
		"c2": "dept.eng.", "d1": "[env]:prod:eu", "d2": "e:prod:eu",
	}
	if v, ok := scopes[s.ID]; ok {
		return &v
	}
	return nil
}

// labelsOf returns the third-level label names. A nil element is a NULL label name, which must be
// a missing element attribute on the check side.
func labelsOf(s Seed) []*string {
	switch s.ID {
	case "a1":
		return []*string{ptr("gold"), ptr("silver")}
	case "a6":
		return []*string{nil, ptr("silver")}
	case "a8":
		return []*string{ptr("silver")}
	case "c1":
		return []*string{ptr("Gold")}
	default:
		return nil
	}
}

func ptr[T any](v T) *T { return &v }

// categoryID and subCategoryID name the per-seed category graph. Each seed gets its own
// categories so no two rows share a relation — a shared graph would let a filter match through
// another row's data and still agree with the oracle.
func categoryID(s Seed, i int) string    { return fmt.Sprintf("%s-cat%d", s.ID, i) }
func subCategoryID(s Seed, i int) string { return fmt.Sprintf("%s-sub%d", s.ID, i) }
