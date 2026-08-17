// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The shared adversarial corpus. Everything about what is tested — the hostile rows, the action
// catalog and pinned PDP version come from ../conformance/, while direct outcomes come from this
// adapter's manifest. Copying any of it into this file would let this adapter
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

// Seed is one hostile row. Note is corpus documentation this harness never reads; it is named so
// strict decoding accepts it, and it is the one seed key seedKeys deliberately omits.
type Seed struct {
	AOptionalString *string `json:"aOptionalString"`
	// ParentSeedID names the seed whose scalars this row's to-one `parent` carries, or nil for a
	// row with no parent. See conformance/README.md, "The real to-one relation".
	ParentSeedID     *string  `json:"parentSeedId"` //nolint:tagliatelle // The corpus spells it parentSeedId; Go's ID suffix is not the JSON name.
	ID               string   `json:"id"`
	AString          string   `json:"aString"`
	Note             string   `json:"note"`
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

// SeedsFile is conformance/seeds.json. Every key the file carries is named, including the prose
// ones, because it is decoded with unknown fields rejected.
type SeedsFile struct {
	Schema        string    `json:"$schema"` //nolint:tagliatelle // JSON Schema's reserved key.
	Description   string    `json:"description"`
	ResourceKind  string    `json:"resourceKind"`
	PrincipalNote string    `json:"principalNote"`
	RelationNote  string    `json:"relationNote"`
	Seeds         []Seed    `json:"seeds"`
	Principal     Principal `json:"principal"`
}

// DerivedEntry is one seed's deterministic derived fields, read from
// conformance/derived-fields.json rather than recomputed here. A nil value is a NULL column and a
// missing check() attribute; a nil element of Labels is a NULL label name.
type DerivedEntry struct {
	CreatedAt *string   `json:"createdAt"`
	Scope     *string   `json:"scope"`
	ADouble   *float64  `json:"aDouble"`
	CreatedBy string    `json:"createdBy"`
	Labels    []*string `json:"labels"`
}

// DerivedFile is conformance/derived-fields.json.
type DerivedFile struct {
	Schema      string                  `json:"$schema"` //nolint:tagliatelle // JSON Schema's reserved key.
	Description string                  `json:"description"`
	Entries     map[string]DerivedEntry `json:"derived"`
	Fields      []string                `json:"fields"`
}

// AdapterEntry is one rejected outcome from this adapter's adapterctl manifest.
type AdapterEntry struct {
	Action  string `json:"action"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type OracleExpectation struct {
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

type CatalogAction struct {
	Name              string            `json:"name"`
	OracleExpectation OracleExpectation `json:"oracleExpectation"`
}

type CatalogFile struct {
	SchemaVersion int             `json:"schemaVersion"`
	Actions       []CatalogAction `json:"actions"`
}

type ManifestOutcome struct {
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type ManifestFile struct {
	SchemaVersion int                        `json:"schemaVersion"`
	Adapter       string                     `json:"adapter"`
	Outcomes      map[string]ManifestOutcome `json:"outcomes"`
}

type CheckResource struct {
	Kind string         `json:"kind"`
	ID   string         `json:"id"`
	Attr map[string]any `json:"attr"`
}

type CheckResourcesFile struct {
	SchemaVersion int             `json:"schemaVersion"`
	Principal     Principal       `json:"principal"`
	Resources     []CheckResource `json:"resources"`
}

// Corpus is the parsed corpus plus this adapter's derived classification.
type Corpus struct {
	UpstreamBlockedActions map[string]bool
	Dir                    string
	CerbosVersion          string
	// CerbosImage is the fully pinned PDP reference — tag AND digest, so a re-pointed tag
	// cannot change which build a run tested against.
	CerbosImage        string
	Catalog            CatalogFile
	Manifest           ManifestFile
	CheckResources     CheckResourcesFile
	Seeds              SeedsFile
	Derived            DerivedFile
	OracleActions      []string
	ThrowingActions    []AdapterEntry
	NullOmittedActions []AdapterEntry
	SelectedAction     string
}

// seedKeys is the exact set of seeds.json row keys this harness consumes. `note` is corpus prose
// and is the one documented exclusion.
//
// The same parsed seed feeds the stored row AND the check() oracle, so a key this harness does not
// know about would vanish from both sides at once and the differential would agree for the wrong
// reason — the projection trap conformance/README.md describes for adapterctl.json, applied to the
// seeds. Asserting set equality rather than only rejecting unknown fields catches both directions:
// a corpus key nothing here consumes, and a consumed key the corpus no longer carries (which would
// otherwise decode to its zero value on both sides).
var seedKeys = []string{
	"aBool", "aNumber", "aOptionalString", "aString", "id", "parentSeedId",
	"subCategoryNames", "tags",
}

// seedNoteKey is documentation, never read by any harness.
const seedNoteKey = "note"

// tagKeys guards the one nested object array a seed carries. The top-level guard says nothing
// about a key added inside an element, and an element key is dropped from both sides just as
// silently.
var tagKeys = []string{"id", "name"}

// derivedKeys is the exact set of per-seed derived fields this harness consumes, guarded the same
// way and for the same reason as seedKeys.
var derivedKeys = []string{"aDouble", "createdAt", "createdBy", "labels", "scope"}

// principalKeys is the exact set of top-level keys the corpus principal carries.
//
// `id` and `roles` are deliberately IN scope, not excluded. A role dropped on the way in changes
// every policy decision at once, which is at least as bad as a dropped attribute; that it is less
// likely to happen is a reason to expect the assertion to stay quiet, not a reason to omit it.
// Guarding them one level above the attributes is the same two-level shape seedKeys and tagKeys
// use for a row and its `tags[]` elements.
var principalKeys = []string{"attr", "id", "roles"}

// principalAttrKeys is the exact set of principal attributes this harness consumes.
//
// The corpus principal feeds the PLAN under test and the check() ORACLE, so an attribute dropped on
// the way in vanishes from both sides at once: the plan folds to ALWAYS_DENIED and the oracle,
// built from the same principal, agrees. That is how langchain-chromadb's hardcoded attribute
// allowlist let `pv-exists` pass while testing nothing (conformance/README.md, "Adding a new
// hostile shape", step 7). This harness hands Principal.Attr to the SDK verbatim, which is correct;
// the guard is what proves it still does, in both directions — a corpus attribute nothing here
// consumes, and a consumed attribute the corpus no longer carries.
var principalAttrKeys = []string{"allowedTags", "context", "fewTeams", "manyTeams"}

// cerbosImageRepository is the PDP image the corpus pins. The tag comes from CERBOS_VERSION and
// the digest from CERBOS_IMAGE_DIGEST; conformance/scripts/validate-corpus.sh asserts the two
// agree everywhere they are restated.
const cerbosImageRepository = "ghcr.io/cerbos/cerbos"

// loadCorpus reads the v1 control plane and derives this adapter's executable cases directly from
// its manifest outcomes. No second classification source is part of this seam.
func loadCorpus(tb testing.TB, adapterName string) *Corpus {
	tb.Helper()

	dir := findConformanceDir(tb)
	c := &Corpus{Dir: dir}

	readJSONStrict(tb, filepath.Join(dir, "seeds.json"), &c.Seeds)
	readJSONStrict(tb, filepath.Join(dir, "derived-fields.json"), &c.Derived)
	readJSONStrict(tb, filepath.Join(dir, "catalog.json"), &c.Catalog)
	readJSONStrict(tb, filepath.Join(dir, "check-resources.json"), &c.CheckResources)
	readJSON(tb, filepath.Join(filepath.Dir(dir), adapterName, "adapterctl.json"), &c.Manifest)
	assertCorpusCoverage(tb, dir, c)
	if c.Catalog.SchemaVersion != 1 || c.Manifest.SchemaVersion != 1 || c.CheckResources.SchemaVersion != 1 {
		tb.Fatal("control-plane files must use schemaVersion 1")
	}
	if c.Manifest.Adapter != adapterName {
		tb.Fatalf("adapterctl.json names adapter %q, want %q", c.Manifest.Adapter, adapterName)
	}
	if len(c.CheckResources.Resources) != len(c.Seeds.Seeds) {
		tb.Fatalf("check-resources.json has %d resources for %d seeded rows",
			len(c.CheckResources.Resources), len(c.Seeds.Seeds))
	}

	version, err := os.ReadFile(filepath.Join(dir, "CERBOS_VERSION"))
	if err != nil {
		tb.Fatalf("reading CERBOS_VERSION: %v", err)
	}
	c.CerbosVersion = strings.TrimSpace(string(version))

	digest, err := os.ReadFile(filepath.Join(dir, "CERBOS_IMAGE_DIGEST"))
	if err != nil {
		tb.Fatalf("reading CERBOS_IMAGE_DIGEST: %v", err)
	}
	c.CerbosImage = cerbosImageRepository + ":" + c.CerbosVersion + "@" + strings.TrimSpace(string(digest))

	c.UpstreamBlockedActions = make(map[string]bool)
	selectedAction := strings.TrimSpace(os.Getenv("ADAPTERCTL_ACTION"))
	c.SelectedAction = selectedAction
	catalogActions := make(map[string]bool, len(c.Catalog.Actions))
	for _, catalogAction := range c.Catalog.Actions {
		catalogActions[catalogAction.Name] = true
	}
	if selectedAction != "" && !catalogActions[selectedAction] {
		tb.Fatalf("ADAPTERCTL_ACTION names unknown catalog action %q", selectedAction)
	}
	for _, catalogAction := range c.Catalog.Actions {
		if selectedAction != "" && catalogAction.Name != selectedAction {
			continue
		}
		outcome, ok := manifestOutcomeForAction(
			c.Manifest.Outcomes, catalogAction.Name, selectedAction)
		if !ok {
			tb.Fatalf("adapterctl.json has no outcome for catalog action %q", catalogAction.Name)
		}
		entry := AdapterEntry{Action: catalogAction.Name, Reason: outcome.Reason, Message: outcome.Message}
		switch outcome.Status {
		case "matched":
			c.OracleActions = append(c.OracleActions, catalogAction.Name)
		case "rejected":
			requireMessage(tb, "adapterctl.json outcome "+catalogAction.Name, outcome.Message)
			if outcome.Reason == "" {
				tb.Fatalf("adapterctl.json rejected outcome %q has no reason", catalogAction.Name)
			}
			if catalogAction.Name == "null-eq-missing" {
				c.NullOmittedActions = append(c.NullOmittedActions, entry)
			} else {
				c.ThrowingActions = append(c.ThrowingActions, entry)
			}
		case "upstream-blocked":
			if outcome.Reason == "" {
				tb.Fatalf("adapterctl.json upstream-blocked outcome %q has no reason", catalogAction.Name)
			}
			c.UpstreamBlockedActions[catalogAction.Name] = true
		case "unassessed":
			tb.Fatalf("adapterctl.json outcome %q is unassessed", catalogAction.Name)
		default:
			tb.Fatalf("adapterctl.json outcome %q has unknown status %q", catalogAction.Name, outcome.Status)
		}
	}
	if selectedAction == "" {
		for action := range c.Manifest.Outcomes {
			if !catalogActions[action] {
				tb.Fatalf("adapterctl.json has outcome for unknown catalog action %q", action)
			}
		}
		if len(c.Manifest.Outcomes) != len(c.Catalog.Actions) {
			tb.Fatalf("adapterctl.json has %d outcomes for %d catalog actions",
				len(c.Manifest.Outcomes), len(c.Catalog.Actions))
		}
	}

	return c
}

// validateMessage rejects a throwing classification that pins no error message. The message is what
// turns "it threw" into "it threw for the declared reason": without it a mapper typo or an
// unrelated validation satisfies the throw suite just as well as the documented limitation
// (cerbos/query-plan-adapters#326).
//
// Split from requireMessage so the guard itself is unit-testable rather than only reachable through
// a corpus that already satisfies it.
func validateMessage(label, message string) error {
	if message == "" {
		return fmt.Errorf(
			"adapterctl.json pins no throw message for %s: the throw suite would accept a failure for any reason",
			label)
	}
	return nil
}

// manifestOutcomeForAction makes a new or explicitly unassessed action executable only while
// discovery selects that exact action. An unscoped run still sees the manifest as written and
// therefore retains the full outcome-accounting gate.
func manifestOutcomeForAction(
	outcomes map[string]ManifestOutcome, action, selectedAction string,
) (ManifestOutcome, bool) {
	outcome, ok := outcomes[action]
	if action == selectedAction && (!ok || outcome.Status == "unassessed") {
		return ManifestOutcome{Status: "matched"}, true
	}
	return outcome, ok
}

func TestSelectedDiscoveryOutcome(t *testing.T) {
	t.Parallel()

	for name, outcomes := range map[string]map[string]ManifestOutcome{
		"missing":    {},
		"unassessed": {"new-action": {Status: "unassessed"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			outcome, ok := manifestOutcomeForAction(outcomes, "new-action", "new-action")
			require.Equal(t, ManifestOutcome{Status: "matched"}, outcome)
			require.True(t, ok)
		})
	}
}

func TestSelectedDiscoveryOutcomePreservesAssessment(t *testing.T) {
	t.Parallel()

	want := ManifestOutcome{Status: "rejected", Reason: "unsupported", Message: "cannot translate"}
	got, ok := manifestOutcomeForAction(
		map[string]ManifestOutcome{"known-action": want}, "known-action", "known-action")
	require.Equal(t, want, got)
	require.True(t, ok)
}

func TestUnscopedMissingOutcomeStaysMissing(t *testing.T) {
	t.Parallel()

	_, ok := manifestOutcomeForAction(map[string]ManifestOutcome{}, "new-action", "")
	require.False(t, ok)
}

// requireMessage fails the run when validateMessage rejects the pin.
func requireMessage(tb testing.TB, label, message string) {
	tb.Helper()
	if err := validateMessage(label, message); err != nil {
		tb.Fatal(err)
	}
}

// AllClassifiedActions returns every action the corpus classifies, so the harness can assert that
// the groups it consumes cover the policy exactly once.
func (c *Corpus) AllClassifiedActions() []string {
	seen := append([]string{}, c.OracleActions...)
	for _, outcome := range c.ThrowingActions {
		seen = append(seen, outcome.Action)
	}
	for _, outcome := range c.NullOmittedActions {
		seen = append(seen, outcome.Action)
	}
	for action := range c.UpstreamBlockedActions {
		seen = append(seen, action)
	}
	sort.Strings(seen)
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

// readJSONStrict decodes a corpus file with unknown fields rejected, so a key the target struct
// does not name is a build-the-wrong-thing error rather than a silent drop.
func readJSONStrict(tb testing.TB, path string, out any) {
	tb.Helper()

	data, err := os.ReadFile(path) //nolint:gosec // corpus paths are derived from the repo layout
	if err != nil {
		tb.Fatalf("reading %s: %v", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		tb.Fatalf("parsing %s: %v", path, err)
	}
}

// assertCorpusCoverage proves the harness consumes every seed key, every principal key and every
// derived field the corpus defines, and nothing it does not. Strict decoding alone cannot do this:
// it rejects an added key but says nothing about one that disappears, and a disappeared key decodes
// to its zero value on both sides of the differential.
func assertCorpusCoverage(tb testing.TB, dir string, c *Corpus) {
	tb.Helper()

	var raw struct {
		Principal map[string]json.RawMessage   `json:"principal"`
		Seeds     []map[string]json.RawMessage `json:"seeds"`
	}
	readJSON(tb, filepath.Join(dir, "seeds.json"), &raw)
	if len(raw.Seeds) != len(c.Seeds.Seeds) {
		tb.Fatalf("seeds.json decoded %d rows but carries %d", len(c.Seeds.Seeds), len(raw.Seeds))
	}
	for i, seed := range raw.Seeds {
		label := fmt.Sprintf("seeds.json seeds[%d]", i)
		assertKeys(tb, label, keysOf(seed), seedKeys, seedNoteKey)

		var tags []map[string]json.RawMessage
		if err := json.Unmarshal(seed["tags"], &tags); err != nil {
			tb.Fatalf("parsing %s.tags: %v", label, err)
		}
		for j, tag := range tags {
			assertKeys(tb, fmt.Sprintf("%s.tags[%d]", label, j), keysOf(tag), tagKeys)
		}
	}

	assertPrincipalCoverage(tb, raw.Principal)

	assertKeys(tb, "derived-fields.json fields", c.Derived.Fields, derivedKeys)

	var rawDerived struct {
		Derived map[string]map[string]json.RawMessage `json:"derived"`
	}
	readJSON(tb, filepath.Join(dir, "derived-fields.json"), &rawDerived)
	for _, seed := range c.Seeds.Seeds {
		entry, ok := rawDerived.Derived[seed.ID]
		if !ok {
			tb.Fatalf("derived-fields.json has no entry for seed %q", seed.ID)
		}
		assertKeys(tb, fmt.Sprintf("derived-fields.json derived[%q]", seed.ID), keysOf(entry), derivedKeys)
	}
	if len(rawDerived.Derived) != len(c.Seeds.Seeds) {
		tb.Fatalf("derived-fields.json has %d entries for %d seeds", len(rawDerived.Derived), len(c.Seeds.Seeds))
	}
}

// assertPrincipalCoverage guards the corpus principal the way assertCorpusCoverage guards a seed
// row: the top-level keys, then the keys one level in.
//
// The attribute VALUES are asserted too, because a key-set guard says nothing about a change inside
// one and three of the four attributes are lists. The corpus carries exactly two shapes — a string
// and a list of strings — and every harness converts on that basis, so a third shape has to fail
// here rather than be coerced by one adapter's SDK and passed through untyped by another. It is the
// same reason the seed guard descends into `tags[]`.
func assertPrincipalCoverage(tb testing.TB, principal map[string]json.RawMessage) {
	tb.Helper()

	assertKeys(tb, "seeds.json principal", keysOf(principal), principalKeys)

	var attr map[string]json.RawMessage
	if err := json.Unmarshal(principal["attr"], &attr); err != nil {
		tb.Fatalf("parsing seeds.json principal.attr: %v", err)
	}
	assertKeys(tb, "seeds.json principal.attr", keysOf(attr), principalAttrKeys)
	for key, value := range attr {
		assertPrincipalAttrShape(tb, "seeds.json principal.attr."+key, value)
	}
}

// assertPrincipalAttrShape fails unless the value is one of the two shapes the corpus carries.
func assertPrincipalAttrShape(tb testing.TB, label string, value json.RawMessage) {
	tb.Helper()

	var scalar string
	if json.Unmarshal(value, &scalar) == nil {
		return
	}
	var list []string
	if json.Unmarshal(value, &list) == nil {
		return
	}
	tb.Fatalf("%s is neither a string nor a list of strings, the only two shapes this harness consumes: a reshaped principal attribute feeds the plan and the check() oracle at once", label)
}

// assertKeys fails unless got is exactly want, ignoring order, plus any of the optional keys.
func assertKeys(tb testing.TB, label string, got, want []string, optional ...string) {
	tb.Helper()

	allowed := make(map[string]bool, len(want)+len(optional))
	required := make(map[string]bool, len(want))
	for _, key := range want {
		allowed[key] = true
		required[key] = true
	}
	for _, key := range optional {
		allowed[key] = true
	}

	for _, key := range got {
		if !allowed[key] {
			tb.Fatalf("%s carries %q, which this harness does not consume: an unconsumed corpus field is dropped from the stored row and the check() oracle at once", label, key)
		}
		delete(required, key)
	}
	for key := range required {
		tb.Fatalf("%s is missing %q, which this harness consumes", label, key)
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// TestCorpusCoverage runs the corpus consistency guards on their own, without the containers the
// differential suite needs. A corpus field this harness stopped consuming — on either side of the
// differential at once — fails here in milliseconds rather than after a PDP and a database have
// started. loadCorpus does the asserting; the only thing left to check is that it had anything to
// assert against.
//
// Run it with -count=1 after editing the corpus: ../conformance is outside this module, so Go's
// test cache does not track it and will otherwise serve the previous result. CI checks out fresh,
// so this only bites local runs.
func TestCorpusCoverage(t *testing.T) {
	t.Parallel()

	if corpus := loadCorpus(t, adapterName); len(corpus.Seeds.Seeds) == 0 {
		t.Fatal("corpus has no seeds")
	}
}

func TestControlPlaneMetadata(t *testing.T) {
	t.Parallel()

	corpus := loadCorpus(t, adapterName)
	expectedActions := len(corpus.Catalog.Actions)
	if corpus.SelectedAction != "" {
		expectedActions = 1
	}
	require.Len(t, corpus.AllClassifiedActions(), expectedActions)
	require.Len(t, corpus.CheckResources.Resources, len(corpus.Seeds.Seeds))
}

// -- deterministic derived fields (conformance/README.md, "Deterministic derived fields") -------
//
// These are part of the shared contract, not adapter-specific fixtures: the same values feed both
// the seeded rows and the check() oracle, so an error here would make both sides agree for the
// wrong reason and nothing downstream could catch it. They are therefore read from
// conformance/derived-fields.json rather than restated here — restating them is how a
// transcription error becomes self-consistent and invisible.

func (c *Corpus) derived(s Seed) DerivedEntry {
	entry, ok := c.Derived.Entries[s.ID]
	if !ok {
		// Unreachable: assertCorpusCoverage proves every seed has an entry.
		panic("no derived-fields.json entry for seed " + s.ID)
	}
	return entry
}

func (c *Corpus) createdBy(s Seed) string { return c.derived(s).CreatedBy }

func (c *Corpus) aDouble(s Seed) *float64 { return c.derived(s).ADouble }

func (c *Corpus) createdAt(s Seed) *string { return c.derived(s).CreatedAt }

func (c *Corpus) scopeOf(s Seed) *string { return c.derived(s).Scope }

// labelsOf returns the third-level label names. A nil element is a NULL label name, which must be
// a missing element attribute on the check side.
func (c *Corpus) labelsOf(s Seed) []*string { return c.derived(s).Labels }

// categoryID and subCategoryID name the per-seed category graph. Each seed gets its own
// categories so no two rows share a relation — a shared graph would let a filter match through
// another row's data and still agree with the oracle.
func categoryID(s Seed, i int) string    { return fmt.Sprintf("%s-cat%d", s.ID, i) }
func subCategoryID(s Seed, i int) string { return fmt.Sprintf("%s-sub%d", s.ID, i) }

// -- the real to-one relation (conformance/README.md, "The real to-one relation") ----------------
//
// `parentSeedId` names the seed whose four scalars a row's `parent` carries, and that seed's own
// `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels. Every
// resource owns a FRESH parent (and inner) row rather than pointing at the named seed's own row,
// so no two resources share one and a filter that returned the parent instead of the child cannot
// agree with the oracle by accident — the same rule the per-seed category graph follows.

// parentSeedOf returns the seed one hop out, or nil when this level has no parent. Passing a nil
// seed returns nil, so the two-level chain is `parentSeedOf(parentSeedOf(seed))`.
func (c *Corpus) parentSeedOf(s *Seed) *Seed {
	if s == nil || s.ParentSeedID == nil {
		return nil
	}
	for i := range c.Seeds.Seeds {
		if c.Seeds.Seeds[i].ID == *s.ParentSeedID {
			return &c.Seeds.Seeds[i]
		}
	}
	// Unreachable: validate-corpus.sh proves every parentSeedId names a seed.
	panic("seeds.json parentSeedId names no seed: " + *s.ParentSeedID)
}

// relationAttr is one level of the chain as check() attributes. A NULL column is a MISSING
// attribute one hop out, exactly as it is on the resource row itself.
func relationAttr(s *Seed) map[string]any {
	attr := map[string]any{"aBool": s.ABool, "aString": s.AString, "aNumber": s.ANumber}
	if s.AOptionalString != nil {
		attr["aOptionalString"] = *s.AOptionalString
	}
	return attr
}

// parentID and innerID name the per-resource chain rows.
func parentID(s Seed) string { return s.ID + "-parent" }
func innerID(s Seed) string  { return s.ID + "-parent-inner" }

// TestValidateMessageRejectsAnUnpinnedThrow proves the guard fires: adding a throwing action without
// pinning its message must fail this harness rather than silently degrade its throw case to a bare
// "it threw" (cerbos/query-plan-adapters#326).
func TestValidateMessageRejectsAnUnpinnedThrow(t *testing.T) {
	t.Parallel()

	require.ErrorContains(t, validateMessage("synthetic-entry", ""), "pins no throw message")
	require.NoError(t, validateMessage("synthetic-entry", "a pinned mechanism"))
}
