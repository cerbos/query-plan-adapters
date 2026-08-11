// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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

// UnsupportedShape is an entry in expectedUnsupported. Messages carries one entry per adapter that
// must reject the shape, keyed by adapter name; the corpus asserts that key set.
type UnsupportedShape struct {
	Messages map[string]string `json:"messages"`
	Action   string            `json:"action"`
	Shape    string            `json:"shape"`
}

// AdapterEntry is an entry in adapterUnsupported / adapterSupportedExpected /
// nullRepresentationOmitted.
//
// Messages is the per-adapter message map a nullRepresentationOmitted entry carries; the other two
// groups leave it nil, and adapterUnsupported uses the flat Message below instead because it is
// already per-adapter.
//
// Message is the substring this adapter's error must contain. adapterUnsupported carries it and
// loadCorpus requires it; adapterSupportedExpected and nullRepresentationOmitted do not throw for
// the reason recorded here, so they leave it empty.
type AdapterEntry struct {
	Messages map[string]string `json:"messages"`
	Action   string            `json:"action"`
	Reason   string            `json:"reason"`
	Message  string            `json:"message"`
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
	SkippedActions map[string]bool
	Dir            string
	CerbosVersion  string
	// CerbosImage is the fully pinned PDP reference — tag AND digest, so a re-pointed tag
	// cannot change which build a run tested against.
	CerbosImage        string
	Actions            ActionsFile
	Seeds              SeedsFile
	Derived            DerivedFile
	OracleActions      []string
	ThrowingActions    []AdapterEntry
	NullOmittedActions []AdapterEntry
}

// seedKeys is the exact set of seeds.json row keys this harness consumes. `note` is corpus prose
// and is the one documented exclusion.
//
// The same parsed seed feeds the stored row AND the check() oracle, so a key this harness does not
// know about would vanish from both sides at once and the differential would agree for the wrong
// reason — the projection trap conformance/README.md describes for actions.json, applied to the
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

// cerbosImageRepository is the PDP image the corpus pins. The tag comes from CERBOS_VERSION and
// the digest from CERBOS_IMAGE_DIGEST; conformance/scripts/validate-corpus.sh asserts the two
// agree everywhere they are restated.
const cerbosImageRepository = "ghcr.io/cerbos/cerbos"

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

	readJSONStrict(tb, filepath.Join(dir, "seeds.json"), &c.Seeds)
	readJSONStrict(tb, filepath.Join(dir, "derived-fields.json"), &c.Derived)
	readJSON(tb, filepath.Join(dir, "actions.json"), &c.Actions)
	assertCorpusCoverage(tb, dir, c)

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

	for _, entry := range unsupported {
		requireMessage(tb, fmt.Sprintf("adapterUnsupported.%s.%s", adapterName, entry.Action), entry.Message)
		c.ThrowingActions = append(c.ThrowingActions, entry)
	}
	for _, shape := range c.Actions.ExpectedUnsupported {
		if !supportedExpectedSet[shape.Action] {
			message := shape.Messages[adapterName]
			requireMessage(tb,
				fmt.Sprintf("expectedUnsupported.%s.messages.%s", shape.Action, adapterName), message)
			c.ThrowingActions = append(c.ThrowingActions, AdapterEntry{
				Action:  shape.Action,
				Reason:  shape.Shape,
				Message: message,
			})
		}
	}

	// Every adapter must reject these, so the message map names the whole roster and this
	// harness resolves its own entry exactly as it does for a throwing action.
	for _, entry := range c.Actions.NullRepresentationOmitted {
		message := entry.Messages[adapterName]
		requireMessage(tb,
			fmt.Sprintf("nullRepresentationOmitted.%s.messages.%s", entry.Action, adapterName), message)
		entry.Message = message
		c.NullOmittedActions = append(c.NullOmittedActions, entry)
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
			"actions.json pins no throw message for %s: the throw suite would accept a failure for any reason",
			label)
	}
	return nil
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

// OracleComparedActions is the set of actions this adapter actually compares against the check()
// oracle. It applies the same skip the oracle run applies, so the two cannot drift: today no
// knownDivergences action is also a conformance action, and the subtraction is a no-op — but a
// divergence registered on one later must drop out of both at once, not just the run. The
// degeneracy guard asserts membership against this, so a guard entry that guards nothing fails
// loudly instead of going inert (cerbos/query-plan-adapters#324).
func (c *Corpus) OracleComparedActions() map[string]bool {
	compared := make(map[string]bool, len(c.OracleActions))
	for _, action := range c.OracleActions {
		if !c.SkippedActions[action] {
			compared[action] = true
		}
	}
	return compared
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

// assertCorpusCoverage proves the harness consumes every seed key and every derived field the
// corpus defines, and nothing it does not. Strict decoding alone cannot do this: it rejects an
// added key but says nothing about one that disappears, and a disappeared key decodes to its zero
// value on both sides of the differential.
func assertCorpusCoverage(tb testing.TB, dir string, c *Corpus) {
	tb.Helper()

	var raw struct {
		Seeds []map[string]json.RawMessage `json:"seeds"`
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
