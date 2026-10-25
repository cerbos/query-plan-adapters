package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The verdicts a harness gives a fuzz case. A refusal is the invariant working; the last three are
// bugs, and an over-grant is an authorization bug.
const (
	verdictMatch      = "match"
	verdictRefused    = "refused"
	verdictOverGrant  = "over-grant"
	verdictUnderGrant = "under-grant"
	verdictError      = "error"
)

var verdicts = map[string]bool{verdictMatch: true, verdictRefused: true, verdictOverGrant: true, verdictUnderGrant: true, verdictError: true}

func failing(verdict string) bool { return verdict != verdictMatch && verdict != verdictRefused }

// fuzzLegResults is <dir>/results/<adapter>-<store>.json, written by a harness in fuzz mode.
type fuzzLegResults struct {
	Adapter string            `json:"adapter"`
	Store   string            `json:"store"`
	Results []fuzzCaseVerdict `json:"results"`
}

type fuzzCaseVerdict struct {
	PDP      string   `json:"pdp"`
	ID       string   `json:"id"`
	Verdict  string   `json:"verdict"`
	Expected []string `json:"expected,omitempty"`
	Returned []string `json:"returned,omitempty"`
	Detail   string   `json:"detail,omitempty"`
}

func (l *fuzzLegResults) leg() string { return l.Adapter + " / " + l.Store }

func readLegResults(dir string) ([]*fuzzLegResults, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "results", "*.json"))
	if err != nil {
		return nil, err
	}
	var legs []*fuzzLegResults
	var errs []error
	for _, p := range paths {
		var l fuzzLegResults
		if err := decodeJSONFile(p, &l); err != nil {
			errs = append(errs, err)
			continue
		}
		if l.Adapter == "" || l.Store == "" {
			errs = append(errs, fmt.Errorf("%s: adapter and store are required", p))
		}
		for _, r := range l.Results {
			if !verdicts[r.Verdict] {
				errs = append(errs, fmt.Errorf("%s: case %s has verdict %q", p, r.ID, r.Verdict))
			}
		}
		legs = append(legs, &l)
	}
	sort.Slice(legs, func(i, j int) bool { return legs[i].leg() < legs[j].leg() })
	return legs, errors.Join(errs...)
}

func runFuzzReport(args []string) error {
	fs := flag.NewFlagSet("fuzz-report", flag.ExitOnError)
	dir := fs.String("dir", "", "the fuzz directory: manifest.json, the goldens and results/")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("fuzz-report: -dir is required")
	}
	var m fuzzManifest
	if err := decodeJSONFile(filepath.Join(*dir, "manifest.json"), &m); err != nil {
		return err
	}
	legs, err := readLegResults(*dir)
	if err != nil {
		return err
	}
	if len(legs) == 0 {
		return fmt.Errorf("fuzz-report: no results under %s", filepath.Join(*dir, "results"))
	}
	cases, err := readFuzzCases(*dir)
	if err != nil {
		return err
	}
	report, failures := fuzzReport(m, cases, legs)
	if err := os.WriteFile(filepath.Join(*dir, "report.md"), []byte(report), 0o644); err != nil {
		return err
	}
	fmt.Print(report)
	if failures > 0 {
		return fmt.Errorf("%d fuzz failures; see %s", failures, filepath.Join(*dir, "report.md"))
	}
	return nil
}

// legFailure is one leg's failing verdict on one case.
type legFailure struct {
	leg string
	r   fuzzCaseVerdict
}

// fuzzReport renders report.md and counts the failures that fail the run: every failing verdict on
// a case some leg answered differently from the rest, and every leg that reported nothing. A case
// every translating leg got wrong in the same way is listed apart and not counted: that agreement
// points at the plan and check() disagreeing, which no adapter can fix. A leg that reports fewer
// results than the run holds is noted with the cases it lacks, since a harness may replay only the
// cases a store shape reads (sqlalchemy's postgresql-pgArray), but a case that hung leaves a gap too.
func fuzzReport(m fuzzManifest, cases []*fuzzCase, legs []*fuzzLegResults) (string, int) {
	describe := map[string]string{}
	for _, fc := range cases {
		describe[fc.ID] = fc.describe()
	}
	want := len(cases) * len(m.Tags)

	type key struct{ pdp, id string }
	byCase := map[key][]legFailure{}
	var b strings.Builder
	failures := 0

	fmt.Fprintf(&b, "# Fuzz report: seed %d\n\n", m.Seed)
	fmt.Fprintf(&b, "%d of %d generated cases kept (manifest.json says why the others were dropped), replayed against PDP %s. Reproduce with `go -C conformance/generator run . fuzz -seed %d -count %d -out <dir>`.\n\n",
		m.Kept, m.Count, strings.Join(m.Tags, " and "), m.Seed, m.Count)
	b.WriteString("| Leg | Match | Refused | Over-grant | Under-grant | Error | Refusal rate |\n|---|---:|---:|---:|---:|---:|---:|\n")
	var partial []string
	for _, l := range legs {
		n := map[string]int{}
		for _, r := range l.Results {
			n[r.Verdict]++
			if failing(r.Verdict) {
				k := key{r.PDP, r.ID}
				byCase[k] = append(byCase[k], legFailure{l.leg(), r})
			}
		}
		rate := 0.0
		if len(l.Results) > 0 {
			rate = 100 * float64(n[verdictRefused]) / float64(len(l.Results))
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %.0f%% |\n", l.leg(), n[verdictMatch], n[verdictRefused], n[verdictOverGrant], n[verdictUnderGrant], n[verdictError], rate)
		switch {
		case len(l.Results) == 0:
			partial = append(partial, fmt.Sprintf("- %s reported no results: its harness replayed nothing.", l.leg()))
			failures++
		case len(l.Results) < want:
			partial = append(partial, fmt.Sprintf("- %s reported %d results; the run holds %d. Missing: %s.", l.leg(), len(l.Results), want, missingCases(l, cases, m.Tags)))
		}
	}
	if len(partial) > 0 {
		b.WriteString("\n## Partial legs\n\n" + strings.Join(partial, "\n") + "\n")
	}

	keys := make([]key, 0, len(byCase))
	for k := range byCase {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].id != keys[j].id {
			return keys[i].id < keys[j].id
		}
		return keys[i].pdp < keys[j].pdp
	})
	var adapter, planner []key
	for _, k := range keys {
		if suspectPlanner(byCase[k], legs, k.pdp, k.id) {
			planner = append(planner, k)
			continue
		}
		adapter = append(adapter, k)
		failures += len(byCase[k])
	}

	section := func(title, lead string, ks []key) {
		if len(ks) == 0 {
			return
		}
		fmt.Fprintf(&b, "\n## %s\n\n%s\n", title, lead)
		for _, k := range ks {
			fmt.Fprintf(&b, "\n### %s (PDP %s)\n\n`%s`\n\n", k.id, k.pdp, describe[k.id])
			b.WriteString("| Leg | Verdict | Expected | Returned | Detail |\n|---|---|---|---|---|\n")
			for _, f := range byCase[k] {
				fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", f.leg, f.r.Verdict, idList(f.r.Expected), idList(f.r.Returned), tableCell(f.r.Detail))
			}
		}
	}
	section("Adapter failures", fmt.Sprintf("%d cases, by PDP. Shrink one with `go -C conformance/generator run . fuzz-shrink -dir <dir> -case <id> -adapter <adapter> -store <store>`.", len(adapter)), adapter)
	section("Suspected planner divergences", fmt.Sprintf("%d cases, by PDP, that every leg that translated them answered with the same wrong rows. Not counted as failures: check whether the plan and `check()` disagree before blaming an adapter.", len(planner)), planner)
	if len(keys) == 0 {
		b.WriteString("\nNo failures.\n")
	}
	return b.String(), failures
}

// missingCases names, up to ten, the cases a leg has no verdict for: a case that hung until the test
// runner's timeout leaves no verdict behind, which this is the only place to see.
func missingCases(l *fuzzLegResults, cases []*fuzzCase, tags []string) string {
	seen := map[string]bool{}
	for _, r := range l.Results {
		seen[r.PDP+" "+r.ID] = true
	}
	var missing []string
	for _, fc := range cases {
		for _, tag := range tags {
			if !seen[tag+" "+fc.ID] {
				missing = append(missing, fmt.Sprintf("%s (PDP %s)", fc.ID, tag))
			}
		}
	}
	if len(missing) > 10 {
		missing = append(missing[:10], fmt.Sprintf("and %d more", len(missing)-10))
	}
	return strings.Join(missing, ", ")
}

// suspectPlanner reports whether two or more legs translated the case, and every one that did
// returned the same wrong rows.
func suspectPlanner(fails []legFailure, legs []*fuzzLegResults, pdp, id string) bool {
	translated := 0
	for _, l := range legs {
		for _, r := range l.Results {
			if r.PDP == pdp && r.ID == id && r.Verdict != verdictRefused {
				translated++
			}
		}
	}
	if translated < 2 || len(fails) != translated {
		return false
	}
	first := strings.Join(fails[0].r.Returned, ",")
	for _, f := range fails {
		if f.r.Verdict == verdictError || strings.Join(f.r.Returned, ",") != first {
			return false
		}
	}
	return true
}

func tableCell(s string) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	s = strings.ReplaceAll(s, "|", `\|`)
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
