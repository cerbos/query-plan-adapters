package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// Steering points a fuzz run at an adapter's weak spots. Every generated leaf records its features:
// the grammar production it came from and the attributes it reads, or the corpus case it was taken
// from; a case adds the tree operators above its leaves. An earlier run's verdicts then say, per
// feature, in how many cases an adapter translated it. A case that matched translated every feature
// in it, so that count is exact, where blaming a refusal on one feature of the case would be a guess.
// The next run draws each feature in inverse proportion to that count, toward one of two goals:
//
//   - coverage (the default) also weighs each feature by how often the adapter translated it when
//     drawn, so the run spends its cases on shapes the adapter translates but has rarely been
//     checked on, where an over-grant can still hide, and not on shapes it always refuses.
//   - refusals leaves that weight out, so the run piles onto the shapes the adapter refuses most:
//     the queue a hill-climb works through, with the refusal messages to start from.

const (
	steerCoverage = "coverage"
	steerRefusals = "refusals"
	denyFeature   = "tree.deny"
	steerMinBoost = 0.25
	steerMaxBoost = 8
)

func attrFeature(path string) string { return "attr:" + path }
func corpusFeature(id string) string { return "corpus:" + id }
func treeFeature(op string) string   { return "tree." + op }

// featureKind groups features whose counts are compared with each other: the grammar's productions,
// attributes, corpus atoms and tree operators each occur at their own rate.
func featureKind(feature string) string {
	switch {
	case strings.HasPrefix(feature, "attr:"):
		return "attr"
	case strings.HasPrefix(feature, "corpus:"):
		return "corpus"
	case strings.HasPrefix(feature, "tree."):
		return "tree"
	}
	return "grammar"
}

// caseFeatures lists the distinct features of a case, sorted.
func caseFeatures(fc *fuzzCase) []string {
	seen := map[string]bool{}
	var walk func(n *fnode)
	walk = func(n *fnode) {
		if n.Op == "atom" {
			for _, ft := range n.Feats {
				seen[ft] = true
			}
		} else {
			seen[treeFeature(n.Op)] = true
		}
		for _, k := range n.Kids {
			walk(k)
		}
	}
	walk(fc.Allow)
	if fc.Deny != nil {
		seen[denyFeature] = true
		walk(fc.Deny)
	}
	out := make([]string, 0, len(seen))
	for ft := range seen {
		out = append(out, ft)
	}
	sort.Strings(out)
	return out
}

// featureTally is how one feature fared on the legs counted: one count per leg, PDP and case.
type featureTally struct {
	Drawn, Matched, Refused, Failed int
	refusals                        map[string]int
}

// tallyFeatures counts, per feature, the verdicts of the cases holding it, over the legs of the
// named adapters (every leg when adapters is empty).
func tallyFeatures(cases []*fuzzCase, legs []*fuzzLegResults, adapters []string) map[string]*featureTally {
	feats := map[string][]string{}
	for _, fc := range cases {
		feats[fc.ID] = caseFeatures(fc)
	}
	want := map[string]bool{}
	for _, a := range adapters {
		want[a] = true
	}
	tally := map[string]*featureTally{}
	for _, l := range legs {
		if len(want) > 0 && !want[l.Adapter] {
			continue
		}
		for _, r := range l.Results {
			for _, ft := range feats[r.ID] {
				t := tally[ft]
				if t == nil {
					t = &featureTally{refusals: map[string]int{}}
					tally[ft] = t
				}
				t.Drawn++
				switch r.Verdict {
				case verdictMatch:
					t.Matched++
				case verdictRefused:
					t.Refused++
					t.refusals[refusalGist(r.Detail)]++
				default:
					t.Failed++
				}
			}
		}
	}
	return tally
}

// refusalGist shortens a refusal message to its first line, so messages that differ only in the
// expression they quote after it still group.
func refusalGist(detail string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(detail), "\n")
	if r := []rune(line); len(r) > 120 {
		line = string(r[:120]) + "…"
	}
	return line
}

// topRefusal is the refusal message a feature's refused cases gave most, and how often.
func (t *featureTally) topRefusal() (string, int) {
	best, n := "", 0
	for msg, c := range t.refusals {
		if c > n || (c == n && msg < best) {
			best, n = msg, c
		}
	}
	return best, n
}

// steerBoosts turns a tally into a boost for every feature the fuzzer can draw. Within each kind the
// need is (median matched + 1) / (matched + 1); toward coverage it is multiplied by the feature's
// match rate over the kind's median rate, both smoothed so a feature never drawn counts as translated
// half the time. The boost is held between steerMinBoost and steerMaxBoost.
func steerBoosts(universe []string, tally map[string]*featureTally, goal string) map[string]float64 {
	matched := func(ft string) float64 {
		if t := tally[ft]; t != nil {
			return float64(t.Matched)
		}
		return 0
	}
	rate := func(ft string) float64 {
		if t := tally[ft]; t != nil {
			return float64(t.Matched+1) / float64(t.Drawn+2)
		}
		return 0.5
	}
	byKind := map[string][]string{}
	for _, ft := range universe {
		byKind[featureKind(ft)] = append(byKind[featureKind(ft)], ft)
	}
	median := func(fts []string, by func(string) float64) float64 {
		xs := make([]float64, len(fts))
		for i, ft := range fts {
			xs[i] = by(ft)
		}
		sort.Float64s(xs)
		return xs[len(xs)/2]
	}
	boosts := map[string]float64{}
	for _, fts := range byKind {
		midMatched, midRate := median(fts, matched), median(fts, rate)
		for _, ft := range fts {
			b := (midMatched + 1) / (matched(ft) + 1)
			if goal == steerCoverage {
				b *= rate(ft) / midRate
			}
			boosts[ft] = min(max(b, steerMinBoost), steerMaxBoost)
		}
	}
	return boosts
}

// features lists every feature this fuzzer can draw.
func (f *fuzzer) features() []string {
	seen := map[string]bool{denyFeature: true}
	for _, op := range []string{"and", "or", "not", "cond"} {
		seen[treeFeature(op)] = true
	}
	for _, p := range productions {
		seen[p.name] = true
	}
	for _, group := range [][]string{fuzzStrings, fuzzInts, fuzzDoubles, fuzzBools, fuzzStrLists, fuzzTimes, fuzzPStrings, fuzzPStrLists, fuzzFixedPaths} {
		for _, p := range group {
			seen[attrFeature(p)] = true
		}
	}
	for _, a := range f.atoms {
		seen[corpusFeature(a.ID)] = true
	}
	out := make([]string, 0, len(seen))
	for ft := range seen {
		out = append(out, ft)
	}
	sort.Strings(out)
	return out
}

// fuzzSteer is what a steered run was steered by, kept in its manifest.
type fuzzSteer struct {
	From     []string           `json:"from"`
	Goal     string             `json:"goal"`
	Adapters []string           `json:"adapters,omitempty"`
	Boosts   map[string]float64 `json:"boosts"`
}

// steerFrom tallies the earlier fuzz directories' verdicts for the named adapters and sets the
// fuzzer's boosts from them.
func (f *fuzzer) steerFrom(dirs, adapters []string, goal string) (*fuzzSteer, error) {
	if goal != steerCoverage && goal != steerRefusals {
		return nil, fmt.Errorf("fuzz: -steer-goal is %q or %q, not %q", steerCoverage, steerRefusals, goal)
	}
	var cases []*fuzzCase
	var legs []*fuzzLegResults
	for _, d := range dirs {
		cs, err := readFuzzCases(d)
		if err != nil {
			return nil, err
		}
		ls, err := readLegResults(d)
		if err != nil {
			return nil, err
		}
		if len(ls) == 0 {
			return nil, fmt.Errorf("fuzz: -steer %s has no results; replay it through a harness first", filepath.Join(d, "results"))
		}
		cases = append(cases, cs...)
		legs = append(legs, ls...)
	}
	tally := tallyFeatures(cases, legs, adapters)
	if len(tally) == 0 {
		return nil, fmt.Errorf("fuzz: -steer %s holds no verdicts for %s, or its cases predate features", strings.Join(dirs, ","), strings.Join(adapters, ","))
	}
	f.steer = steerBoosts(f.features(), tally, goal)
	return &fuzzSteer{From: dirs, Goal: goal, Adapters: adapters, Boosts: f.steer}, nil
}

// weakSpots renders, per adapter, the grammar productions, attributes and corpus atoms behind most
// of its refusals, with the refusal message they gave most. A refused case is put down to the feature in it
// that the adapter translated least often anywhere in the run, so a feature merely drawn beside an
// untranslatable one is not blamed for it. Counts are per leg, PDP and case.
func weakSpots(cases []*fuzzCase, legs []*fuzzLegResults, limit int) string {
	feats := map[string][]string{}
	for _, fc := range cases {
		for _, ft := range caseFeatures(fc) {
			if featureKind(ft) != "tree" {
				feats[fc.ID] = append(feats[fc.ID], ft)
			}
		}
	}
	var adapters []string
	seen := map[string]bool{}
	for _, l := range legs {
		if !seen[l.Adapter] {
			seen[l.Adapter] = true
			adapters = append(adapters, l.Adapter)
		}
	}
	var b strings.Builder
	for _, a := range adapters {
		tally := tallyFeatures(cases, legs, []string{a})
		// Smoothed, so a feature drawn twice and refused twice is not taken for one refused every time.
		rate := func(ft string) float64 { return float64(tally[ft].Matched+1) / float64(tally[ft].Drawn+2) }
		blamed := map[string]*featureTally{}
		for _, l := range legs {
			if l.Adapter != a {
				continue
			}
			for _, r := range l.Results {
				if r.Verdict != verdictRefused || len(feats[r.ID]) == 0 {
					continue
				}
				culprit := feats[r.ID][0]
				for _, ft := range feats[r.ID][1:] {
					if rate(ft) < rate(culprit) {
						culprit = ft
					}
				}
				t := blamed[culprit]
				if t == nil {
					t = &featureTally{refusals: map[string]int{}}
					blamed[culprit] = t
				}
				t.Refused++
				t.refusals[refusalGist(r.Detail)]++
			}
		}
		var spots []string
		for ft := range blamed {
			spots = append(spots, ft)
		}
		if len(spots) == 0 {
			continue
		}
		sort.Slice(spots, func(i, j int) bool {
			if blamed[spots[i]].Refused != blamed[spots[j]].Refused {
				return blamed[spots[i]].Refused > blamed[spots[j]].Refused
			}
			return spots[i] < spots[j]
		})
		if len(spots) > limit {
			spots = spots[:limit]
		}
		fmt.Fprintf(&b, "\n### %s\n\n| Feature | Drawn | Matched | Refusals put down to it | Most common of those refusals |\n|---|---:|---:|---:|---|\n", a)
		for _, ft := range spots {
			t := tally[ft]
			msg, n := blamed[ft].topRefusal()
			fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %s (%d) |\n", ft, t.Drawn, t.Matched, blamed[ft].Refused, tableCell(msg), n)
		}
	}
	return b.String()
}
