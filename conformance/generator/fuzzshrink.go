package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// runFuzzShrink reduces one failing fuzz case to a smaller one that still fails on the same leg.
// Each round records every one-step reduction of the current case in one batch, replays the batch
// through the leg's harness, and keeps the smallest reduction that still fails. It stops when none
// does, and writes the result as a case ready to paste into conformance/cases/.
func runFuzzShrink(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fuzz-shrink", flag.ExitOnError)
	dir := fs.String("dir", "", "the fuzz directory the failing case was recorded into")
	id := fs.String("case", "", "the failing case id, fuzz/s<seed>/<index>")
	adapter := fs.String("adapter", "", "the adapter whose harness fails it, as run-harness.sh names it")
	store := fs.String("store", "", "the store the harness reports the failure under, as its results file names it")
	rounds := fs.Int("rounds", 40, "the most reduction rounds to run")
	confFlag := fs.String("conformance", "", "the conformance directory (default: located from the working directory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *id == "" || *adapter == "" || *store == "" {
		return errors.New("fuzz-shrink: -dir, -case, -adapter and -store are required")
	}
	conf, err := conformanceDirOr(*confFlag)
	if err != nil {
		return err
	}
	in, err := loadFuzzInputs(conf)
	if err != nil {
		return err
	}
	var m fuzzManifest
	if err := decodeJSONFile(filepath.Join(*dir, "manifest.json"), &m); err != nil {
		return err
	}
	cases, err := readFuzzCases(*dir)
	if err != nil {
		return err
	}
	var current *fuzzCase
	for _, fc := range cases {
		if fc.ID == *id {
			current = fc
		}
	}
	if current == nil {
		return fmt.Errorf("fuzz-shrink: %s has no case %q", *dir, *id)
	}

	leg := legRunner{conf: conf, adapter: *adapter, store: *store}
	verdict, err := leg.failingVerdict(ctx, in, []*fuzzCase{current}, m.Seed)
	if err != nil {
		return err
	}
	if verdict[current.ID] == "" {
		return fmt.Errorf("fuzz-shrink: %s passes on %s / %s when replayed alone; nothing to shrink", *id, *adapter, *store)
	}
	fmt.Fprintf(os.Stderr, "==> %s: %s on %s / %s, size %d\n", *id, verdict[current.ID], *adapter, *store, current.size())

	for round := 1; round <= *rounds; round++ {
		cands := reductions(current)
		if len(cands) == 0 {
			break
		}
		for i, c := range cands {
			c.ID = fmt.Sprintf("fuzz/shrink/r%02d-%03d", round, i)
		}
		got, err := leg.failingVerdict(ctx, in, cands, m.Seed)
		if err != nil {
			return err
		}
		var next *fuzzCase
		for _, c := range cands { // cands is smallest first
			if got[c.ID] != "" {
				next = c
				break
			}
		}
		if next == nil {
			break
		}
		fmt.Fprintf(os.Stderr, "==> round %d: %s, size %d\n", round, got[next.ID], next.size())
		current = next
	}

	snippet, err := caseSnippet(current, *id, *adapter, *store)
	if err != nil {
		return err
	}
	out := filepath.Join(*dir, "shrunk", strings.ReplaceAll(*id, "/", "-")+".yaml")
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(out, snippet, 0o644); err != nil {
		return err
	}
	fmt.Print(string(snippet))
	fmt.Fprintf(os.Stderr, "==> wrote %s\n", out)
	return nil
}

// reductions lists every case one step smaller than fc, smallest first: the DENY rule dropped or
// reduced, and in either tree a node replaced by one of its children or an and/or losing one operand.
func reductions(fc *fuzzCase) []*fuzzCase {
	var out []*fuzzCase
	if fc.Deny != nil {
		out = append(out, &fuzzCase{Allow: fc.Allow.clone()})
		for _, d := range treeReductions(fc.Deny) {
			out = append(out, &fuzzCase{Allow: fc.Allow.clone(), Deny: d})
		}
	}
	for _, a := range treeReductions(fc.Allow) {
		var deny *fnode
		if fc.Deny != nil {
			deny = fc.Deny.clone()
		}
		out = append(out, &fuzzCase{Allow: a, Deny: deny})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].size() < out[j].size() })
	return out
}

func treeReductions(n *fnode) []*fnode {
	var out []*fnode
	for _, k := range n.Kids {
		out = append(out, k.clone())
	}
	if (n.Op == "and" || n.Op == "or") && len(n.Kids) > 2 {
		for i := range n.Kids {
			c := &fnode{Op: n.Op}
			for j, k := range n.Kids {
				if j != i {
					c.Kids = append(c.Kids, k.clone())
				}
			}
			out = append(out, c)
		}
	}
	for i, k := range n.Kids {
		for _, r := range treeReductions(k) {
			c := n.clone()
			c.Kids[i] = r
			out = append(out, c)
		}
	}
	return out
}

// legRunner replays fuzz cases through one adapter's harness.
type legRunner struct {
	conf, adapter, store string
}

// harnessStores are the adapters whose one run-harness.sh leg replays every store and writes one
// results file per store.
var harnessStores = map[string]bool{"sqlalchemy": true, "ent": true, "pgx": true}

// failingVerdict records cases into a scratch directory, replays them through the leg, and returns
// each case's verdict on that leg when it fails, or "" when it passes or was dropped unrecorded.
func (l legRunner) failingVerdict(ctx context.Context, in *fuzzInputs, cases []*fuzzCase, seed uint64) (map[string]string, error) {
	scratch, err := os.MkdirTemp("", "fuzz-shrink-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)
	rec, dropped, err := recordFuzz(ctx, in, cases, seed)
	if err != nil {
		return nil, err
	}
	if len(rec.cases) == 0 {
		return map[string]string{}, nil
	}
	if err := writeFuzzDir(scratch, fuzzManifest{Seed: seed, Count: len(cases), Tags: in.tags(), Dropped: dropped}, rec); err != nil {
		return nil, err
	}
	harnessStore := l.store
	if harnessStores[l.adapter] {
		harnessStore = "all"
	}
	cmd := exec.CommandContext(ctx, filepath.Join(l.conf, "scripts", "run-harness.sh"), l.adapter, harnessStore)
	cmd.Env = append(os.Environ(), "QPA_FUZZ_DIR="+scratch)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	// A failing case fails the harness, so its exit status says nothing the results file does not.
	_ = cmd.Run()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var res fuzzLegResults
	path := filepath.Join(scratch, "results", l.adapter+"-"+l.store+".json")
	if err := decodeJSONFile(path, &res); err != nil {
		return nil, fmt.Errorf("the %s / %s harness wrote no fuzz results: %w", l.adapter, l.store, err)
	}
	out := map[string]string{}
	for _, r := range res.Results {
		if failing(r.Verdict) {
			out[r.ID] = r.Verdict
		}
	}
	return out, nil
}

// caseSnippet renders the shrunk case as a conformance/cases/<area>.yaml entry. The id, intent and
// trap are a person's to write: they say what the shape is and what goes wrong, which the fuzzer
// does not know.
func caseSnippet(fc *fuzzCase, from, adapter, store string) ([]byte, error) {
	c := fc.asCase(0)
	entry := map[string]any{
		"id":     "<area>/<operator>/<variant>",
		"tier":   "adversarial",
		"intent": "TODO: one line on what the shape is.",
		"trap":   fmt.Sprintf("TODO: one line on what the translation gets wrong. Found by fuzzing (%s, %s / %s).", from, adapter, store),
	}
	if fc.Deny == nil {
		entry["condition"] = c.Rules[0]["condition"]
	} else {
		rules := make([]map[string]any, len(c.Rules))
		for i, r := range c.Rules {
			rules[i] = map[string]any{"effect": r["effect"], "condition": r["condition"]}
		}
		entry["rules"] = rules
	}
	node, err := orderedMapping(entry)
	if err != nil {
		return nil, err
	}
	seq := &yaml.Node{Kind: yaml.SequenceNode, Content: []*yaml.Node{node}}
	return yaml.Marshal(seq)
}
