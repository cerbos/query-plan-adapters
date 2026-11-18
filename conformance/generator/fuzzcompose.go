package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// fuzz-compose builds cases from plans already recorded, with no PDP. CEL's && allows a row exactly
// when both operands allow it, and || when either does, errors included, so a plan built as
// and(P, Q) or or(P, Q) from two recorded plans has an exact oracle: the intersection or union of
// their allowed rows. The result is a fuzz directory every harness replays as it replays `fuzz`.
//
//	go -C conformance/generator run . fuzz-compose -seed 7 -count 500 -out /tmp/compose
//	go -C conformance/generator run . fuzz-compose -seed 7 -from /tmp/fuzz -out /tmp/compose

// compNode is one node of a composed plan: a recorded case (Leaf), or and/or over Kids.
type compNode struct {
	Op   string
	Leaf string
	Kids []*compNode
}

func (n *compNode) String() string {
	if n.Op == "" {
		return n.Leaf
	}
	parts := make([]string, len(n.Kids))
	for i, k := range n.Kids {
		parts[i] = k.String()
	}
	return n.Op + "(" + strings.Join(parts, ", ") + ")"
}

func (n *compNode) asFnode() *fnode {
	if n.Op == "" {
		return &fnode{Op: "atom", CEL: "case(" + n.Leaf + ")", From: n.Leaf}
	}
	out := &fnode{Op: n.Op}
	for _, k := range n.Kids {
		out.Kids = append(out.Kids, k.asFnode())
	}
	return out
}

func (n *compNode) condition(byID map[string]*Golden) any {
	if n.Op == "" {
		return deepCopyJSON(byID[n.Leaf].Plan["condition"])
	}
	operands := make([]any, len(n.Kids))
	for i, k := range n.Kids {
		operands[i] = k.condition(byID)
	}
	return map[string]any{"expression": map[string]any{"operator": n.Op, "operands": operands}}
}

func (n *compNode) allowed(byID map[string]*Golden) map[string]bool {
	if n.Op == "" {
		out := map[string]bool{}
		for _, id := range byID[n.Leaf].Allowed {
			out[id] = true
		}
		return out
	}
	acc := n.Kids[0].allowed(byID)
	for _, k := range n.Kids[1:] {
		next := k.allowed(byID)
		for id := range acc {
			if n.Op == "and" && !next[id] {
				delete(acc, id)
			}
		}
		if n.Op == "or" {
			for id := range next {
				acc[id] = true
			}
		}
	}
	return acc
}

func runFuzzCompose(args []string) error {
	fset := flag.NewFlagSet("fuzz-compose", flag.ExitOnError)
	seed := fset.Uint64("seed", 1, "the generator seed; the same seed and inputs give the same cases")
	count := fset.Int("count", 500, "how many composed cases to write")
	from := fset.String("from", "", "a fuzz directory to compose its recorded cases (default: the corpus goldens)")
	out := fset.String("out", "", "the directory to write into; must be empty or missing, and outside conformance/")
	confFlag := fset.String("conformance", "", "the conformance directory (default: located from the working directory)")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("fuzz-compose: -out is required")
	}
	if *count < 1 {
		return errors.New("fuzz-compose: -count must be at least 1")
	}
	conf, err := conformanceDirOr(*confFlag)
	if err != nil {
		return err
	}
	if err := ensureEmptyOutside(*out, conf); err != nil {
		return err
	}

	root, tags, rows, err := composeInputs(conf, *from)
	if err != nil {
		return err
	}
	in := &fuzzInputs{}
	if *from != "" {
		if err := in.useFuzzDataset(*from); err != nil {
			return err
		}
	}
	byTag := map[string]map[string]*Golden{}
	for _, tag := range tags {
		if byTag[tag], err = readGoldenDir(filepath.Join(root, tag)); err != nil {
			return err
		}
	}
	failed := map[string]bool{}
	if *from != "" {
		legs, err := readLegResults(*from)
		if err != nil {
			return err
		}
		for _, l := range legs {
			for _, r := range l.Results {
				if failing(r.Verdict) {
					failed[r.ID] = true
				}
			}
		}
	}
	leaves := composable(byTag, tags, failed)
	if len(leaves) < 2 {
		return fmt.Errorf("fuzz-compose: %d recorded cases can be composed; need at least 2", len(leaves))
	}

	rng := rand.New(rand.NewPCG(*seed, 0xc2b2ae3d27d4eb4f))
	m := fuzzManifest{Seed: *seed, Count: *count, Tags: tags, Dropped: map[string]string{}, Kind: "compose", From: *from}
	rec := &fuzzRecording{goldens: map[string][]*Golden{}, dataset: in.dataset}
	seen := map[string]bool{}
	for i := 0; i < *count; i++ {
		id := fmt.Sprintf("compose/s%d/%04d", *seed, i)
		tree := composeTree(rng, leaves, 2)
		for try := 0; try < 8 && degenerate(tree, byTag, tags); try++ {
			tree = composeTree(rng, leaves, 2)
		}
		desc := tree.String()
		if seen[desc] {
			m.Dropped[id] = "the same composition as an earlier case"
			continue
		}
		seen[desc] = true
		rec.cases = append(rec.cases, &fuzzCase{ID: id, Allow: tree.asFnode()})
		for _, tag := range tags {
			rec.goldens[tag] = append(rec.goldens[tag], composedGolden(id, tag, *seed, tree, byTag[tag], rows))
		}
	}
	if err := writeFuzzDir(*out, m, rec); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "==> fuzz-compose seed %d: %d cases from %d recorded plans in %s\n", *seed, len(rec.cases), len(leaves), *out)
	return nil
}

// composeInputs is where the recorded goldens live, which PDP tags they cover, and how many rows the
// dataset they were recorded against holds.
func composeInputs(conf, from string) (string, []string, int, error) {
	if from == "" {
		in, err := loadFuzzInputs(conf)
		if err != nil {
			return "", nil, 0, err
		}
		return filepath.Join(conf, "golden"), in.tags(), len(in.ds.Resources), nil
	}
	var m fuzzManifest
	if err := decodeJSONFile(filepath.Join(from, "manifest.json"), &m); err != nil {
		return "", nil, 0, err
	}
	dsDir := conf
	if _, err := os.Stat(filepath.Join(from, "seeds.json")); err == nil {
		dsDir = from
	}
	ds, err := loadDataset(dsDir)
	if err != nil {
		return "", nil, 0, err
	}
	return from, m.Tags, len(ds.Resources), nil
}

func readGoldenDir(dir string) (map[string]*Golden, error) {
	out := map[string]*Golden{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return err
		}
		var g Golden
		if err := decodeJSONFile(path, &g); err != nil {
			return err
		}
		out[g.ID] = &g
		return nil
	})
	return out, err
}

// composable is every case whose recorded plan is a condition that judges adapters on every tag:
// no plan error, no declared planner divergence, and not folded to always allowed or denied. A case
// some leg of the -from run already failed is left out, so a composed failure is a new one.
func composable(byTag map[string]map[string]*Golden, tags []string, failed map[string]bool) []string {
	var ids []string
	for id := range byTag[tags[0]] {
		ok := !failed[id]
		for _, tag := range tags {
			g := byTag[tag][id]
			if g == nil || g.PlanError != nil || g.PlannerDivergence != nil || g.Plan["kind"] != "KIND_CONDITIONAL" || g.Plan["condition"] == nil {
				ok = false
				break
			}
		}
		if ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// composeTree draws an and/or tree whose leaves are recorded cases, at most depth operators deep.
func composeTree(rng *rand.Rand, leaves []string, depth int) *compNode {
	n := &compNode{Op: []string{"and", "or"}[rng.IntN(2)]}
	for range 2 + rng.IntN(2) {
		if depth > 1 && rng.IntN(3) == 0 {
			n.Kids = append(n.Kids, composeTree(rng, leaves, depth-1))
			continue
		}
		n.Kids = append(n.Kids, &compNode{Leaf: leaves[rng.IntN(len(leaves))]})
	}
	return n
}

// degenerate reports whether the tree allows no row on some tag. Most random conjunctions allow
// nothing, so these are redrawn a few times.
func degenerate(tree *compNode, byTag map[string]map[string]*Golden, tags []string) bool {
	for _, tag := range tags {
		n := len(tree.allowed(byTag[tag]))
		if n == 0 {
			return true
		}
	}
	return false
}

func composedGolden(id, tag string, seed uint64, tree *compNode, byID map[string]*Golden, rows int) *Golden {
	set := tree.allowed(byID)
	allowed := make([]string, 0, len(set))
	for r := range set {
		allowed = append(allowed, r)
	}
	sort.Strings(allowed)
	return &Golden{
		Allowed: allowed,
		ID:      id,
		Intent:  tree.String(),
		Oracle:  oracleOf(len(allowed), rows),
		PDP:     tag,
		Plan:    map[string]any{"kind": "KIND_CONDITIONAL", "condition": tree.condition(byID)},
		Request: goldenRequest{Action: id, ResourceKind: resourceKind},
		Rules:   []map[string]any{},
		Tier:    "adversarial",
		Trap:    fmt.Sprintf("Composed by fuzz-compose -seed %d from recorded plans, with no PDP: and allows the intersection of its operands' rows, or their union. A translation that is right for each operand alone and wrong together fails here.", seed),
	}
}

func deepCopyJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = deepCopyJSON(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = deepCopyJSON(x)
		}
		return out
	}
	return v
}
