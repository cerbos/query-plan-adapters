package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
)

// fuzz-exhaustive records every condition the grammar can draw up to a size, instead of a sample:
// where a seeded run says "no hole found in these 500", a clean exhaustive run says "no hole in any
// shape of this size". A shape is one path through the grammar's structural choices (production,
// attribute, operator, tree operator); its literals are drawn from a seeded stream, so -lits k
// records k draws of each.
//
//	size 1: every atom, as the whole ALLOW condition
//	size 2: !atom, and ALLOW atom with DENY atom' for every pair of production representatives
//	size 3: atom && atom' and atom || atom' for every unordered pair of representatives, and !!atom
//
// A representative is a production's first shape, so the pairs cover every production against
// every other while the atoms alone cover every attribute and operator.

// enumerator is a chooser that walks every sequence of choices a draw makes, depth first. IntN
// replays the current path and extends it with 0; next moves to the following path.
type enumerator struct {
	path, arity []int
	pos         int
}

func (e *enumerator) IntN(n int) int {
	if e.pos == len(e.path) {
		e.path = append(e.path, 0)
		e.arity = append(e.arity, n)
	}
	e.arity[e.pos] = n
	v := e.path[e.pos]
	e.pos++
	return v
}

func (e *enumerator) Float64() float64 { panic("fuzz-exhaustive: a structural draw read Float64") }

// next advances to the next unvisited path, or reports false when every path has been drawn.
func (e *enumerator) next() bool {
	e.path, e.arity = e.path[:e.pos], e.arity[:e.pos]
	for i := len(e.path) - 1; i >= 0; i-- {
		if e.path[i]+1 < e.arity[i] {
			e.path[i]++
			e.path, e.arity = e.path[:i+1], e.arity[:i+1]
			e.pos = 0
			return true
		}
	}
	return false
}

// exhaustiveAtoms draws every shape of every production, lits times each, deduplicated by CEL. The
// second result is each production's first shape.
func (f *fuzzer) exhaustiveAtoms(seed uint64, lits int) (atoms, reps []*fnode) {
	seen := map[string]bool{}
	for pi, p := range productions {
		e := &enumerator{}
		f.rng = e
		first := true
		for {
			for k := range lits {
				f.lit = rand.New(rand.NewPCG(seed, uint64(pi)<<32|uint64(len(atoms))<<8|uint64(k)))
				e.pos = 0
				f.feats = nil
				cel := p.draw(f)
				n := &fnode{Op: "atom", CEL: cel, Feats: append([]string{p.name}, f.feats...)}
				if first {
					reps = append(reps, n)
					first = false
				}
				if !seen[cel] {
					seen[cel] = true
					atoms = append(atoms, n)
				}
			}
			if !e.next() {
				break
			}
		}
	}
	return atoms, reps
}

func exhaustiveCases(atoms, reps []*fnode, size int) []*fuzzCase {
	var cases []*fuzzCase
	add := func(allow, deny *fnode) {
		cases = append(cases, &fuzzCase{ID: fmt.Sprintf("fuzz/x%d/%05d", size, len(cases)), Allow: allow, Deny: deny})
	}
	not := func(n *fnode) *fnode { return &fnode{Op: "not", Kids: []*fnode{n.clone()}} }
	for _, a := range atoms {
		add(a.clone(), nil)
	}
	if size < 2 {
		return cases
	}
	for _, a := range atoms {
		add(not(a), nil)
	}
	for _, a := range reps {
		for _, b := range reps {
			add(a.clone(), b.clone())
		}
	}
	if size < 3 {
		return cases
	}
	for i, a := range reps {
		add(not(not(a)), nil)
		for _, b := range reps[i+1:] {
			for _, op := range []string{"and", "or"} {
				add(&fnode{Op: op, Kids: []*fnode{a.clone(), b.clone()}}, nil)
			}
		}
	}
	return cases
}

func runFuzzExhaustive(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fuzz-exhaustive", flag.ExitOnError)
	size := fs.Int("size", 3, "the largest condition size to enumerate, in nodes: 1, 2 or 3")
	lits := fs.Int("lits", 1, "how many literal draws to record of each shape")
	seed := fs.Uint64("seed", 1, "seeds the literal draws, the generated rows and the edge rows")
	rows := fs.Int("rows", 0, "how many generated rows to add to the dataset (as fuzz -rows)")
	edges := fs.Int("edges", 0, "at most how many rows to add on the edges of the run's literals (as fuzz -edges)")
	list := fs.Bool("list", false, "print the conditions and record nothing")
	out := fs.String("out", "", "the directory to write into; must be empty or missing, and outside conformance/")
	confFlag := fs.String("conformance", "", "the conformance directory (default: located from the working directory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *size < 1 || *size > 3 {
		return errors.New("fuzz-exhaustive: -size must be 1, 2 or 3")
	}
	if *out == "" && !*list {
		return errors.New("fuzz-exhaustive: -out is required")
	}
	conf, err := conformanceDirOr(*confFlag)
	if err != nil {
		return err
	}
	in, err := loadFuzzInputs(conf)
	if err != nil {
		return err
	}
	if *rows > 0 {
		if err := in.generateRows(*seed, *rows, nil, 0); err != nil {
			return err
		}
	}
	f := newFuzzer(*seed, in.corpus, in.ds)
	atoms, reps := f.exhaustiveAtoms(*seed, *lits)
	cases := exhaustiveCases(atoms, reps, *size)
	fmt.Fprintf(os.Stderr, "==> fuzz-exhaustive: %d atoms, %d production representatives, %d cases up to size %d\n", len(atoms), len(reps), len(cases), *size)
	if *list {
		for _, c := range cases {
			fmt.Println(c.ID, c.describe())
		}
		return nil
	}
	if err := ensureEmptyOutside(*out, conf); err != nil {
		return err
	}
	if *edges > 0 {
		if err := in.generateRows(*seed, *rows, edgeValues(f.lits), *edges); err != nil {
			return err
		}
	}
	recorded, dropped, err := recordFuzz(ctx, in, cases, *seed)
	if err != nil {
		return err
	}
	m := fuzzManifest{Seed: *seed, Count: len(cases), Rows: *rows, Edges: *edges, Kind: fmt.Sprintf("exhaustive-%d", *size), Tags: in.tags(), Dropped: dropped}
	if err := writeFuzzDir(*out, m, recorded); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "==> fuzz-exhaustive: kept %d of %d cases in %s\n", len(recorded.cases), len(cases), *out)
	return nil
}
