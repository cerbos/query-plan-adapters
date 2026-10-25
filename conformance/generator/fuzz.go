package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

// The fuzz subcommands. None of them writes under conformance/: a fuzz run is recorded into a
// directory of its own, which harnesses replay when QPA_FUZZ_DIR names it (README.md, "Fuzzing").
//
//	go -C conformance/generator run . fuzz -seed 7 -count 500 -out /tmp/fuzz
//	go -C conformance/generator run . fuzz-report -dir /tmp/fuzz
//	go -C conformance/generator run . fuzz-shrink -dir /tmp/fuzz -case fuzz/s7/0042 -adapter drizzle -store sqlite

// fuzzManifest is <out>/manifest.json: what a fuzz directory holds and how to remake it.
type fuzzManifest struct {
	Seed    uint64            `json:"seed"`
	Count   int               `json:"count"`
	Tags    []string          `json:"tags"`
	Kept    int               `json:"kept"`
	Dropped map[string]string `json:"dropped"`
}

// fuzzCaseFile is <out>/cases.json: every kept case's condition trees, for fuzz-shrink.
const fuzzCaseFile = "cases.json"

func runSubcommand(name string, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch name {
	case "fuzz":
		return runFuzz(ctx, args)
	case "fuzz-report":
		return runFuzzReport(args)
	case "fuzz-shrink":
		return runFuzzShrink(ctx, args)
	}
	return fmt.Errorf("unknown subcommand %q", name)
}

func runFuzz(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fuzz", flag.ExitOnError)
	seed := fs.Uint64("seed", 1, "the generator seed; the same seed and PDP pins give the same cases and goldens")
	count := fs.Int("count", 500, "how many cases to generate before dropping the ones no adapter can be judged on")
	out := fs.String("out", "", "the directory to write into; must be empty or missing, and outside conformance/")
	confFlag := fs.String("conformance", "", "the conformance directory (default: located from the working directory)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("fuzz: -out is required")
	}
	if *count < 1 {
		return errors.New("fuzz: -count must be at least 1")
	}
	conf, err := conformanceDirOr(*confFlag)
	if err != nil {
		return err
	}
	if err := ensureEmptyOutside(*out, conf); err != nil {
		return err
	}
	in, err := loadFuzzInputs(conf)
	if err != nil {
		return err
	}

	if missing := unreadGrammarPaths(in.corpus); len(missing) > 0 {
		return fmt.Errorf("fuzz: no hand-written case reads %s, so no harness need map it; drop it from the grammar (fuzzgen.go)", strings.Join(missing, ", "))
	}
	f := newFuzzer(*seed, in.corpus, in.ds)
	if f.atoms, err = boolAtoms(ctx, in, f.atoms, *seed); err != nil {
		return err
	}
	cases := f.fuzzCases(*seed, *count)
	recorded, dropped, err := recordFuzz(ctx, in, cases, *seed)
	if err != nil {
		return err
	}
	m := fuzzManifest{Seed: *seed, Count: *count, Tags: in.tags(), Dropped: dropped}
	if err := writeFuzzDir(*out, m, recorded); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "==> fuzz seed %d: kept %d of %d cases in %s\n", *seed, len(recorded.cases), *count, *out)
	return nil
}

// fuzzInputs is what every fuzz subcommand reads from conformance/.
type fuzzInputs struct {
	conf     string
	corpus   *Corpus
	ds       *Dataset
	versions []pdpVersion // previous, then current
}

func (in *fuzzInputs) tags() []string {
	tags := make([]string, len(in.versions))
	for i, v := range in.versions {
		tags[i] = v.Tag
	}
	return tags
}

func loadFuzzInputs(conf string) (*fuzzInputs, error) {
	corpus, err := loadCases(filepath.Join(conf, "cases"))
	if err != nil {
		return nil, fmt.Errorf("case files: %w", err)
	}
	ds, err := loadDataset(conf)
	if err != nil {
		return nil, err
	}
	var versions struct {
		Current  pdpVersion `json:"current"`
		Previous pdpVersion `json:"previous"`
	}
	if err := decodeJSONFile(filepath.Join(conf, "pdp-versions.json"), &versions); err != nil {
		return nil, err
	}
	return &fuzzInputs{conf: conf, corpus: corpus, ds: ds, versions: []pdpVersion{versions.Previous, versions.Current}}, nil
}

func conformanceDirOr(flagValue string) (string, error) {
	if flagValue != "" {
		return filepath.Abs(flagValue)
	}
	return findConformanceDir()
}

// ensureEmptyOutside refuses an output directory inside conformance/, where a fuzz golden could be
// mistaken for, or committed beside, a corpus golden.
func ensureEmptyOutside(out, conf string) error {
	abs, err := filepath.Abs(out)
	if err != nil {
		return err
	}
	if rel, err := filepath.Rel(conf, abs); err == nil && !strings.HasPrefix(rel, "..") {
		return fmt.Errorf("fuzz: -out %s is inside %s; write fuzz runs elsewhere", out, conf)
	}
	entries, err := os.ReadDir(abs)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("fuzz: -out %s is not empty", out)
	}
	return nil
}

// fuzzRecording is a set of fuzz cases with their goldens, by PDP tag.
type fuzzRecording struct {
	cases   []*fuzzCase
	goldens map[string][]*Golden // tag -> goldens, in cases order
	policy  []byte
}

// recordFuzz compiles the cases into one policy, drops any the PDP will not load, and records the
// rest against both pinned PDPs. A case that fails to plan, or whose plan folds to always allowed or
// always denied while check() disagrees, is dropped: the first has nothing to translate, the second is
// the planner answering a different question from check(), which no adapter can match.
func recordFuzz(ctx context.Context, in *fuzzInputs, cases []*fuzzCase, seed uint64) (*fuzzRecording, map[string]string, error) {
	dropped := map[string]string{}
	current := in.versions[len(in.versions)-1]
	cases, err := dropUncompilable(ctx, in, cases, seed, imageOf(current), dropped)
	if err != nil {
		return nil, nil, err
	}
	if len(cases) == 0 {
		return &fuzzRecording{goldens: map[string][]*Golden{}}, dropped, nil
	}
	corpus := fuzzCorpus(in.corpus, cases, seed)
	policy, err := buildPolicy(corpus)
	if err != nil {
		return nil, nil, err
	}
	staged, err := stagePolicies(policy, filepath.Join(in.conf, "policies", "derived_roles.yaml"))
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(staged)

	byTag := map[string][]*Golden{}
	for _, v := range in.versions {
		fmt.Fprintf(os.Stderr, "==> PDP %s: recording %d fuzz cases\n", v.Tag, len(cases))
		goldens, err := recordWith(ctx, imageOf(v), v.Tag, staged, corpus, in.ds)
		if err != nil {
			return nil, nil, err
		}
		byTag[v.Tag] = goldens
	}

	rec := &fuzzRecording{goldens: map[string][]*Golden{}, policy: policy}
	for i, fc := range cases {
		reason := ""
		for _, v := range in.versions {
			if r := unjudgeable(byTag[v.Tag][i], len(in.ds.Resources)); r != "" {
				reason = fmt.Sprintf("PDP %s: %s", v.Tag, r)
				break
			}
		}
		if reason != "" {
			dropped[fc.ID] = reason
			continue
		}
		rec.cases = append(rec.cases, fc)
		for _, v := range in.versions {
			rec.goldens[v.Tag] = append(rec.goldens[v.Tag], byTag[v.Tag][i])
		}
	}
	return rec, dropped, nil
}

// unjudgeable says why a recorded golden cannot judge an adapter, or "" when it can.
func unjudgeable(g *Golden, seeds int) string {
	if g.PlanError != nil {
		return "the plan failed: " + *g.PlanError
	}
	switch g.Plan["kind"] {
	case "KIND_ALWAYS_ALLOWED":
		if len(g.Allowed) != seeds {
			return fmt.Sprintf("the plan is always allowed but check() allows %d of %d seeds", len(g.Allowed), seeds)
		}
	case "KIND_ALWAYS_DENIED":
		if len(g.Allowed) != 0 {
			return fmt.Sprintf("the plan is always denied but check() allows %d seeds", len(g.Allowed))
		}
	}
	return ""
}

// fuzzCorpus is a corpus holding the fuzz cases in place of the hand-written ones, keeping the
// hand-written files' variables and derived-role imports so a composed corpus expression that reads
// V.<name> still compiles.
func fuzzCorpus(corpus *Corpus, cases []*fuzzCase, seed uint64) *Corpus {
	out := &Corpus{Files: corpus.Files}
	for _, fc := range cases {
		out.Cases = append(out.Cases, fc.asCase(seed))
	}
	return out
}

// boolAtoms keeps the corpus atoms that compile as an operand of a boolean operator. A hand-written
// case may be a whole condition that is not a bool (collection/except/as-whole-condition is a list),
// which compiles at a condition's root and nowhere inside one.
func boolAtoms(ctx context.Context, in *fuzzInputs, atoms []corpusAtom, seed uint64) ([]corpusAtom, error) {
	probes := make([]*fuzzCase, len(atoms))
	for i, a := range atoms {
		probes[i] = &fuzzCase{ID: fmt.Sprintf("fuzz/atom/%04d", i), Allow: &fnode{Op: "not", Kids: []*fnode{{Op: "atom", CEL: a.CEL}}}}
	}
	current := in.versions[len(in.versions)-1]
	kept, err := dropUncompilable(ctx, in, probes, seed, imageOf(current), map[string]string{})
	if err != nil {
		return nil, err
	}
	ok := map[string]bool{}
	for _, p := range kept {
		ok[p.ID] = true
	}
	var out []corpusAtom
	for i, a := range atoms {
		if ok[probes[i].ID] {
			out = append(out, a)
		}
	}
	return out, nil
}

// dropUncompilable runs `cerbos compile` over the cases' policy and, while it fails, bisects to the
// cases that break it and drops them. The grammar is typed, so a failure is rare, but one bad action
// would otherwise stop the PDP from loading the whole policy.
func dropUncompilable(ctx context.Context, in *fuzzInputs, cases []*fuzzCase, seed uint64, image string, dropped map[string]string) ([]*fuzzCase, error) {
	compiles := func(set []*fuzzCase) (bool, string, error) {
		if len(set) == 0 {
			return true, "", nil
		}
		policy, err := buildPolicy(fuzzCorpus(in.corpus, set, seed))
		if err != nil {
			return false, "", err
		}
		dir, err := stagePolicies(policy, filepath.Join(in.conf, "policies", "derived_roles.yaml"))
		if err != nil {
			return false, "", err
		}
		defer os.RemoveAll(dir)
		out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "-v", dir+":/policies:ro", image, "compile", "--skip-tests", "/policies").CombinedOutput()
		if ctx.Err() != nil {
			return false, "", ctx.Err()
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return false, strings.TrimSpace(string(out)), nil
		}
		if err != nil {
			return false, "", fmt.Errorf("docker run %s compile: %w: %s", image, err, out)
		}
		return true, "", nil
	}

	var keep []*fuzzCase
	var bisect func(set []*fuzzCase) error
	bisect = func(set []*fuzzCase) error {
		ok, msg, err := compiles(set)
		if err != nil {
			return err
		}
		if ok {
			keep = append(keep, set...)
			return nil
		}
		if len(set) == 1 {
			dropped[set[0].ID] = "the policy does not compile: " + compileError(msg)
			return nil
		}
		mid := len(set) / 2
		if err := bisect(set[:mid]); err != nil {
			return err
		}
		return bisect(set[mid:])
	}
	if err := bisect(cases); err != nil {
		return nil, err
	}
	// Bisection appends in order, so keep is already in the cases' order.
	return keep, nil
}

// compileError keeps the lines of `cerbos compile` output that say what failed, joined into one.
func compileError(out string) string {
	var keep []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.Contains(line, "failed to compile") {
			keep = append(keep, line)
		}
	}
	return strings.Join(keep, " ")
}

// writeFuzzDir writes manifest.json, cases.json, policy.yaml and <tag>/<case id>.json.
func writeFuzzDir(out string, m fuzzManifest, rec *fuzzRecording) error {
	m.Kept = len(rec.cases)
	files := map[string]any{"manifest.json": m, fuzzCaseFile: rec.cases}
	if rec.cases == nil {
		files[fuzzCaseFile] = []*fuzzCase{}
	}
	for tag, goldens := range rec.goldens {
		for _, g := range goldens {
			files[filepath.Join(tag, g.ID+".json")] = g
		}
	}
	for _, tag := range m.Tags {
		if err := os.MkdirAll(filepath.Join(out, tag), 0o755); err != nil {
			return err
		}
	}
	for name, v := range files {
		raw, err := marshalJSON(v)
		if err != nil {
			return err
		}
		path := filepath.Join(out, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			return err
		}
	}
	if rec.policy != nil {
		return os.WriteFile(filepath.Join(out, "policy.yaml"), rec.policy, 0o644)
	}
	return nil
}

func readFuzzCases(dir string) ([]*fuzzCase, error) {
	var cases []*fuzzCase
	raw, err := os.ReadFile(filepath.Join(dir, fuzzCaseFile))
	if err != nil {
		return nil, err
	}
	return cases, json.Unmarshal(raw, &cases)
}
