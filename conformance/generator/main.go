// Command generator records the conformance corpus's goldens.
//
// It reads the hand-written case files (conformance/cases/*.yaml), compiles them into one
// resource policy (policies/conformance.yaml), projects seeds.json + derived-fields.json into the
// check() dataset (resources.json), and then, for every PDP version in pdp-versions.json, starts
// that PDP in Docker and records each case's PlanResources filter and per-seed check() decisions
// into golden/<tag>/<case id>.json. golden/CHANGES.md is the previous -> current diff.
//
// Usage (its own module, so from the repository root it needs -C; `go run ./conformance/generator`
// does not resolve, since the root has no go.mod):
//
//	go -C conformance/generator run .           # regenerate in place
//	go -C conformance/generator run . -check    # exit 1 if regenerating would change a file
//	go run . -cases <dir> -out <conformance dir> # from conformance/generator
//
// Requires Docker with the pinned images pullable.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

type pdpVersion struct {
	Tag    string `json:"tag"`
	Digest string `json:"digest"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	outFlag := flag.String("out", "", "the conformance directory to read corpus inputs from and write generated files to (default: located from the working directory)")
	casesFlag := flag.String("cases", "", "the case file directory (default: <out>/cases)")
	check := flag.Bool("check", false, "write nothing; exit non-zero if regenerating would change any generated file")
	flag.Parse()

	out := *outFlag
	if out == "" {
		dir, err := findConformanceDir()
		if err != nil {
			return err
		}
		out = dir
	}
	casesDir := *casesFlag
	if casesDir == "" {
		casesDir = filepath.Join(out, "cases")
	}

	corpus, err := loadCases(casesDir)
	if err != nil {
		return fmt.Errorf("case files: %w", err)
	}
	ds, err := loadDataset(out)
	if err != nil {
		return err
	}
	var versions struct {
		Current  pdpVersion `json:"current"`
		Previous pdpVersion `json:"previous"`
	}
	if err := decodeJSONFile(filepath.Join(out, "pdp-versions.json"), &versions); err != nil {
		return err
	}
	for _, v := range []pdpVersion{versions.Current, versions.Previous} {
		if v.Tag == "" || !strings.HasPrefix(v.Digest, "sha256:") {
			return fmt.Errorf("pdp-versions.json: every version needs a tag and a sha256 digest, got %+v", v)
		}
	}
	if versions.Current.Tag == versions.Previous.Tag {
		return errors.New("pdp-versions.json: current and previous name the same tag")
	}
	for _, c := range corpus.Cases {
		if c.PlannerDivergence == nil {
			continue
		}
		for _, tag := range c.PlannerDivergence.PDP {
			if tag != versions.Current.Tag && tag != versions.Previous.Tag {
				fmt.Fprintf(os.Stderr, "warning: case %q: plannerDivergence lists PDP %s, which pdp-versions.json no longer records\n", c.ID, tag)
			}
		}
	}

	files := map[string][]byte{}
	policy, err := buildPolicy(corpus)
	if err != nil {
		return err
	}
	files["policies/conformance.yaml"] = policy
	if files["resources.json"], err = buildResourcesJSON(ds); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	staged, err := stagePolicies(policy, filepath.Join(out, "policies", "derived_roles.yaml"))
	if err != nil {
		return err
	}
	defer os.RemoveAll(staged)

	recorded := map[string][]*Golden{}
	var violations []string
	for _, v := range []pdpVersion{versions.Previous, versions.Current} {
		image := fmt.Sprintf("ghcr.io/cerbos/cerbos:%s@%s", v.Tag, v.Digest)
		fmt.Fprintf(os.Stderr, "==> PDP %s: recording %d cases\n", v.Tag, len(corpus.Cases))
		goldens, err := recordWith(ctx, image, v.Tag, staged, corpus, ds)
		if err != nil {
			return err
		}
		recorded[v.Tag] = goldens
		violations = append(violations, degeneracyErrors(corpus, v.Tag, goldens)...)
		for _, g := range goldens {
			raw, err := marshalJSON(g)
			if err != nil {
				return err
			}
			files[filepath.ToSlash(filepath.Join("golden", v.Tag, g.ID+".json"))] = raw
		}
	}
	files["golden/CHANGES.md"] = []byte(changesMarkdown(versions.Previous.Tag, versions.Current.Tag, recorded[versions.Previous.Tag], recorded[versions.Current.Tag]))

	if *check {
		diffs, err := compareTree(out, files)
		if err != nil {
			return err
		}
		if len(diffs) > 0 {
			return fmt.Errorf("generated files are out of date; run the generator and commit the result:\n  %s", strings.Join(diffs, "\n  "))
		}
	} else {
		if err := writeTree(out, files); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "==> wrote %d files under %s\n", len(files), out)
	}

	if len(violations) > 0 {
		return fmt.Errorf("degenerate oracles disagree with their declarations:\n  %s", strings.Join(violations, "\n  "))
	}
	return nil
}

func recordWith(ctx context.Context, image, tag, policiesDir string, corpus *Corpus, ds *Dataset) ([]*Golden, error) {
	p, err := startPDP(ctx, image, tag, policiesDir)
	if err != nil {
		return nil, err
	}
	defer p.stop()
	return record(ctx, p, tag, corpus, ds)
}

// findConformanceDir accepts the repository root, conformance/, conformance/generator/ or
// anything below them as the working directory.
func findConformanceDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if isConformanceDir(dir) {
			return dir, nil
		}
		if c := filepath.Join(dir, "conformance"); isConformanceDir(c) {
			return c, nil
		}
		if filepath.Dir(dir) == dir {
			return "", fmt.Errorf("no conformance directory above %s; pass -out", wd)
		}
	}
}

func isConformanceDir(dir string) bool {
	for _, f := range []string{"seeds.json", "generator/go.mod"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			return false
		}
	}
	return true
}

// goldenFiles lists every file under <out>/golden, relative to out.
func goldenFiles(out string) (map[string]bool, error) {
	found := map[string]bool{}
	root := filepath.Join(out, "golden")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && path == root {
				return filepath.SkipDir
			}
			return err
		}
		if !d.IsDir() {
			rel, err := filepath.Rel(out, path)
			if err != nil {
				return err
			}
			found[filepath.ToSlash(rel)] = true
		}
		return nil
	})
	return found, err
}

func compareTree(out string, files map[string][]byte) ([]string, error) {
	var diffs []string
	for _, rel := range sortedKeys(files) {
		have, err := os.ReadFile(filepath.Join(out, rel))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			diffs = append(diffs, "missing: "+rel)
		case err != nil:
			return nil, err
		case !bytes.Equal(have, files[rel]):
			diffs = append(diffs, "changed: "+rel)
		}
	}
	existing, err := goldenFiles(out)
	if err != nil {
		return nil, err
	}
	var stale []string
	for rel := range existing {
		if _, ok := files[rel]; !ok {
			stale = append(stale, "stale:   "+rel)
		}
	}
	sort.Strings(stale)
	return append(diffs, stale...), nil
}

func writeTree(out string, files map[string][]byte) error {
	for rel, data := range files {
		path := filepath.Join(out, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	existing, err := goldenFiles(out)
	if err != nil {
		return err
	}
	for rel := range existing {
		if _, ok := files[rel]; !ok {
			if err := os.Remove(filepath.Join(out, rel)); err != nil {
				return err
			}
		}
	}
	return removeEmptyDirs(filepath.Join(out, "golden"))
}

// removeEmptyDirs deletes directories left empty by a removed case or a dropped PDP version.
func removeEmptyDirs(root string) error {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, d := range dirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			if err := os.Remove(d); err != nil {
				return err
			}
		}
	}
	return nil
}
