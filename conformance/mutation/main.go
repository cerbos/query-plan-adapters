// Command mutation mutates an adapter's translator and replays its conformance
// harness against each mutant. A mutant the harness still passes is a shape the
// corpus does not pin.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type adapter struct {
	files     []string
	cmd       []string
	env       func(adapterDir string) []string
	link      []string
	nonViable *regexp.Regexp
	coverage  string
}

var adapters = map[string]adapter{
	"ent": {
		files:     []string{"internal/queryplan/*.go", "render.go", "adapter.go", "schema.go"},
		cmd:       []string{"go", "test", "-count=1", "-vet=off", "-ldflags=-s -w", "-failfast", "-run", "TestAdversarialConformance/sqlite", "."},
		nonViable: regexp.MustCompile(`\[build failed\]|\[setup failed\]`),
		coverage:  "go",
	},
	"pgx": {
		files:     []string{"render.go", "adapter.go"},
		cmd:       []string{"go", "test", "-count=1", "-vet=off", "-ldflags=-s -w", "-failfast", "-run", "TestAdversarialConformance", "."},
		nonViable: regexp.MustCompile(`\[build failed\]|\[setup failed\]`),
		coverage:  "go",
	},
	"sqlalchemy": {
		files: []string{"src/cerbos_sqlalchemy/*.py"},
		cmd: []string{".venv/bin/pytest", "tests/test_adversarial_conformance.py", "-q", "-x",
			"-p", "no:cacheprovider", "-k", "sqlite-declared-0.58.0"},
		env:       func(dir string) []string { return []string{"PYTHONPATH=" + filepath.Join(dir, "src")} },
		link:      []string{".venv"},
		nonViable: regexp.MustCompile(`SyntaxError|IndentationError`),
		coverage:  "python",
	},
	"duckdb": {
		files: []string{"src/cerbos_duckdb/*.py"},
		cmd: []string{".venv/bin/pytest", "tests/test_conformance.py", "-q", "-x",
			"-p", "no:cacheprovider", "-k", "duckdb-http-0.58.0"},
		env:       func(dir string) []string { return []string{"PYTHONPATH=" + filepath.Join(dir, "src")} },
		link:      []string{".venv"},
		nonViable: regexp.MustCompile(`SyntaxError|IndentationError`),
		coverage:  "python",
	},
	"drizzle": {
		files:     []string{"src/*.ts"},
		cmd:       []string{"node_modules/.bin/jest", "--config", "jest.adversarial.config.js", "--bail", "--ci"},
		link:      []string{"node_modules"},
		nonViable: regexp.MustCompile(`TS\d{4}|SyntaxError`),
		coverage:  "jest",
	},
}

type result struct {
	ID       string  `json:"id"`
	File     string  `json:"file"`
	Line     int     `json:"line"`
	Op       string  `json:"op"`
	Original string  `json:"original"`
	Mutated  string  `json:"mutated"`
	Status   string  `json:"status"`
	Seconds  float64 `json:"seconds"`
	Failure  string  `json:"failure,omitempty"`
}

// goErrCheck matches error propagation, which is not translation.
var goErrCheck = regexp.MustCompile(`err (?:!=|==) nil`)

type mutant struct {
	result
	src string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mutation:", err)
		os.Exit(1)
	}
}

func run() error {
	name := flag.String("adapter", "", "the adapter to mutate")
	out := flag.String("out", "", "directory for results.jsonl and report.md (default: a temporary directory)")
	only := flag.String("files", "", "comma-separated substrings; mutate only files whose path contains one")
	ops := flag.String("ops", "", "comma-separated operators (equality,logical,boundary,negation,boolean,guard)")
	workers := flag.Int("workers", 2, "mutants run at once, each in its own copy of the adapter")
	list := flag.Bool("list", false, "print the mutants and exit")
	reportOnly := flag.Bool("report", false, "rewrite report.md from results.jsonl and exit")
	rerun := flag.String("survivors-of", "", "a results.jsonl; run only the mutants that survived or went unreached there (with QPA_FUZZ_DIR set, against a fuzz round)")
	flag.Parse()

	a, ok := adapters[*name]
	if !ok {
		return fmt.Errorf("unknown -adapter %q", *name)
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	if *out == "" {
		if *out, err = os.MkdirTemp("", "qpa-mutation-"+*name+"-"); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	resultsPath := filepath.Join(*out, "results.jsonl")
	if *reportOnly {
		return writeReport(*out, *name, resultsPath)
	}

	mutants, err := enumerate(filepath.Join(root, *name), a, split(*only), split(*ops))
	if err != nil {
		return err
	}
	if *list {
		for _, m := range mutants {
			fmt.Printf("%s\t%s\t%s\n", m.ID, m.Original, m.Mutated)
		}
		return nil
	}
	if *rerun != "" {
		prev, err := statuses(*rerun)
		if err != nil {
			return err
		}
		mutants = slices.DeleteFunc(mutants, func(m mutant) bool { return prev[m.ID] != "survived" && prev[m.ID] != "unreached" })
	}
	done, err := completed(resultsPath)
	if err != nil {
		return err
	}
	mutants = slices.DeleteFunc(mutants, func(m mutant) bool { return done[m.ID] })
	fmt.Fprintf(os.Stderr, "%d mutants to run (%d already in %s)\n", len(mutants), len(done), resultsPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dirs := make([]string, *workers)
	for i := range dirs {
		if dirs[i], err = mirror(root, *name, a.link, filepath.Join(*out, fmt.Sprintf("worker-%d", i))); err != nil {
			return err
		}
	}
	defer func() {
		for _, d := range dirs {
			os.RemoveAll(filepath.Dir(d))
		}
	}()

	baseline := time.Now()
	if status, output := execute(ctx, a, dirs[0], 30*time.Minute); status != "survived" {
		return fmt.Errorf("the unmutated harness does not pass (%s):\n%s", status, tail(output, 40))
	}
	timeout := 4*time.Since(baseline) + 30*time.Second
	reached, err := measure(ctx, a, dirs[0], filepath.Join(*out, "coverage"))
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "baseline %s, timeout %s\n", time.Since(baseline).Round(time.Second), timeout.Round(time.Second))

	f, err := os.OpenFile(resultsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var mu sync.Mutex
	var wg sync.WaitGroup
	n := 0
	record := func(r result) {
		mu.Lock()
		defer mu.Unlock()
		n++
		line, _ := json.Marshal(r)
		f.Write(append(line, '\n'))
		fmt.Fprintf(os.Stderr, "[%d/%d] %-9s %s\n", n, len(mutants), r.Status, r.ID)
	}
	jobs := make(chan mutant)
	for _, dir := range dirs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range jobs {
				r, err := runMutant(ctx, a, dir, m, timeout)
				if err != nil {
					fmt.Fprintln(os.Stderr, "mutation:", err)
					continue
				}
				if ctx.Err() != nil {
					return
				}
				record(r)
			}
		}()
	}
	for _, m := range mutants {
		if ctx.Err() != nil {
			break
		}
		if reached != nil && !reached[m.File][m.Line] {
			r := m.result
			r.Status = "unreached"
			record(r)
			continue
		}
		jobs <- m
	}
	close(jobs)
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return writeReport(*out, *name, resultsPath)
}

func split(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func repoRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("locating the repository: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func enumerate(dir string, a adapter, only, ops []string) ([]mutant, error) {
	var files []string
	for _, g := range a.files {
		matches, err := filepath.Glob(filepath.Join(dir, g))
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			rel, _ := filepath.Rel(dir, m)
			if strings.Contains(rel, "_test.") || strings.HasSuffix(rel, ".test.ts") || filepath.Base(rel) == "corpus.ts" {
				continue
			}
			if len(only) > 0 && !slices.ContainsFunc(only, func(s string) bool { return strings.Contains(rel, s) }) {
				continue
			}
			files = append(files, rel)
		}
	}
	sort.Strings(files)
	var out []mutant
	for _, rel := range files {
		l, _ := langOf(rel)
		b, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			return nil, err
		}
		src := string(b)
		for _, s := range sites(src, l) {
			if len(ops) > 0 && !slices.Contains(ops, s.op) {
				continue
			}
			if l == langGo && goErrCheck.MatchString(src[max(0, s.start-4):min(len(src), s.end+4)]) {
				continue
			}
			line := strings.Count(src[:s.start], "\n") + 1
			ls := strings.LastIndexByte(src[:s.start], '\n') + 1
			mutated := src[:s.start] + s.repl + src[s.end:]
			orig, newLine := lineAt(src, ls), lineAt(mutated, ls)
			out = append(out, mutant{
				result: result{
					ID:       fmt.Sprintf("%s:%d:%d:%s", rel, line, s.start-ls+1, s.op),
					File:     rel,
					Line:     line,
					Op:       s.op,
					Original: strings.TrimSpace(orig),
					Mutated:  strings.TrimSpace(newLine),
				},
				src: mutated,
			})
		}
	}
	return out, nil
}

func lineAt(s string, from int) string {
	if end := strings.IndexByte(s[from:], '\n'); end >= 0 {
		return s[from : from+end]
	}
	return s[from:]
}

func completed(path string) (map[string]bool, error) {
	st, err := statuses(path)
	done := map[string]bool{}
	for id := range st {
		done[id] = true
	}
	return done, err
}

func statuses(path string) (map[string]string, error) {
	done := map[string]string{}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return done, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r result
		if json.Unmarshal(sc.Bytes(), &r) == nil {
			done[r.ID] = r.Status
		}
	}
	return done, sc.Err()
}

// mirror builds a tree under dest where every top-level entry of root but the
// adapter is a symlink, and the adapter is a copy whose link directories are
// symlinks. It returns the copy's adapter directory.
func mirror(root, name string, link []string, dest string) (string, error) {
	os.RemoveAll(dest)
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.Name() == name || e.Name() == ".git" {
			continue
		}
		if err := os.Symlink(filepath.Join(root, e.Name()), filepath.Join(dest, e.Name())); err != nil {
			return "", err
		}
	}
	src := filepath.Join(root, name)
	dst := filepath.Join(dest, name)
	err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			if slices.Contains(link, rel) {
				if err := os.Symlink(p, target); err != nil {
					return err
				}
				return filepath.SkipDir
			}
			return os.MkdirAll(target, 0o755)
		}
		if d.Type()&fs.ModeSymlink != 0 {
			to, err := os.Readlink(p)
			if err != nil {
				return err
			}
			return os.Symlink(to, target)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, info.Mode().Perm())
	})
	return dst, err
}

func runMutant(ctx context.Context, a adapter, dir string, m mutant, timeout time.Duration) (result, error) {
	path := filepath.Join(dir, m.File)
	orig, err := os.ReadFile(path)
	if err != nil {
		return m.result, err
	}
	if err := os.WriteFile(path, []byte(m.src), 0o644); err != nil {
		return m.result, err
	}
	defer os.WriteFile(path, orig, 0o644)
	start := time.Now()
	status, output := execute(ctx, a, dir, timeout)
	r := m.result
	r.Status = status
	r.Seconds = time.Since(start).Round(100 * time.Millisecond).Seconds()
	if status == "killed" {
		r.Failure = firstFailure(output)
	}
	return r, nil
}

func execute(ctx context.Context, a adapter, dir string, timeout time.Duration) (string, string) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, a.cmd[0], a.cmd[1:]...)
	if strings.Contains(a.cmd[0], "/") {
		cmd.Path = filepath.Join(dir, a.cmd[0])
		cmd.Err = nil
	}
	cmd.Dir = dir
	cmd.Env = os.Environ()
	if a.env != nil {
		cmd.Env = append(cmd.Env, a.env(dir)...)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	output, err := cmd.CombinedOutput()
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		return "timeout", string(output)
	case err == nil:
		return "survived", string(output)
	case a.nonViable.Match(output):
		return "nonviable", string(output)
	}
	return "killed", string(output)
}

var failureLine = regexp.MustCompile(`(?m)^(?:\s*--- FAIL: (\S+)|FAILED (\S+)|\s*● (.+?) › (.+)$)`)

func firstFailure(output string) string {
	best := ""
	for _, m := range failureLine.FindAllStringSubmatch(output, -1) {
		for _, g := range m[1:] {
			if g = strings.TrimSpace(g); strings.Count(g, "/") > strings.Count(best, "/") {
				best = g
			}
		}
	}
	return best
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func writeReport(out, name, resultsPath string) error {
	f, err := os.Open(resultsPath)
	if err != nil {
		return err
	}
	defer f.Close()
	var rs []result
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r result
		if json.Unmarshal(sc.Bytes(), &r) == nil {
			rs = append(rs, r)
		}
	}
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].File != rs[j].File {
			return rs[i].File < rs[j].File
		}
		return rs[i].Line < rs[j].Line
	})
	count := map[string]int{}
	for _, r := range rs {
		count[r.Status]++
	}
	viable := len(rs) - count["nonviable"] - count["unreached"]
	var b strings.Builder
	fmt.Fprintf(&b, "# Mutation run: %s\n\n", name)
	fmt.Fprintf(&b, "%d mutants: %d killed, %d timed out, %d survived, %d on lines the harness never runs, %d did not build.\n", len(rs),
		count["killed"], count["timeout"], count["survived"], count["unreached"], count["nonviable"])
	if viable > 0 {
		fmt.Fprintf(&b, "Mutation score %.1f%% (killed and timed out over every reached mutant that built).\n",
			100*float64(count["killed"]+count["timeout"])/float64(viable))
	}
	fmt.Fprintf(&b, "\n## Survivors\n\n| Mutant | Original | Mutated |\n|---|---|---|\n")
	for _, r := range rs {
		if r.Status == "survived" {
			fmt.Fprintf(&b, "| `%s` | `%s` | `%s` |\n", r.ID, cell(r.Original), cell(r.Mutated))
		}
	}
	fmt.Fprintf(&b, "\n## Unreached\n\nNo case executes these lines.\n\n| Mutant | Original |\n|---|---|\n")
	for _, r := range rs {
		if r.Status == "unreached" {
			fmt.Fprintf(&b, "| `%s` | `%s` |\n", r.ID, cell(r.Original))
		}
	}
	return os.WriteFile(filepath.Join(out, "report.md"), []byte(b.String()), 0o644)
}

func cell(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	s = strings.ReplaceAll(s, "`", "'")
	if len(s) > 160 {
		s = s[:157] + "..."
	}
	return s
}
