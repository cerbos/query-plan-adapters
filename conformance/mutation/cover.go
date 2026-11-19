package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// coverage maps an adapter-relative file to the lines the unmutated harness executes.
type coverage map[string]map[int]bool

func (c coverage) mark(file string, from, to int) {
	if c[file] == nil {
		c[file] = map[int]bool{}
	}
	for l := from; l <= to; l++ {
		c[file][l] = true
	}
}

func measure(ctx context.Context, a adapter, dir, tmp string) (coverage, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, err
	}
	switch a.coverage {
	case "go":
		return goCoverage(ctx, a, dir, tmp)
	case "python":
		return pyCoverage(ctx, a, dir, tmp)
	case "jest":
		return jestCoverage(ctx, a, dir, tmp)
	}
	return nil, nil
}

func runIn(ctx context.Context, a adapter, dir string, argv ...string) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	if strings.Contains(argv[0], "/") {
		cmd.Path = filepath.Join(dir, argv[0])
		cmd.Err = nil
	}
	cmd.Dir = dir
	cmd.Env = os.Environ()
	if a.env != nil {
		cmd.Env = append(cmd.Env, a.env(dir)...)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("measuring coverage: %w\n%s", err, tail(string(out), 30))
	}
	return nil
}

func goCoverage(ctx context.Context, a adapter, dir, tmp string) (coverage, error) {
	profile := filepath.Join(tmp, "cover.out")
	argv := append(append([]string{}, a.cmd[:len(a.cmd)-1]...), "-coverpkg=./...", "-coverprofile="+profile, a.cmd[len(a.cmd)-1])
	if err := runIn(ctx, a, dir, argv...); err != nil {
		return nil, err
	}
	mod, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		return nil, err
	}
	module := strings.TrimSpace(strings.TrimPrefix(strings.SplitN(string(mod), "\n", 2)[0], "module"))
	f, err := os.Open(profile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	c := coverage{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		colon := strings.LastIndexByte(line, ':')
		if colon < 0 || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line[colon+1:])
		if len(fields) != 3 || fields[2] == "0" {
			continue
		}
		span := strings.Split(fields[0], ",")
		from, _ := strconv.Atoi(strings.Split(span[0], ".")[0])
		to, _ := strconv.Atoi(strings.Split(span[1], ".")[0])
		c.mark(strings.TrimPrefix(line[:colon], module+"/"), from, to)
	}
	return c, sc.Err()
}

const pyTracer = `
import json, sys
src, out = sys.argv[1], sys.argv[2]
lines = {}
mon = sys.monitoring
mon.use_tool_id(mon.COVERAGE_ID, "mutation")
def on_line(code, line):
    if code.co_filename.startswith(src):
        lines.setdefault(code.co_filename, set()).add(line)
    return mon.DISABLE
mon.register_callback(mon.COVERAGE_ID, mon.events.LINE, on_line)
mon.set_events(mon.COVERAGE_ID, mon.events.LINE)
import pytest
code = pytest.main(sys.argv[3:])
json.dump({k: sorted(v) for k, v in lines.items()}, open(out, "w"))
sys.exit(code)
`

func pyCoverage(ctx context.Context, a adapter, dir, tmp string) (coverage, error) {
	out := filepath.Join(tmp, "cover.json")
	python := filepath.Join(filepath.Dir(a.cmd[0]), "python")
	argv := append([]string{python, "-c", pyTracer, dir + "/", out}, a.cmd[1:]...)
	if err := runIn(ctx, a, dir, argv...); err != nil {
		return nil, err
	}
	var raw map[string][]int
	if err := readJSON(out, &raw); err != nil {
		return nil, err
	}
	c := coverage{}
	for file, lines := range raw {
		rel, _ := filepath.Rel(dir, file)
		for _, l := range lines {
			c.mark(rel, l, l)
		}
	}
	return c, nil
}

func jestCoverage(ctx context.Context, a adapter, dir, tmp string) (coverage, error) {
	argv := append(append([]string{}, a.cmd...), "--coverage", "--coverageProvider=v8",
		"--coverageReporters=json", "--coverageDirectory="+tmp)
	if err := runIn(ctx, a, dir, argv...); err != nil {
		return nil, err
	}
	var raw map[string]struct {
		StatementMap map[string]struct {
			Start struct{ Line int } `json:"start"`
			End   struct{ Line int } `json:"end"`
		} `json:"statementMap"`
		S map[string]int `json:"s"`
	}
	if err := readJSON(filepath.Join(tmp, "coverage-final.json"), &raw); err != nil {
		return nil, err
	}
	c := coverage{}
	for file, fc := range raw {
		rel, _ := filepath.Rel(dir, file)
		for id, n := range fc.S {
			if n > 0 {
				c.mark(rel, fc.StatementMap[id].Start.Line, fc.StatementMap[id].End.Line)
			}
		}
	}
	return c, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
