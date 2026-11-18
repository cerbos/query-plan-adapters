package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func summary(dir string) error {
	files, _ := filepath.Glob(filepath.Join(dir, "golden", "*", "*", "*.json"))
	fmt.Printf("== %s (%d goldens)\n", filepath.Base(dir), len(files))
	for _, f := range files {
		var g map[string]any
		raw, _ := os.ReadFile(f)
		if err := json.Unmarshal(raw, &g); err != nil {
			return err
		}
		kind := ""
		if p, ok := g["plan"].(map[string]any); ok {
			kind = strings.TrimPrefix(fmt.Sprint(p["kind"]), "KIND_")
		}
		if g["planError"] != nil {
			kind = "ERROR " + fmt.Sprint(g["planError"])
		}
		rel, _ := filepath.Rel(filepath.Join(dir, "golden"), f)
		fmt.Printf("  %-55s %-16s %-15s %v\n", rel, kind, g["oracle"], g["allowed"])
	}
	return nil
}

type result struct {
	Domain    string   `json:"domain"`
	PDP       string   `json:"pdp"`
	Principal string   `json:"principal"`
	Action    string   `json:"action"`
	Verdict   string   `json:"verdict"`
	Returned  []string `json:"returned"`
	Allowed   []string `json:"allowed"`
	Detail    string   `json:"detail"`
}

type resultFile struct {
	Adapter string   `json:"adapter"`
	Store   string   `json:"store"`
	Results []result `json:"results"`
}

var verdicts = []string{"over-grant", "under-grant", "error", "refused", "pass"}

func report(root string) error {
	files, _ := filepath.Glob(filepath.Join(root, "results", "*", "*.json"))
	if len(files) == 0 {
		return fmt.Errorf("no results under %s", filepath.Join(root, "results"))
	}
	var b strings.Builder
	var overs []string
	b.WriteString("# Domain stress results\n\n| adapter | store | domain | pass | refused | error | under-grant | over-grant |\n|---|---|---|---|---|---|---|---|\n")
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var rf resultFile
		if err := json.Unmarshal(raw, &rf); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		per := map[string]map[string]int{}
		for _, r := range rf.Results {
			if per[r.Domain] == nil {
				per[r.Domain] = map[string]int{}
			}
			per[r.Domain][r.Verdict]++
			if r.Verdict == "over-grant" {
				overs = append(overs, fmt.Sprintf("- %s/%s %s %s %s/%s: returned %v, allowed %v. %s", rf.Adapter, rf.Store, r.PDP, r.Domain, r.Principal, r.Action, r.Returned, r.Allowed, r.Detail))
			}
		}
		doms := make([]string, 0, len(per))
		for d := range per {
			doms = append(doms, d)
		}
		sort.Strings(doms)
		for _, d := range doms {
			c := per[d]
			fmt.Fprintf(&b, "| %s | %s | %s | %d | %d | %d | %d | %d |\n", rf.Adapter, rf.Store, d, c["pass"], c["refused"], c["error"], c["under-grant"], c["over-grant"])
		}
	}
	out := b.String()
	if len(overs) > 0 {
		out = "## Over-grants\n\n" + strings.Join(overs, "\n") + "\n\n" + out
	}
	fmt.Print(out)
	return nil
}
