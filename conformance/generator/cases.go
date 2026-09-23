package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// idPattern is the case id format: three lower-case kebab-case segments, <area>/<operator>/<variant>.
var idPattern = regexp.MustCompile(`^[a-z0-9-]+(/[a-z0-9-]+){2}$`)

var tiers = map[string]bool{"core": true, "extended": true, "adversarial": true}

var effects = map[string]bool{"EFFECT_ALLOW": true, "EFFECT_DENY": true}

// caseFile is one hand-written conformance/cases/<area>.yaml.
type caseFile struct {
	Area               string            `yaml:"area"`
	Description        string            `yaml:"description"`
	Variables          map[string]string `yaml:"variables"`
	ImportDerivedRoles []string          `yaml:"importDerivedRoles"`
	Cases              []Case            `yaml:"cases"`

	path string
}

// Case is one conformance case: one Cerbos action, named by its id.
type Case struct {
	ID                string           `yaml:"id"`
	Tier              string           `yaml:"tier"`
	Intent            string           `yaml:"intent"`
	Trap              string           `yaml:"trap"`
	Notes             string           `yaml:"notes"`
	Condition         any              `yaml:"condition"`
	Rules             []map[string]any `yaml:"rules"`
	Degenerate        *Degenerate      `yaml:"degenerate"`
	PlannerDivergence *Divergence      `yaml:"plannerDivergence"`

	area string
}

// Degenerate declares an oracle that is empty or total by construction.
//
// PDP optionally limits the declaration to the listed tags, for a case whose oracle changed between
// the two pinned versions; empty means every pinned version.
type Degenerate struct {
	Oracle string   `yaml:"oracle" json:"oracle"`
	Reason string   `yaml:"reason" json:"reason"`
	PDP    []string `yaml:"pdp" json:"pdp,omitempty"`
}

// covers reports whether the declaration applies to the given PDP tag.
func (d *Degenerate) covers(tag string) bool {
	if d == nil {
		return false
	}
	if len(d.PDP) == 0 {
		return true
	}
	for _, t := range d.PDP {
		if t == tag {
			return true
		}
	}
	return false
}

// Divergence declares a planner bug: the plan and check() disagree on the listed PDP tags.
type Divergence struct {
	Issue  string   `yaml:"issue" json:"issue"`
	PDP    []string `yaml:"pdp" json:"pdp"`
	Reason string   `yaml:"reason" json:"reason"`
}

func (d *Divergence) covers(tag string) bool {
	if d == nil {
		return false
	}
	for _, t := range d.PDP {
		if t == tag {
			return true
		}
	}
	return false
}

// Corpus is every case file, areas in alphabetical order, cases in file order.
type Corpus struct {
	Files []*caseFile
	Cases []*Case
}

func loadCases(dir string) (*Corpus, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no case files (*.yaml) in %s", dir)
	}

	var errs []error
	corpus := &Corpus{}
	for _, path := range paths {
		f, err := readCaseFile(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		corpus.Files = append(corpus.Files, f)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	sort.Slice(corpus.Files, func(i, j int) bool { return corpus.Files[i].Area < corpus.Files[j].Area })

	seen := map[string]string{}
	variables := map[string]string{}
	variableFrom := map[string]string{}
	for _, f := range corpus.Files {
		for name, expr := range f.Variables {
			if prev, ok := variables[name]; ok && prev != expr {
				errs = append(errs, fmt.Errorf("variable %q is defined differently in %s and %s", name, variableFrom[name], f.path))
			}
			variables[name] = expr
			variableFrom[name] = f.path
		}
		for i := range f.Cases {
			c := &f.Cases[i]
			c.area = f.Area
			if prev, ok := seen[c.ID]; ok {
				errs = append(errs, fmt.Errorf("%s: case id %q is already used in %s", f.path, c.ID, prev))
			}
			seen[c.ID] = f.path
			if err := c.validate(); err != nil {
				errs = append(errs, fmt.Errorf("%s: case %q: %w", f.path, c.ID, err))
			}
			corpus.Cases = append(corpus.Cases, c)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return corpus, nil
}

func readCaseFile(path string) (*caseFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	f := &caseFile{path: path}
	if err := dec.Decode(f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: expected exactly one YAML document", path)
	}

	want := strings.TrimSuffix(filepath.Base(path), ".yaml")
	switch {
	case f.Area == "":
		return nil, fmt.Errorf("%s: missing area", path)
	case f.Area != want:
		return nil, fmt.Errorf("%s: area %q must match the file name %q", path, f.Area, want)
	case strings.TrimSpace(f.Description) == "":
		return nil, fmt.Errorf("%s: missing description", path)
	case len(f.Cases) == 0:
		return nil, fmt.Errorf("%s: no cases", path)
	}
	return f, nil
}

func (c *Case) validate() error {
	var errs []error
	if !idPattern.MatchString(c.ID) {
		errs = append(errs, fmt.Errorf("id must match %s", idPattern))
	}
	if !tiers[c.Tier] {
		errs = append(errs, fmt.Errorf("tier %q is not one of core, extended, adversarial", c.Tier))
	}
	if strings.TrimSpace(c.Intent) == "" {
		errs = append(errs, errors.New("missing intent"))
	}
	if strings.TrimSpace(c.Trap) == "" {
		errs = append(errs, errors.New("missing trap"))
	}
	if (c.Condition == nil) == (len(c.Rules) == 0) {
		errs = append(errs, errors.New("exactly one of condition and rules is required"))
	}
	for i, r := range c.Rules {
		if _, ok := r["actions"]; ok {
			errs = append(errs, fmt.Errorf("rules[%d]: actions is implied by the case id and must not be given", i))
		}
		effect, _ := r["effect"].(string)
		if !effects[effect] {
			errs = append(errs, fmt.Errorf("rules[%d]: effect %v is not EFFECT_ALLOW or EFFECT_DENY", i, r["effect"]))
		}
	}
	if d := c.Degenerate; d != nil {
		if d.Oracle != "empty" && d.Oracle != "total" {
			errs = append(errs, fmt.Errorf("degenerate.oracle %q is not empty or total", d.Oracle))
		}
		if strings.TrimSpace(d.Reason) == "" {
			errs = append(errs, errors.New("degenerate.reason is required"))
		}
	}
	if d := c.PlannerDivergence; d != nil {
		if strings.TrimSpace(d.Issue) == "" || strings.TrimSpace(d.Reason) == "" || len(d.PDP) == 0 {
			errs = append(errs, errors.New("plannerDivergence needs issue, pdp and reason"))
		}
	}
	return errors.Join(errs...)
}

// rules returns the case's rules as they appear in the generated policy, minus `actions`:
// a `condition` case becomes one ALLOW rule for USER, and a rule with neither roles nor
// derivedRoles gets roles [USER].
func (c *Case) rules() []map[string]any {
	if c.Condition != nil {
		return []map[string]any{{
			"effect":    "EFFECT_ALLOW",
			"roles":     []any{"USER"},
			"condition": c.Condition,
		}}
	}
	out := make([]map[string]any, 0, len(c.Rules))
	for _, r := range c.Rules {
		rule := make(map[string]any, len(r)+1)
		for k, v := range r {
			rule[k] = v
		}
		_, hasRoles := rule["roles"]
		_, hasDerived := rule["derivedRoles"]
		if !hasRoles && !hasDerived {
			rule["roles"] = []any{"USER"}
		}
		out = append(out, rule)
	}
	return out
}
