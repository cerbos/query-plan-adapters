package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// Golden is one conformance/golden/<tag>/<id>.json. Field order is key order (sorted).
type Golden struct {
	Allowed           []string         `json:"allowed"`
	DegenerateReason  *string          `json:"degenerateReason"`
	ID                string           `json:"id"`
	Intent            string           `json:"intent"`
	Oracle            string           `json:"oracle"`
	PDP               string           `json:"pdp"`
	Plan              map[string]any   `json:"plan"`
	PlanError         *string          `json:"planError"`
	PlannerDivergence *Divergence      `json:"plannerDivergence"`
	Request           goldenRequest    `json:"request"`
	Rules             []map[string]any `json:"rules"`
	Tier              string           `json:"tier"`
	Trap              string           `json:"trap"`
}

type goldenRequest struct {
	Action       string `json:"action"`
	ResourceKind string `json:"resourceKind"`
}

const workers = 8

// record plans and checks every case against one running PDP.
func record(ctx context.Context, p *pdp, tag string, corpus *Corpus, ds *Dataset) ([]*Golden, error) {
	goldens := make([]*Golden, len(corpus.Cases))
	errs := make([]error, len(corpus.Cases))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				goldens[i], errs[i] = recordCase(ctx, p, tag, corpus.Cases[i], ds)
			}
		}()
	}
	for i := range corpus.Cases {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("PDP %s, case %q: %w", tag, corpus.Cases[i].ID, err)
		}
	}
	return goldens, nil
}

func recordCase(ctx context.Context, p *pdp, tag string, c *Case, ds *Dataset) (*Golden, error) {
	pr, err := p.plan(ctx, ds.Principal, c.ID)
	if err != nil {
		return nil, err
	}
	allowed, err := p.check(ctx, ds, c.ID)
	if err != nil {
		return nil, err
	}
	sort.Strings(allowed)
	if allowed == nil {
		allowed = []string{}
	}

	g := &Golden{
		Allowed: allowed,
		ID:      c.ID,
		Intent:  c.Intent,
		Oracle:  oracleOf(len(allowed), len(ds.Resources)),
		PDP:     tag,
		Plan:    pr.Filter,
		Request: goldenRequest{Action: c.ID, ResourceKind: resourceKind},
		Rules:   c.rules(),
		Tier:    c.Tier,
		Trap:    c.Trap,
	}
	if pr.Error != "" {
		msg := pr.Error
		g.PlanError = &msg
	}
	if c.Degenerate.covers(tag) {
		reason := c.Degenerate.Reason
		g.DegenerateReason = &reason
	}
	if c.PlannerDivergence.covers(tag) {
		g.PlannerDivergence = c.PlannerDivergence
	}
	return g, nil
}

func oracleOf(allowed, total int) string {
	switch allowed {
	case 0:
		return "empty"
	case total:
		return "total"
	default:
		return "discriminating"
	}
}

// degeneracyErrors compares each recorded oracle with the case's declaration.
func degeneracyErrors(corpus *Corpus, tag string, goldens []*Golden) []string {
	var out []string
	for i, c := range corpus.Cases {
		g := goldens[i]
		if c.PlannerDivergence.covers(tag) {
			continue // check() and the plan disagree on this tag; its oracle proves nothing about seeds
		}
		switch {
		case c.Degenerate.covers(tag) && c.Degenerate.Oracle != g.Oracle:
			out = append(out, fmt.Sprintf("PDP %s, case %q: declares degenerate oracle %q, but check() allows %v (%s)", tag, c.ID, c.Degenerate.Oracle, g.Allowed, g.Oracle))
		case !c.Degenerate.covers(tag) && g.Oracle != "discriminating":
			out = append(out, fmt.Sprintf("PDP %s, case %q: oracle is %s but the case declares no `degenerate`; fix the seeds, or declare it with the reason it is degenerate by construction", tag, c.ID, g.Oracle))
		}
	}
	return out
}

// changesMarkdown is conformance/golden/CHANGES.md: what moved between the previous and the
// current PDP.
func changesMarkdown(prevTag, curTag string, prev, cur []*Golden) string {
	type change struct{ id, before, after string }
	var plans, allowed, planErrors []change
	for i := range cur {
		p, c := prev[i], cur[i]
		if !reflect.DeepEqual(p.Plan, c.Plan) {
			plans = append(plans, change{c.ID, compactJSON(p.Plan), compactJSON(c.Plan)})
		}
		if !reflect.DeepEqual(p.Allowed, c.Allowed) {
			allowed = append(allowed, change{c.ID, idList(p.Allowed), idList(c.Allowed)})
		}
		if !reflect.DeepEqual(p.PlanError, c.PlanError) {
			planErrors = append(planErrors, change{c.ID, strOrNone(p.PlanError), strOrNone(c.PlanError)})
		}
	}
	byID := func(cs []change) { sort.Slice(cs, func(i, j int) bool { return cs[i].id < cs[j].id }) }
	byID(plans)
	byID(allowed)
	byID(planErrors)

	var b strings.Builder
	fmt.Fprintf(&b, "<!-- %s -->\n\n", generatedNotes)
	fmt.Fprintf(&b, "# PDP %s → %s\n\n", prevTag, curTag)
	fmt.Fprintf(&b, "What changes for the %d cases between the previous PDP (`%s`) and the current one (`%s`).\n\n", len(cur), prevTag, curTag)
	fmt.Fprintf(&b, "| Change | Cases |\n|---|---|\n| Plan | %d |\n| Allowed set | %d |\n| Plan error | %d |\n\n", len(plans), len(allowed), len(planErrors))
	if len(plans)+len(allowed)+len(planErrors) == 0 {
		b.WriteString("## No differences\n\nEvery case records the same plan, allowed set and plan error on both versions.\n")
		return b.String()
	}
	section := func(title, lang string, cs []change) {
		if len(cs) == 0 {
			return
		}
		fmt.Fprintf(&b, "## %s\n", title)
		for _, c := range cs {
			fmt.Fprintf(&b, "\n### `%s`\n\n", c.id)
			if lang == "" {
				fmt.Fprintf(&b, "- `%s`: %s\n- `%s`: %s\n", prevTag, c.before, curTag, c.after)
			} else {
				fmt.Fprintf(&b, "`%s`:\n\n```%s\n%s\n```\n\n`%s`:\n\n```%s\n%s\n```\n", prevTag, lang, c.before, curTag, lang, c.after)
			}
		}
		b.WriteString("\n")
	}
	section("Plan changed", "json", plans)
	section("Allowed set changed", "", allowed)
	section("Plan error changed", "text", planErrors)
	return strings.TrimRight(b.String(), "\n") + "\n"
}

func compactJSON(v any) string {
	if v == nil || reflect.ValueOf(v).IsNil() {
		return "null"
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprintf("<%v>", err)
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func idList(ids []string) string {
	if len(ids) == 0 {
		return "(none)"
	}
	return strings.Join(ids, ", ")
}

func strOrNone(s *string) string {
	if s == nil {
		return "(none)"
	}
	return *s
}
