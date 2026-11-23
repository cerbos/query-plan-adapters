package main

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// fuzz -edges N adds up to N rows that sit on the edges of the run's own literals. Where -rows
// recombines values the seeds already hold, an edge row holds what a literal the run compares
// against makes interesting: the literal itself, one step either side of it, its case and padding
// variants, and the LIKE wildcards in its place, so a translation that is off by one comparison,
// collation or escape sees a row that tells.

// edgeKeys maps the pool path a literal was drawn for to the row field that holds that attribute.
// Relation paths read the related row's own fields, and edge rows are relatable, so a later row's
// parent can be one of them.
var edgeKeys = map[string]string{
	"aString":                           "aString",
	"obj.inner":                         "aString",
	"parent.aString":                    "aString",
	"parent.inner.aString":              "aString",
	"aOptionalString":                   "aOptionalString",
	"owner":                             "aOptionalString",
	"parent.aOptionalString":            "aOptionalString",
	"scope":                             "scope",
	"coOwner":                           "scope",
	"aNumber":                           "aNumber",
	"parent.aNumber":                    "aNumber",
	"aDouble":                           "aDouble",
	"aNumberList[]":                     "listElement",
	"tagNames[]":                        "tagName",
	"tags[].name":                       "tagName",
	"categories[].subCategories[].name": "subCategoryName",
	"createdAt":                         "createdAt",
	"updatedAt":                         "updatedAt",
	"createdBy":                         "createdBy",
}

// noteLiteral records a literal the fuzzer drew for an attribute path.
func (f *fuzzer) noteLiteral(path string, v any) {
	if f.lits == nil {
		f.lits = map[string][]any{}
	}
	f.lits[path] = append(f.lits[path], v)
}

// edgeValues derives, per row field, the edge values of every literal the run drew.
func edgeValues(lits map[string][]any) map[string][]any {
	out := map[string][]any{}
	seen := map[string]map[string]bool{}
	add := func(key string, v any) {
		raw, _ := json.Marshal(v)
		if seen[key] == nil {
			seen[key] = map[string]bool{}
		}
		if !seen[key][string(raw)] {
			seen[key][string(raw)] = true
			out[key] = append(out[key], v)
		}
	}
	paths := make([]string, 0, len(lits))
	for p := range lits {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, path := range paths {
		vs := lits[path]
		key, ok := edgeKeys[path]
		if !ok {
			continue
		}
		for _, v := range vs {
			switch t := v.(type) {
			case string:
				if key == "createdAt" || key == "updatedAt" || key == "createdBy" {
					for _, e := range timeEdges(t) {
						add(key, e)
					}
					continue
				}
				for _, e := range stringEdges(t, key == "aString") {
					add(key, e)
				}
			case float64:
				for _, e := range numberEdges(t, key == "aDouble") {
					add(key, e)
				}
			}
		}
	}
	return out
}

// stringEdges are s, the strings either side of it in code-point order, its padded and case
// variants, and LIKE wildcards standing in for its characters. A NUL variant only where a row may
// carry one (aString, under the nul-in-string capability).
func stringEdges(s string, nul bool) []string {
	out := []string{s, s + " ", " " + s, strings.ToUpper(s), strings.ToLower(s), s + "a", "a" + s}
	r := []rune(s)
	if len(r) > 0 {
		last := r[len(r)-1]
		out = append(out, string(r[:len(r)-1]), string(r[1:]))
		for _, d := range []rune{-1, 1} {
			if n := last + d; n > 0 && utf8.ValidRune(n) && unicode.IsPrint(n) {
				out = append(out, string(r[:len(r)-1])+string(n))
			}
		}
		out = append(out, string(unicode.ToUpper(r[0]))+string(r[1:]))
		for _, w := range []string{"_", "%"} {
			out = append(out, w+string(r[1:]), string(r[:len(r)-1])+w)
		}
	}
	if nul {
		out = append(out, s+"\x00", s+"\x00x")
	}
	return out
}

// numberEdges are x and one step either side: 1 for an int column; for a double one, 0.5 and the
// step in x's fifteenth significant digit, the finest every store keeps (Prisma writes 15 digits,
// and a subnormal step underflows PostgreSQL arithmetic). Ints stay inside int32, the narrowest
// column a harness declares.
func numberEdges(x float64, double bool) []any {
	if !double {
		var out []any
		for _, d := range []float64{-1, 0, 1} {
			n := math.Round(x) + d
			if n >= math.MinInt32 && n <= math.MaxInt32 {
				out = append(out, json.Number(strconv.FormatInt(int64(n), 10)))
			}
		}
		return out
	}
	step := 1e-15
	if x != 0 {
		step = math.Pow(10, math.Floor(math.Log10(math.Abs(x)))-14)
	}
	var out []any
	for _, n := range []float64{x, x + step, x - step, x + 0.5, x - 0.5, -x} {
		out = append(out, json.Number(strconv.FormatFloat(n, 'g', 15, 64)))
	}
	return out
}

// timeEdges are t and a millisecond and a second either side. Never finer: a sub-millisecond
// instant is a known mapping hazard on millisecond stores (README.md, "Mapping hazards").
func timeEdges(s string) []string {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	var out []string
	for _, d := range []time.Duration{0, time.Millisecond, -time.Millisecond, time.Second, -time.Second} {
		out = append(out, t.Add(d).UTC().Format("2006-01-02T15:04:05.000Z"))
	}
	return out
}

// edgeRows spreads the edge values over at most n rows: row i holds value i of every field's
// shuffled list, cycling the shorter lists, so each value lands in a row when no field has more
// than n.
func edgeRows(rng *rand.Rand, edges map[string][]any, n int) []map[string]any {
	keys := make([]string, 0, len(edges))
	for k := range edges {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	most := 0
	for _, k := range keys {
		vs := edges[k]
		rng.Shuffle(len(vs), func(i, j int) { vs[i], vs[j] = vs[j], vs[i] })
		most = max(most, len(vs))
	}
	rows := make([]map[string]any, min(n, most))
	for i := range rows {
		rows[i] = map[string]any{}
		for _, k := range keys {
			vs := edges[k]
			rows[i][k] = vs[i%len(vs)]
		}
	}
	return rows
}
