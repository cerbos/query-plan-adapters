package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// The corpus keys this projection reads. An unread key would vanish from the check() resource
// and from every harness's stored row at once, so the differential would agree without testing
// it: any key outside these sets is an error, and so is a missing one.
var (
	seedKeys     = []string{"id", "aBool", "aString", "aNumber", "aOptionalString", "aNumberList", "aBoolList", "tags", "subCategoryNames", "parentSeedId"}
	seedNoteKey  = "note"
	tagKeys      = []string{"id", "name"}
	derivedKeys  = []string{"createdBy", "aDouble", "createdAt", "updatedAt", "scope", "labels"}
	principalKey = []string{"id", "roles", "attr"}
)

// Dataset is the corpus as the PDP sees it: the principal verbatim, and one check() resource
// per seed, in seeds.json order.
type Dataset struct {
	Principal map[string]any
	Resources []Resource
}

// Resource is one seed projected to check() attributes.
type Resource struct {
	// Field order is key order: the file's keys are sorted.
	Attr map[string]any `json:"attr"`
	ID   string         `json:"id"`
}

func (d *Dataset) ids() []string {
	ids := make([]string, len(d.Resources))
	for i, r := range d.Resources {
		ids[i] = r.ID
	}
	return ids
}

func decodeJSONFile(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func loadDataset(conformanceDir string) (*Dataset, error) {
	var seedsFile struct {
		Principal map[string]any   `json:"principal"`
		Seeds     []map[string]any `json:"seeds"`
	}
	if err := decodeJSONFile(filepath.Join(conformanceDir, "seeds.json"), &seedsFile); err != nil {
		return nil, err
	}
	var derivedFile struct {
		Fields  []string                  `json:"fields"`
		Derived map[string]map[string]any `json:"derived"`
	}
	if err := decodeJSONFile(filepath.Join(conformanceDir, "derived-fields.json"), &derivedFile); err != nil {
		return nil, err
	}

	var errs []error
	if err := assertKeys("seeds.json principal", keysOf(seedsFile.Principal), principalKey, nil); err != nil {
		errs = append(errs, err)
	}
	if err := assertKeys("derived-fields.json fields", derivedFile.Fields, derivedKeys, nil); err != nil {
		errs = append(errs, err)
	}

	byID := map[string]map[string]any{}
	for i, seed := range seedsFile.Seeds {
		if err := assertKeys(fmt.Sprintf("seeds.json seeds[%d]", i), keysOf(seed), seedKeys, []string{seedNoteKey}); err != nil {
			errs = append(errs, err)
			continue
		}
		for j, tag := range seed["tags"].([]any) {
			if err := assertKeys(fmt.Sprintf("seeds.json seeds[%d].tags[%d]", i, j), keysOf(tag.(map[string]any)), tagKeys, nil); err != nil {
				errs = append(errs, err)
			}
		}
		id := seed["id"].(string)
		if _, dup := byID[id]; dup {
			errs = append(errs, fmt.Errorf("seeds.json: duplicate seed id %q", id))
		}
		byID[id] = seed
		d, ok := derivedFile.Derived[id]
		if !ok {
			errs = append(errs, fmt.Errorf("derived-fields.json has no entry for seed %q", id))
			continue
		}
		if err := assertKeys(fmt.Sprintf("derived-fields.json derived[%q]", id), keysOf(d), derivedKeys, nil); err != nil {
			errs = append(errs, err)
		}
	}
	if len(derivedFile.Derived) != len(byID) {
		errs = append(errs, errors.New("derived-fields.json must carry exactly one entry for each seed id"))
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	parentOf := func(seed map[string]any) (map[string]any, error) {
		if seed == nil || seed["parentSeedId"] == nil {
			return nil, nil
		}
		pid := seed["parentSeedId"].(string)
		p, ok := byID[pid]
		if !ok {
			return nil, fmt.Errorf("seeds.json: %q names parent %q, which is not a seed id", seed["id"], pid)
		}
		return p, nil
	}

	ds := &Dataset{Principal: seedsFile.Principal}
	for _, seed := range seedsFile.Seeds {
		id := seed["id"].(string)
		attr, err := checkAttr(seed, derivedFile.Derived[id], parentOf)
		if err != nil {
			return nil, err
		}
		ds.Resources = append(ds.Resources, Resource{ID: id, Attr: attr})
	}
	return ds, nil
}

// checkAttr is the seed -> check() resource projection every harness has used
// (activerecord/spec/support/adversarial_oracle.rb, prisma/src/adversarial.test.ts):
//
//   - a NULL column is a MISSING attribute (aOptionalString, aDouble, scope, createdAt,
//     updatedAt), so CEL errors and check() denies, like SQL's UNKNOWN;
//   - except owner, coOwner, tagNames, aNumberList and aBoolList, which send explicit nulls,
//     because CEL treats a null value differently from a missing one (#308);
//   - a NULL tag name or label name is a missing element attribute;
//   - mainCategory only when the seed has a category; parent / parent.inner only when the
//     to-one chain has that level (ADR 0005).
func checkAttr(seed, derived map[string]any, parentOf func(map[string]any) (map[string]any, error)) (map[string]any, error) {
	tags := seed["tags"].([]any)
	tagAttrs := make([]any, 0, len(tags))
	tagNames := make([]any, 0, len(tags))
	for _, t := range tags {
		tag := t.(map[string]any)
		ta := map[string]any{"id": tag["id"]}
		if tag["name"] != nil {
			ta["name"] = tag["name"]
		}
		tagAttrs = append(tagAttrs, ta)
		tagNames = append(tagNames, tag["name"])
	}

	labels, _ := derived["labels"].([]any)
	subNames := seed["subCategoryNames"].([]any)
	// A seed with subCategoryNames owns ONE category holding every name as a subcategory, so a
	// category can hold several subcategories, some matching a predicate and some not.
	categories := []any{}
	if len(subNames) > 0 {
		subAttrs := make([]any, 0, len(subNames))
		for _, sub := range subNames {
			labelAttrs := make([]any, 0, len(labels))
			for _, l := range labels {
				if l == nil {
					labelAttrs = append(labelAttrs, map[string]any{})
				} else {
					labelAttrs = append(labelAttrs, map[string]any{"name": l})
				}
			}
			subAttrs = append(subAttrs, map[string]any{
				"name":   sub,
				"labels": labelAttrs,
			})
		}
		categories = append(categories, map[string]any{
			"name":          "business",
			"subCategories": subAttrs,
		})
	}

	attr := map[string]any{
		"aBool":       seed["aBool"],
		"aString":     seed["aString"],
		"aNumber":     seed["aNumber"],
		"createdBy":   derived["createdBy"],
		"obj":         map[string]any{"inner": seed["aString"]},
		"tags":        tagAttrs,
		"owner":       seed["aOptionalString"],
		"coOwner":     derived["scope"],
		"tagNames":    tagNames,
		"aNumberList": seed["aNumberList"],
		"aBoolList":   seed["aBoolList"],
		"categories":  categories,
	}

	parent, err := parentOf(seed)
	if err != nil {
		return nil, err
	}
	if parent != nil {
		pa := relationAttr(parent)
		inner, err := parentOf(parent)
		if err != nil {
			return nil, err
		}
		if inner != nil {
			pa["inner"] = relationAttr(inner)
		}
		attr["parent"] = pa
	}

	setUnlessNull(attr, "aOptionalString", seed["aOptionalString"])
	setUnlessNull(attr, "aDouble", derived["aDouble"])
	setUnlessNull(attr, "scope", derived["scope"])
	setUnlessNull(attr, "createdAt", derived["createdAt"])
	setUnlessNull(attr, "updatedAt", derived["updatedAt"])

	if len(subNames) > 0 {
		subCats := make([]any, 0, len(subNames))
		for _, n := range subNames {
			subCats = append(subCats, map[string]any{"name": n})
		}
		attr["mainCategory"] = map[string]any{
			"name":          "business",
			"subCategories": subCats,
			"subNames":      subNames,
		}
	}
	return attr, nil
}

// relationAttr is one hop of the to-one chain; a NULL column is omitted, as on the root.
func relationAttr(seed map[string]any) map[string]any {
	a := map[string]any{
		"aBool":   seed["aBool"],
		"aString": seed["aString"],
		"aNumber": seed["aNumber"],
	}
	setUnlessNull(a, "aOptionalString", seed["aOptionalString"])
	return a
}

func setUnlessNull(m map[string]any, k string, v any) {
	if v != nil {
		m[k] = v
	}
}

func keysOf(m map[string]any) []string {
	return sortedKeys(m)
}

func assertKeys(label string, got, want, optional []string) error {
	wantSet := map[string]bool{}
	for _, k := range want {
		wantSet[k] = true
	}
	for _, k := range optional {
		wantSet[k] = true
	}
	gotSet := map[string]bool{}
	var unexpected, missing []string
	for _, k := range got {
		gotSet[k] = true
		if !wantSet[k] {
			unexpected = append(unexpected, k)
		}
	}
	for _, k := range want {
		if !gotSet[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(unexpected)
	sort.Strings(missing)
	if len(unexpected) > 0 {
		return fmt.Errorf("%s carries %v, which the generator does not project: an unread field disappears from the check() resource and from every harness's rows at once", label, unexpected)
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s is missing %v, which the generator reads", label, missing)
	}
	return nil
}

func buildResourcesJSON(ds *Dataset) ([]byte, error) {
	return marshalJSON(map[string]any{
		"kind":      resourceKind,
		"principal": ds.Principal,
		"resources": ds.Resources,
	})
}

// marshalJSON is the one encoding every generated JSON file uses: keys sorted (encoding/json
// sorts map keys), 2-space indent, no HTML escaping, trailing newline.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return keepNegativeZero(buf.Bytes()), nil
}

// keepNegativeZero rewrites the number token -0 as -0.0. encoding/json writes the double -0.0 as
// -0, which most JSON readers (Ruby, JavaScript's integer-looking path, Jackson into Object) parse
// as the integer 0 and so lose the sign — and a -0.0 divisor decides which infinity a division
// yields. -0.0 is the same JSON number, spelled so every reader keeps it a double.
func keepNegativeZero(in []byte) []byte {
	out := make([]byte, 0, len(in))
	inString := false
	for i := 0; i < len(in); i++ {
		c := in[i]
		switch {
		case inString:
			if c == '\\' && i+1 < len(in) {
				out = append(out, c, in[i+1])
				i++
				continue
			}
			if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
		case c == '-' && i+1 < len(in) && in[i+1] == '0' && (i+2 == len(in) || !isNumberByte(in[i+2])):
			out = append(out, "-0.0"...)
			i++
			continue
		}
		out = append(out, c)
	}
	return out
}

func isNumberByte(c byte) bool {
	return (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-'
}
