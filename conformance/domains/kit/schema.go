package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type ColType struct {
	Base     string
	Nullable bool
	Ref      string
}

func parseColType(s string) (ColType, error) {
	var c ColType
	s = strings.TrimSpace(s)
	if i := strings.Index(s, "->"); i >= 0 {
		c.Ref = strings.TrimSpace(s[i+2:])
		s = strings.TrimSpace(s[:i])
	}
	if strings.HasSuffix(s, "?") {
		c.Nullable = true
		s = strings.TrimSuffix(s, "?")
	}
	switch s {
	case "string", "int", "bigint", "float", "decimal", "bool", "timestamp", "json", "string[]":
	default:
		return c, fmt.Errorf("unknown column type %q", s)
	}
	c.Base = s
	return c, nil
}

type Column struct {
	Name string
	Type ColType
}

type Table struct {
	Name    string
	PK      []string
	Columns []Column
}

func (t *Table) col(name string) (Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

type Attr struct {
	Name       string
	Column     string
	One        string
	Many       string
	FK         string
	Value      string
	Via        string
	Embedded   bool
	OrderBy    string
	Attributes []*Attr
}

type Schema struct {
	Kind   string
	Nulls  string
	Root   string
	Tables []*Table
	byName map[string]*Table
	Attrs  []*Attr
	Note   string
}

type rawSchema struct {
	Kind     string    `yaml:"kind"`
	Nulls    string    `yaml:"nulls"`
	Note     string    `yaml:"note"`
	Tables   yaml.Node `yaml:"tables"`
	Resource struct {
		Table      string    `yaml:"table"`
		Attributes yaml.Node `yaml:"attributes"`
	} `yaml:"resource"`
}

func loadSchema(dir string) (*Schema, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "schema.yaml"))
	if err != nil {
		return nil, err
	}
	var r rawSchema
	if err := yaml.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("schema.yaml: %w", err)
	}
	s := &Schema{Kind: r.Kind, Nulls: r.Nulls, Root: r.Resource.Table, byName: map[string]*Table{}, Note: r.Note}
	if s.Nulls == "" {
		s.Nulls = "omitted"
	}
	if s.Nulls != "omitted" && s.Nulls != "explicit" {
		return nil, fmt.Errorf("nulls must be omitted or explicit")
	}
	if r.Tables.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("tables must be a mapping")
	}
	for i := 0; i < len(r.Tables.Content); i += 2 {
		name := r.Tables.Content[i].Value
		body := r.Tables.Content[i+1]
		t := &Table{Name: name}
		var tb struct {
			PK      any       `yaml:"pk"`
			Columns yaml.Node `yaml:"columns"`
		}
		if err := body.Decode(&tb); err != nil {
			return nil, fmt.Errorf("table %s: %w", name, err)
		}
		switch pk := tb.PK.(type) {
		case string:
			t.PK = []string{pk}
		case []any:
			for _, p := range pk {
				t.PK = append(t.PK, fmt.Sprint(p))
			}
		default:
			return nil, fmt.Errorf("table %s: pk required", name)
		}
		for j := 0; j < len(tb.Columns.Content); j += 2 {
			ct, err := parseColType(tb.Columns.Content[j+1].Value)
			if err != nil {
				return nil, fmt.Errorf("table %s column %s: %w", name, tb.Columns.Content[j].Value, err)
			}
			t.Columns = append(t.Columns, Column{Name: tb.Columns.Content[j].Value, Type: ct})
		}
		s.Tables = append(s.Tables, t)
		s.byName[name] = t
	}
	for _, t := range s.Tables {
		for _, p := range t.PK {
			if _, ok := t.col(p); !ok {
				return nil, fmt.Errorf("table %s: pk column %s missing", t.Name, p)
			}
		}
		for _, c := range t.Columns {
			if c.Type.Ref != "" && s.byName[c.Type.Ref] == nil {
				return nil, fmt.Errorf("table %s column %s: unknown ref %s", t.Name, c.Name, c.Type.Ref)
			}
		}
	}
	root := s.byName[s.Root]
	if root == nil || len(root.PK) != 1 {
		return nil, fmt.Errorf("resource.table %q must be a table with a single-column pk", s.Root)
	}
	s.Attrs, err = parseAttrs(&r.Resource.Attributes, s, root)
	return s, err
}

func parseAttrs(n *yaml.Node, s *Schema, t *Table) ([]*Attr, error) {
	var out []*Attr
	for i := 0; i < len(n.Content); i += 2 {
		name := n.Content[i].Value
		v := n.Content[i+1]
		a := &Attr{Name: name}
		if v.Kind == yaml.ScalarNode {
			a.Column = v.Value
			if _, ok := t.col(a.Column); !ok {
				return nil, fmt.Errorf("attribute %s: table %s has no column %s", name, t.Name, a.Column)
			}
			out = append(out, a)
			continue
		}
		var body struct {
			One     string    `yaml:"one"`
			Many    string    `yaml:"many"`
			FK      string    `yaml:"fk"`
			Value   string    `yaml:"value"`
			Via     string    `yaml:"via"`
			OrderBy string    `yaml:"orderBy"`
			Attrs   yaml.Node `yaml:"attributes"`
		}
		if err := v.Decode(&body); err != nil {
			return nil, fmt.Errorf("attribute %s: %w", name, err)
		}
		a.One, a.Many, a.FK, a.Value, a.Via, a.OrderBy = body.One, body.Many, body.FK, body.Value, body.Via, body.OrderBy
		var target *Table
		switch {
		case a.One != "":
			c, ok := t.col(a.One)
			if !ok || c.Type.Ref == "" {
				return nil, fmt.Errorf("attribute %s: %s.%s is not a foreign key", name, t.Name, a.One)
			}
			target = s.byName[c.Type.Ref]
		case a.Many != "":
			child := s.byName[a.Many]
			if child == nil {
				return nil, fmt.Errorf("attribute %s: unknown table %s", name, a.Many)
			}
			if c, ok := child.col(a.FK); !ok || c.Type.Ref != t.Name {
				return nil, fmt.Errorf("attribute %s: %s.%s must reference %s", name, a.Many, a.FK, t.Name)
			}
			target = child
			if a.Via != "" {
				c, ok := child.col(a.Via)
				if !ok || c.Type.Ref == "" {
					return nil, fmt.Errorf("attribute %s: via %s is not a foreign key", name, a.Via)
				}
				target = s.byName[c.Type.Ref]
			}
			if a.Value != "" {
				if _, ok := target.col(a.Value); !ok {
					return nil, fmt.Errorf("attribute %s: %s has no column %s", name, target.Name, a.Value)
				}
				continue_ := len(body.Attrs.Content) == 0
				if !continue_ {
					return nil, fmt.Errorf("attribute %s: value and attributes are exclusive", name)
				}
				out = append(out, a)
				continue
			}
		case len(body.Attrs.Content) > 0:
			a.Embedded = true
			target = t
		default:
			return nil, fmt.Errorf("attribute %s: needs a column, one, many or attributes", name)
		}
		sub, err := parseAttrs(&body.Attrs, s, target)
		if err != nil {
			return nil, fmt.Errorf("%s.%w", name, err)
		}
		a.Attributes = sub
		out = append(out, a)
	}
	return out, nil
}

type Data struct {
	Tables     map[string][]map[string]any `yaml:"tables" json:"tables"`
	Principals map[string]map[string]any   `yaml:"principals" json:"principals"`
	Actions    []string                    `yaml:"actions" json:"actions"`
}

func loadData(dir string, s *Schema) (*Data, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "data.yaml"))
	if err != nil {
		return nil, err
	}
	var d Data
	if err := yaml.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("data.yaml: %w", err)
	}
	for name, rows := range d.Tables {
		t := s.byName[name]
		if t == nil {
			return nil, fmt.Errorf("data.yaml: unknown table %s", name)
		}
		for i, row := range rows {
			for k := range row {
				if _, ok := t.col(k); !ok {
					return nil, fmt.Errorf("data.yaml: %s row %d: unknown column %s", name, i, k)
				}
			}
			for _, c := range t.Columns {
				v, ok := row[c.Name]
				if !ok || v == nil {
					if !c.Type.Nullable {
						return nil, fmt.Errorf("data.yaml: %s row %d: %s is not nullable", name, i, c.Name)
					}
					row[c.Name] = nil
				}
			}
		}
	}
	for _, t := range s.Tables {
		if _, ok := d.Tables[t.Name]; !ok {
			d.Tables[t.Name] = nil
		}
	}
	names := make([]string, 0, len(d.Principals))
	for n := range d.Principals {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		p := d.Principals[n]
		if p["id"] == nil || p["roles"] == nil {
			return nil, fmt.Errorf("principal %s needs id and roles", n)
		}
	}
	return &d, nil
}
