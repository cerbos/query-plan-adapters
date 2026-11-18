package main

import (
	"fmt"
	"sort"
	"time"
)

type Resource struct {
	ID   string         `json:"id"`
	Attr map[string]any `json:"attr"`
}

type projector struct {
	s     *Schema
	d     *Data
	index map[string]map[string]map[string]any
}

func project(s *Schema, d *Data) ([]Resource, error) {
	p := &projector{s: s, d: d, index: map[string]map[string]map[string]any{}}
	for _, t := range s.Tables {
		if len(t.PK) != 1 {
			continue
		}
		m := map[string]map[string]any{}
		for _, row := range d.Tables[t.Name] {
			k := fmt.Sprint(row[t.PK[0]])
			if _, dup := m[k]; dup {
				return nil, fmt.Errorf("%s: duplicate pk %s", t.Name, k)
			}
			m[k] = row
		}
		p.index[t.Name] = m
	}
	root := s.byName[s.Root]
	var out []Resource
	for _, row := range d.Tables[s.Root] {
		attr, err := p.object(root, row, s.Attrs)
		if err != nil {
			return nil, fmt.Errorf("%s %v: %w", s.Root, row[root.PK[0]], err)
		}
		out = append(out, Resource{ID: fmt.Sprint(row[root.PK[0]]), Attr: attr})
	}
	return out, nil
}

func (p *projector) object(t *Table, row map[string]any, attrs []*Attr) (map[string]any, error) {
	obj := map[string]any{}
	set := func(name string, v any) {
		if v == nil && p.s.Nulls == "omitted" {
			return
		}
		obj[name] = v
	}
	for _, a := range attrs {
		switch {
		case a.Column != "":
			c, _ := t.col(a.Column)
			v, err := convert(c, row[a.Column])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", a.Name, err)
			}
			set(a.Name, v)
		case a.Embedded:
			sub, err := p.object(t, row, a.Attributes)
			if err != nil {
				return nil, fmt.Errorf("%s.%w", a.Name, err)
			}
			obj[a.Name] = sub
		case a.One != "":
			c, _ := t.col(a.One)
			fk := row[a.One]
			if fk == nil {
				set(a.Name, nil)
				continue
			}
			target := p.s.byName[c.Type.Ref]
			trow := p.index[target.Name][fmt.Sprint(fk)]
			if trow == nil {
				return nil, fmt.Errorf("%s: %s %v not found", a.Name, target.Name, fk)
			}
			sub, err := p.object(target, trow, a.Attributes)
			if err != nil {
				return nil, fmt.Errorf("%s.%w", a.Name, err)
			}
			set(a.Name, sub)
		case a.Many != "":
			child := p.s.byName[a.Many]
			var rows []map[string]any
			for _, crow := range p.d.Tables[a.Many] {
				if fmt.Sprint(crow[a.FK]) == fmt.Sprint(row[t.PK[0]]) {
					rows = append(rows, crow)
				}
			}
			if a.OrderBy != "" {
				sort.SliceStable(rows, func(i, j int) bool { return fmt.Sprint(rows[i][a.OrderBy]) < fmt.Sprint(rows[j][a.OrderBy]) })
			}
			elemTable := child
			list := []any{}
			for _, crow := range rows {
				erow := crow
				if a.Via != "" {
					c, _ := child.col(a.Via)
					elemTable = p.s.byName[c.Type.Ref]
					erow = p.index[elemTable.Name][fmt.Sprint(crow[a.Via])]
					if erow == nil {
						return nil, fmt.Errorf("%s: %s %v not found", a.Name, elemTable.Name, crow[a.Via])
					}
				}
				if a.Value != "" {
					c, _ := elemTable.col(a.Value)
					v, err := convert(c, erow[a.Value])
					if err != nil {
						return nil, err
					}
					list = append(list, v)
					continue
				}
				sub, err := p.object(elemTable, erow, a.Attributes)
				if err != nil {
					return nil, fmt.Errorf("%s[].%w", a.Name, err)
				}
				list = append(list, sub)
			}
			obj[a.Name] = list
		}
	}
	return obj, nil
}

func convert(c Column, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch c.Type.Base {
	case "string":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%s: want string, got %T", c.Name, v)
		}
		return s, nil
	case "int", "bigint":
		switch n := v.(type) {
		case int:
			return n, nil
		}
		return nil, fmt.Errorf("%s: want int, got %T", c.Name, v)
	case "float", "decimal":
		switch n := v.(type) {
		case int:
			return float64(n), nil
		case float64:
			return n, nil
		}
		return nil, fmt.Errorf("%s: want number, got %T", c.Name, v)
	case "bool":
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("%s: want bool, got %T", c.Name, v)
		}
		return b, nil
	case "timestamp":
		var t time.Time
		switch x := v.(type) {
		case time.Time:
			t = x
		case string:
			var err error
			if t, err = time.Parse(time.RFC3339Nano, x); err != nil {
				return nil, fmt.Errorf("%s: %w", c.Name, err)
			}
		default:
			return nil, fmt.Errorf("%s: want timestamp, got %T", c.Name, v)
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	case "json":
		return v, nil
	case "string[]":
		l, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("%s: want list, got %T", c.Name, v)
		}
		return l, nil
	}
	return nil, fmt.Errorf("unknown type %s", c.Type.Base)
}
