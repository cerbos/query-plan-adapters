// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent

import (
	"fmt"
	"strings"

	"github.com/cerbos/query-plan-adapters/ent/internal/queryplan"
)

// Schema is the part of an ent schema graph the adapter reads: each type's table, columns and
// edges, with every edge resolved to the joins ent itself runs to traverse it. Build one with
// the entcerbos package, at runtime from your schema types or at code generation as an entc
// extension, rather than by hand.
type Schema struct {
	Types []SchemaType
}

// SchemaType is one ent type.
type SchemaType struct {
	// Columns maps each column to its ent field type name (field.Type.String()).
	Columns map[string]string
	Name    string
	Table   string
	// ID is the id column, empty for a composite id.
	ID    string
	Edges []SchemaEdge
}

// SchemaEdge is one edge of a type, as ent resolved it.
type SchemaEdge struct {
	Name string
	// Type is the name of the type the edge reaches.
	Type string
	// Join is the path from the owner's row to the target's row: one step, or two through an
	// M2M join table.
	Join []SchemaJoin
	// Unique is ent's to-one flag: the edge reaches at most one row.
	Unique bool
}

// SchemaJoin is one step of an edge: the rows of Table whose ToColumn equals FromColumn on the
// row the step starts from.
type SchemaJoin struct {
	Table      string
	FromColumn string
	ToColumn   string
}

// SchemaMap maps plan references to schema entries, for Schema.Mapper.
type SchemaMap map[string]SchemaEntry

// SchemaEntry maps one attribute onto a column or an edge of the schema. Whether an edge is read
// as a collection or through a to-one hop, and the tables and columns joining it, come from the
// schema.
type SchemaEntry struct {
	// Element, Fields, SubqueryFilter and Position apply to a to-many Edge, as the Relation fields
	// of the same names do. Element and Fields are read from the edge's target type.
	Element *SchemaEntry
	Fields  map[string]SchemaEntry
	// Edge is a dot-separated path of edge names from the entry's type. Empty for a column on the
	// entry's own row.
	Edge string
	// Column is a column on the row the entry reads: the entry's own row, or the target of a
	// to-one Edge. Empty with a to-one Edge for the joined row itself.
	Column         string
	Position       string
	SubqueryFilter []Restriction
	// ValueType is inferred from the column's ent field type when left as ValueDefault.
	ValueType      queryplan.ValueType
	NullConvention NullConvention
}

// Mapper resolves every entry against the schema, starting from the root type, and returns an
// ordinary MapperMap. An edge or column the schema does not have is an error. Entries the schema
// cannot express can be added to the returned map directly.
func (s *Schema) Mapper(root string, entries SchemaMap) (MapperMap, error) {
	typ, err := s.typeNamed(root)
	if err != nil {
		return nil, err
	}
	out := make(MapperMap, len(entries))
	for reference, entry := range entries {
		resolved, err := s.resolve(typ, entry)
		if err != nil {
			return nil, fmt.Errorf("cerbosent: %s: %w", reference, err)
		}
		out[reference] = resolved
	}
	return out, nil
}

func (s *Schema) typeNamed(name string) (*SchemaType, error) {
	for i := range s.Types {
		if s.Types[i].Name == name {
			return &s.Types[i], nil
		}
	}
	return nil, fmt.Errorf("cerbosent: the schema has no type %q", name)
}

func (s *Schema) resolve(typ *SchemaType, entry SchemaEntry) (Entry, error) {
	if entry.Edge == "" {
		if entry.Position != "" || entry.Element != nil || entry.Fields != nil || entry.SubqueryFilter != nil {
			return Entry{}, fmt.Errorf("position, element, fields and subquery filter need a to-many edge")
		}
		return column(typ, entry)
	}

	target, rel, unique, err := s.walk(typ, entry.Edge)
	if err != nil {
		return Entry{}, err
	}
	rel.SubqueryFilter = entry.SubqueryFilter

	if unique {
		if entry.Position != "" || entry.Element != nil || entry.Fields != nil {
			return Entry{}, fmt.Errorf("edge %q is to-one; Position, Element and Fields need a to-many edge", entry.Edge)
		}
		if entry.Column == "" {
			return Entry{ScalarRelation: rel, NullConvention: entry.NullConvention}, nil
		}
		col, err := column(target, entry)
		if err != nil {
			return Entry{}, err
		}
		col.ScalarRelation = rel
		return col, nil
	}

	if entry.Column != "" {
		return Entry{}, fmt.Errorf("edge %q is to-many; map its element with Element or Fields, not Column", entry.Edge)
	}
	rel.Position = entry.Position
	if rel.Position != "" {
		if _, ok := target.Columns[rel.Position]; !ok {
			return Entry{}, fmt.Errorf("type %s has no column %q", target.Name, rel.Position)
		}
	}
	if entry.Element != nil {
		if entry.Element.Edge != "" {
			return Entry{}, fmt.Errorf("an Element is a column of the edge's target, not an edge")
		}
		element, err := column(target, *entry.Element)
		if err != nil {
			return Entry{}, err
		}
		rel.Field = &element
	}
	if entry.Fields != nil {
		rel.Fields = make(map[string]Entry, len(entry.Fields))
		for name, field := range entry.Fields {
			resolved, err := s.resolve(target, field)
			if err != nil {
				return Entry{}, fmt.Errorf("field %s: %w", name, err)
			}
			rel.Fields[name] = resolved
		}
	}
	return Entry{Relation: rel}, nil
}

// walk follows a dotted edge path and lowers the joins it makes into a Relation: the first
// step correlates with the starting row, and every table before the last becomes a Hop.
func (s *Schema) walk(typ *SchemaType, path string) (*SchemaType, *Relation, bool, error) {
	var steps []SchemaJoin
	unique := true
	for name := range strings.SplitSeq(path, ".") {
		edge, err := edgeNamed(typ, name)
		if err != nil {
			return nil, nil, false, err
		}
		if len(edge.Join) == 0 {
			return nil, nil, false, fmt.Errorf("edge %s.%s has no join", typ.Name, name)
		}
		steps = append(steps, edge.Join...)
		unique = unique && edge.Unique
		if typ, err = s.typeNamed(edge.Type); err != nil {
			return nil, nil, false, err
		}
	}

	last := len(steps) - 1
	rel := &Relation{
		Table:        steps[last].Table,
		SourceColumn: steps[0].FromColumn,
		TargetColumn: steps[0].ToColumn,
	}
	for i := last - 1; i >= 0; i-- {
		rel.Via = append(rel.Via, Hop{
			Table:       steps[i].Table,
			ChildColumn: steps[i+1].ToColumn,
			JoinColumn:  steps[i+1].FromColumn,
		})
	}
	return typ, rel, unique, nil
}

func edgeNamed(typ *SchemaType, name string) (*SchemaEdge, error) {
	for i := range typ.Edges {
		if typ.Edges[i].Name == name {
			return &typ.Edges[i], nil
		}
	}
	return nil, fmt.Errorf("type %s has no edge %q", typ.Name, name)
}

func column(typ *SchemaType, entry SchemaEntry) (Entry, error) {
	fieldType, ok := typ.Columns[entry.Column]
	if !ok {
		return Entry{}, fmt.Errorf("type %s has no column %q", typ.Name, entry.Column)
	}
	valueType := entry.ValueType
	if valueType == ValueDefault {
		valueType = valueTypeOf(fieldType)
	}
	return Entry{Column: entry.Column, ValueType: valueType, NullConvention: entry.NullConvention}, nil
}

// valueTypeOf reads a ValueType from an ent field type name. A type with no CEL counterpart the
// translator handles, such as JSON, bytes or a UUID, stays undeclared.
func valueTypeOf(fieldType string) queryplan.ValueType {
	switch fieldType {
	case "bool":
		return ValueBool
	case "time.Time":
		return ValueTimestamp
	case "string":
		return ValueString
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"float32", "float64":
		return ValueNumber
	default:
		return ValueDefault
	}
}
