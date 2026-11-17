// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package cerbosent

import (
	"fmt"
	"slices"
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
	// Fields maps each ent field name, the id's included, to its column.
	Fields map[string]string
	Name   string
	Table  string
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

// SchemaMap maps plan references to schema entries, for Schema.Mapper. Each entry overrides
// what the mapper would infer for its reference.
type SchemaMap map[string]SchemaEntry

// SchemaEntry maps one attribute onto a column or an edge of the schema. Whether an edge is read
// as a collection or through a to-one hop, and the tables and columns joining it, come from the
// schema.
type SchemaEntry struct {
	// Declared, when set, is used as it is, with nothing read from the schema.
	Declared *Entry
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

// SchemaOption configures Schema.Mapper.
type SchemaOption func(*schemaOptions)

type schemaOptions struct {
	nullConvention NullConvention
}

// WithInferredNullConvention declares the NULL convention of every inferred column on the
// resource row or read through a to-one edge. Columns inside a collection's elements follow the
// call's NullRepresentation.
func WithInferredNullConvention(c NullConvention) SchemaOption {
	return func(o *schemaOptions) { o.nullConvention = c }
}

// Mapper resolves plan references against the schema, starting from the root type.
//
// A reference with no override is inferred from its name: `request.resource.id` is the id
// column, and each segment of `request.resource.attr.a.b.c` names an edge to follow or, last, a
// column, by its ent name (`owner_id`) or that name in lower camel case (`ownerId`). A path
// ending on a to-one edge reads the joined row; one ending on a to-many edge is a collection whose
// elements expose their columns and to-many edges the same way. A name the schema does not have,
// or one that matches two things, is left unmapped, which the translator refuses.
//
// overrides replaces inference for the references it names: a custom column or edge, a list's
// Position or element column, a SubqueryFilter, or a null convention. An override the schema
// cannot resolve is an error here.
func (s *Schema) Mapper(root string, overrides SchemaMap, opts ...SchemaOption) (Mapper, error) {
	typ, err := s.typeNamed(root)
	if err != nil {
		return nil, err
	}
	o := schemaOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	fixed := make(MapperMap, len(overrides))
	for reference, entry := range overrides {
		resolved, err := s.resolve(typ, entry)
		if err != nil {
			return nil, fmt.Errorf("cerbosent: %s: %w", reference, err)
		}
		fixed[reference] = resolved
	}
	return MapperFunc(func(reference string) (Entry, bool) {
		if entry, ok := fixed[reference]; ok {
			return entry, true
		}
		return s.infer(typ, reference, o)
	}), nil
}

const elementDepth = 3

func (s *Schema) infer(typ *SchemaType, reference string, o schemaOptions) (Entry, bool) {
	if reference == "request.resource.id" {
		if typ.ID == "" {
			return Entry{}, false
		}
		e, err := column(typ, SchemaEntry{Column: typ.ID, NullConvention: o.nullConvention})
		return e, err == nil
	}
	path, ok := strings.CutPrefix(reference, "request.resource.attr.")
	if !ok {
		return Entry{}, false
	}
	segments := strings.Split(path, ".")
	var edges []string
	cur := typ
	for i, segment := range segments {
		edge, edgeOK := matchEdge(cur, segment)
		col, colOK := matchColumn(cur, segment)
		switch {
		case edgeOK && !colOK:
			edges = append(edges, edge.Name)
			next, err := s.typeNamed(edge.Type)
			if err != nil {
				return Entry{}, false
			}
			cur = next
		case colOK && !edgeOK && i == len(segments)-1:
			entry := SchemaEntry{Edge: strings.Join(edges, "."), Column: col, NullConvention: o.nullConvention}
			e, err := s.resolve(typ, entry)
			return e, err == nil
		default:
			return Entry{}, false
		}
	}

	target, rel, unique, err := s.walk(typ, strings.Join(edges, "."))
	if err != nil {
		return Entry{}, false
	}
	if unique {
		return Entry{ScalarRelation: rel, NullConvention: o.nullConvention}, true
	}
	rel.Fields = s.elementFields(target, elementDepth)
	return Entry{Relation: rel}, true
}

// elementFields exposes a collection element's columns and to-many edges under both spellings of
// their names.
func (s *Schema) elementFields(typ *SchemaType, depth int) map[string]Entry {
	fields := map[string]Entry{}
	for name, col := range typ.Fields {
		e, _ := column(typ, SchemaEntry{Column: col})
		for _, spelling := range spellings(name) {
			fields[spelling] = e
		}
	}
	if depth == 0 {
		return fields
	}
	for _, edge := range typ.Edges {
		if edge.Unique {
			continue
		}
		target, rel, _, err := s.walk(typ, edge.Name)
		if err != nil {
			continue
		}
		rel.Fields = s.elementFields(target, depth-1)
		for _, name := range spellings(edge.Name) {
			fields[name] = Entry{Relation: rel}
		}
	}
	return fields
}

func spellings(name string) []string {
	if camel := lowerCamel(name); camel != name {
		return []string{name, camel}
	}
	return []string{name}
}

func lowerCamel(name string) string {
	parts := strings.Split(name, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

func matchEdge(typ *SchemaType, segment string) (*SchemaEdge, bool) {
	var found *SchemaEdge
	for i := range typ.Edges {
		if slices.Contains(spellings(typ.Edges[i].Name), segment) {
			if found != nil {
				return nil, false
			}
			found = &typ.Edges[i]
		}
	}
	return found, found != nil
}

func matchColumn(typ *SchemaType, segment string) (string, bool) {
	found := ""
	for name, col := range typ.Fields {
		if slices.Contains(spellings(name), segment) {
			if found != "" {
				return "", false
			}
			found = col
		}
	}
	return found, found != ""
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
	if entry.Declared != nil {
		return *entry.Declared, nil
	}
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
