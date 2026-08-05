// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import "fmt"

// RelationKind distinguishes a to-one hop from a to-many collection.
type RelationKind uint8

const (
	// RelationOne is a to-one hop: `R.attr.owner.name` reads a column on a single related row.
	RelationOne RelationKind = iota
	// RelationMany is a to-many collection: `R.attr.tags` is what a collection macro iterates.
	RelationMany
)

// Relation describes how a collection- or object-valued attribute reaches another table.
//
// The correlation is always `<Table>.<TargetColumn> = <parent>.<SourceColumn>`, where the parent
// is the enclosing scope's table — the root table at the top level, or the previous hop's table
// inside a nested lambda. Getting that rebase wrong is what the corpus's `outer-attr-depth2` and
// `w2-outer-relation` actions exist to catch.
type Relation struct {
	Field        *Entry
	Fields       map[string]Entry
	Table        string
	SourceColumn string
	TargetColumn string
	Via          []Hop
	Kind         RelationKind
}

// Hop is one intermediate table in a flattened relation chain.
type Hop struct {
	// Table is the intermediate table being joined in.
	Table string
	// ChildColumn is the referencing column on the table one step further in — the element
	// table for the first hop, the previous hop's table after that.
	ChildColumn string
	// JoinColumn is the referenced column on this hop's table.
	JoinColumn string
}

// ValueType marks an attribute whose stored representation needs special handling.
type ValueType uint8

const (
	// ValueDefault is an ordinary column.
	ValueDefault ValueType = iota
	// ValueTimestamp marks a column stored as a temporal type, so a `timestamp()` conversion
	// around it is a no-op rather than an unsupported shape.
	ValueTimestamp
)

// Entry is what a plan variable resolves to.
type Entry struct {
	Relation         *Relation
	Column           string
	Qualifier        string
	ValueType        ValueType
	ScalarCollection bool
}

// Mapper resolves a plan variable — the full reference as the planner emits it, e.g.
// `request.resource.attr.aString` — to the storage it lives in.
//
// Resolution is deliberately fail-closed: an unmapped reference is an error, never a guessed
// column name. Snake-casing an attribute automatically (which the original Go helpers did) turns
// a policy typo into a silent SQL error or, worse, a match against the wrong column.
type Mapper interface {
	Resolve(reference string) (Entry, bool)
}

// MapperFunc adapts a function to the Mapper interface.
type MapperFunc func(reference string) (Entry, bool)

// Resolve implements Mapper.
func (f MapperFunc) Resolve(reference string) (Entry, bool) { return f(reference) }

// MapperMap is the common case: a static table of references to entries.
type MapperMap map[string]Entry

// Resolve implements Mapper.
func (m MapperMap) Resolve(reference string) (Entry, bool) {
	e, ok := m[reference]
	return e, ok
}

// rootMapper stamps the resource table onto every entry the caller's mapper returns.
//
// Without it, an entry resolved from inside a lambda body would inherit whatever scope happened
// to be current, so `R.attr.aBool` referenced two lambdas deep would read the innermost element's
// alias instead of the resource row. Every Entry therefore carries the qualifier of the row it is
// read from — the resource table here, the element alias in scopedMapper — and the translator
// never has to guess from context.
type rootMapper struct {
	parent Mapper
	table  string
}

func (r rootMapper) Resolve(reference string) (Entry, bool) {
	entry, ok := r.parent.Resolve(reference)
	if !ok {
		return Entry{}, false
	}
	if entry.Qualifier == "" {
		entry.Qualifier = r.table
	}
	return entry, true
}

// scopedMapper resolves lambda-local references (`t.name`) against the element fields of the
// collection currently being iterated, falling back to the enclosing mapper for everything else —
// which is how an outer resource attribute stays readable from inside a nested lambda body.
type scopedMapper struct {
	parent   Mapper
	relation *Relation
	variable string
	alias    string
}

func (s scopedMapper) Resolve(reference string) (Entry, bool) {
	if reference == s.variable {
		// A bare reference to the iteration variable: only meaningful for a scalar collection,
		// where the element *is* the column.
		if s.relation.Field != nil {
			e := *s.relation.Field
			e.Qualifier = s.alias
			return e, true
		}
		return Entry{}, false
	}

	if prefix := s.variable + "."; len(reference) > len(prefix) && reference[:len(prefix)] == prefix {
		field := reference[len(prefix):]
		e, ok := s.relation.Fields[field]
		if !ok {
			return Entry{}, false
		}
		e.Qualifier = s.alias
		return e, true
	}

	return s.parent.Resolve(reference)
}

// requireRelation resolves a reference that must name a collection.
func requireRelation(m Mapper, reference string) (Entry, error) {
	entry, ok := m.Resolve(reference)
	if !ok {
		return Entry{}, fmt.Errorf("no mapping for collection attribute %q", reference)
	}
	if entry.Relation == nil {
		return Entry{}, fmt.Errorf(
			"attribute %q is mapped to a column but is used as a collection; map it as a relation",
			reference,
		)
	}
	return entry, nil
}
