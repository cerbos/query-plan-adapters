// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

package queryplan

import (
	"fmt"
	"strings"
)

// Relation describes how a collection- or object-valued attribute reaches another table.
//
// The correlation is always `<Table>.<TargetColumn> = <parent>.<SourceColumn>`, where the parent
// is the enclosing scope's table — the root table at the top level, or the previous hop's table
// inside a nested lambda. Getting that rebase wrong is what the corpus's `outer-attr-depth2` and
// `w2-outer-relation` actions exist to catch.
type Relation struct {
	// Field maps a scalar collection's element to its single column, e.g. `subCategoryNames`
	// where each element is the string itself rather than an object.
	Field *Entry
	// Fields maps an object collection's element fields, e.g. `tags` where `t.name` reads the
	// related table's `name` column.
	Fields map[string]Entry
	// Table is the element table — the one a lambda body reads its fields from.
	Table string
	// SourceColumn is the column on the parent scope's row that the outermost table matches.
	SourceColumn string
	// TargetColumn is the matching column on the outermost table — Table itself when Via is
	// empty, otherwise the last hop's table.
	TargetColumn string
	// Via lists intermediate tables between Table and the parent, innermost first. It is empty
	// for a direct relation and non-empty for a flattened chain such as
	// `mainCategory.subCategories`, which reaches the resource through its category table.
	Via []Hop
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
	// Relation is set when the attribute reaches another table, and Column is empty.
	Relation *Relation
	// Column names a column on the row this entry is read from.
	Column string
	// Qualifier is the table or alias the column is read from. Callers normally leave it empty
	// and let the translator fill it in — the resource table at the top level, a collection
	// element's alias inside a lambda. Set it to read a column from somewhere else.
	Qualifier string
	// ValueType marks a column whose stored representation needs special handling.
	ValueType ValueType
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

	if field, ok := strings.CutPrefix(reference, s.variable+"."); ok {
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

// guardedMapper rejects a caller-supplied qualifier that collides with the prefix used for
// generated subquery aliases.
//
// RootTable is checked once up front, but an Entry may name its own qualifier, and a colliding one
// would let an inner subquery alias shadow it — the correlation would compare the subquery's row
// against itself and the filter would match rows the PDP denies, with no SQL error to notice.
type guardedMapper struct {
	parent Mapper
}

func (g guardedMapper) Resolve(reference string) (Entry, bool) {
	entry, ok := g.parent.Resolve(reference)
	if !ok || strings.HasPrefix(entry.Qualifier, aliasPrefix) {
		return Entry{}, false
	}
	return entry, true
}
