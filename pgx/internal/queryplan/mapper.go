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
	// SubqueryFilter is the predicate the application itself applies when it reads Table — a
	// soft-delete flag, a tenant column, a subtype discriminator, whatever narrows the rows that
	// became the resource attributes.
	//
	// The translator reads Table bare. It is given a table name and two column names and has no
	// association metadata to consult, so nothing that narrows the application's own read reaches
	// the generated subquery, and the subquery then examines rows the application never
	// serialised — a filter that returns rows the PDP denies. Declaring the predicate here
	// restores the equality. It is optional because the translator cannot detect the omission;
	// leaving it empty emits exactly the SQL it emitted before the field existed. See "Mapping
	// hazards" in the adapter's README.
	//
	// Entries are ANDed into the subquery's correlation predicate, so the restriction narrows the
	// rows the subquery EXAMINES rather than the rows it returns. That is what keeps it right
	// under negation: `all` lowers to a false-witness EXISTS, and restricting the scan turns it
	// into "every visible row satisfies the body" instead of "every row in the table does".
	SubqueryFilter []Restriction
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
	// SubqueryFilter restricts this hop's table the same way Relation.SubqueryFilter restricts
	// the element table. A hop is read bare too, and it is the to-ONE parent whose absence must
	// deny (#309) — a row the application's own reads hide is, for this purpose, absent.
	SubqueryFilter []Restriction
}

// RestrictOp is the comparison a Restriction applies.
type RestrictOp uint8

const (
	// RestrictEq is `column = value`. It is the zero value, so a Restriction that names only a
	// column and a value is an equality.
	RestrictEq RestrictOp = iota
	// RestrictNe is `column <> value`. It does not match NULL columns; pair it with
	// RestrictIsNull under an OR if the application's own predicate treats NULL as matching.
	RestrictNe
	// RestrictIsNull is `column IS NULL` — the usual soft-delete spelling.
	RestrictIsNull
	// RestrictIsNotNull is `column IS NOT NULL`.
	RestrictIsNotNull
	// RestrictIn is `column IN (values…)`. An empty Values list hides every row.
	RestrictIn
	// RestrictNotIn is `column NOT IN (values…)`. An empty Values list hides none.
	RestrictNotIn
)

// Restriction is one caller-declared predicate on the rows a relation subquery may see.
//
// The vocabulary is deliberately narrow — equality, inequality, null tests and membership over a
// single column — because that is what the hazards it exists for look like: a `deleted_at IS
// NULL` soft delete, a `tenant_id = $1` scope, a `kind IN (…)` discriminator. A predicate that
// does not fit is a signal that the mapping cannot faithfully reproduce the application's read,
// and the honest response is to not map that relation rather than to declare an approximation.
//
// Value and Values are read according to Op, and the pairing is not enforced at compile time. It
// does not need to be, because every way of getting it wrong FAILS CLOSED: Values alongside
// RestrictEq leaves Value nil and renders `column = NULL`, which is UNKNOWN and admits nothing,
// and Value alongside RestrictIn leaves Values empty, which folds to FALSE. A mismatched pairing
// therefore hides rows rather than exposing them. TestRestrictionMismatchFailsClosed pins it.
type Restriction struct {
	// Value is compared with Column. Ignored by RestrictIsNull and RestrictIsNotNull.
	Value any
	// Column is a column on the table this restriction applies to, unqualified.
	Column string
	// Values is the membership list for RestrictIn and RestrictNotIn.
	Values []any
	// Op is the comparison. The zero value is RestrictEq.
	Op RestrictOp
}

// ValueType marks an attribute whose stored representation needs special handling.
type ValueType uint8

const (
	// ValueDefault is an ordinary column.
	ValueDefault ValueType = iota
	// ValueTimestamp marks a column stored as a temporal type, so a `timestamp()` conversion
	// around it is a no-op rather than an unsupported shape.
	ValueTimestamp
	// ValueBool marks a column stored as a boolean. It exists so `string()` over one can fail
	// closed: SQLite and MySQL have no boolean type and render the stored 1/0 as "1", while CEL
	// and PostgreSQL render "true", and nothing in the plan says which type a column holds
	// (cerbos/query-plan-adapters#376).
	ValueBool
	// ValueString marks a column stored as text. It exists so CEL's `+` between two columns can
	// be resolved to concatenation: the operator is overloaded on strings and the plan carries no
	// operand types, so without a declaration the shape fails closed rather than emitting a
	// numeric `+` that MySQL silently answers with 0 (cerbos/query-plan-adapters#391).
	ValueString
)

// NullConvention declares, for one attribute, that its column can be SQL NULL and how the caller
// represents that NULL in the attributes it sends to check().
//
// It is per attribute rather than per call because one policy suite can legitimately mix the two
// conventions — the same column can be mapped twice, sent as an explicit null under one attribute
// name and omitted under another. Options.NullRepresentation cannot express that, which is what
// made https://github.com/cerbos/query-plan-adapters/issues/308 unfixable with the call-level
// option alone.
//
// The zero value declares nothing, which is why this is a separate type from NullRepresentation
// rather than a reuse of it: NullExplicit is NullRepresentation's zero value, and an Entry that
// declared a convention merely by existing would change the SQL emitted for every mapped column.
type NullConvention uint8

const (
	// NullConventionUnset declares nothing. The column is treated as NOT NULL when rendering a
	// comparison — the historical translation — and Options.NullRepresentation still governs
	// whether a null comparison operand is rejected.
	NullConventionUnset NullConvention = iota
	// NullConventionExplicit means a NULL column is sent as an explicit null attribute, so CEL
	// holds a null VALUE: `null != "x"` is TRUE and `null == "x"` is FALSE, both definite. SQL's
	// UNKNOWN excludes the row under BOTH polarities, so the equality family (eq, ne, in) is
	// rendered so it can never be UNKNOWN. Ordering and string operators are left alone: they
	// raise a no-overload error on a null receiver in CEL, which denies exactly as UNKNOWN does.
	NullConventionExplicit
	// NullConventionOmitted means a NULL column sends no attribute, so CEL raises a
	// missing-attribute error and check() denies. UNKNOWN already excludes the row under both
	// polarities, so the rendering is unchanged; what the declaration adds is the same
	// null-operand rejection Options.NullRepresentation = NullOmitted performs, scoped to this
	// attribute.
	NullConventionOmitted
)

// Entry is what a plan variable resolves to.
type Entry struct {
	// Relation is set when the attribute reaches another table, and Column is empty.
	Relation *Relation
	// ScalarRelation reads Column from another table through a to-ONE hop, as a correlated
	// scalar subquery — `R.attr.parent.aString`, where `parent` is a joined row rather than a
	// column on this one. It reuses Relation's vocabulary (Table, SourceColumn, TargetColumn,
	// Via, SubqueryFilter) because the chain is described identically; only the projection
	// differs, and Via carries the intermediate levels of a multi-level chain.
	//
	// It is a separate field from Relation rather than "Relation plus a Column" so the to-ONE
	// claim is explicit. The translator cannot check it: nothing in a table name says the
	// correlation matches at most one row, and against a to-MANY relation the database would
	// either pick an arbitrary row or fail at runtime depending on dialect. Declaring it here is
	// the caller asserting the uniqueness their schema enforces.
	ScalarRelation *Relation
	// Column names a column on the row this entry is read from — the enclosing scope's row, or
	// ScalarRelation's element row when that is set.
	Column string
	// Qualifier is the table or alias the column is read from. Callers normally leave it empty
	// and let the translator fill it in — the resource table at the top level, a collection
	// element's alias inside a lambda. Set it to read a column from somewhere else.
	Qualifier string
	// ValueType marks a column whose stored representation needs special handling.
	ValueType ValueType
	// NullConvention declares this column's NULL representation. See NullConvention.
	NullConvention NullConvention
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
