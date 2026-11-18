// Package domains maps the domain stress kit (../../domains) onto PostgreSQL tables by hand, one
// file per domain.
package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

// Domain is one domain's mapping.
type Domain struct {
	Mapper cerbospgx.MapperMap
	Name   string
	Table  string
	ID     string
	Nulls  cerbospgx.NullRepresentation
}

// All is every domain.
var All = []Domain{Healthcare, Fintech, Logistics, Education, Legal, Retail, HR, Media, IoT, Government}

// The adapter does not export its value type's name, so a type travels as an Entry template.
var (
	str = cerbospgx.Entry{ValueType: cerbospgx.ValueString}
	num = cerbospgx.Entry{ValueType: cerbospgx.ValueNumber}
	boo = cerbospgx.Entry{ValueType: cerbospgx.ValueBool}
	ts  = cerbospgx.Entry{ValueType: cerbospgx.ValueTimestamp}
)

// join is one step of a path: the rows of table whose to column equals from on the previous row.
type join struct{ table, from, to string }

// path lowers a chain of joins from the resource row into a relation, every table before the
// last becoming a hop.
func path(steps ...join) *cerbospgx.Relation {
	last := len(steps) - 1
	rel := &cerbospgx.Relation{Table: steps[last].table, SourceColumn: steps[0].from, TargetColumn: steps[0].to}
	for i := last - 1; i >= 0; i-- {
		rel.Via = append(rel.Via, cerbospgx.Hop{Table: steps[i].table, ChildColumn: steps[i+1].to, JoinColumn: steps[i+1].from})
	}
	return rel
}

// attrs builds a domain's mapper, stamping its null convention on every scalar entry.
type attrs struct {
	m    cerbospgx.MapperMap
	null cerbospgx.NullConvention
}

func newAttrs(null cerbospgx.NullConvention) *attrs {
	return &attrs{m: cerbospgx.MapperMap{}, null: null}
}

func (a *attrs) col(name, column string, vt cerbospgx.Entry) *attrs {
	a.m["request.resource."+name] = cerbospgx.Entry{Column: column, ValueType: vt.ValueType, NullConvention: a.null}
	return a
}

func (a *attrs) via(name string, rel *cerbospgx.Relation, column string, vt cerbospgx.Entry) *attrs {
	a.m["request.resource.attr."+name] = cerbospgx.Entry{ScalarRelation: rel, Column: column, ValueType: vt.ValueType, NullConvention: a.null}
	return a
}

func (a *attrs) list(name string, rel *cerbospgx.Relation, column string, vt cerbospgx.Entry) *attrs {
	rel.Field = &cerbospgx.Entry{Column: column, ValueType: vt.ValueType}
	a.m["request.resource.attr."+name] = cerbospgx.Entry{Relation: rel}
	return a
}

func (a *attrs) objects(name string, rel *cerbospgx.Relation, fields map[string]cerbospgx.Entry) *attrs {
	rel.Fields = fields
	a.m["request.resource.attr."+name] = cerbospgx.Entry{Relation: rel}
	return a
}

func f(column string, vt cerbospgx.Entry) cerbospgx.Entry {
	return cerbospgx.Entry{Column: column, ValueType: vt.ValueType}
}
