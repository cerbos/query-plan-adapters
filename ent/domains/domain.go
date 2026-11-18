// Package domains maps the domain stress kit (../../domains) onto generated ent clients: one file
// per domain holds its Schema.Mapper overrides and the query that applies the predicate.
package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
)

// Domain is one domain's mapping.
type Domain struct {
	Mapper func() (cerbosent.Mapper, error)
	IDs    func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error)
	Name   string
	Table  string
	Nulls  cerbosent.NullRepresentation
}

// All is every domain.
var All = []Domain{Healthcare, Fintech, Logistics, Education, Legal, Retail, HR, Media, IoT, Government}

func where(p *entsql.Predicate) func(*entsql.Selector) {
	return func(s *entsql.Selector) { s.Where(p) }
}

// scalarList maps a list of scalars kept one per row in a table ent cannot model, because its
// primary key is composite.
func scalarList(table, fk, value, parent string, via ...cerbosent.Hop) cerbosent.SchemaEntry {
	return cerbosent.SchemaEntry{Declared: &cerbosent.Entry{Relation: &cerbosent.Relation{
		Table: table, SourceColumn: parent, TargetColumn: fk, Via: via,
		Field: &cerbosent.Entry{Column: value, ValueType: cerbosent.ValueString},
	}}}
}

const (
	omitted  = cerbosent.NullConventionOmitted
	explicit = cerbosent.NullConventionExplicit
)
