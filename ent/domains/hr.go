package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/hr"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/hr/employee"
)

var HR = Domain{
	Name: "hr", Table: employee.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return hr.CerbosSchema.Mapper("Employee", cerbosent.SchemaMap{
			"request.resource.attr.email":  {Column: employee.FieldWorkEmail},
			"request.resource.attr.skills": scalarList("employee_skills", "employee_id", "skill", employee.FieldID),
		}, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return hr.NewClient(hr.Driver(drv)).Employee.Query().Where(where(p)).IDs(ctx)
	},
}
