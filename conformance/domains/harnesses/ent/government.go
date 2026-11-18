package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/government"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/government/agency"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/government/benefitcase"
)

var Government = Domain{
	Name: "government", Table: benefitcase.Table, Nulls: cerbosent.NullExplicit,
	Mapper: func() (cerbosent.Mapper, error) {
		return government.CerbosSchema.Mapper("BenefitCase", cerbosent.SchemaMap{
			"request.resource.attr.agency.code": {Edge: benefitcase.EdgeAgency, Column: agency.FieldID, NullConvention: explicit},
			"request.resource.attr.flags":       scalarList("case_flags", "case_id", "flag", benefitcase.FieldID),
		}, cerbosent.WithInferredNullConvention(explicit))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return government.NewClient(government.Driver(drv)).BenefitCase.Query().Where(where(p)).IDs(ctx)
	},
}
