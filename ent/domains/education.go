package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/education"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/education/submission"
)

var Education = Domain{
	Name: "education", Table: submission.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return education.CerbosSchema.Mapper("Submission", cerbosent.SchemaMap{
			"request.resource.attr.members": scalarList("submission_members", "submission_id", "student_id", submission.FieldID),
		}, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return education.NewClient(education.Driver(drv)).Submission.Query().Where(where(p)).IDs(ctx)
	},
}
