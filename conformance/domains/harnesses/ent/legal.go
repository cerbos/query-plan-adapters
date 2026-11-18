package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/legal"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/legal/document"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/legal/matter"
)

var Legal = Domain{
	Name: "legal", Table: document.Table, Nulls: cerbosent.NullExplicit,
	Mapper: func() (cerbosent.Mapper, error) {
		viaMatter := cerbosent.Hop{Table: matter.Table, ChildColumn: "matter_id", JoinColumn: matter.FieldID}
		return legal.CerbosSchema.Mapper("Document", cerbosent.SchemaMap{
			"request.resource.attr.matter.team":     scalarList("matter_team", matter.FieldID, "user_id", document.FieldMatterID, viaMatter),
			"request.resource.attr.matter.screened": scalarList("matter_screens", matter.FieldID, "user_id", document.FieldMatterID, viaMatter),
		}, cerbosent.WithInferredNullConvention(explicit))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return legal.NewClient(legal.Driver(drv)).Document.Query().Where(where(p)).IDs(ctx)
	},
}
