package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/retail"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/retail/order"
)

var Retail = Domain{
	Name: "retail", Table: order.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return retail.CerbosSchema.Mapper("Order", cerbosent.SchemaMap{
			"request.resource.attr.isGift": {Column: order.FieldGift},
		}, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return retail.NewClient(retail.Driver(drv)).Order.Query().Where(where(p)).IDs(ctx)
	},
}
