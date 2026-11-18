package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/fintech"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/fintech/branch"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/fintech/payment"
)

var Fintech = Domain{
	Name: "fintech", Table: payment.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return fintech.CerbosSchema.Mapper("Payment", cerbosent.SchemaMap{
			"request.resource.attr.counterparty.name":    {Column: payment.FieldCpName},
			"request.resource.attr.counterparty.iban":    {Column: payment.FieldCpIban},
			"request.resource.attr.counterparty.country": {Column: payment.FieldCpCountry},
			"request.resource.attr.flags":                scalarList("payment_flags", "payment_id", "flag", payment.FieldID),
			"request.resource.attr.account.customer.branch.code": {
				Edge: "account.customer.branch", Column: branch.FieldID, NullConvention: omitted,
			},
		}, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return fintech.NewClient(fintech.Driver(drv)).Payment.Query().Where(where(p)).IDs(ctx)
	},
}
