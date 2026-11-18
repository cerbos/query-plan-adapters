package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/iot"
	"github.com/cerbos/query-plan-adapters/conformance/domains/harnesses/ent/gen/iot/device"
)

var IoT = Domain{
	Name: "iot", Table: device.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return iot.CerbosSchema.Mapper("Device", nil, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return iot.NewClient(iot.Driver(drv)).Device.Query().Where(where(p)).IDs(ctx)
	},
}
