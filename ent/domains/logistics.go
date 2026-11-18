package domains

import (
	"context"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"

	cerbosent "github.com/cerbos/query-plan-adapters/ent"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/logistics"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/logistics/checkpoint"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/logistics/hub"
	"github.com/cerbos/query-plan-adapters/ent/domains/gen/logistics/shipment"
)

var Logistics = Domain{
	Name: "logistics", Table: shipment.Table, Nulls: cerbosent.NullOmitted,
	Mapper: func() (cerbosent.Mapper, error) {
		return logistics.CerbosSchema.Mapper("Shipment", cerbosent.SchemaMap{
			"request.resource.attr.trackingNumber": {Column: shipment.FieldTrackingNo},
			"request.resource.attr.shipper":        {Column: shipment.FieldShipperAccount},
			"request.resource.attr.handling":       scalarList("shipment_handling", "shipment_id", "code", shipment.FieldID),
			"request.resource.attr.checkpoints": {Edge: shipment.EdgeCheckpoints, Fields: map[string]cerbosent.SchemaEntry{
				"event":     {Column: checkpoint.FieldEvent},
				"scannedAt": {Column: checkpoint.FieldScannedAt},
				"hub.code":  {Edge: checkpoint.EdgeHub, Column: hub.FieldCode},
			}},
		}, cerbosent.WithInferredNullConvention(omitted))
	},
	IDs: func(ctx context.Context, drv dialect.Driver, p *entsql.Predicate) ([]string, error) {
		return logistics.NewClient(logistics.Driver(drv)).Shipment.Query().Where(where(p)).IDs(ctx)
	},
}
