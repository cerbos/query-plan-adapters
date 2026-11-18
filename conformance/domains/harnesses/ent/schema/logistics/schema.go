package logistics

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

func table(name string) []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: name}}
}

type Hub struct{ ent.Schema }

func (Hub) Annotations() []schema.Annotation { return table("hubs") }
func (Hub) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("code"), field.String("country"), field.String("region")}
}
func (Hub) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("outbound", Shipment.Type),
		edge.To("inbound", Shipment.Type),
		edge.To("scans", Checkpoint.Type),
	}
}

type Carrier struct{ ent.Schema }

func (Carrier) Annotations() []schema.Annotation { return table("carriers") }
func (Carrier) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name"), field.String("scac")}
}
func (Carrier) Edges() []ent.Edge { return []ent.Edge{edge.To("shipments", Shipment.Type)} }

type Shipment struct{ ent.Schema }

func (Shipment) Annotations() []schema.Annotation { return table("shipments") }
func (Shipment) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("tracking_no"),
		field.String("status"),
		field.String("origin_hub_id"),
		field.String("destination_hub_id"),
		field.String("carrier_id").Optional().Nillable(),
		field.Float("weight_kg"),
		field.Bool("hazmat"),
		field.Bool("customs_hold").Optional().Nillable(),
		field.String("shipper_account"),
		field.Time("eta").Optional().Nillable(),
		field.Float("declared_value"),
	}
}
func (Shipment) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("origin", Hub.Type).Ref("outbound").Unique().Required().Field("origin_hub_id"),
		edge.From("destination", Hub.Type).Ref("inbound").Unique().Required().Field("destination_hub_id"),
		edge.From("carrier", Carrier.Type).Ref("shipments").Unique().Field("carrier_id"),
		edge.To("checkpoints", Checkpoint.Type),
	}
}

type Checkpoint struct{ ent.Schema }

func (Checkpoint) Annotations() []schema.Annotation { return table("checkpoints") }
func (Checkpoint) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("shipment_id"),
		field.String("hub_id"),
		field.String("event"),
		field.Time("scanned_at"),
	}
}
func (Checkpoint) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("shipment", Shipment.Type).Ref("checkpoints").Unique().Required().Field("shipment_id"),
		edge.From("hub", Hub.Type).Ref("scans").Unique().Required().Field("hub_id"),
	}
}
