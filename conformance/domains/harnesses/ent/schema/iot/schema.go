package iot

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

type Org struct{ ent.Schema }

func (Org) Annotations() []schema.Annotation { return table("orgs") }
func (Org) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name"), field.String("plan")}
}
func (Org) Edges() []ent.Edge { return []ent.Edge{edge.To("sites", Site.Type)} }

type Site struct{ ent.Schema }

func (Site) Annotations() []schema.Annotation { return table("sites") }
func (Site) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("org_id"), field.String("region"), field.String("name")}
}
func (Site) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("org", Org.Type).Ref("sites").Unique().Required().Field("org_id"),
		edge.To("devices", Device.Type),
	}
}

type Device struct{ ent.Schema }

func (Device) Annotations() []schema.Annotation { return table("devices") }
func (Device) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("serial"),
		field.String("model"),
		field.String("firmware"),
		field.String("site_id"),
		field.Time("last_seen_at").Optional().Nillable(),
		field.Bool("online"),
		field.Int("battery_pct").Optional().Nillable(),
		field.JSON("labels", map[string]string{}),
		field.Bool("decommissioned"),
	}
}
func (Device) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("site", Site.Type).Ref("devices").Unique().Required().Field("site_id"),
		edge.To("alerts", DeviceAlert.Type),
	}
}

type DeviceAlert struct{ ent.Schema }

func (DeviceAlert) Annotations() []schema.Annotation { return table("device_alerts") }
func (DeviceAlert) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("device_id"),
		field.Int("severity"),
		field.Bool("acknowledged"),
		field.String("code"),
	}
}
func (DeviceAlert) Edges() []ent.Edge {
	return []ent.Edge{edge.From("device", Device.Type).Ref("alerts").Unique().Required().Field("device_id")}
}
