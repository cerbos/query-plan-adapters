package retail

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

type Store struct{ ent.Schema }

func (Store) Annotations() []schema.Annotation { return table("stores") }
func (Store) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("region"), field.String("country")}
}
func (Store) Edges() []ent.Edge { return []ent.Edge{edge.To("orders", Order.Type)} }

type Customer struct{ ent.Schema }

func (Customer) Annotations() []schema.Annotation { return table("customers") }
func (Customer) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("email"), field.String("tier"), field.Int("loyalty_points")}
}
func (Customer) Edges() []ent.Edge { return []ent.Edge{edge.To("orders", Order.Type)} }

type Order struct{ ent.Schema }

func (Order) Annotations() []schema.Annotation { return table("order") }
func (Order) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("customer_id"),
		field.String("store_id").Optional().Nillable(),
		field.String("status"),
		field.String("channel"),
		field.Int64("total_cents"),
		field.String("currency"),
		field.String("coupon_code").Optional().Nillable(),
		field.Time("placed_at"),
		field.Bool("gift"),
	}
}
func (Order) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("customer", Customer.Type).Ref("orders").Unique().Required().Field("customer_id"),
		edge.From("store", Store.Type).Ref("orders").Unique().Field("store_id"),
		edge.To("lines", OrderLine.Type),
	}
}

type OrderLine struct{ ent.Schema }

func (OrderLine) Annotations() []schema.Annotation { return table("order_line") }
func (OrderLine) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("order_id"),
		field.Int("line_no"),
		field.String("sku"),
		field.String("category"),
		field.Int("quantity"),
		field.Int64("unit_price_cents"),
	}
}
func (OrderLine) Edges() []ent.Edge {
	return []ent.Edge{edge.From("order", Order.Type).Ref("lines").Unique().Required().Field("order_id")}
}
