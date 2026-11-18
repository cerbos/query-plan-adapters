package legal

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

type LegalClient struct{ ent.Schema }

func (LegalClient) Annotations() []schema.Annotation { return table("clients") }
func (LegalClient) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name"), field.String("industry").Optional().Nillable()}
}
func (LegalClient) Edges() []ent.Edge { return []ent.Edge{edge.To("matters", Matter.Type)} }

type Matter struct{ ent.Schema }

func (Matter) Annotations() []schema.Annotation { return table("matters") }
func (Matter) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("client_id"),
		field.String("practice_area"),
		field.String("status"),
		field.Time("opened_at"),
		field.String("lead_partner_id").Optional().Nillable(),
	}
}
func (Matter) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("client", LegalClient.Type).Ref("matters").Unique().Required().Field("client_id"),
		edge.To("documents", Document.Type),
	}
}

type Document struct{ ent.Schema }

func (Document) Annotations() []schema.Annotation { return table("documents") }
func (Document) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("matter_id"),
		field.String("title"),
		field.String("privilege").Optional().Nillable(),
		field.Int("classification"),
		field.Bool("legal_hold"),
		field.String("created_by"),
		field.Time("retention_until").Optional().Nillable(),
	}
}
func (Document) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("matter", Matter.Type).Ref("documents").Unique().Required().Field("matter_id"),
		edge.To("shares", DocumentShare.Type),
	}
}

type DocumentShare struct{ ent.Schema }

func (DocumentShare) Annotations() []schema.Annotation { return table("document_shares") }
func (DocumentShare) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("document_id"), field.String("party_type"), field.String("party_id")}
}
func (DocumentShare) Edges() []ent.Edge {
	return []ent.Edge{edge.From("document", Document.Type).Ref("shares").Unique().Required().Field("document_id")}
}
