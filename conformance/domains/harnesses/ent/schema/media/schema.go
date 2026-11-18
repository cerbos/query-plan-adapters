package media

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

type Author struct{ ent.Schema }

func (Author) Annotations() []schema.Annotation { return table("authors") }
func (Author) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("display_name"), field.Bool("staff")}
}
func (Author) Edges() []ent.Edge {
	return []ent.Edge{edge.To("articles", Article.Type), edge.From("contributed", Article.Type).Ref("contributors")}
}

type Section struct{ ent.Schema }

func (Section) Annotations() []schema.Annotation { return table("sections") }
func (Section) Fields() []ent.Field {
	return []ent.Field{field.String("id").StorageKey("slug"), field.String("name"), field.String("desk")}
}
func (Section) Edges() []ent.Edge { return []ent.Edge{edge.To("articles", Article.Type)} }

type Article struct{ ent.Schema }

func (Article) Annotations() []schema.Annotation { return table("articles") }
func (Article) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("slug"),
		field.String("section_slug"),
		field.String("author_id"),
		field.String("status"),
		field.String("language"),
		field.Time("embargo_until").Optional().Nillable(),
		field.Time("published_at").Optional().Nillable(),
		field.Bool("paywalled"),
		field.Int("word_count"),
		field.Strings("keywords"),
	}
}
func (Article) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("section", Section.Type).Ref("articles").Unique().Required().Field("section_slug"),
		edge.From("author", Author.Type).Ref("articles").Unique().Required().Field("author_id"),
		edge.To("contributors", Author.Type).StorageKey(edge.Table("article_contributors"), edge.Columns("article_id", "author_id")),
	}
}
