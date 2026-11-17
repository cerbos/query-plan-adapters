// Copyright 2021-2026 Zenauth Ltd.
// SPDX-License-Identifier: Apache-2.0

// Package conformanceschema is the conformance dataset's tables written as ent schema types, so
// the harness reads its relations from ent rather than declaring them.
package conformanceschema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// All is every type.
func All() []ent.Interface {
	return []ent.Interface{
		Resource{}, Tag{}, Category{}, SubCategory{}, Label{}, Parent{}, Inner{}, NumberElem{}, BoolElem{},
	}
}

func table(name string) []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: name}}
}

func scalars() []ent.Field {
	return []ent.Field{
		field.Bool("a_bool").Optional(),
		field.String("a_string").Optional(),
		field.Int("a_number").Optional(),
		field.String("a_optional_string").Optional(),
	}
}

type Resource struct{ ent.Schema }

func (Resource) Annotations() []schema.Annotation { return table("adversarial_resource") }

func (Resource) Fields() []ent.Field {
	return append(scalars(),
		field.String("id"),
		field.Float("a_double").Optional(),
		field.String("created_by"),
		field.String("scope").Optional(),
		field.Time("created_at").Optional(),
		field.Time("updated_at").Optional(),
	)
}

func (Resource) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("tags", Tag.Type).StorageKey(edge.Column("resource_id")),
		edge.To("categories", Category.Type).StorageKey(edge.Column("resource_id")),
		edge.To("parent", Parent.Type).Unique().StorageKey(edge.Column("resource_id")),
		edge.To("number_elems", NumberElem.Type).StorageKey(edge.Column("resource_id")),
		edge.To("bool_elems", BoolElem.Type).StorageKey(edge.Column("resource_id")),
	}
}

type Tag struct{ ent.Schema }

func (Tag) Annotations() []schema.Annotation { return table("adversarial_tag") }

func (Tag) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id").StorageKey("pk"),
		field.String("tag_id"),
		field.String("name").Optional(),
		field.Int("position"),
	}
}

type Category struct{ ent.Schema }

func (Category) Annotations() []schema.Annotation { return table("adversarial_category") }

func (Category) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name")}
}

func (Category) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("sub_categories", SubCategory.Type).StorageKey(edge.Column("category_id")),
	}
}

type SubCategory struct{ ent.Schema }

func (SubCategory) Annotations() []schema.Annotation { return table("adversarial_sub_category") }

func (SubCategory) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name")}
}

func (SubCategory) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("labels", Label.Type).StorageKey(edge.Column("sub_category_id")),
	}
}

type Label struct{ ent.Schema }

func (Label) Annotations() []schema.Annotation { return table("adversarial_label") }

func (Label) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name").Optional()}
}

type Parent struct{ ent.Schema }

func (Parent) Annotations() []schema.Annotation { return table("adversarial_parent") }

func (Parent) Fields() []ent.Field { return append(scalars(), field.String("id")) }

func (Parent) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("resource", Resource.Type).Ref("parent").Unique().Required(),
		edge.To("inner", Inner.Type).Unique().StorageKey(edge.Column("parent_id")),
	}
}

type Inner struct{ ent.Schema }

func (Inner) Annotations() []schema.Annotation { return table("adversarial_inner") }

func (Inner) Fields() []ent.Field { return append(scalars(), field.String("id")) }

func (Inner) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("parent", Parent.Type).Ref("inner").Unique().Required(),
	}
}

type NumberElem struct{ ent.Schema }

func (NumberElem) Annotations() []schema.Annotation { return table("adversarial_number_elem") }

func (NumberElem) Fields() []ent.Field {
	return []ent.Field{field.Int("id").StorageKey("pk"), field.Float("value").Optional(), field.Int("position")}
}

type BoolElem struct{ ent.Schema }

func (BoolElem) Annotations() []schema.Annotation { return table("adversarial_bool_elem") }

func (BoolElem) Fields() []ent.Field {
	return []ent.Field{field.Int("id").StorageKey("pk"), field.Bool("value").Optional(), field.Int("position")}
}
