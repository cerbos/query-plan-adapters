package hr

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

type Department struct{ ent.Schema }

func (Department) Annotations() []schema.Annotation { return table("departments") }
func (Department) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name"), field.String("cost_center")}
}
func (Department) Edges() []ent.Edge { return []ent.Edge{edge.To("employees", Employee.Type)} }

type Location struct{ ent.Schema }

func (Location) Annotations() []schema.Annotation { return table("locations") }
func (Location) Fields() []ent.Field {
	return []ent.Field{field.String("id").StorageKey("code"), field.String("country"), field.String("city")}
}
func (Location) Edges() []ent.Edge { return []ent.Edge{edge.To("employees", Employee.Type)} }

type Employee struct{ ent.Schema }

func (Employee) Annotations() []schema.Annotation { return table("employees") }
func (Employee) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("manager_id").Optional().Nillable(),
		field.String("department_id"),
		field.String("location_code"),
		field.Int("level"),
		field.String("employment_type"),
		field.String("salary_band").Optional().Nillable(),
		field.Time("hired_on"),
		field.Time("terminated_on").Optional().Nillable(),
		field.String("work_email"),
	}
}
func (Employee) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("reports", Employee.Type).From("manager").Unique().Field("manager_id"),
		edge.From("department", Department.Type).Ref("employees").Unique().Required().Field("department_id"),
		edge.From("location", Location.Type).Ref("employees").Unique().Required().Field("location_code"),
	}
}
