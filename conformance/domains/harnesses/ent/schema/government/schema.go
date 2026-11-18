package government

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

type Agency struct{ ent.Schema }

func (Agency) Annotations() []schema.Annotation { return table("agencies") }
func (Agency) Fields() []ent.Field {
	return []ent.Field{field.String("id").StorageKey("code"), field.String("name"), field.String("state")}
}
func (Agency) Edges() []ent.Edge { return []ent.Edge{edge.To("cases", BenefitCase.Type)} }

type Applicant struct{ ent.Schema }

func (Applicant) Annotations() []schema.Annotation { return table("applicants") }
func (Applicant) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("county_fips"),
		field.Int("household_size"),
		field.String("citizenship").Optional().Nillable(),
	}
}
func (Applicant) Edges() []ent.Edge { return []ent.Edge{edge.To("cases", BenefitCase.Type)} }

type BenefitCase struct{ ent.Schema }

func (BenefitCase) Annotations() []schema.Annotation { return table("cases") }
func (BenefitCase) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("case_number"),
		field.String("agency_code"),
		field.String("applicant_id"),
		field.String("status"),
		field.Int("clearance_level"),
		field.String("program"),
		field.Time("opened_on"),
		field.Time("closed_on").Optional().Nillable(),
		field.Bool("pii"),
		field.String("assigned_to").Optional().Nillable(),
		field.Float("monthly_amount").Optional().Nillable(),
	}
}
func (BenefitCase) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("agency", Agency.Type).Ref("cases").Unique().Required().Field("agency_code"),
		edge.From("applicant", Applicant.Type).Ref("cases").Unique().Required().Field("applicant_id"),
	}
}
