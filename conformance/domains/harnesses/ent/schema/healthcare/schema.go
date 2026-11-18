package healthcare

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Hospital struct{ ent.Schema }

func (Hospital) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "hospitals"}}
}

func (Hospital) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name"), field.String("region")}
}

func (Hospital) Edges() []ent.Edge { return []ent.Edge{edge.To("wards", Ward.Type)} }

type Ward struct{ ent.Schema }

func (Ward) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "wards"}}
}

func (Ward) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("name"), field.String("hospital_id"), field.Bool("secure")}
}

func (Ward) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("hospital", Hospital.Type).Ref("wards").Unique().Required().Field("hospital_id"),
		edge.To("patients", Patient.Type),
	}
}

type Patient struct{ ent.Schema }

func (Patient) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "patients"}}
}

func (Patient) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("mrn"),
		field.String("ward_id").Optional().Nillable(),
		field.Bool("consent_research"),
		field.Bool("vip").Optional().Nillable(),
		field.Int("birth_year"),
	}
}

func (Patient) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("ward", Ward.Type).Ref("patients").Unique().Field("ward_id"),
		edge.To("notes", ClinicalNote.Type),
	}
}

type ClinicalNote struct{ ent.Schema }

func (ClinicalNote) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "clinical_notes"}}
}

func (ClinicalNote) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").StorageKey("note_id"),
		field.String("patient_id"),
		field.String("author_id"),
		field.String("type"),
		field.Int("sensitivity").Optional().Nillable(),
		field.Time("signed_at").Optional().Nillable(),
		field.Bool("is_deleted"),
	}
}

func (ClinicalNote) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("patient", Patient.Type).Ref("notes").Unique().Required().Field("patient_id"),
		edge.To("codes", NoteCode.Type),
	}
}

type NoteCode struct{ ent.Schema }

func (NoteCode) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "note_codes"}}
}

func (NoteCode) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("note_id"), field.String("system"), field.String("code")}
}

func (NoteCode) Edges() []ent.Edge {
	return []ent.Edge{edge.From("note", ClinicalNote.Type).Ref("codes").Unique().Required().Field("note_id")}
}
