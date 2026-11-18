package education

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
func (Department) Fields() []ent.Field              { return []ent.Field{field.String("id"), field.String("name")} }
func (Department) Edges() []ent.Edge                { return []ent.Edge{edge.To("courses", Course.Type)} }

type Course struct{ ent.Schema }

func (Course) Annotations() []schema.Annotation { return table("courses") }
func (Course) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("code"), field.String("term"), field.String("department_id")}
}
func (Course) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("department", Department.Type).Ref("courses").Unique().Required().Field("department_id"),
		edge.To("staff", CourseStaff.Type),
		edge.To("assignments", Assignment.Type),
	}
}

type CourseStaff struct{ ent.Schema }

func (CourseStaff) Annotations() []schema.Annotation { return table("course_staff") }
func (CourseStaff) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("course_id"), field.String("user_id"), field.String("role")}
}
func (CourseStaff) Edges() []ent.Edge {
	return []ent.Edge{edge.From("course", Course.Type).Ref("staff").Unique().Required().Field("course_id")}
}

type Assignment struct{ ent.Schema }

func (Assignment) Annotations() []schema.Annotation { return table("assignments") }
func (Assignment) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("course_id"),
		field.String("title"),
		field.Time("due_at"),
		field.Int("max_points"),
		field.Bool("group_work"),
	}
}
func (Assignment) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("course", Course.Type).Ref("assignments").Unique().Required().Field("course_id"),
		edge.To("submissions", Submission.Type),
	}
}

type Submission struct{ ent.Schema }

func (Submission) Annotations() []schema.Annotation { return table("submissions") }
func (Submission) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("assignment_id"),
		field.String("student_id"),
		field.Time("submitted_at").Optional().Nillable(),
		field.Float("score").Optional().Nillable(),
		field.Bool("late"),
		field.Float("plagiarism_score").Optional().Nillable(),
		field.String("status"),
		field.Bool("locked"),
	}
}
func (Submission) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("assignment", Assignment.Type).Ref("submissions").Unique().Required().Field("assignment_id"),
	}
}
