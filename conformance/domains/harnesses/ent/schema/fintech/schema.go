package fintech

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

type Branch struct{ ent.Schema }

func (Branch) Annotations() []schema.Annotation { return table("branches") }
func (Branch) Fields() []ent.Field {
	return []ent.Field{field.String("id").StorageKey("code"), field.String("country")}
}
func (Branch) Edges() []ent.Edge { return []ent.Edge{edge.To("customers", Customer.Type)} }

type Customer struct{ ent.Schema }

func (Customer) Annotations() []schema.Annotation { return table("customers") }
func (Customer) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("legal_name"),
		field.Int("kyc_level"),
		field.String("branch_code"),
		field.Bool("pep").Optional().Nillable(),
	}
}
func (Customer) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("branch", Branch.Type).Ref("customers").Unique().Required().Field("branch_code"),
		edge.To("accounts", Account.Type),
	}
}

type Account struct{ ent.Schema }

func (Account) Annotations() []schema.Annotation { return table("accounts") }
func (Account) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("customer_id"),
		field.String("currency"),
		field.String("status"),
		field.Float("overdraft_limit").Optional().Nillable(),
	}
}
func (Account) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("customer", Customer.Type).Ref("accounts").Unique().Required().Field("customer_id"),
		edge.To("payments", Payment.Type),
	}
}

type Payment struct{ ent.Schema }

func (Payment) Annotations() []schema.Annotation { return table("payments") }
func (Payment) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("account_id"),
		field.Float("amount"),
		field.String("currency"),
		field.String("status"),
		field.Float("risk_score").Optional().Nillable(),
		field.Time("created_at"),
		field.String("cp_name"),
		field.String("cp_iban"),
		field.String("cp_country"),
		field.String("initiated_by"),
	}
}
func (Payment) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("account", Account.Type).Ref("payments").Unique().Required().Field("account_id"),
		edge.To("approvals", PaymentApproval.Type),
	}
}

type PaymentApproval struct{ ent.Schema }

func (PaymentApproval) Annotations() []schema.Annotation { return table("payment_approvals") }
func (PaymentApproval) Fields() []ent.Field {
	return []ent.Field{field.String("id"), field.String("payment_id"), field.String("approver_id"), field.String("decision")}
}
func (PaymentApproval) Edges() []ent.Edge {
	return []ent.Edge{edge.From("payment", Payment.Type).Ref("approvals").Unique().Required().Field("payment_id")}
}
