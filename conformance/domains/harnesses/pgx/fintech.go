package domains

import cerbospgx "github.com/cerbos/query-plan-adapters/pgx"

var Fintech = func() Domain {
	account := join{"accounts", "account_id", "id"}
	customer := join{"customers", "customer_id", "id"}
	branch := join{"branches", "branch_code", "code"}
	a := newAttrs(cerbospgx.NullConventionOmitted).
		col("id", "id", str).
		col("attr.amount", "amount", num).
		col("attr.currency", "currency", str).
		col("attr.status", "status", str).
		col("attr.riskScore", "risk_score", num).
		col("attr.createdAt", "created_at", ts).
		col("attr.initiatedBy", "initiated_by", str).
		col("attr.counterparty.name", "cp_name", str).
		col("attr.counterparty.iban", "cp_iban", str).
		col("attr.counterparty.country", "cp_country", str).
		list("flags", path(join{"payment_flags", "id", "payment_id"}), "flag", str).
		objects("approvals", path(join{"payment_approvals", "id", "payment_id"}), map[string]cerbospgx.Entry{
			"approverId": f("approver_id", str), "decision": f("decision", str),
		}).
		via("account.currency", path(account), "currency", str).
		via("account.status", path(account), "status", str).
		via("account.overdraftLimit", path(account), "overdraft_limit", num).
		via("account.customer.id", path(account, customer), "id", str).
		via("account.customer.kycLevel", path(account, customer), "kyc_level", num).
		via("account.customer.pep", path(account, customer), "pep", boo).
		via("account.customer.branch.code", path(account, customer, branch), "code", str).
		via("account.customer.branch.country", path(account, customer, branch), "country", str)
	return Domain{Name: "fintech", Table: "payments", ID: "id", Nulls: cerbospgx.NullOmitted, Mapper: a.m}
}()
