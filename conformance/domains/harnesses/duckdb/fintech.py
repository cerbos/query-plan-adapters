from _kit import BOOL, NUM, STR, TS, Attrs, f, path

ROOT, ID = "payments_v", "id"

VIEWS = """
CREATE VIEW payments_v AS
SELECT p.*,
    (SELECT coalesce(list(x.flag ORDER BY x.flag), []) FROM payment_flags x
        WHERE x.payment_id = p.id) AS flags,
    (SELECT coalesce(list({'approver_id': a.approver_id, 'decision': a.decision} ORDER BY a.id), [])
        FROM payment_approvals a WHERE a.payment_id = p.id) AS approvals
FROM payments p
"""


def mapping(nulls):
    account = ("accounts", "account_id", "id")
    customer = ("customers", "customer_id", "id")
    branch = ("branches", "branch_code", "code")
    return (
        Attrs(nulls)
        .col("id", "id", STR)
        .col("attr.amount", "amount", NUM)
        .col("attr.currency", "currency", STR)
        .col("attr.status", "status", STR)
        .col("attr.riskScore", "risk_score", NUM)
        .col("attr.createdAt", "created_at", TS)
        .col("attr.initiatedBy", "initiated_by", STR)
        .col("attr.counterparty.name", "cp_name", STR)
        .col("attr.counterparty.iban", "cp_iban", STR)
        .col("attr.counterparty.country", "cp_country", STR)
        .values("flags", "flags", STR)
        .objects(
            "approvals",
            "approvals",
            {"approverId": f("approver_id", STR), "decision": f("decision", STR)},
        )
        .via("account.currency", path(account), "currency", STR)
        .via("account.status", path(account), "status", STR)
        .via("account.overdraftLimit", path(account), "overdraft_limit", NUM)
        .via("account.customer.id", path(account, customer), "id", STR)
        .via("account.customer.kycLevel", path(account, customer), "kyc_level", NUM)
        .via("account.customer.pep", path(account, customer), "pep", BOOL)
        .via(
            "account.customer.branch.code", path(account, customer, branch), "code", STR
        )
        .via(
            "account.customer.branch.country",
            path(account, customer, branch),
            "country",
            STR,
        )
        .m
    )
