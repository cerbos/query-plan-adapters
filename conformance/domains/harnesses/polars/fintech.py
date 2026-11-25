from _frames import many, one, struct

R = "request.resource.attr."
ID = "id"


def frame(t):
    customers = one(
        t["customers"],
        "branch_code",
        t["branches"],
        "code",
        "branch",
        code="code",
        country="country",
    )
    accounts = one(
        t["accounts"],
        "customer_id",
        customers,
        "id",
        "customer",
        id="id",
        kycLevel="kyc_level",
        pep="pep",
        branch="branch",
    )
    payments = one(
        t["payments"],
        "account_id",
        accounts,
        "id",
        "account",
        currency="currency",
        status="status",
        overdraftLimit="overdraft_limit",
        customer="customer",
    )
    payments = payments.with_columns(
        struct(name="cp_name", iban="cp_iban", country="cp_country").alias(
            "counterparty"
        )
    )
    payments = many(
        payments, "id", t["payment_flags"], "payment_id", "flags", "flag", "flag"
    )
    return many(
        payments,
        "id",
        t["payment_approvals"],
        "payment_id",
        "approvals",
        "id",
        approverId="approver_id",
        decision="decision",
    )


ATTR_MAP = {
    R + "riskScore": "risk_score",
    R + "createdAt": "created_at",
    R + "initiatedBy": "initiated_by",
}
