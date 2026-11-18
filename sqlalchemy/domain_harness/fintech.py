from _types import KEY, BOOL, DECIMAL, FLOAT, INT, STR, TS, scalar
from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from cerbos_sqlalchemy import Relation, related, relation_overrides

Base = declarative_base()


class Branch(Base):
    __tablename__ = "branches"
    code = Column(KEY, primary_key=True)
    country = Column(STR, nullable=False)


class Customer(Base):
    __tablename__ = "customers"
    id = Column(KEY, primary_key=True)
    legal_name = Column(STR, nullable=False)
    kyc_level = Column(INT, nullable=False)
    branch_code = Column(KEY, ForeignKey("branches.code"), nullable=False)
    pep = Column(BOOL, nullable=True)
    branch = relationship(Branch, viewonly=True)


class Account(Base):
    __tablename__ = "accounts"
    id = Column(KEY, primary_key=True)
    customer_id = Column(KEY, ForeignKey("customers.id"), nullable=False)
    currency = Column(STR, nullable=False)
    status = Column(STR, nullable=False)
    overdraft_limit = Column(DECIMAL, nullable=True)
    customer = relationship(Customer, viewonly=True)


class Payment(Base):
    __tablename__ = "payments"
    id = Column(KEY, primary_key=True)
    account_id = Column(KEY, ForeignKey("accounts.id"), nullable=False)
    amount = Column(DECIMAL, nullable=False)
    currency = Column(STR, nullable=False)
    status = Column(STR, nullable=False)
    risk_score = Column(FLOAT, nullable=True)
    created_at = Column(TS, nullable=False)
    cp_name = Column(STR, nullable=False)
    cp_iban = Column(STR, nullable=False)
    cp_country = Column(STR, nullable=False)
    initiated_by = Column(STR, nullable=False)
    account = relationship(Account, viewonly=True)
    flags = relationship("PaymentFlag", viewonly=True)
    approvals = relationship("PaymentApproval", viewonly=True)


class PaymentFlag(Base):
    __tablename__ = "payment_flags"
    payment_id = Column(KEY, ForeignKey("payments.id"), primary_key=True)
    flag = Column(KEY, primary_key=True)


class PaymentApproval(Base):
    __tablename__ = "payment_approvals"
    id = Column(KEY, primary_key=True)
    payment_id = Column(KEY, ForeignKey("payments.id"), nullable=False)
    approver_id = Column(STR, nullable=False)
    decision = Column(STR, nullable=False)


ROOT = Payment
R = "request.resource.attr."

INFERRED = {
    R + "counterparty.name": Payment.cp_name,
    R + "counterparty.iban": Payment.cp_iban,
    R + "counterparty.country": Payment.cp_country,
    R + "flags": related(Payment.flags, PaymentFlag.flag),
}

P = Payment
_account = (Account.id == P.account_id,)
_customer = (Customer.id == Account.customer_id, *_account)
FLAGS = Relation([PaymentFlag.payment_id == P.id], [P], member=PaymentFlag.flag)
APPROVALS = Relation([PaymentApproval.payment_id == P.id], [P])
DECLARED = {
    R + "amount": P.amount,
    R + "currency": P.currency,
    R + "status": P.status,
    R + "riskScore": P.risk_score,
    R + "createdAt": P.created_at,
    R + "initiatedBy": P.initiated_by,
    R + "counterparty.name": P.cp_name,
    R + "counterparty.iban": P.cp_iban,
    R + "counterparty.country": P.cp_country,
    R + "flags": FLAGS,
    "f": PaymentFlag.flag,
    R + "approvals": APPROVALS,
    "a": APPROVALS,
    "a.approverId": PaymentApproval.approver_id,
    "a.decision": PaymentApproval.decision,
    R + "account.currency": scalar(P, Account.currency, *_account),
    R + "account.status": scalar(P, Account.status, *_account),
    R + "account.overdraftLimit": scalar(P, Account.overdraft_limit, *_account),
    R + "account.customer.id": scalar(P, Customer.id, *_customer),
    R + "account.customer.kycLevel": scalar(P, Customer.kyc_level, *_customer),
    R + "account.customer.pep": scalar(P, Customer.pep, *_customer),
    R + "account.customer.branch.code": scalar(P, Customer.branch_code, *_customer),
    R + "account.customer.branch.country": scalar(
        P, Branch.country, Branch.code == Customer.branch_code, *_customer
    ),
}


def mapping(style, dialect):
    if style == "inferred":
        return {"attr_map": INFERRED}
    return {"attr_map": DECLARED, "operator_override_fns": relation_overrides()}
