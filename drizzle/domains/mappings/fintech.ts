import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function fintech(k: Kit): DomainMapping {
  const branches = k.table("branches", {
    code: k.text("code").primaryKey(),
    country: k.text("country").notNull(),
  });
  const customers = k.table("customers", {
    id: k.text("id").primaryKey(),
    legalName: k.text("legal_name").notNull(),
    kycLevel: k.int("kyc_level").notNull(),
    branchCode: k.text("branch_code").notNull().references(() => branches.code),
    pep: k.bool("pep"),
  });
  const accounts = k.table("accounts", {
    id: k.text("id").primaryKey(),
    customerId: k.text("customer_id").notNull().references(() => customers.id),
    currency: k.text("currency").notNull(),
    status: k.text("status").notNull(),
    overdraftLimit: k.decimal("overdraft_limit"),
  });
  const payments = k.table("payments", {
    id: k.text("id").primaryKey(),
    accountId: k.text("account_id").notNull().references(() => accounts.id),
    amount: k.decimal("amount").notNull(),
    currency: k.text("currency").notNull(),
    status: k.text("status").notNull(),
    riskScore: k.float("risk_score"),
    createdAt: k.timestamp("created_at").notNull(),
    cpName: k.text("cp_name").notNull(),
    cpIban: k.text("cp_iban").notNull(),
    cpCountry: k.text("cp_country").notNull(),
    initiatedBy: k.text("initiated_by").notNull(),
  });
  const paymentFlags = k.table(
    "payment_flags",
    {
      paymentId: k.text("payment_id").notNull().references(() => payments.id),
      flag: k.text("flag").notNull(),
    },
    ["paymentId", "flag"]
  );
  const paymentApprovals = k.table("payment_approvals", {
    id: k.text("id").primaryKey(),
    paymentId: k.text("payment_id").notNull().references(() => payments.id),
    approverId: k.text("approver_id").notNull(),
    decision: k.text("decision").notNull(),
  });

  const schema = {
    branches,
    customers,
    accounts,
    payments,
    paymentFlags,
    paymentApprovals,
    customersRelations: relations(customers, ({ one }) => ({
      branch: one(branches, { fields: [customers.branchCode], references: [branches.code] }),
    })),
    accountsRelations: relations(accounts, ({ one }) => ({
      customer: one(customers, { fields: [accounts.customerId], references: [customers.id] }),
    })),
    paymentsRelations: relations(payments, ({ one, many }) => ({
      account: one(accounts, { fields: [payments.accountId], references: [accounts.id] }),
      flags: many(paymentFlags),
      approvals: many(paymentApprovals),
    })),
    paymentFlagsRelations: relations(paymentFlags, ({ one }) => ({
      payment: one(payments, { fields: [paymentFlags.paymentId], references: [payments.id] }),
    })),
    paymentApprovalsRelations: relations(paymentApprovals, ({ one }) => ({
      payment: one(payments, { fields: [paymentApprovals.paymentId], references: [payments.id] }),
    })),
  };

  return {
    table: payments,
    id: payments.id,
    schema,
    mapper: {
      "request.resource.attr.createdAt": { column: payments.createdAt, valueType: "timestamp" },
      "request.resource.attr.counterparty.name": payments.cpName,
      "request.resource.attr.counterparty.iban": payments.cpIban,
      "request.resource.attr.counterparty.country": payments.cpCountry,
      "request.resource.attr.flags": {
        collectionValueType: "scalar",
        relation: { name: "flags", field: paymentFlags.flag },
      },
    },
  };
}
