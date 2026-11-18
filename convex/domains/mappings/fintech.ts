import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    amount: v.number(),
    currency: v.string(),
    status: v.string(),
    riskScore: v.optional(v.number()),
    createdAt: v.string(),
    initiatedBy: v.string(),
    counterparty: v.object({ name: v.string(), iban: v.string(), country: v.string() }),
    flags: v.array(v.string()),
    approvals: v.array(v.object({ approverId: v.string(), decision: v.string() })),
    account: v.object({
      currency: v.string(),
      status: v.string(),
      overdraftLimit: v.optional(v.number()),
      customer: v.object({
        id: v.string(),
        kycLevel: v.number(),
        pep: v.optional(v.boolean()),
        branch: v.object({ code: v.string(), country: v.string() }),
      }),
    }),
  },
  nulls: "omitted",
  mapper: mapperOf({
    amount: false,
    currency: false,
    status: false,
    riskScore: true,
    createdAt: false,
    initiatedBy: false,
    "counterparty.country": false,
    flags: false,
    approvals: false,
    "account.currency": false,
    "account.status": false,
    "account.overdraftLimit": true,
    "account.customer.id": false,
    "account.customer.kycLevel": false,
    "account.customer.pep": true,
    "account.customer.branch.code": false,
  }),
};

export default mapping;
