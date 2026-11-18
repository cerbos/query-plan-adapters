import { bool, flatten, float, id, int, json, list, opt, req } from "./mapping";
import type { DomainMapping } from "./mapping";

const mapping: DomainMapping = {
  fieldNameMapper: Object.fromEntries([
    id,
    req("amount", float),
    req("currency"),
    req("status"),
    opt("riskScore", float),
    req("createdAt"),
    req("initiatedBy"),
    req("counterparty.name"),
    req("counterparty.iban"),
    req("counterparty.country"),
    list("flags"),
    json("approvals", { required: true }),
    req("account.currency"),
    req("account.status"),
    opt("account.overdraftLimit", float),
    req("account.customer.id"),
    req("account.customer.kycLevel", int),
    opt("account.customer.pep", bool),
    req("account.customer.branch.code"),
    req("account.customer.branch.country"),
  ]),
  toMetadata: (resource) => flatten(resource),
};

export default mapping;
