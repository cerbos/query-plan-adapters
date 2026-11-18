import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const branch = new Schema({ code: String, country: String }, sub);
const customer = new Schema({ id: String, kycLevel: Number, pep: Boolean, branch }, sub);

const account = new Schema(
  { currency: String, status: String, overdraftLimit: Number, customer },
  sub,
);

const counterparty = new Schema({ name: String, iban: String, country: String }, sub);
const approval = new Schema({ approverId: String, decision: String }, sub);

const schema = new Schema(
  {
    _id: String,
    amount: Number,
    currency: String,
    status: String,
    riskScore: Number,
    createdAt: Date,
    initiatedBy: String,
    counterparty,
    flags: [String],
    approvals: [approval],
    account,
  },
  root,
);

const mapping: DomainMapping = {
  schema,
  nulls: "omitted",
  mapper: {
    "request.resource.id": { field: "_id" },
  },
};

export default mapping;
