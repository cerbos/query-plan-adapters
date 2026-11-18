import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const matter = new Schema(
  {
    status: String,
    practiceArea: String,
    leadPartnerId: String,
    team: [String],
    screened: [String],
    client: new Schema({ id: String, industry: String }, sub),
  },
  sub,
);

const share = new Schema({ partyType: String, partyId: String }, sub);

const schema = new Schema(
  {
    _id: String,
    title: String,
    privilege: String,
    classification: Number,
    legalHold: Boolean,
    createdBy: String,
    retentionUntil: Date,
    shares: [share],
    matter,
  },
  root,
);

const mapping: DomainMapping = {
  schema,
  nulls: "explicit",
  mapper: {
    "request.resource.id": { field: "_id" },
    "request.resource.attr.matter.team": { field: "matter.team" },
    "request.resource.attr.matter.screened": { field: "matter.screened" },
  },
};

export default mapping;
