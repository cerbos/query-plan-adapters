import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const alert = new Schema({ severity: Number, acknowledged: Boolean, code: String }, sub);
const site = new Schema(
  { region: String, org: new Schema({ id: String, plan: String }, sub) },
  sub,
);

const schema = new Schema(
  {
    _id: String,
    serial: String,
    model: String,
    firmware: String,
    lastSeenAt: Date,
    online: Boolean,
    batteryPct: Number,
    labels: { type: Map, of: String },
    labelKeys: [String],
    decommissioned: Boolean,
    site,
    alerts: [alert],
  },
  root,
);

const mapping: DomainMapping = {
  schema,
  nulls: "omitted",
  mapper: {
    "request.resource.id": { field: "_id" },
    "request.resource.attr.labels": { field: "labelKeys" },
    "request.resource.attr.labels.env": { field: "labels.env", valueType: "string" },
    "request.resource.attr.labels.restricted": { field: "labels.restricted", valueType: "string" },
  },
  toDocument: ({ id, attr }) => ({
    _id: id,
    ...attr,
    labelKeys: Object.keys(attr["labels"] as Record<string, string>),
  }),
};

export default mapping;
