import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const hub = new Schema({ code: String, country: String, region: String }, sub);
const carrier = new Schema({ scac: String }, sub);

const checkpoint = new Schema(
  { event: String, scannedAt: Date, hub: new Schema({ code: String }, sub) },
  sub,
);

const schema = new Schema(
  {
    _id: String,
    trackingNumber: String,
    status: String,
    weightKg: Number,
    hazmat: Boolean,
    customsHold: Boolean,
    shipper: String,
    eta: Date,
    declaredValue: Number,
    origin: hub,
    destination: hub,
    carrier,
    handling: [String],
    checkpoints: [checkpoint],
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
