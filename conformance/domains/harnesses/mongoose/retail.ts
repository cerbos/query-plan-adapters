import { Schema } from "mongoose";

import { root, sub } from "./mapping";
import type { DomainMapping } from "./mapping";

const line = new Schema(
  { sku: String, category: String, quantity: Number, unitPriceCents: Number },
  sub,
);

const schema = new Schema(
  {
    _id: String,
    status: String,
    channel: String,
    totalCents: Number,
    currency: String,
    couponCode: String,
    placedAt: Date,
    isGift: Boolean,
    customer: new Schema({ id: String, tier: String, loyaltyPoints: Number }, sub),
    store: new Schema({ region: String, country: String }, sub),
    lines: [line],
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
