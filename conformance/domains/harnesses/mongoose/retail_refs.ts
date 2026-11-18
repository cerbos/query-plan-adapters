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
    customer: { type: String, ref: "dom_retail_customer" },
    store: { type: String, ref: "dom_retail_store" },
    lines: [line],
  },
  root,
);

const mapping: DomainMapping = {
  schema,
  nulls: "omitted",
  mapper: {
    "request.resource.id": { field: "_id" },
    "request.resource.attr.customer.id": { field: "customer" },
  },
  toDocument: ({ id, attr: { customer, store, ...rest } }) => ({
    _id: id,
    ...rest,
    customer: (customer as { id: string }).id,
    store: store ? `${(store as { region: string }).region}-${(store as { country: string }).country}` : undefined,
  }),
};

export default mapping;
