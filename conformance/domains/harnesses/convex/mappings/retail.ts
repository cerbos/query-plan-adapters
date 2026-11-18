import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    status: v.string(),
    channel: v.string(),
    totalCents: v.number(),
    currency: v.string(),
    couponCode: v.optional(v.string()),
    placedAt: v.string(),
    isGift: v.boolean(),
    customer: v.object({ id: v.string(), tier: v.string(), loyaltyPoints: v.number() }),
    store: v.optional(v.object({ region: v.string(), country: v.string() })),
    lines: v.array(
      v.object({
        sku: v.string(),
        category: v.string(),
        quantity: v.number(),
        unitPriceCents: v.number(),
      }),
    ),
  },
  nulls: "omitted",
  mapper: mapperOf({
    status: false,
    channel: false,
    totalCents: false,
    couponCode: true,
    "customer.id": false,
    "customer.tier": false,
    store: true,
    "store.region": true,
    lines: false,
  }),
};

export default mapping;
