import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const hub = v.object({ code: v.string(), country: v.string(), region: v.string() });

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    trackingNumber: v.string(),
    status: v.string(),
    weightKg: v.number(),
    hazmat: v.boolean(),
    customsHold: v.optional(v.boolean()),
    shipper: v.string(),
    eta: v.optional(v.string()),
    declaredValue: v.number(),
    origin: hub,
    destination: hub,
    carrier: v.optional(v.object({ scac: v.string() })),
    handling: v.array(v.string()),
    checkpoints: v.array(
      v.object({ event: v.string(), scannedAt: v.string(), hub: v.object({ code: v.string() }) }),
    ),
  },
  nulls: "omitted",
  mapper: mapperOf({
    status: false,
    weightKg: false,
    hazmat: false,
    customsHold: true,
    shipper: false,
    declaredValue: false,
    "origin.code": false,
    "origin.country": false,
    "destination.code": false,
    "destination.country": false,
    carrier: true,
    "carrier.scac": true,
    handling: false,
    checkpoints: false,
  }),
};

export default mapping;
