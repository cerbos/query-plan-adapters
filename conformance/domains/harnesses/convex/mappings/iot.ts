import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    serial: v.string(),
    model: v.string(),
    firmware: v.string(),
    lastSeenAt: v.optional(v.string()),
    online: v.boolean(),
    batteryPct: v.optional(v.number()),
    labels: v.record(v.string(), v.string()),
    decommissioned: v.boolean(),
    site: v.object({
      region: v.string(),
      org: v.object({ id: v.string(), plan: v.string() }),
    }),
    alerts: v.array(
      v.object({ severity: v.number(), acknowledged: v.boolean(), code: v.string() }),
    ),
  },
  nulls: "omitted",
  mapper: mapperOf({
    firmware: false,
    online: false,
    batteryPct: true,
    labels: false,
    "labels.env": true,
    "labels.restricted": true,
    decommissioned: false,
    "site.region": false,
    "site.org.id": false,
    alerts: false,
  }),
};

export default mapping;
