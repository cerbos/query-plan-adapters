import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const nullable = <T extends Parameters<typeof v.union>[0]>(validator: T) => v.union(validator, v.null());

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    title: v.string(),
    privilege: nullable(v.string()),
    classification: v.number(),
    legalHold: v.boolean(),
    createdBy: v.string(),
    retentionUntil: nullable(v.string()),
    shares: v.array(v.object({ partyType: v.string(), partyId: v.string() })),
    matter: v.object({
      status: v.string(),
      practiceArea: v.string(),
      leadPartnerId: nullable(v.string()),
      team: v.array(v.string()),
      screened: v.array(v.string()),
      client: v.object({ id: v.string(), industry: nullable(v.string()) }),
    }),
  },
  nulls: "explicit",
  mapper: mapperOf({
    privilege: false,
    classification: false,
    legalHold: false,
    retentionUntil: false,
    shares: false,
    "matter.status": false,
    "matter.practiceArea": false,
    "matter.leadPartnerId": false,
    "matter.team": false,
    "matter.screened": false,
    "matter.client.id": false,
  }),
};

export default mapping;
