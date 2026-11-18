import { v } from "convex/values";

import { mapperOf } from "./helpers";
import type { DomainMapping } from "./helpers";

const mapping: DomainMapping = {
  table: {
    id: v.string(),
    type: v.string(),
    authorId: v.string(),
    sensitivity: v.optional(v.number()),
    signedAt: v.optional(v.string()),
    isDeleted: v.boolean(),
    careTeam: v.array(v.string()),
    codes: v.array(v.object({ system: v.string(), code: v.string() })),
    patient: v.object({
      mrn: v.string(),
      consentResearch: v.boolean(),
      vip: v.optional(v.boolean()),
      birthYear: v.number(),
      ward: v.optional(
        v.object({
          id: v.string(),
          secure: v.boolean(),
          hospital: v.object({ region: v.string() }),
        }),
      ),
    }),
  },
  nulls: "omitted",
  mapper: mapperOf({
    noteType: ["type", false],
    deleted: ["isDeleted", false],
    authorId: false,
    sensitivity: true,
    signedAt: true,
    careTeam: false,
    codes: false,
    "patient.mrn": false,
    "patient.consentResearch": false,
    "patient.vip": true,
    "patient.birthYear": false,
    "patient.ward": true,
    "patient.ward.id": true,
    "patient.ward.secure": true,
    "patient.ward.hospital.region": true,
  }),
  toDocument: ({ id, attr: { noteType, deleted, ...rest } }) => ({
    id,
    type: noteType,
    isDeleted: deleted,
    ...rest,
  }),
};

export default mapping;
