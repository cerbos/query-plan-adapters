import type { DomainMapping } from "../types";

export default {
  model: "ClinicalNote",
  mapper: {
    "request.resource.attr.noteType": { field: "type" },
    "request.resource.attr.deleted": { field: "isDeleted" },
    "request.resource.attr.careTeam": { relation: { name: "careTeam", field: "clinicianId" } },
  },
} satisfies DomainMapping;
