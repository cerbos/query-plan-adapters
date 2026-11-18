import type { DomainMapping } from "../types";

export default {
  model: "Case",
  nullAttributeRepresentation: "explicit",
  mapper: {
    "request.resource.attr.flags": { relation: { name: "flags", field: "flag" } },
  },
} satisfies DomainMapping;
