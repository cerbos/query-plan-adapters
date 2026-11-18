import type { DomainMapping } from "../types";

export default {
  model: "Document",
  nullAttributeRepresentation: "explicit",
  mapper: {
    "request.resource.attr.matter": {
      relation: {
        name: "matter",
        fields: {
          team: { relation: { name: "team", field: "userId" } },
          screened: { relation: { name: "screened", field: "userId" } },
        },
      },
    },
  },
} satisfies DomainMapping;
