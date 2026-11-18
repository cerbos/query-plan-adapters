import type { DomainMapping } from "../types";

export default {
  model: "Article",
  mapper: {
    "request.resource.attr.contributors": {
      relation: {
        name: "contributors",
        fields: {
          id: { field: "authorId" },
          staff: { relation: { name: "author", field: "staff" } },
        },
      },
    },
  },
} satisfies DomainMapping;
