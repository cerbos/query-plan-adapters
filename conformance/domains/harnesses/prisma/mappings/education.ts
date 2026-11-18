import type { DomainMapping } from "../types";

export default {
  model: "Submission",
  mapper: {
    "request.resource.attr.members": { relation: { name: "members", field: "studentId" } },
  },
} satisfies DomainMapping;
