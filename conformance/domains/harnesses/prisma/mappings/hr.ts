import type { DomainMapping } from "../types";

export default {
  model: "Employee",
  mapper: {
    "request.resource.attr.email": { field: "workEmail" },
    "request.resource.attr.skills": { relation: { name: "skills", field: "skill" } },
  },
} satisfies DomainMapping;
