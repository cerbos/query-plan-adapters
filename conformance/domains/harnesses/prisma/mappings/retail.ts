import type { DomainMapping } from "../types";

export default {
  model: "Order",
  mapper: {
    "request.resource.attr.isGift": { field: "gift" },
  },
} satisfies DomainMapping;
