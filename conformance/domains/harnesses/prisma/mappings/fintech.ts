import type { DomainMapping } from "../types";

export default {
  model: "Payment",
  mapper: {
    "request.resource.attr.counterparty.name": { field: "cpName" },
    "request.resource.attr.counterparty.iban": { field: "cpIban" },
    "request.resource.attr.counterparty.country": { field: "cpCountry" },
    "request.resource.attr.flags": { relation: { name: "flags", field: "flag" } },
  },
} satisfies DomainMapping;
