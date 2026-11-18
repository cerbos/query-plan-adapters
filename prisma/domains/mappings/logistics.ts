import type { DomainMapping } from "../types";

export default {
  model: "Shipment",
  mapper: {
    "request.resource.attr.trackingNumber": { field: "trackingNo" },
    "request.resource.attr.shipper": { field: "shipperAccount" },
    "request.resource.attr.handling": { relation: { name: "handling", field: "code" } },
    "request.resource.attr.checkpoints": {
      relation: { name: "checkpoints", fields: { hub: { relation: { name: "hub" } } } },
    },
  },
} satisfies DomainMapping;
