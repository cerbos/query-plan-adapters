import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function logistics(k: Kit): DomainMapping {
  const hubs = k.table("hubs", {
    id: k.text("id").primaryKey(),
    code: k.text("code").notNull(),
    country: k.text("country").notNull(),
    region: k.text("region").notNull(),
  });

  const carriers = k.table("carriers", {
    id: k.text("id").primaryKey(),
    name: k.text("name").notNull(),
    scac: k.text("scac").notNull(),
  });

  const shipments = k.table("shipments", {
    id: k.text("id").primaryKey(),
    trackingNo: k.text("tracking_no").notNull(),
    status: k.text("status").notNull(),
    originHubId: k.text("origin_hub_id").notNull().references(() => hubs.id),
    destinationHubId: k.text("destination_hub_id").notNull().references(() => hubs.id),
    carrierId: k.text("carrier_id").references(() => carriers.id),
    weightKg: k.float("weight_kg").notNull(),
    hazmat: k.bool("hazmat").notNull(),
    customsHold: k.bool("customs_hold"),
    shipperAccount: k.text("shipper_account").notNull(),
    eta: k.timestamp("eta"),
    declaredValue: k.decimal("declared_value").notNull(),
  });

  const shipmentHandling = k.table(
    "shipment_handling",
    {
      shipmentId: k.text("shipment_id").notNull().references(() => shipments.id),
      code: k.text("code").notNull(),
    },
    ["shipmentId", "code"]
  );

  const checkpoints = k.table("checkpoints", {
    id: k.text("id").primaryKey(),
    shipmentId: k.text("shipment_id").notNull().references(() => shipments.id),
    hubId: k.text("hub_id").notNull().references(() => hubs.id),
    event: k.text("event").notNull(),
    scannedAt: k.timestamp("scanned_at").notNull(),
  });

  const schema = {
    hubs,
    carriers,
    shipments,
    shipmentHandling,
    checkpoints,
    shipmentsRelations: relations(shipments, ({ one, many }) => ({
      origin: one(hubs, { fields: [shipments.originHubId], references: [hubs.id], relationName: "origin" }),
      destination: one(hubs, {
        fields: [shipments.destinationHubId],
        references: [hubs.id],
        relationName: "destination",
      }),
      carrier: one(carriers, { fields: [shipments.carrierId], references: [carriers.id] }),
      handling: many(shipmentHandling),
      checkpoints: many(checkpoints),
    })),
    shipmentHandlingRelations: relations(shipmentHandling, ({ one }) => ({
      shipment: one(shipments, { fields: [shipmentHandling.shipmentId], references: [shipments.id] }),
    })),
    checkpointsRelations: relations(checkpoints, ({ one }) => ({
      shipment: one(shipments, { fields: [checkpoints.shipmentId], references: [shipments.id] }),
      hub: one(hubs, { fields: [checkpoints.hubId], references: [hubs.id] }),
    })),
  };

  return {
    table: shipments,
    id: shipments.id,
    schema,
    mapper: {
      "request.resource.attr.trackingNumber": shipments.trackingNo,
      "request.resource.attr.shipper": shipments.shipperAccount,
      "request.resource.attr.eta": { column: shipments.eta, valueType: "timestamp" },
      "request.resource.attr.handling": {
        collectionValueType: "scalar",
        relation: { name: "handling", field: shipmentHandling.code },
      },
    },
  };
}
