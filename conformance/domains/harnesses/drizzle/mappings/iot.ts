import { eq, ne, relations, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function iot(k: Kit): DomainMapping {
  const orgs = k.table("orgs", {
    id: k.text("id").primaryKey(),
    name: k.text("name").notNull(),
    plan: k.text("plan").notNull(),
  });

  const sites = k.table("sites", {
    id: k.text("id").primaryKey(),
    orgId: k.text("org_id").notNull().references(() => orgs.id),
    region: k.text("region").notNull(),
    name: k.text("name").notNull(),
  });

  const devices = k.table("devices", {
    id: k.text("id").primaryKey(),
    serial: k.text("serial").notNull(),
    model: k.text("model").notNull(),
    firmware: k.text("firmware").notNull(),
    siteId: k.text("site_id").notNull().references(() => sites.id),
    lastSeenAt: k.timestamp("last_seen_at"),
    online: k.bool("online").notNull(),
    batteryPct: k.int("battery_pct"),
    labels: k.json("labels").notNull(),
    decommissioned: k.bool("decommissioned").notNull(),
  });

  const deviceAlerts = k.table("device_alerts", {
    id: k.text("id").primaryKey(),
    deviceId: k.text("device_id").notNull().references(() => devices.id),
    severity: k.int("severity").notNull(),
    acknowledged: k.bool("acknowledged").notNull(),
    code: k.text("code").notNull(),
  });

  const schema = {
    orgs,
    sites,
    devices,
    deviceAlerts,
    sitesRelations: relations(sites, ({ one }) => ({
      org: one(orgs, { fields: [sites.orgId], references: [orgs.id] }),
    })),
    devicesRelations: relations(devices, ({ one, many }) => ({
      site: one(sites, { fields: [devices.siteId], references: [sites.id] }),
      alerts: many(deviceAlerts),
    })),
    deviceAlertsRelations: relations(deviceAlerts, ({ one }) => ({
      device: one(devices, { fields: [deviceAlerts.deviceId], references: [devices.id] }),
    })),
  };

  const label = (key: string): SQL =>
    k.dialect === "sqlite"
      ? sql`json_extract(${devices.labels}, ${`$.${key}`})`
      : k.dialect === "postgres"
        ? sql`(${devices.labels} ->> ${key})`
        : sql`(${devices.labels} ->> ${sql.raw(`'$.${key}'`)})`;

  const labelMapping = (key: string) => ({
    transform: ({ operator, value }: { operator: string; value: unknown }): SQL => {
      if (operator === "eq") return eq(label(key), value);

      if (operator === "ne") return ne(label(key), value);
      throw new Error(`labels.${key}: unsupported operator ${operator}`);
    },
  });

  return {
    table: devices,
    id: devices.id,
    schema,
    mapper: {
      "request.resource.attr.lastSeenAt": { column: devices.lastSeenAt, valueType: "timestamp" },
      "request.resource.attr.labels.env": labelMapping("env"),
      "request.resource.attr.labels.restricted": labelMapping("restricted"),
      "request.resource.attr.labels": {
        transform: ({ operator, value }: { operator: string; value: unknown }): SQL => {
          if (operator !== "in" || typeof value !== "string") throw new Error(`labels: unsupported ${operator}`);

          return k.dialect === "sqlite"
            ? sql`json_type(${devices.labels}, ${`$.${value}`}) is not null`
            : k.dialect === "postgres"
              ? sql`jsonb_exists(${devices.labels}, ${value})`
              : sql`json_contains_path(${devices.labels}, 'one', ${`$.${value}`})`;
        },
      },
    },
  };
}
