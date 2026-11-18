import { relations } from "drizzle-orm";
import type { DomainMapping, Kit } from "../kit";

export default function retail(k: Kit): DomainMapping {
  const stores = k.table("stores", {
    id: k.text("id").primaryKey(),
    region: k.text("region").notNull(),
    country: k.text("country").notNull(),
  });

  const customers = k.table("customers", {
    id: k.text("id").primaryKey(),
    email: k.text("email").notNull(),
    tier: k.text("tier").notNull(),
    loyaltyPoints: k.int("loyalty_points").notNull(),
  });

  const order = k.table("order", {
    id: k.text("id").primaryKey(),
    customerId: k.text("customer_id").notNull().references(() => customers.id),
    storeId: k.text("store_id").references(() => stores.id),
    status: k.text("status").notNull(),
    channel: k.text("channel").notNull(),
    totalCents: k.bigint("total_cents").notNull(),
    currency: k.text("currency").notNull(),
    couponCode: k.text("coupon_code"),
    placedAt: k.timestamp("placed_at").notNull(),
    gift: k.bool("gift").notNull(),
  });

  const orderLine = k.table("order_line", {
    id: k.text("id").primaryKey(),
    orderId: k.text("order_id").notNull().references(() => order.id),
    lineNo: k.int("line_no").notNull(),
    sku: k.text("sku").notNull(),
    category: k.text("category").notNull(),
    quantity: k.int("quantity").notNull(),
    unitPriceCents: k.bigint("unit_price_cents").notNull(),
  });

  const schema = {
    stores,
    customers,
    order,
    orderLine,
    orderRelations: relations(order, ({ one, many }) => ({
      customer: one(customers, { fields: [order.customerId], references: [customers.id] }),
      store: one(stores, { fields: [order.storeId], references: [stores.id] }),
      lines: many(orderLine),
    })),
    orderLineRelations: relations(orderLine, ({ one }) => ({
      order: one(order, { fields: [orderLine.orderId], references: [order.id] }),
    })),
  };

  return {
    table: order,
    id: order.id,
    schema,
    mapper: {
      "request.resource.attr.isGift": order.gift,
      "request.resource.attr.placedAt": { column: order.placedAt, valueType: "timestamp" },
    },
  };
}
