import { defineSchema, defineTable } from "convex/server";

import { MAPPINGS } from "../mappings";

export default defineSchema(
  Object.fromEntries(
    Object.entries(MAPPINGS).map(([domain, mapping]) => [domain, defineTable(mapping.table)]),
  ),
);
