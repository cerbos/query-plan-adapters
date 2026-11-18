import * as my from "drizzle-orm/mysql-core";
import * as pg from "drizzle-orm/pg-core";
import * as lite from "drizzle-orm/sqlite-core";
import type { Mapper, NullAttributeRepresentation } from "../src";
import type { RelationsSchema } from "../src/types";

export type Dialect = "sqlite" | "postgres" | "mysql";

/** The column builders one domain's tables are declared with, per dialect, matching generated/sql/<dialect>.sql. */
export interface Kit {
  dialect: Dialect;
  table(name: string, columns: Record<string, any>, primaryKey?: string[]): any;
  text(name: string): any;
  int(name: string): any;
  bigint(name: string): any;
  float(name: string): any;
  decimal(name: string): any;
  bool(name: string): any;
  timestamp(name: string): any;
  json(name: string): any;
  stringArray(name: string): any;
}

export interface DomainMapping {
  table: any;
  id: any;
  schema: RelationsSchema;
  mapper: Mapper;
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

export function kit(dialect: Dialect): Kit {
  if (dialect === "sqlite") {
    return {
      dialect,
      table: (name, columns, pk) =>
        lite.sqliteTable(name, columns, pk ? (t: any) => [lite.primaryKey({ columns: pk.map((c) => t[c]) as any })] : undefined),
      text: (n) => lite.text(n),
      int: (n) => lite.integer(n),
      bigint: (n) => lite.integer(n),
      float: (n) => lite.real(n),
      decimal: (n) => lite.numeric(n, { mode: "number" }),
      bool: (n) => lite.integer(n, { mode: "boolean" }),
      timestamp: (n) => lite.text(n),
      json: (n) => lite.text(n, { mode: "json" }),
      stringArray: (n) => lite.text(n, { mode: "json" }),
    };
  }
  if (dialect === "postgres") {
    return {
      dialect,
      table: (name, columns, pk) =>
        pg.pgTable(name, columns, pk ? (t: any) => [pg.primaryKey({ columns: pk.map((c) => t[c]) as any })] : undefined),
      text: (n) => pg.text(n),
      int: (n) => pg.integer(n),
      bigint: (n) => pg.bigint(n, { mode: "number" }),
      float: (n) => pg.doublePrecision(n),
      decimal: (n) => pg.numeric(n, { precision: 18, scale: 2, mode: "number" }),
      bool: (n) => pg.boolean(n),
      timestamp: (n) => pg.timestamp(n, { withTimezone: true, mode: "string" }),
      json: (n) => pg.jsonb(n),
      stringArray: (n) => pg.text(n).array(),
    };
  }
  return {
    dialect,
    table: (name, columns, pk) =>
      my.mysqlTable(name, columns, pk ? (t: any) => [my.primaryKey({ columns: pk.map((c) => t[c]) as any })] : undefined),
    text: (n) => my.varchar(n, { length: 255 }),
    int: (n) => my.int(n),
    bigint: (n) => my.bigint(n, { mode: "number" }),
    float: (n) => my.double(n),
    decimal: (n) => my.decimal(n, { precision: 18, scale: 2, mode: "number" }),
    bool: (n) => my.boolean(n),
    timestamp: (n) => my.datetime(n, { mode: "string", fsp: 6 }),
    json: (n) => my.json(n),
    stringArray: (n) => my.json(n),
  };
}
