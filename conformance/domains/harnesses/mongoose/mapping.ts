import type { Model, Schema } from "mongoose";

import type { Mapper } from "@cerbos/orm-mongoose";
import type { Resource } from "./common";

export interface DomainMapping {
  schema: Schema;
  mapper: Mapper;
  nulls: "omitted" | "explicit";
  toDocument?: (resource: Resource) => Record<string, unknown>;
  load?: (docs: Record<string, unknown>[]) => Promise<void>;
  query?: (model: Model<any>, filter: Record<string, unknown>) => Promise<string[]>;
}

export const sub = { _id: false, id: false } as const;
export const root = { id: false, versionKey: false } as const;
