import type { Metadata } from "chromadb";

import type { FieldMapper, FieldNameMapperConfig } from "../../../../langchain-chromadb/src";
import type { Resource } from "./common";

export interface DomainMapping {
  fieldNameMapper: FieldMapper;
  toMetadata: (resource: Resource) => Metadata;
}

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

/**
 * Writes a resource as flat Chroma metadata: a scalar under its dotted path, a NULL as no key,
 * a list as JSON text plus its non-null scalar elements under `<key>.elements`, and every path
 * named in `json` (a map, a to-one parent read whole, an explicit null) as JSON text.
 */
export function flatten(
  { id, attr }: Resource,
  json: string[] = [],
): Metadata {
  const metadata: Metadata = { id };

  const walk = (prefix: string, value: Json | undefined): void => {
    if (value === undefined) return;

    if (json.includes(prefix)) {
      metadata[prefix] = JSON.stringify(value);

      if (value === null || typeof value !== "object") return;
    }

    if (value === null) return;

    if (Array.isArray(value)) {
      metadata[prefix] = JSON.stringify(value);
      const elements = value.filter((v) => v !== null && typeof v !== "object");

      if (elements.length > 0 && elements.length === value.filter((v) => v !== null).length) {
        metadata[`${prefix}.elements`] = elements as string[];
      }

      return;
    }

    if (typeof value === "object") {
      if (json.includes(prefix)) return;

      for (const [key, child] of Object.entries(value)) walk(prefix ? `${prefix}.${key}` : key, child);

      return;
    }

    if (!json.includes(prefix)) metadata[prefix] = value;
  };

  walk("", attr as Json);

  return metadata;
}

const A = "request.resource.attr.";

/** A scalar key that every record carries. */
export const req = (field: string, extra: Partial<FieldNameMapperConfig> = {}): [string, FieldNameMapperConfig] => [
  A + field,
  { field, required: true, ...extra },
];

/** A scalar key that a record may lack. */
export const opt = (field: string, extra: Partial<FieldNameMapperConfig> = {}): [string, FieldNameMapperConfig] => [
  A + field,
  { field, ...extra },
];

/** A key holding JSON text: a list, a map, a list of objects or an explicit null. */
export const json = (
  field: string,
  extra: Partial<FieldNameMapperConfig> = {},
): [string, FieldNameMapperConfig] => [A + field, { field, valueType: "json", ...extra }];

/** A list of strings stored as JSON text with its elements beside it for `$contains`. */
export const list = (field: string, required = true): [string, FieldNameMapperConfig] => [
  A + field,
  { field, valueType: "json", required, listElements: { field: `${field}.elements`, type: "string" } },
];

export const id: [string, FieldNameMapperConfig] = ["request.resource.id", { field: "id", required: true }];

export const int = { numericType: "integer" } as const;
export const float = { numericType: "float" } as const;
export const bool = { valueType: "boolean" } as const;
