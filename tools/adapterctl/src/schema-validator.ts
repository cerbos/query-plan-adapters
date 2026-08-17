import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { Ajv2020 } from "ajv/dist/2020.js";
import type { ErrorObject, ValidateFunction } from "ajv";

import { ValidationError } from "./errors.ts";

export type SchemaName = "manifest" | "catalog" | "check-resources" | "consumer-cases";

const schemaFiles: Record<SchemaName, string> = {
  manifest: "manifest-v1.schema.json",
  catalog: "catalog-v1.schema.json",
  "check-resources": "check-resources-v1.schema.json",
  "consumer-cases": "consumer-cases-v1.schema.json",
};

function formatError(path: string, error: ErrorObject): string {
  const location = error.instancePath === "" ? path : `${path}${error.instancePath}`;
  if (error.keyword === "additionalProperties") {
    const property = error.params["additionalProperty"];
    return `${location}: unknown key ${String(property)}`;
  }
  return `${location}: ${error.message ?? "schema validation failed"}`;
}

export class SchemaValidator {
  readonly validators: Map<SchemaName, ValidateFunction>;

  private constructor(validators: Map<SchemaName, ValidateFunction>) {
    this.validators = validators;
  }

  static async create(toolRoot: string): Promise<SchemaValidator> {
    const ajv = new Ajv2020({ allErrors: true, strict: true });
    const validators = new Map<SchemaName, ValidateFunction>();
    for (const name of Object.keys(schemaFiles)) {
      if (!isSchemaName(name)) continue;
      const source = await readFile(join(toolRoot, "schemas", schemaFiles[name]), "utf8");
      const schema: unknown = JSON.parse(source);
      if (!isRecord(schema)) throw new Error(`${schemaFiles[name]}: expected schema object`);
      validators.set(name, ajv.compile({ ...schema, $async: false }));
    }
    return new SchemaValidator(validators);
  }

  validate(name: SchemaName, path: string, value: unknown): void {
    const validate = this.validators.get(name);
    if (validate === undefined) throw new Error(`schema validator not loaded: ${name}`);
    if (validate(value)) return;
    const errors = validate.errors?.map((error) => formatError(path, error)) ?? [
      `${path}: schema validation failed`,
    ];
    throw new ValidationError(errors);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSchemaName(value: string): value is SchemaName {
  return value === "manifest" || value === "catalog" ||
    value === "check-resources" || value === "consumer-cases";
}
