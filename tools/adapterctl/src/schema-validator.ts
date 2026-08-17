import { Ajv2020 } from "ajv/dist/2020.js";
import type { ErrorObject, ValidateFunction } from "ajv";

import { ValidationError } from "./errors.ts";
import {
  catalogDocumentSchema,
  checkResourcesDocumentSchema,
  consumerCasesDocumentSchema,
  manifestDocumentSchema,
  type CatalogDocument,
  type CheckResourcesDocument,
  type ConsumerCasesDocument,
  type ManifestDocument,
} from "./schemas.ts";

type ValidationRequest =
  | { name: "manifest"; path: string; value: unknown }
  | { name: "catalog"; path: string; value: unknown }
  | { name: "check-resources"; path: string; value: unknown }
  | { name: "consumer-cases"; path: string; value: unknown };

type BoundaryDocument =
  | ManifestDocument
  | CatalogDocument
  | CheckResourcesDocument
  | ConsumerCasesDocument;

type Validators = {
  manifest: ValidateFunction<ManifestDocument>;
  catalog: ValidateFunction<CatalogDocument>;
  "check-resources": ValidateFunction<CheckResourcesDocument>;
  "consumer-cases": ValidateFunction<ConsumerCasesDocument>;
};

function formatError(args: { path: string; error: ErrorObject }): string {
  const location = args.error.instancePath === "" ? args.path : `${args.path}${args.error.instancePath}`;
  if (args.error.keyword === "additionalProperties") {
    const property = args.error.params["additionalProperty"];
    return `${location}: unknown key ${String(property)}`;
  }
  return `${location}: ${args.error.message ?? "schema validation failed"}`;
}

export class SchemaValidator {
  readonly validators: Validators;

  private constructor(validators: Validators) {
    this.validators = validators;
  }

  static create(): SchemaValidator {
    const ajv = new Ajv2020({ allErrors: true, strict: true });
    return new SchemaValidator({
      manifest: ajv.compile<ManifestDocument>(manifestDocumentSchema),
      catalog: ajv.compile<CatalogDocument>(catalogDocumentSchema),
      "check-resources": ajv.compile<CheckResourcesDocument>(checkResourcesDocumentSchema),
      "consumer-cases": ajv.compile<ConsumerCasesDocument>(consumerCasesDocumentSchema),
    });
  }

  validate(args: { name: "manifest"; path: string; value: unknown }): ManifestDocument;
  validate(args: { name: "catalog"; path: string; value: unknown }): CatalogDocument;
  validate(args: {
    name: "check-resources";
    path: string;
    value: unknown;
  }): CheckResourcesDocument;
  validate(args: {
    name: "consumer-cases";
    path: string;
    value: unknown;
  }): ConsumerCasesDocument;
  validate(args: ValidationRequest): BoundaryDocument {
    switch (args.name) {
      case "manifest":
        return this.validateValue({ validator: this.validators.manifest, ...args });
      case "catalog":
        return this.validateValue({ validator: this.validators.catalog, ...args });
      case "check-resources":
        return this.validateValue({ validator: this.validators["check-resources"], ...args });
      case "consumer-cases":
        return this.validateValue({ validator: this.validators["consumer-cases"], ...args });
      default: {
        const exhaustive: never = args;
        return exhaustive;
      }
    }
  }

  private validateValue<Document>(args: {
    validator: ValidateFunction<Document>;
    path: string;
    value: unknown;
  }): Document {
    if (args.validator(args.value)) return args.value;
    const errors = args.validator.errors?.map((error) => formatError({ path: args.path, error })) ?? [
      `${args.path}: schema validation failed`,
    ];
    throw new ValidationError(errors);
  }
}
