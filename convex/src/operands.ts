import type {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { Mapper, MapperConfig } from "./index";

// Reading plan nodes and the caller's mapper. Shared by the three passes over a plan: validation,
// the Convex filter it pushes down, and the in-memory evaluator that answers the rest.

export type ComparisonOperator = "eq" | "ne" | "lt" | "le" | "gt" | "ge";

export const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
export const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
export const isVariable = (
  e: PlanExpressionOperand,
): e is PlanExpressionVariable => "name" in e;

export const operandAt = (
  operands: PlanExpressionOperand[],
  index: number,
  errorMessage: string,
): PlanExpressionOperand => {
  const operand = operands[index];
  if (!operand) {
    throw new UnsupportedQueryPlanError(errorMessage);
  }
  return operand;
};

const mapperConfig = (
  reference: string,
  mapper: Mapper,
): MapperConfig | undefined =>
  typeof mapper === "function" ? mapper(reference) : mapper[reference];

/** Whether the caller's mapper declares `reference` at all — an entry with no `field` included. */
export const isMappedReference = (reference: string, mapper: Mapper): boolean =>
  mapperConfig(reference, mapper) !== undefined;

/**
 * The document path a plan reference maps to. An entry that names no `field` keeps the plan path;
 * a reference with no entry at all never gets here, because `assertEveryReferenceMapped` refuses
 * it before translation.
 */
export const resolveField = (reference: string, mapper: Mapper): string =>
  mapperConfig(reference, mapper)?.field ?? reference;

/**
 * The mapper as the `"omitted"` convention reads it: every entry that does not declare `nullable`
 * is `nullable: true`, and `nullable: false` still opts an entry out.
 *
 * The call-level convention is the default for an attribute that declares nothing (ADR 0004).
 * Under `"omitted"`, a NULL field sends no attribute and CEL denies the document on a
 * missing-attribute error, while the pushed-down `q.neq(...)` and a negated comparison match a
 * document the path is absent from. Refusing null operands alone left `R.attr.x != "a"` returning
 * those documents on every field that did not declare `nullable`
 * (cerbos/query-plan-adapters#493). A nullable field stays with the post-filter, whose evaluator
 * has the missing-attribute error.
 */
export const withOmittedNullDefault = (mapper: Mapper): Mapper => {
  const nullableByDefault = (config: MapperConfig): MapperConfig => ({
    ...config,
    nullable: config.nullable ?? true,
  });
  if (typeof mapper === "function") {
    return (key) => {
      const config = mapper(key);
      return config && nullableByDefault(config);
    };
  }
  return Object.fromEntries(
    Object.entries(mapper).map(([key, config]) => [
      key,
      nullableByDefault(config),
    ]),
  );
};

/** Whether the mapped path may be absent from a document — CEL's missing-attribute case. */
export const isNullableField = (reference: string, mapper: Mapper): boolean =>
  mapperConfig(reference, mapper)?.nullable ?? false;
