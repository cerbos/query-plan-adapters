import type {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";

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
    throw new Error(errorMessage);
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

/** Whether the mapped path may be absent from a document — CEL's missing-attribute case. */
export const isNullableField = (reference: string, mapper: Mapper): boolean =>
  mapperConfig(reference, mapper)?.nullable ?? false;
