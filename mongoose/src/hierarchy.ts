import type { PlanExpressionOperand } from "@cerbos/core";

import { buildFieldFilter, withNullableGuards } from "./guards";
import type { Mapper, MongooseFilter } from "./index";
import { resolveFieldReference, resolveMapperConfig } from "./mapper";
import { isExpression, isValue, isVariable } from "./operands";
import { escapeRegexValue } from "./regex";

export type HierarchyOperator = "ancestorOf" | "descendentOf" | "overlaps";

type HierarchyOperand =
  | { kind: "field"; name: string; separator: string }
  | { kind: "value"; value: string; separator: string };

const parseHierarchyOperand = (
  operand: PlanExpressionOperand,
): HierarchyOperand => {
  if (!isExpression(operand) || operand.operator !== "hierarchy") {
    throw new Error("Hierarchy operators require hierarchy() operands");
  }
  const [valueOperand, separatorOperand] = operand.operands;
  if (!valueOperand) {
    throw new Error("hierarchy operator requires a path operand");
  }
  // An omitted separator is CEL's default; a present one must be a non-empty string constant.
  let separator = ".";
  if (separatorOperand) {
    if (
      !isValue(separatorOperand) ||
      typeof separatorOperand.value !== "string" ||
      !separatorOperand.value
    ) {
      throw new Error("hierarchy separator must be a non-empty string");
    }
    separator = separatorOperand.value;
  }
  if (isVariable(valueOperand)) {
    return { kind: "field", name: valueOperand.name, separator };
  }
  if (isValue(valueOperand) && typeof valueOperand.value === "string") {
    return { kind: "value", value: valueOperand.value, separator };
  }
  throw new Error("hierarchy path must be a field or string value");
};

/** `"a.b.c"` → `["a", "a.b"]`: every proper ancestor of the path. */
const hierarchyPrefixes = (value: string, separator: string): string[] => {
  const segments = value.split(separator);
  return segments
    .slice(0, -1)
    .map((_, index) => segments.slice(0, index + 1).join(separator));
};

/** `ancestorOf`/`descendentOf`/`overlaps` between one field and one constant path. */
export const buildHierarchyFilter = (
  operator: HierarchyOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
): MongooseFilter => {
  const [leftOperand, rightOperand] = operands;
  if (!leftOperand || !rightOperand) {
    throw new Error(`${operator} requires two hierarchy operands`);
  }
  const left = parseHierarchyOperand(leftOperand);
  const right = parseHierarchyOperand(rightOperand);
  for (const operand of [left, right]) {
    if (operand.kind === "field") {
      const type = resolveMapperConfig(operand.name, mapper)?.valueType;
      if (type !== undefined && type !== "string") {
        throw new Error(
          "hierarchy requires a string field: the declared scalar type cannot be compared with a path prefix",
        );
      }
    }
  }
  if (left.separator !== right.separator) {
    throw new Error(
      `${operator} requires one field and one value with the same separator`,
    );
  }

  let field: Extract<HierarchyOperand, { kind: "field" }>;
  let value: Extract<HierarchyOperand, { kind: "value" }>;
  if (left.kind === "field" && right.kind === "value") {
    field = left;
    value = right;
  } else if (left.kind === "value" && right.kind === "field") {
    field = right;
    value = left;
  } else {
    throw new Error(`${operator} requires one field and one value`);
  }
  const { path, relation } = resolveFieldReference(field.name, mapper);
  if (relation?.type === "many") {
    throw new Error("Hierarchy fields cannot be collection relations");
  }

  const ancestors = hierarchyPrefixes(value.value, value.separator);
  const descendentFilter = buildFieldFilter(path, {
    $regex: `^${escapeRegexValue(value.value + value.separator)}`,
  });

  let filter: MongooseFilter;
  if (operator === "overlaps") {
    filter = {
      $or: [
        buildFieldFilter(path, { $in: [...ancestors, value.value] }),
        descendentFilter,
      ],
    };
  } else {
    // `ancestorOf(field, value)` and `descendentOf(value, field)` both ask for a field that is an
    // ancestor of the constant; the other two spellings ask for a descendent.
    const fieldMustBeAncestor =
      (operator === "ancestorOf") === (left.kind === "field");
    filter = fieldMustBeAncestor
      ? buildFieldFilter(path, { $in: ancestors })
      : descendentFilter;
  }
  return withNullableGuards(filter, operands, mapper);
};
