import type { PlanExpressionOperand } from "@cerbos/core";
import { or, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";

import {
  buildColumnExpression,
  isColumn,
  isMappingConfig,
  resolveFieldReference,
} from "./mapper";
import { isNameOperand, isOperatorCall, isValueOperand } from "./operands";
import {
  FALSE_CONDITION,
  buildStringMatchCondition,
  columnExpression,
  constantExpression,
} from "./predicates";
import { wrapRelationChain } from "./relations";
import type { BuildFilterOptions, Mapper, ResolvedMapping } from "./types";

/**
 * Cerbos's `hierarchy()` operators — `ancestorOf`, `descendentOf`, `overlaps` — between one
 * hierarchy stored in a column and one the plan carries as a constant.
 */

type HierarchyOperator = "ancestorOf" | "descendentOf" | "overlaps";

type ConstantHierarchy = {
  kind: "constant";
  segments: string[];
  delimiter: string;
};

type FieldHierarchy = {
  kind: "field";
  reference: string;
  resolved: ResolvedMapping;
  delimiter: string;
};

type ResolvedHierarchy = ConstantHierarchy | FieldHierarchy;

const resolveHierarchy = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): ResolvedHierarchy => {
  if (!isOperatorCall(operand, "hierarchy")) {
    throw new Error("Hierarchy operators require hierarchy(...) operands");
  }
  if (operand.operands.length < 1 || operand.operands.length > 2) {
    throw new Error("'hierarchy' operator requires one or two operands");
  }
  const [pathOperand, delimiterOperand] = operand.operands;
  if (!pathOperand) {
    throw new Error("'hierarchy' operator is missing its path operand");
  }
  let delimiter = ".";
  if (delimiterOperand) {
    if (
      !isValueOperand(delimiterOperand) ||
      typeof delimiterOperand.value !== "string"
    ) {
      throw new Error("Hierarchy delimiter must be a string value");
    }
    if (delimiterOperand.value === "") {
      // Cerbos splits a path on an empty delimiter into one segment per CHARACTER, so the
      // relation becomes a strict string-prefix test. The descendant lowering below is
      // `LIKE prefix || delimiter || '%'`, which with an empty delimiter matches the path
      // ITSELF (never its own descendant) as well as every string extension of it — the
      // corpus's hier-empty-delim over-granted a2 that way — so the shape is refused rather
      // than emitted with the wrong boundary.
      throw new Error(
        "Hierarchy delimiter must be a non-empty string: an empty delimiter splits the path per character, and the prefix LIKE this adapter emits would also match the path itself",
      );
    }
    delimiter = delimiterOperand.value;
  }

  if (isValueOperand(pathOperand)) {
    if (typeof pathOperand.value !== "string") {
      throw new Error(
        "Hierarchy path must be a string value or field reference",
      );
    }
    return {
      kind: "constant",
      segments: pathOperand.value.split(delimiter),
      delimiter,
    };
  }
  if (isNameOperand(pathOperand)) {
    return {
      kind: "field",
      reference: pathOperand.name,
      resolved: resolveFieldReference(pathOperand.name, mapper),
      delimiter,
    };
  }
  throw new Error(
    "Segmented hierarchy expressions are not supported by the Drizzle adapter",
  );
};

/** Every proper ancestor path of `segments`, shortest first. */
const hierarchyStrictPrefixes = (
  segments: string[],
  delimiter: string,
): string[] => {
  const prefixes: string[] = [];
  for (let length = 1; length < segments.length; length += 1) {
    prefixes.push(segments.slice(0, length).join(delimiter));
  }
  return prefixes;
};

const buildFieldHierarchyFilter = (
  operator: HierarchyOperator,
  field: FieldHierarchy,
  constant: ConstantHierarchy,
  fieldIsLeft: boolean,
  options: BuildFilterOptions,
): SQL => {
  const { mapping } = field.resolved;
  const fieldColumn = isColumn(mapping)
    ? mapping
    : isMappingConfig(mapping)
      ? mapping.column
      : undefined;
  // A hierarchy over a non-string column is a CEL error: UNKNOWN under both polarities.
  if (fieldColumn && fieldColumn.dataType !== "string") return sql`null`;

  const fieldExpr = buildColumnExpression(mapping, field.reference);
  const constantPath = constant.segments.join(field.delimiter);
  const prefixes = hierarchyStrictPrefixes(constant.segments, field.delimiter);
  const isOneOfPrefixes = () =>
    sql`${fieldExpr} in ${prefixes.map((prefix) => sql`${prefix}`)}`;
  const isDescendantOfConstant = () =>
    buildStringMatchCondition(
      "startsWith",
      columnExpression(fieldExpr),
      constantExpression(sql`${constantPath + field.delimiter}`),
    );

  const fieldIsAncestor =
    (operator === "ancestorOf" && fieldIsLeft) ||
    (operator === "descendentOf" && !fieldIsLeft);

  let filter: SQL;
  if (operator === "overlaps") {
    const combined = or(
      ...(prefixes.length > 0 ? [isOneOfPrefixes()] : []),
      sql`${fieldExpr} = ${constantPath}`,
      isDescendantOfConstant(),
    );
    if (!combined) {
      throw new Error("Unable to combine hierarchy overlap conditions");
    }
    filter = combined;
  } else if (fieldIsAncestor) {
    filter = prefixes.length === 0 ? FALSE_CONDITION : isOneOfPrefixes();
  } else {
    filter = isDescendantOfConstant();
  }

  return wrapRelationChain(
    field.resolved.relations,
    filter,
    field.reference,
    options,
  );
};

export const buildHierarchyFilter = (
  operator: HierarchyOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`'${operator}' operator requires exactly two operands`);
  }
  const [leftOperand, rightOperand] = operands;
  if (!leftOperand || !rightOperand) {
    throw new Error(`'${operator}' operator is missing operands`);
  }
  const left = resolveHierarchy(leftOperand, mapper);
  const right = resolveHierarchy(rightOperand, mapper);
  if (left.kind === "field" && right.kind === "constant") {
    return buildFieldHierarchyFilter(operator, left, right, true, options);
  }
  if (left.kind === "constant" && right.kind === "field") {
    return buildFieldHierarchyFilter(operator, right, left, false, options);
  }
  if (left.kind === "field") {
    throw new Error(
      `'${operator}' between two field-backed hierarchies is not supported`,
    );
  }
  throw new Error(
    `'${operator}' between two constant hierarchies should be folded by the planner`,
  );
};
