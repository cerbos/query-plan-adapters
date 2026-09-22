import type { PlanExpressionOperand } from "@cerbos/core";
import { and, not, or, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";

import {
  buildCollectionOperatorFilter,
  isCollectionOperator,
} from "./collections";
import { buildComparisonFilter } from "./comparison";
import { buildHierarchyFilter } from "./hierarchy";
import { buildHasIntersectionFilter } from "./intersection";
import {
  buildColumnExpression,
  columnForOperand,
  getMappingEntry,
  isMappingConfig,
  isRelationValue,
  isScalarCollection,
  mappingNullRepresentation,
  resolveFieldReference,
  resolveRelationDefaultField,
} from "./mapper";
import {
  isExpressionOperand,
  isNameOperand,
  isValueOperand,
} from "./operands";
import {
  applyComparison,
  assertNullOperandTranslatable,
  buildStringMatchCondition,
  constantCondition,
  operandExpression,
  withPolarity,
} from "./predicates";
import type { StringMatchOperator } from "./predicates";
import { wrapCombinedRelations, wrapRelationChain } from "./relations";
import type { BuildFilterOptions, Mapper } from "./types";
import { resolveScalarOperand } from "./values";

/**
 * Operands in CONDITION position: the plan's boolean structure, and the dispatch from each
 * operator to the module that translates it. Adding an operator means adding its `case` here.
 */

/**
 * Field-or-constant string matching (contains/startsWith/endsWith) with the receiver as
 * the haystack and the needle as the pattern, in wire order — the planner preserves
 * source order, so `"const".contains(R.attr.col)` arrives as contains(value, variable)
 * and must NOT be operand-order normalized (a swap silently inverts haystack and needle).
 */
const buildStringMatchFilter = (
  operator: StringMatchOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new Error(`'${operator}' operator requires exactly two operands`);
  }
  const [receiverOperand, needleOperand] = operands;
  if (!receiverOperand || !needleOperand) {
    throw new Error(
      `'${operator}' operator requires receiver and needle operands`,
    );
  }

  // Column receiver with a constant needle: transform/function mappings own their own
  // match semantics, so keep routing those through applyComparison.
  if (isNameOperand(receiverOperand) && isValueOperand(needleOperand)) {
    const resolved = resolveFieldReference(receiverOperand.name, mapper);
    const mapping = resolved.mapping;
    if (
      typeof mapping === "function" ||
      isRelationValue(mapping) ||
      (isMappingConfig(mapping) && mapping.transform !== undefined)
    ) {
      return wrapRelationChain(
        resolved.relations,
        applyComparison(mapping, operator, needleOperand.value, options),
        receiverOperand.name,
        options,
      );
    }
  }

  if (
    isValueOperand(needleOperand) &&
    typeof needleOperand.value !== "string"
  ) {
    throw new Error(`The '${operator}' operator requires a string value`);
  }
  // A string operator over a non-string column is a CEL no-overload error: UNKNOWN.
  for (const operand of [receiverOperand, needleOperand]) {
    const column = columnForOperand(operand, mapper);
    if (column && column.dataType !== "string") return sql`null`;
  }
  const receiver = resolveScalarOperand(receiverOperand, mapper, options);
  const needle = resolveScalarOperand(needleOperand, mapper, options);
  const filter = buildStringMatchCondition(
    operator,
    operandExpression(receiver.expr, receiverOperand),
    operandExpression(needle.expr, needleOperand),
    [receiverOperand, needleOperand].map((operand) =>
      columnForOperand(operand, mapper),
    ),
  );
  const reference = isNameOperand(receiverOperand)
    ? receiverOperand.name
    : isNameOperand(needleOperand)
      ? needleOperand.name
      : `'${operator}' operand`;
  return wrapCombinedRelations(
    filter,
    receiver.relations,
    needle.relations,
    reference,
    options,
  );
};

/** `R.attr.x in R.attr.list`: a field that is a member of a scalar collection mapping. */
const buildVariableMembershipFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new Error("'in' operator requires exactly two operands");
  }
  const [memberOperand, collectionOperand] = operands;
  if (
    !memberOperand ||
    !collectionOperand ||
    !isNameOperand(memberOperand) ||
    !isNameOperand(collectionOperand)
  ) {
    throw new Error(
      "Variable membership requires scalar and collection field references",
    );
  }

  const member = resolveFieldReference(memberOperand.name, mapper);
  if (
    member.relations.some((relation) => !options.skipRelations?.has(relation))
  ) {
    throw new Error(
      "Variable membership requires its first operand to resolve to a scalar field",
    );
  }
  if (!isScalarCollection(getMappingEntry(collectionOperand.name, mapper))) {
    throw new Error("Variable membership requires a scalar collection mapping");
  }
  const collection = resolveRelationDefaultField(
    resolveFieldReference(collectionOperand.name, mapper),
    collectionOperand.name,
  );
  if (collection.relations.length === 0) {
    throw new Error(
      "Variable membership requires its second operand to resolve to a collection",
    );
  }

  const memberExpr = buildColumnExpression(member.mapping, memberOperand.name);
  const elementExpr = buildColumnExpression(
    collection.mapping,
    collectionOperand.name,
  );
  const nullMatch = and(
    sql`${memberExpr} is null`,
    sql`${elementExpr} is null`,
  );
  const match = or(sql`${memberExpr} = ${elementExpr}`, nullMatch);
  if (!match) {
    throw new Error("Unable to combine variable membership conditions");
  }
  const membership = wrapRelationChain(
    collection.relations,
    match,
    collectionOperand.name,
    options,
  );
  return mappingNullRepresentation(member.mapping) === "explicit"
    ? membership
    : sql`(case when ${memberExpr} is null then null else ${membership} end)`;
};

/**
 * `in` with one field and one constant. Whichever side is the name operand is the column —
 * the planner emits both `R.attr.x in [..]` (name, values) and `"v" in R.attr.list`
 * (value, name), and both mean membership against the column.
 */
const buildMembershipFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.every(isNameOperand)) {
    return buildVariableMembershipFilter(operands, mapper, options);
  }
  const fieldOperand = operands.find(isNameOperand);
  if (!fieldOperand) {
    throw new Error("Comparison operator missing field operand");
  }
  const valueOperand = operands.find(isValueOperand);
  if (!valueOperand) {
    throw new Error("Comparison operator missing value operand");
  }
  if (isValueOperand(operands[0]!) && Array.isArray(operands[0]!.value)) {
    throw new Error(
      "List-element membership is not supported: a scalar relation mapping cannot compare a list value with one element",
    );
  }
  const unresolved = resolveFieldReference(fieldOperand.name, mapper);
  const resolved = isScalarCollection(
    getMappingEntry(fieldOperand.name, mapper),
  )
    ? resolveRelationDefaultField(unresolved, fieldOperand.name)
    : unresolved;
  return wrapRelationChain(
    resolved.relations,
    applyComparison(resolved.mapping, "in", valueOperand.value, options),
    fieldOperand.name,
    options,
  );
};

/** A ternary in boolean position: each branch guarded by the (un)satisfied condition. */
const buildTernaryFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL => {
  // An UNKNOWN condition leaves BOTH arms UNKNOWN — excluded under either polarity —
  // matching the CEL error → deny.
  if (operands.length !== 3) {
    throw new Error("'if' operator requires exactly three operands");
  }
  const [condOperand, thenOperand, elseOperand] = operands;
  if (!condOperand || !thenOperand || !elseOperand) {
    throw new Error("'if' operator is missing operands");
  }
  const cond = buildFilterFromExpression(condOperand, mapper, options);
  const thenFilter = buildFilterFromExpression(
    thenOperand,
    mapper,
    options,
    negated,
  );
  const elseFilter = buildFilterFromExpression(
    elseOperand,
    mapper,
    options,
    negated,
  );
  const combined = or(and(cond, thenFilter), and(not(cond), elseFilter));
  if (!combined) {
    throw new Error("'if' operator produced an empty filter");
  }
  return combined;
};

/**
 * Build a boolean filter from a plan operand, tracking negation polarity instead of
 * emitting a plain SQL NOT at each `not` node. Plain NOT is correct for leaf
 * comparisons (an UNKNOWN comparison stays UNKNOWN — excluded — under NOT, matching
 * the CEL error → deny), but NOT over a collection macro is not its complement in
 * CEL's error semantics, so negation is pushed inward (De Morgan through and/or,
 * polarity-specific collection translations) until it lands on a leaf.
 */
export const buildFilterFromExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated = false,
): SQL => {
  // Bare variable in boolean position (e.g. `R.attr.aBool` as an and/or/not operand).
  // Routed through wrapRelationChain, not wrapWithRelations: a bare boolean read through a
  // to-one hop must require that hop, or `!R.attr.parent.aBool` returns every parentless row
  // (cerbos/query-plan-adapters#375).
  if (isNameOperand(expression)) {
    const resolved = resolveFieldReference(expression.name, mapper);
    const filter = applyComparison(resolved.mapping, "eq", true, options);
    return withPolarity(
      wrapRelationChain(resolved.relations, filter, expression.name, options),
      negated,
    );
  }
  if (isValueOperand(expression)) {
    return constantCondition(Boolean(expression.value) !== negated);
  }
  if (!isExpressionOperand(expression)) {
    throw new Error("Invalid expression operand");
  }

  const { operator, operands } = expression;

  switch (operator) {
    case "and":
    case "or": {
      if (operands.length === 0) {
        throw new Error(`'${operator}' operator requires at least one operand`);
      }
      const filters = operands.map((operand) =>
        buildFilterFromExpression(operand, mapper, options, negated),
      );
      // De Morgan under negation: !(a AND b) = !a OR !b (and vice versa).
      const combineWithAnd = (operator === "and") !== negated;
      const combined = combineWithAnd ? and(...filters) : or(...filters);
      if (!combined) {
        throw new Error(`'${operator}' operator produced an empty filter`);
      }
      return combined;
    }
    case "not": {
      if (operands.length !== 1) {
        throw new Error("'not' operator requires exactly one operand");
      }
      const operand = operands[0];
      if (!operand) {
        throw new Error("'not' operator is missing operand");
      }
      return buildFilterFromExpression(operand, mapper, options, !negated);
    }
    case "if":
      return buildTernaryFilter(operands, mapper, options, negated);
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      if (operands.length !== 2) {
        throw new Error(`'${operator}' operator requires exactly two operands`);
      }
      const [left, right] = operands;
      if (!left || !right) {
        throw new Error("Comparison operator requires two operands");
      }
      return buildComparisonFilter(
        operator,
        left,
        right,
        mapper,
        options,
        negated,
      );
    }
    case "contains":
    case "startsWith":
    case "endsWith":
      return withPolarity(
        buildStringMatchFilter(operator, operands, mapper, options),
        negated,
      );
    case "in":
      return withPolarity(
        buildMembershipFilter(operands, mapper, options),
        negated,
      );
    case "matches":
      throw new Error(
        "'matches' is not supported because SQL regex dialects do not guarantee CEL/RE2 semantics",
      );
    case "hasIntersection":
      // Only the call-level option is passed on: an enclosing lambda's `skipRelations` is not.
      return withPolarity(
        buildHasIntersectionFilter(operands, mapper, {
          nullRepresentation: options.nullRepresentation,
        }),
        negated,
      );
    case "ancestorOf":
    case "descendentOf":
    case "overlaps":
      return withPolarity(
        buildHierarchyFilter(operator, operands, mapper, options),
        negated,
      );
    default:
      if (isCollectionOperator(operator)) {
        return buildCollectionOperatorFilter(
          operator,
          operands,
          mapper,
          negated,
          options,
        );
      }
      throw new Error(`Unsupported operator: ${operator}`);
  }
};

/**
 * Reject a null literal inside a list or struct constructor anywhere in the plan when the
 * call-level representation is `"omitted"`: such a constructor has no mapper entry to declare
 * its convention, so the call-level option is the only one that applies.
 */
export const rejectNullConstructors = (
  operand: PlanExpressionOperand,
  options: BuildFilterOptions,
  inConstructor = false,
): void => {
  if (isValueOperand(operand)) {
    if (
      inConstructor &&
      (operand.value === null ||
        (Array.isArray(operand.value) && operand.value.includes(null)))
    ) {
      assertNullOperandTranslatable(
        "a null literal in a collection or struct constructor",
        options,
      );
    }
  } else if (isExpressionOperand(operand)) {
    const nested =
      inConstructor ||
      ["list", "struct", "set-field"].includes(operand.operator);
    operand.operands.forEach((child) =>
      rejectNullConstructors(child, options, nested),
    );
  }
};
