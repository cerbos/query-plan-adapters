import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { and, not, or, sql } from "drizzle-orm";
import type { AnyColumn, SQL } from "drizzle-orm";

import { UnsupportedQueryPlanError } from "./errors";
import {
  buildCollectionOperatorFilter,
  isCollectionOperator,
  substituteLambdaOperand,
} from "./collections";
import { buildComparisonFilter } from "./comparison";
import { buildHierarchyFilter } from "./hierarchy";
import { indexedMembership, resolveIndexedMembership } from "./indexed";
import { buildHasIntersectionFilter } from "./intersection";
import {
  buildColumnExpression,
  columnForOperand,
  getMappingEntry,
  isColumn,
  isMappingConfig,
  isRelationValue,
  isScalarCollection,
  mappingNullRepresentation,
  resolveFieldReference,
  resolveRelationDefaultField,
} from "./mapper";
import {
  extractLambdaComponents,
  isExpressionOperand,
  isNameOperand,
  isOperatorCall,
  isValueOperand,
} from "./operands";
import {
  applyComparison,
  assertNullOperandTranslatable,
  buildStringMatchCondition,
  characterLength,
  columnExpression,
  constantCondition,
  constantExpression,
  FALSE_CONDITION,
  operandExpression,
  UNKNOWN_CONDITION,
  withPolarity,
} from "./predicates";
import type { StringMatchOperator } from "./predicates";
import { compileRegex } from "./regex";
import { wrapCombinedRelations, wrapRelationChain } from "./relations";
import type { BaseMapperEntry, BuildFilterOptions, Mapper } from "./types";
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
    throw new UnsupportedQueryPlanError(`'${operator}' operator requires exactly two operands`);
  }
  const [receiverOperand, needleOperand] = operands;
  if (!receiverOperand || !needleOperand) {
    throw new UnsupportedQueryPlanError(
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
    throw new UnsupportedQueryPlanError(`The '${operator}' operator requires a string value`);
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
    characterLength([
      columnForOperand(receiverOperand, mapper),
      columnForOperand(needleOperand, mapper),
    ]),
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
    throw new UnsupportedQueryPlanError("'in' operator requires exactly two operands");
  }
  const [memberOperand, collectionOperand] = operands;
  if (
    !memberOperand ||
    !collectionOperand ||
    !isNameOperand(memberOperand) ||
    !isNameOperand(collectionOperand)
  ) {
    throw new UnsupportedQueryPlanError(
      "Variable membership requires scalar and collection field references",
    );
  }

  const member = resolveFieldReference(memberOperand.name, mapper);
  if (
    member.relations.some((relation) => !options.skipRelations?.has(relation))
  ) {
    throw new UnsupportedQueryPlanError(
      "Variable membership requires its first operand to resolve to a scalar field",
    );
  }
  if (!isScalarCollection(getMappingEntry(collectionOperand.name, mapper))) {
    throw new UnsupportedQueryPlanError("Variable membership requires a scalar collection mapping");
  }
  const collection = resolveRelationDefaultField(
    resolveFieldReference(collectionOperand.name, mapper),
    collectionOperand.name,
  );
  if (collection.relations.length === 0) {
    throw new UnsupportedQueryPlanError(
      "Variable membership requires its second operand to resolve to a collection",
    );
  }

  // An enclosing lambda's element read against a collection stored in the SAME table renders
  // as the same `"table"."column"` the membership subquery ranges over, so SQL would test each
  // row against itself rather than against the enclosing element (#509).
  const memberColumn = isMappingConfig(member.mapping)
    ? member.mapping.column
    : isColumn(member.mapping)
      ? member.mapping
      : undefined;
  if (
    memberColumn !== undefined &&
    collection.relations.some(
      (relation) =>
        !options.skipRelations?.has(relation) &&
        relation.table === memberColumn.table,
    )
  ) {
    throw new UnsupportedQueryPlanError(
      `Cannot test '${memberOperand.name}' for membership in '${collectionOperand.name}': ` +
        "both are stored in the same table, so the membership subquery would compare each " +
        "row with itself instead of with the enclosing element",
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
    throw new UnsupportedQueryPlanError("Unable to combine variable membership conditions");
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
const SCALAR_ELEMENT_TYPES = new Set(["string", "number", "boolean"]);

/** A list or map constant, as opposed to a scalar or null. */
const isCompositeValue = (value: Value): value is Value[] | { [key: string]: Value } =>
  value !== null && typeof value === "object";

/** The column a mapping entry reads, if it reads one. */
const columnOfMapping = (mapping: BaseMapperEntry): AnyColumn | undefined =>
  isMappingConfig(mapping) ? mapping.column : isColumn(mapping) ? mapping : undefined;

/**
 * `x in [e1, e2, …]` where the list is built at evaluation — a constructor whose elements are not
 * all constants. CEL builds the whole list first, so any element that is a missing attribute (a
 * NULL column on the omitted convention, or an expression NULL on it) makes the membership an
 * error, even where another element equals `x`; otherwise it is CEL's equality against each.
 */
const buildListConstructorMembership = (
  element: PlanExpressionOperand,
  members: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (members.length === 0) return FALSE_CONDITION;
  const equalities = members.map((member) =>
    buildComparisonFilter("eq", element, member, mapper, options, false),
  );
  return guardConstructorElements(members, or(...equalities)!, mapper, options);
};

/**
 * `filter` made UNKNOWN wherever a list constructor's element is a missing attribute (a NULL
 * column on the omitted convention, or an expression NULL on it): CEL builds the whole list
 * before reading it, so such an element is an error whatever the rest of the expression says.
 */
const guardConstructorElements = (
  members: PlanExpressionOperand[],
  filter: SQL,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  const errors = members.flatMap((member): SQL[] => {
    if (isValueOperand(member)) return [];
    if (
      isNameOperand(member) &&
      mappingNullRepresentation(resolveFieldReference(member.name, mapper).mapping) === "explicit"
    ) {
      return [];
    }
    return [sql`${resolveScalarOperand(member, mapper, options).expr} is null`];
  });
  return errors.length === 0
    ? filter
    : sql`(case when ${sql.join(errors, sql` or `)} then null else ${filter} end)`;
};

/**
 * `exists` or `all` over a list constructor (`[R.attr.a, R.attr.b].exists(s, ...)`), unrolled into
 * the or/and of the body over each element. CEL's `exists` is true if any element is, and an error
 * otherwise if any element errors, which is SQL's OR over UNKNOWN; `all` mirrors it with AND.
 */
const buildConstructedListMacro = (
  operator: "exists" | "all",
  members: PlanExpressionOperand[],
  lambdaOperand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL => {
  const { variable, expression: body } = extractLambdaComponents(
    lambdaOperand,
    `'${operator}' lambda operand`,
  );
  const combinesWithOr = (operator === "exists") !== negated;
  const filters = members.map((member) =>
    buildFilterFromExpression(
      substituteLambdaOperand(body, variable.name, member),
      mapper,
      options,
      negated,
    ),
  );
  const combined =
    (combinesWithOr ? or(...filters) : and(...filters)) ??
    constantCondition(!combinesWithOr);
  return guardConstructorElements(members, combined, mapper, options);
};

/**
 * `"x" in L.map(t, f(t))` or `"x" in L.filter(t, p(t))`: CEL builds the whole list before testing
 * membership, so an element the projection or the predicate errors on is an error even when
 * another element matches. The membership is the `exists` that finds a matching element, and it
 * holds only where the `all` that evaluates the projection or predicate on every element is not
 * UNKNOWN; elsewhere the result is UNKNOWN, under both polarities.
 */
const buildDerivedListMembership = (
  element: { value: Value },
  derived: { operator: string; operands: PlanExpressionOperand[] },
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL => {
  const [list, lambda] = derived.operands;
  if (derived.operands.length !== 2 || !list || !lambda) {
    throw new UnsupportedQueryPlanError(`'${derived.operator}' operator requires two operands`);
  }
  const { variable, expression } = extractLambdaComponents(
    lambda,
    `'${derived.operator}' lambda operand`,
  );
  const call = (operator: string, ...children: PlanExpressionOperand[]): PlanExpressionOperand => ({
    operator,
    operands: children,
  });
  const macro = (operator: "exists" | "all", body: PlanExpressionOperand) =>
    buildFilterFromExpression(call(operator, list, call("lambda", body, variable)), mapper, options);
  const evaluated = (condition: PlanExpressionOperand) =>
    call("or", condition, call("not", condition));
  const [match, guard] =
    derived.operator === "map"
      ? [
          call("eq", expression, element),
          evaluated(call("eq", expression, element)),
        ]
      : [call("and", call("eq", variable, element), expression), evaluated(expression)];
  return withPolarity(
    sql`(case when ${macro("all", guard)} then ${macro("exists", match)} end)`,
    negated,
  );
};

/**
 * `{"k1": v1, ...}[x] == c` (or `!=`): a constant map indexed by an attribute. CEL reads the entry
 * for `x`, and an absent key is an error, so the comparison is `x in <keys whose value compares
 * true>` among the keys, and UNKNOWN for any other `x`.
 */
const buildConstantMapIndexComparison = (
  operator: "eq" | "ne",
  left: PlanExpressionOperand,
  right: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL | undefined => {
  const [index, constant] = isOperatorCall(left, "index") ? [left, right] : [right, left];
  if (!isOperatorCall(index, "index") || index.operands.length !== 2) return undefined;
  if (!isValueOperand(constant)) return undefined;
  const [map, key] = index.operands as [PlanExpressionOperand, PlanExpressionOperand];
  if (!isValueOperand(map) || !isCompositeValue(map.value) || Array.isArray(map.value)) {
    return undefined;
  }
  if (!isNameOperand(key)) return undefined;
  const entries = Object.entries(map.value);
  const scalar = (candidate: Value) =>
    typeof candidate === "string" || typeof candidate === "number" ||
    typeof candidate === "boolean";
  if (!scalar(constant.value) || entries.some(([, entry]) => typeof entry !== typeof constant.value)) {
    throw new UnsupportedQueryPlanError(
      "A constant map indexed by an attribute is supported only when every entry and the " +
        "compared constant are strings, numbers or booleans of one type",
    );
  }
  const matching = entries
    .filter(([, entry]) => (entry === constant.value) === (operator === "eq"))
    .map(([mapKey]) => mapKey);
  const keys = entries.map(([mapKey]) => mapKey);
  const inKeys = buildMembershipFilter([key, { value: keys }], mapper, options);
  const inMatching = buildMembershipFilter([key, { value: matching }], mapper, options);
  return withPolarity(sql`(case when ${inKeys} then ${inMatching} end)`, negated);
};

const buildMembershipFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  const [elementOperand, collectionOperand] = operands;
  if (
    operands.length === 2 && elementOperand !== undefined &&
    collectionOperand !== undefined && isOperatorCall(collectionOperand, "list")
  ) {
    return buildListConstructorMembership(
      elementOperand,
      collectionOperand.operands,
      mapper,
      options,
    );
  }
  if (collectionOperand !== undefined && isNameOperand(collectionOperand)) {
    const haystack = getMappingEntry(collectionOperand.name, mapper);
    if (
      haystack !== undefined && isMappingConfig(haystack) &&
      haystack.relation?.type === "one"
    ) {
      // CEL reads a to-one relation as a map, and `in` over a map tests its KEYS: the attribute
      // names present on the related row, which a comparison against its columns cannot test.
      throw new UnsupportedQueryPlanError(
        `Cannot translate 'in' over '${collectionOperand.name}': it is a to-one relation, which ` +
          "CEL reads as a map whose keys (the attribute names present on the related row) 'in' " +
          "tests, and SQL has no row of attribute names to search",
      );
    }
  }
  if (operands.every(isNameOperand)) {
    return buildVariableMembershipFilter(operands, mapper, options);
  }
  const fieldOperand = operands.find(isNameOperand);
  if (!fieldOperand) {
    throw new UnsupportedQueryPlanError("Comparison operator missing field operand");
  }
  const valueOperand = operands.find(isValueOperand);
  if (!valueOperand) {
    throw new UnsupportedQueryPlanError("Comparison operator missing value operand");
  }
  const [element] = operands;
  if (isValueOperand(element!) && isCompositeValue(element.value)) {
    // A list or map element equals no string, number or boolean, so over a collection of those it
    // is never a member. Anything else — a JSON column that may hold lists — has no such answer.
    const unresolved = resolveFieldReference(fieldOperand.name, mapper);
    const elementColumn = isScalarCollection(getMappingEntry(fieldOperand.name, mapper))
      ? columnOfMapping(resolveRelationDefaultField(unresolved, fieldOperand.name).mapping)
      : undefined;
    if (
      elementColumn === undefined ||
      !SCALAR_ELEMENT_TYPES.has(elementColumn.dataType) ||
      unresolved.relations.length > 1
    ) {
      throw new UnsupportedQueryPlanError(
        "List-element membership is supported only over a collection of strings, numbers or " +
          "booleans, which a list or map element can never equal",
      );
    }
    return FALSE_CONDITION;
  }
  // `x in {"a": 1}` tests the map's KEYS, as CEL's `in` over a map does.
  const collection =
    operands[1] === valueOperand && isCompositeValue(valueOperand.value) &&
    !Array.isArray(valueOperand.value)
      ? Object.keys(valueOperand.value)
      : valueOperand.value;
  // `"2" in R.attr.list` over a list held in one declared JSON or array column: the column is the
  // collection, not an element, so it is searched element by element rather than compared whole.
  const indexed = resolveIndexedMembership(fieldOperand.name, mapper, options);
  if (indexed) {
    return indexedMembership({ ...indexed, values: [valueOperand.value] });
  }

  const unresolved = resolveFieldReference(fieldOperand.name, mapper);
  const resolved = isScalarCollection(
    getMappingEntry(fieldOperand.name, mapper),
  )
    ? resolveRelationDefaultField(unresolved, fieldOperand.name)
    : unresolved;
  return wrapRelationChain(
    resolved.relations,
    applyComparison(resolved.mapping, "in", collection, options),
    fieldOperand.name,
    options,
  );
};

/**
 * `field.matches("pattern")`, lowered through `compileRegex` into the adapter's exact string
 * predicates — never into the store's own regex dialect, none of which is RE2.
 *
 * A NULL receiver is a missing attribute (or a null value, which has no `matches()`): CEL raises,
 * so the whole predicate is NULL for it, even where a pattern matches every string.
 */
const buildMatchesFilter = (
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  const [receiverOperand, patternOperand] = operands;
  if (
    operands.length !== 2 ||
    receiverOperand === undefined || !isNameOperand(receiverOperand) ||
    patternOperand === undefined || !isValueOperand(patternOperand) ||
    typeof patternOperand.value !== "string"
  ) {
    throw new UnsupportedQueryPlanError(
      "'matches' is supported only with a field receiver and a constant pattern",
    );
  }
  const resolved = resolveFieldReference(receiverOperand.name, mapper);
  const { mapping } = resolved;
  if (
    typeof mapping === "function" ||
    isRelationValue(mapping) ||
    (isMappingConfig(mapping) && mapping.transform !== undefined)
  ) {
    throw new UnsupportedQueryPlanError(
      "'matches' cannot be delegated to a transform or function mapping",
    );
  }
  const column = columnForOperand(receiverOperand, mapper);
  // A non-string receiver is a CEL no-overload error: UNKNOWN.
  if (column && column.dataType !== "string") return sql`null`;
  const plans = compileRegex(patternOperand.value);
  if (plans === "error") return UNKNOWN_CONDITION;

  const expr = buildColumnExpression(mapping, receiverOperand.name);
  const receiver = columnExpression(expr);
  const length = characterLength([column]);
  const match = (operator: StringMatchOperator, literal: string): SQL =>
    buildStringMatchCondition(operator, receiver, constantExpression(sql`${literal}`), length);
  const anyOf = (conditions: SQL[]): SQL =>
    conditions.length === 0 ? FALSE_CONDITION : or(...conditions)!;
  const atLeast = (characters: number): SQL | undefined =>
    characters > 0 ? sql`${length(expr)} >= ${characters}` : undefined;
  const noNewline = (): SQL => not(match("contains", "\n"));

  const conditions = plans.map((plan): SQL => {
    switch (plan.kind) {
      case "equals":
        return sql`${expr} in ${plan.literals.map((literal) => sql`${literal}`)}`;
      case "startsWith":
      case "endsWith":
      case "contains":
        return anyOf(plan.literals.map((literal) => match(plan.kind, literal)));
      case "allCharactersIn": {
        // Remove every allowed character; nothing may be left. REPLACE is case-sensitive and
        // literal on all three stores, as `contains` already relies on.
        const rest = plan.characters.reduce<SQL>(
          (current, character) => sql`replace(${current}, ${character}, '')`,
          expr,
        );
        return and(sql`length(${rest}) = 0`, atLeast(plan.min))!;
      }
      case "noNewline":
        return and(noNewline(), atLeast(plan.min))!;
      case "prefixSuffix":
        return and(
          anyOf(
            plan.pairs.map(([prefix, suffix]) =>
              and(
                match("startsWith", prefix),
                match("endsWith", suffix),
                atLeast([...prefix].length + plan.min + [...suffix].length),
              )!,
            ),
          ),
          noNewline(),
        )!;
    }
  });
  const filter = sql`(case when ${expr} is null then null else ${anyOf(conditions)} end)`;
  return wrapRelationChain(resolved.relations, filter, receiverOperand.name, options);
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
    throw new UnsupportedQueryPlanError("'if' operator requires exactly three operands");
  }
  const [condOperand, thenOperand, elseOperand] = operands;
  if (!condOperand || !thenOperand || !elseOperand) {
    throw new UnsupportedQueryPlanError("'if' operator is missing operands");
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
    throw new UnsupportedQueryPlanError("'if' operator produced an empty filter");
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
    throw new UnsupportedQueryPlanError("Invalid expression operand");
  }

  const { operator, operands } = expression;

  switch (operator) {
    case "and":
    case "or": {
      if (operands.length === 0) {
        throw new UnsupportedQueryPlanError(`'${operator}' operator requires at least one operand`);
      }
      const filters = operands.map((operand) =>
        buildFilterFromExpression(operand, mapper, options, negated),
      );
      // De Morgan under negation: !(a AND b) = !a OR !b (and vice versa).
      const combineWithAnd = (operator === "and") !== negated;
      const combined = combineWithAnd ? and(...filters) : or(...filters);
      if (!combined) {
        throw new UnsupportedQueryPlanError(`'${operator}' operator produced an empty filter`);
      }
      return combined;
    }
    case "not": {
      if (operands.length !== 1) {
        throw new UnsupportedQueryPlanError("'not' operator requires exactly one operand");
      }
      const operand = operands[0];
      if (!operand) {
        throw new UnsupportedQueryPlanError("'not' operator is missing operand");
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
        throw new UnsupportedQueryPlanError(`'${operator}' operator requires exactly two operands`);
      }
      const [left, right] = operands;
      if (!left || !right) {
        throw new UnsupportedQueryPlanError("Comparison operator requires two operands");
      }
      if (operator === "eq" || operator === "ne") {
        const mapIndex = buildConstantMapIndexComparison(
          operator,
          left,
          right,
          mapper,
          options,
          negated,
        );
        if (mapIndex) return mapIndex;
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
      if (
        operands.length === 2 &&
        isValueOperand(operands[0]!) &&
        (isOperatorCall(operands[1]!, "map") || isOperatorCall(operands[1]!, "filter"))
      ) {
        return buildDerivedListMembership(
          operands[0]!,
          operands[1] as { operator: string; operands: PlanExpressionOperand[] },
          mapper,
          options,
          negated,
        );
      }
      return withPolarity(
        buildMembershipFilter(operands, mapper, options),
        negated,
      );
    case "map":
      // map() returns a list, and a list where CEL needs a boolean is a no-overload error at
      // evaluation, denied under both polarities — which UNKNOWN spells exactly. The same
      // holds for filter() (see `collections.ts`) and the list-valued except().
      return UNKNOWN_CONDITION;
    case "except":
      if (operands.length === 2 && !isOperatorCall(operands[1]!, "lambda")) {
        return UNKNOWN_CONDITION;
      }
      return buildCollectionOperatorFilter(operator, operands, mapper, negated, options);
    case "matches":
      return withPolarity(buildMatchesFilter(operands, mapper, options), negated);
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
      if (
        (operator === "exists" || operator === "all") && operands.length === 2 &&
        isOperatorCall(operands[0]!, "list")
      ) {
        return buildConstructedListMacro(
          operator,
          (operands[0] as { operands: PlanExpressionOperand[] }).operands,
          operands[1]!,
          mapper,
          options,
          negated,
        );
      }
      if (isCollectionOperator(operator)) {
        return buildCollectionOperatorFilter(
          operator,
          operands,
          mapper,
          negated,
          options,
        );
      }
      throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
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
