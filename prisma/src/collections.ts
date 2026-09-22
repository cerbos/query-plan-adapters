// The collection macros (`exists`, `all`, `except`, and the refused `exists_one`/`filter`), in
// both polarities, plus `map`, `hasIntersection` and a bare `lambda`.
//
// A macro over a relation lowers to Prisma's `some`/`every`/`none`. Those collapse SQL's UNKNOWN
// to false at the EXISTS boundary, so every macro whose lambda body touches a nullable element
// column carries an explicit guard to keep CEL's error semantics (a NULL element column is a
// missing attribute, which Cerbos treats as deny).

import type { PlanExpressionOperand } from "@cerbos/core";

import {
  buildMembershipFilter,
  buildNullWitnessFilter,
} from "./fields";
import type { PrismaFilter } from "./index";
import {
  currentScope,
  enterLambdaScope,
  getLeafField,
  resolveFieldReference,
} from "./mapping";
import type {
  LambdaScope,
  RelationConfig,
  TranslationContext,
} from "./mapping";
import {
  assertDefined,
  isNamedOperand,
  isOperatorOperand,
  isValueOperand,
  normalizeBinaryOperands,
} from "./plan";
import type { ValueOperand } from "./plan";
import {
  buildLeadingHopsExistFilter,
  relationFilter,
  relationOperator,
  wrapInRelations,
} from "./relations";
import { substituteLambdaVariable } from "./rewrite";
import {
  buildNegatedFilter,
  buildPrismaFilterFromCerbosExpression,
} from "./translate";

function newLambdaScope(
  variableName: string,
  relations: RelationConfig[]
): LambdaScope {
  return {
    variableName,
    relationModel: relations[relations.length - 1]?.model,
    nullableFields: new Set(),
    unknownFilters: [],
  };
}

/**
 * Fold a collection macro whose collection operand is a literal value list.
 *
 * The planner emits this shape when a known-value collection (typically a
 * folded principal attribute) has more than 10 elements — at 10 or fewer it
 * unrolls `exists`/`all` into an or/and chain itself (cerbos/cerbos#2570,
 * cerbos/cerbos#2817; `maxItems = 10` in the planner's struct matcher). Apply
 * the same fold here so the translated filter does not depend on which side of
 * that threshold the collection lands: substitute each element into the lambda
 * body and combine the per-element filters with OR (`exists`) or AND (`all`).
 *
 * An empty list yields `{OR: []}` / `{AND: []}`, which Prisma evaluates to
 * match-nothing / match-everything — exactly CEL's `exists`/`all` semantics
 * over an empty collection.
 */
function foldKnownValueCollection(
  operator: string,
  collection: ValueOperand,
  lambda: PlanExpressionOperand,
  context: TranslationContext,
  negated: boolean
): PrismaFilter {
  if (operator !== "exists" && operator !== "all") {
    throw new Error(
      `${operator} over a literal collection value is not supported. ` +
        "Only exists() and all() can be folded into a flat filter."
    );
  }

  const elements = collection.value;
  if (!Array.isArray(elements)) {
    throw new Error(
      `${operator} over a literal collection requires a list value`
    );
  }

  if (!isOperatorOperand(lambda) || lambda.operator !== "lambda") {
    throw new Error(
      `Second operand of ${operator} must be a lambda expression`
    );
  }
  if (lambda.operands.length !== 2) {
    throw new Error(
      `${operator} over a literal collection supports single-variable lambdas only`
    );
  }

  const body = assertDefined(
    lambda.operands[0],
    "Lambda expression must provide a condition"
  );
  const variable = assertDefined(
    lambda.operands[1],
    "Lambda variable must have a name"
  );
  if (!isNamedOperand(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  const filters = elements.map((element) => {
    const substituted = substituteLambdaVariable(body, variable.name, element);
    return negated
      ? buildNegatedFilter(substituted, context)
      : buildPrismaFilterFromCerbosExpression(substituted, context);
  });

  const combinesWithOr = operator === "exists" ? !negated : negated;
  return combinesWithOr ? { OR: filters } : { AND: filters };
}

type CollectionLambdaParts = {
  head: RelationConfig;
  restRelations: RelationConfig[];
  /** Element predicate, already wrapped through any relations beyond the first. */
  filterValue: PrismaFilter;
  /** Nullable element columns referenced by the lambda body (for 3VL guards). */
  nullableFields: Set<string>;
  /** Element-level predicates under which a nested collection expression is UNKNOWN. */
  unknownFilters: PrismaFilter[];
};

function buildCollectionLambdaParts(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
): CollectionLambdaParts {
  if (operands.length !== 2) {
    throw new Error(`${operator} requires exactly two operands`);
  }

  const collection = assertDefined(
    operands[0],
    `${operator} requires a collection operand`
  );
  const lambda = assertDefined(
    operands[1],
    `${operator} requires a lambda operand`
  );

  if (!isNamedOperand(collection)) {
    throw new Error(
      `First operand of ${operator} must be a collection reference`
    );
  }

  if (!isOperatorOperand(lambda)) {
    throw new Error(
      `Second operand of ${operator} must be a lambda expression`
    );
  }

  const variable = assertDefined(
    lambda.operands[1],
    "Lambda variable must have a name"
  );
  if (!isNamedOperand(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  const { relations } = resolveFieldReference(collection.name, context);
  if (!relations || relations.length === 0) {
    throw new Error(`${operator} operator requires a relation mapping`);
  }
  const head = relations[0]!;
  const restRelations = relations.slice(1);

  const lambdaConditionOperand = assertDefined(
    lambda.operands[0],
    "Lambda expression must provide a condition"
  );

  const scope = newLambdaScope(variable.name, relations);
  const lambdaCondition = buildPrismaFilterFromCerbosExpression(
    lambdaConditionOperand,
    enterLambdaScope(context, collection.name, scope)
  );

  return {
    head,
    restRelations,
    // Chained collection reference (e.g. R.attr.a.b): the lambda's elements live at the END
    // of the chain, so the element predicate must join through every intermediate hop.
    filterValue: wrapInRelations(restRelations, lambdaCondition),
    nullableFields: scope.nullableFields,
    unknownFilters: scope.unknownFilters,
  };
}

function buildUnknownElementFilter(
  parts: CollectionLambdaParts
): PrismaFilter | undefined {
  const filters = [...parts.unknownFilters];
  if (parts.nullableFields.size > 0) {
    filters.push(buildNullWitnessFilter(parts.nullableFields));
  }
  if (filters.length === 0) {
    return undefined;
  }
  return filters.length === 1 ? filters[0] : { OR: filters };
}

/** Wraps an element-level filter through the full relation chain (head + rest). */
function wrapCollectionElementFilter(
  parts: CollectionLambdaParts,
  elementFilter: PrismaFilter
): PrismaFilter {
  return relationFilter(
    parts.head,
    "some",
    wrapInRelations(parts.restRelations, elementFilter)
  );
}

/** `relation: { none: <a nullable element column is NULL> }`, ANDed onto `base` when needed. */
function excludeNullElements(
  base: PrismaFilter,
  parts: CollectionLambdaParts
): PrismaFilter {
  if (parts.nullableFields.size === 0) {
    return base;
  }
  return {
    AND: [
      base,
      relationFilter(parts.head, "none", buildNullWitnessFilter(parts.nullableFields)),
    ],
  };
}

function positiveCollectionFilter(
  operator: string,
  parts: CollectionLambdaParts,
  context: TranslationContext
): PrismaFilter {
  const { head, filterValue } = parts;
  switch (operator) {
    case "exists": {
      // A NULL element keeps the per-element predicate UNKNOWN, so it can never create a
      // false positive here; a CEL error (deny) coincides with "no match" — no guard needed.
      // An enclosing macro still needs to know when this one is UNKNOWN, so it is recorded.
      const filter = relationFilter(head, "some", filterValue);
      const unknownElement = buildUnknownElementFilter(parts);
      if (unknownElement !== undefined) {
        currentScope(context)?.unknownFilters.push({
          AND: [
            { NOT: filter },
            wrapCollectionElementFilter(parts, unknownElement),
          ],
        });
      }
      return filter;
    }
    case "except":
      return relationFilter(head, "some", { NOT: filterValue });
    case "all":
      if (parts.restRelations.length > 0) {
        throw new Error(
          "all() over a multi-hop relation chain is not supported"
        );
      }
      // CEL all() errors when any element evaluation errors without a false witness; SQL
      // `every` would treat those elements as vacuously passing. Exclude rows holding any
      // element whose referenced nullable column is NULL. (`every` already rejects rows
      // with a definitive false witness, matching error absorption.)
      return excludeNullElements(relationFilter(head, "every", filterValue), parts);
    default:
      throw new Error(`Unexpected operator: ${operator}`);
  }
}

/**
 * The filter for a NEGATED collection operator, encoding CEL's error semantics (see
 * buildNegatedFilter).
 */
function negatedCollectionFilter(
  operator: string,
  parts: CollectionLambdaParts
): PrismaFilter {
  const { head, filterValue } = parts;
  // An absent to-one parent must stay denied under negation rather than satisfying it
  // vacuously (#309).
  const leadingHopsExist = buildLeadingHopsExistFilter(
    head,
    parts.restRelations
  );
  const requireLeadingHops = (filter: PrismaFilter): PrismaFilter =>
    leadingHopsExist === undefined
      ? filter
      : { AND: [leadingHopsExist, filter] };

  switch (operator) {
    case "exists": {
      const base = requireLeadingHops({
        NOT: relationFilter(head, "some", filterValue),
      });
      const unknownElement = buildUnknownElementFilter(parts);
      if (unknownElement === undefined) {
        return base;
      }
      // !exists is TRUE only when every element is definitively false: no P-match AND no
      // element whose evaluation is UNKNOWN (NULL column = missing attribute = CEL error).
      return {
        AND: [base, { NOT: wrapCollectionElementFilter(parts, unknownElement) }],
      };
    }
    case "all":
      // !all is TRUE only with a definitive false witness, which also absorbs error
      // elements — exactly `some(NOT P)` in SQL (NULL columns keep NOT P UNKNOWN).
      return relationFilter(head, "some", { NOT: filterValue });
    case "except":
      // !except(c,P) = every element definitively matches P.
      return excludeNullElements(
        requireLeadingHops(relationFilter(head, "none", { NOT: filterValue })),
        parts
      );
    default:
      throw new Error(`Unexpected operator: ${operator}`);
  }
}

function translateCollectionMacro(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext,
  negated: boolean
): PrismaFilter {
  // A literal value list arrives when the planner could not unroll a macro over a known
  // collection (more than 10 elements). Fold it before attempting relation resolution because
  // there is no relation mapping for a literal.
  const [collection, lambda] = operands;
  if (
    operands.length === 2 &&
    collection !== undefined &&
    lambda !== undefined &&
    isValueOperand(collection)
  ) {
    return foldKnownValueCollection(operator, collection, lambda, context, negated);
  }

  if (operator === "exists_one") {
    throw new Error(
      "exists_one requires counting matching elements, which Prisma where-filters cannot express"
    );
  }
  if (operator === "filter") {
    throw new Error(
      "The filter() collection operator returns a list, not a boolean. " +
        "It cannot be used as a standalone condition. " +
        "Use exists() or combine filter() with size() instead."
    );
  }

  const parts = buildCollectionLambdaParts(operator, operands, context);
  return negated
    ? negatedCollectionFilter(operator, parts)
    : positiveCollectionFilter(operator, parts, context);
}

/** A collection macro (exists, all, except, exists_one, filter) in positive position. */
export function handleCollectionOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  return translateCollectionMacro(operator, operands, context, false);
}

/** The same macro under `!`, which is not the plain `NOT` of the positive filter. */
export function buildNegatedCollectionFilter(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  return translateCollectionMacro(operator, operands, context, true);
}

/**
 * Helper function to handle "map" operator
 */
export function handleMapOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error("map requires exactly two operands");
  }

  const collection = assertDefined(
    operands[0],
    "map requires a collection operand"
  );
  const lambda = assertDefined(operands[1], "map requires a lambda operand");

  if (!isNamedOperand(collection)) {
    throw new Error("First operand of map must be a collection reference");
  }

  if (!isOperatorOperand(lambda) || lambda.operator !== "lambda") {
    throw new Error("Second operand of map must be a lambda expression");
  }

  const projection = assertDefined(
    lambda.operands[0],
    "Map lambda expression must provide a projection"
  );
  const variable = assertDefined(
    lambda.operands[1],
    "Map lambda expression must provide a variable"
  );
  if (!isNamedOperand(projection) || !isNamedOperand(variable)) {
    throw new Error("Invalid map lambda expression structure");
  }

  const { relations } = resolveFieldReference(collection.name, context);
  if (!relations || relations.length === 0) {
    throw new Error("map operator requires a relation mapping");
  }

  const scopedContext = enterLambdaScope(
    context,
    collection.name,
    newLambdaScope(variable.name, relations)
  );
  const resolved = resolveFieldReference(projection.name, scopedContext);
  const fieldName = getLeafField(resolved.path);
  const lastRelation = relations[relations.length - 1]!;

  return wrapInRelations(relations, {
    [relationOperator(lastRelation)]: {
      select: { [fieldName]: true },
    },
  });
}

/**
 * Helper function to handle "hasIntersection" operator
 */
export function handleHasIntersectionOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error("hasIntersection requires exactly two operands");
  }

  // Intersection is symmetric, and the planner preserves policy source order — e.g.
  // `hasIntersection(["a"], R.attr.tags)` puts the constant list FIRST. Normalize to
  // field/map-first (see #256).
  ({ operands } = normalizeBinaryOperands("hasIntersection", operands));

  const leftOperand = assertDefined(
    operands[0],
    "hasIntersection requires a left operand"
  );
  const rightOperand = assertDefined(
    operands[1],
    "hasIntersection requires a right operand"
  );

  if (isOperatorOperand(leftOperand) && leftOperand.operator === "map") {
    return handleMapIntersection(leftOperand.operands, rightOperand, context);
  }

  if (!isNamedOperand(leftOperand)) {
    throw new Error(
      "First operand of hasIntersection must be a field reference or map expression"
    );
  }

  if (!isValueOperand(rightOperand)) {
    throw new Error("Second operand of hasIntersection must be a value");
  }

  const { path, relations } = resolveFieldReference(leftOperand.name, context);

  if (!Array.isArray(rightOperand.value)) {
    throw new Error("hasIntersection requires an array value");
  }

  if (relations && relations.length > 0) {
    return buildMembershipFilter(context, { path, relations }, rightOperand.value);
  }

  return { [getLeafField(path)]: { some: rightOperand.value } };
}

/** `hasIntersection(collection.map(x, x.field), [literals])`. */
function handleMapIntersection(
  mapOperands: PlanExpressionOperand[],
  rightOperand: PlanExpressionOperand,
  context: TranslationContext
): PrismaFilter {
  if (!isValueOperand(rightOperand)) {
    throw new Error("Second operand of hasIntersection must be a value");
  }

  const collection = assertDefined(
    mapOperands[0],
    "Map expression must include a collection reference"
  );
  const lambda = assertDefined(
    mapOperands[1],
    "Map expression must include a lambda expression"
  );

  if (!isNamedOperand(collection)) {
    throw new Error("First operand of map must be a collection reference");
  }

  if (!isOperatorOperand(lambda)) {
    throw new Error("Lambda expression must have operands");
  }

  const variable = assertDefined(
    lambda.operands[1],
    "Lambda variable must have a name"
  );
  if (!isNamedOperand(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  const { relations } = resolveFieldReference(collection.name, context);
  if (!relations || relations.length === 0) {
    throw new Error("Map operation requires relations");
  }

  const projection = assertDefined(
    lambda.operands[0],
    "Invalid map lambda expression structure"
  );
  if (!isNamedOperand(projection)) {
    throw new Error("Invalid map lambda expression structure");
  }

  // Resolve the projection through the scoped mapper, collecting nullable element columns.
  const scope = newLambdaScope(variable.name, relations);
  const resolved = resolveFieldReference(
    projection.name,
    enterLambdaScope(context, collection.name, scope)
  );
  const fieldName = getLeafField(resolved.path);

  if (!Array.isArray(rightOperand.value))
    throw new Error("hasIntersection requires a literal list");
  const base = wrapInRelations(
    relations,
    buildMembershipFilter(
      context,
      { ...resolved, path: [fieldName], relations: [] },
      rightOperand.value
    )
  );
  if (scope.nullableFields.size === 0) {
    return base;
  }
  // CEL map() errors if ANY element is missing the projected attribute — even alongside a
  // matching element — and Cerbos treats that error as deny. Exclude rows holding an
  // element whose projected (nullable) column is NULL.
  return {
    AND: [
      base,
      { NOT: wrapInRelations(relations, buildNullWitnessFilter(scope.nullableFields)) },
    ],
  };
}

/**
 * Helper function to handle "lambda" operator
 */
export function handleLambdaOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  const condition = assertDefined(
    operands[0],
    "Lambda requires a condition operand"
  );
  const variable = assertDefined(
    operands[1],
    "Lambda requires a variable operand"
  );

  if (!isNamedOperand(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  return buildPrismaFilterFromCerbosExpression(condition, {
    ...context,
    mapper: (key: string) => ({ field: key.replace(`${variable.name}.`, "") }),
  });
}
