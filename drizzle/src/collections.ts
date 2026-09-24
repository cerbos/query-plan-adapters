import type { PlanExpressionOperand, Value } from "@cerbos/core";
import { and, not, or, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";

import { UnsupportedQueryPlanError } from "./errors";
import { buildFilterFromExpression } from "./filter";
import { createCollectionScope } from "./mapper";
import type { CollectionScope } from "./mapper";
import {
  extractLambdaComponents,
  isExpressionOperand,
  isNameOperand,
  isOperatorCall,
  isValueOperand,
} from "./operands";
import type { ExpressionOperand } from "./operands";
import {
  FALSE_CONDITION,
  TRUE_CONDITION,
  UNKNOWN_CONDITION,
  withPolarity,
} from "./predicates";
import {
  chainCorrelation,
  relationCorrelation,
  relationSource,
  requireLeadingHops,
  resolveTableName,
  wrapWithRelations,
} from "./relations";
import type { BuildFilterOptions, Mapper, RelationMapping } from "./types";

/**
 * CEL's collection macros — `exists`, `all`, `exists_one`, `except`, `filter` — over a relation
 * (a correlated subquery) or over a literal list (folded here, element by element).
 */

type CollectionOperator = "exists" | "exists_one" | "filter" | "all" | "except";

const COLLECTION_OPERATORS = new Set<string>([
  "exists",
  "exists_one",
  "filter",
  "all",
  "except",
]);

export const isCollectionOperator = (
  operator: string,
): operator is CollectionOperator => COLLECTION_OPERATORS.has(operator);

// Operators whose second operand is a lambda that binds an iteration variable.
const LAMBDA_BINDING_OPERATORS = new Set([...COLLECTION_OPERATORS, "map"]);

/** A macro's lambda scope, plus the collection it iterates and the body to evaluate per element. */
interface MacroScope extends CollectionScope {
  collectionName: string;
  conditionOperand: PlanExpressionOperand;
}

const resolveMacroScope = (
  collectionOperand: PlanExpressionOperand,
  lambdaOperand: PlanExpressionOperand,
  context: string,
  mapper: Mapper,
  options: BuildFilterOptions,
): MacroScope => {
  if (!isNameOperand(collectionOperand)) {
    throw new UnsupportedQueryPlanError("Collection operand must be a field reference");
  }
  const { variable, expression } = extractLambdaComponents(
    lambdaOperand,
    context,
  );
  return {
    ...createCollectionScope(
      collectionOperand.name,
      variable.name,
      mapper,
      options.openTables,
    ),
    collectionName: collectionOperand.name,
    conditionOperand: expression,
  };
};

/**
 * The lambda body, translated once, as the per-row condition of the collection's subquery. The
 * iterated table joins `openTables`, so a macro over it again inside the body takes an alias.
 */
const buildRowCondition = (
  scope: MacroScope,
  options: BuildFilterOptions,
): SQL =>
  buildFilterFromExpression(scope.conditionOperand, scope.mapper, {
    ...options,
    skipRelations: scope.skipRelations,
    openTables: [
      ...(options.openTables ?? []),
      resolveTableName(scope.primaryRelation.table, scope.collectionName),
    ],
  });

/** The alias the subquery over the scope's primary relation takes, as `wrapWithRelations` reads it. */
const primaryAlias = (
  scope: MacroScope,
): ReadonlyMap<RelationMapping, string> | undefined =>
  scope.alias === undefined
    ? undefined
    : new Map([[scope.primaryRelation, scope.alias]]);

/**
 * `size(filter(coll, lambda))`: COUNT with the lambda condition as the predicate. An element
 * whose condition is UNKNOWN (NULL column) poisons the whole count to NULL — mirroring CEL,
 * where filter() surfaces the missing-attribute error instead of skipping the element — so the
 * enclosing comparison is UNKNOWN and the row is excluded under both polarities.
 */
export const buildFilteredCount = (
  filterOperand: ExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (filterOperand.operands.length !== 2) {
    throw new UnsupportedQueryPlanError("'filter' operator requires exactly two operands");
  }
  const [collectionOperand, lambdaOperand] = filterOperand.operands;
  if (!collectionOperand || !lambdaOperand) {
    throw new UnsupportedQueryPlanError("'filter' operator requires collection and lambda operands");
  }
  const scope = resolveMacroScope(
    collectionOperand,
    lambdaOperand,
    "'filter' lambda operand",
    mapper,
    options,
  );
  const rowCondition = buildRowCondition(scope, options);
  const chainWhere = chainCorrelation(
    scope.primaryRelation,
    scope.leadingRelations,
    scope.collectionName,
    options,
    scope.alias,
  );
  const source = relationSource(
    scope.primaryRelation,
    scope.collectionName,
    scope.alias,
  );
  return requireLeadingHops(
    scope.leadingRelations,
    sql`(select case when coalesce(sum(case when (${rowCondition}) is null then 1 else 0 end), 0) > 0 then null else coalesce(sum(case when ${rowCondition} then 1 else 0 end), 0) end from ${source} where ${chainWhere})`,
    scope.collectionName,
    options,
  );
};

/** Whether `lambda` binds an iteration variable called `variableName`. */
const rebindsVariable = (
  lambda: PlanExpressionOperand,
  variableName: string,
): boolean => {
  if (!isOperatorCall(lambda, "lambda")) {
    return false;
  }
  const variable = lambda.operands[1];
  return (
    variable !== undefined &&
    isNameOperand(variable) &&
    variable.name === variableName
  );
};

/**
 * Substitute a lambda iteration variable with a concrete collection element
 * inside a lambda body. A bare reference to the variable becomes the element
 * itself; a `variable.path.to.field` reference drills into the element. A
 * nested collection macro whose lambda rebinds the same variable name shadows
 * the outer variable, so substitution only descends into its collection
 * operand.
 */
const substituteLambdaVariable = (
  operand: PlanExpressionOperand,
  variableName: string,
  element: Value,
): PlanExpressionOperand => {
  if (isNameOperand(operand)) {
    if (operand.name === variableName) {
      return { value: element };
    }
    if (operand.name.startsWith(`${variableName}.`)) {
      let current: Value = element;
      for (const segment of operand.name
        .slice(variableName.length + 1)
        .split(".")) {
        if (
          current === null ||
          typeof current !== "object" ||
          Array.isArray(current) ||
          !(segment in current)
        ) {
          throw new UnsupportedQueryPlanError(
            `Cannot resolve "${operand.name}": collection element has no field "${segment}"`,
          );
        }
        const next = current[segment];
        if (next === undefined) {
          throw new UnsupportedQueryPlanError(
            `Cannot resolve "${operand.name}": collection element field "${segment}" is undefined`,
          );
        }
        current = next;
      }
      return { value: current };
    }
    return operand;
  }

  if (!isExpressionOperand(operand)) {
    return operand;
  }
  const substitute = (child: PlanExpressionOperand) =>
    substituteLambdaVariable(child, variableName, element);
  if (
    LAMBDA_BINDING_OPERATORS.has(operand.operator) &&
    operand.operands.length === 2
  ) {
    const [nestedCollection, nestedLambda] = operand.operands;
    if (
      nestedCollection &&
      nestedLambda &&
      rebindsVariable(nestedLambda, variableName)
    ) {
      // The nested lambda shadows our variable: substitute only in the collection operand.
      return {
        operator: operand.operator,
        operands: [substitute(nestedCollection), nestedLambda],
      };
    }
  }
  return {
    operator: operand.operator,
    operands: operand.operands.map(substitute),
  };
};

/**
 * Fold a collection macro whose collection operand is a literal value list.
 *
 * The planner emits this shape when a known-value collection (typically a
 * folded principal attribute) has more than 10 elements — at 10 or fewer it
 * unrolls `exists`/`all` into an or/and chain itself (cerbos/cerbos#2570,
 * cerbos/cerbos#2817; `maxItems = 10` in the planner's struct matcher). Apply
 * the same fold here so the translated filter does not depend on which side of
 * that threshold the collection lands: substitute each element into the lambda
 * body and combine the per-element conditions with OR (`exists`) or AND
 * (`all`). The empty collection keeps CEL identity semantics: `exists` over
 * `[]` is false, `all` over `[]` is true.
 */
const buildKnownValueCollectionFilter = (
  operator: CollectionOperator,
  collectionValue: Value,
  lambdaOperand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
  negated: boolean,
): SQL => {
  if (operator !== "exists" && operator !== "all" && operator !== "exists_one") {
    throw new UnsupportedQueryPlanError(
      `'${operator}' over a literal collection value is not supported. ` +
        "Only exists(), all() and exists_one() can be folded into a flat filter.",
    );
  }
  if (!Array.isArray(collectionValue)) {
    throw new UnsupportedQueryPlanError(
      `'${operator}' over a literal collection requires a list value`,
    );
  }
  const { variable, expression: bodyOperand } = extractLambdaComponents(
    lambdaOperand,
    `'${operator}' lambda operand`,
  );

  if (operator === "exists_one") {
    return withPolarity(
      buildKnownValueExistsOne(collectionValue, variable.name, bodyOperand, mapper, options),
      negated,
    );
  }

  // Push negation through the macro rather than applying a SQL NOT around it:
  // !exists(body) == all(!body), and !all(body) == exists(!body). This keeps
  // CEL evaluation errors as deny instead of treating UNKNOWN as false.
  const combinesWithOr = operator === "exists" ? !negated : negated;
  if (collectionValue.length === 0) {
    return combinesWithOr ? FALSE_CONDITION : TRUE_CONDITION;
  }

  const filters = collectionValue.map((element) =>
    buildFilterFromExpression(
      substituteLambdaVariable(bodyOperand, variable.name, element),
      mapper,
      options,
      negated,
    ),
  );
  const combined = combinesWithOr ? or(...filters) : and(...filters);
  if (!combined) {
    throw new UnsupportedQueryPlanError(
      `Unable to combine folded '${operator}' collection conditions`,
    );
  }
  return combined;
};

/**
 * `exists_one` over a literal list: exactly one element's condition is TRUE. Unlike `exists` and
 * `all`, CEL's `exists_one` evaluates every element and absorbs no error — any erroring element
 * makes the whole macro an error — so a single UNKNOWN element condition makes the result NULL,
 * excluded under both polarities, before the matches are counted.
 */
const buildKnownValueExistsOne = (
  elements: Value[],
  variableName: string,
  bodyOperand: PlanExpressionOperand,
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (elements.length === 0) {
    return FALSE_CONDITION;
  }
  const conditions = elements.map((element) =>
    buildFilterFromExpression(
      substituteLambdaVariable(bodyOperand, variableName, element),
      mapper,
      options,
    ),
  );
  const anyUnknown = sql.join(
    conditions.map((condition) => sql`(${condition}) is null`),
    sql` or `,
  );
  const matches = sql.join(
    conditions.map((condition) => sql`(case when ${condition} then 1 else 0 end)`),
    sql` + `,
  );
  return sql`(case when ${anyUnknown} then null when (${matches}) = 1 then true else false end)`;
};

/**
 * Collection macros with CEL's three-valued semantics. An element whose lambda condition
 * is UNKNOWN (a NULL element column) is a CEL missing-attribute evaluation error:
 *
 * - exists  = TRUE on a true witness (absorbs errors), else error if any element errors;
 * - all     = FALSE on a false witness (absorbs errors), else error if any element errors;
 * - exists_one errors on ANY erroring element, never absorbed.
 *
 * The generated CASE expressions preserve those three states as TRUE, FALSE, or NULL.
 * SQL NOT can therefore apply the requested polarity without turning an evaluation error
 * into an allow, while IS TRUE/FALSE/NULL keeps witnesses distinct inside each macro.
 */
export const buildCollectionOperatorFilter = (
  operator: CollectionOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  negated: boolean,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new UnsupportedQueryPlanError(`'${operator}' operator requires exactly two operands`);
  }
  const [collectionOperand, lambdaOperand] = operands;
  if (!collectionOperand || !lambdaOperand) {
    throw new UnsupportedQueryPlanError(
      `'${operator}' operator requires collection and lambda operands`,
    );
  }
  // A literal value list arrives when the planner could not unroll a macro
  // over a known collection (more than 10 elements) — fold it here instead of
  // requiring a relation mapping that cannot exist for a literal.
  if (isValueOperand(collectionOperand)) {
    return buildKnownValueCollectionFilter(
      operator,
      collectionOperand.value,
      lambdaOperand,
      mapper,
      options,
      negated,
    );
  }

  const scope = resolveMacroScope(
    collectionOperand,
    lambdaOperand,
    `'${operator}' lambda operand`,
    mapper,
    options,
  );
  const { primaryRelation, leadingRelations, collectionName } = scope;
  const rowCondition = buildRowCondition(scope, options);

  // Leading hops already established by an enclosing lambda scope (options.skipRelations)
  // must not be re-joined off the root — the subquery correlates against the enclosing
  // scope's table instead.
  const wrapLeading = (inner: SQL): SQL =>
    wrapWithRelations(leadingRelations, inner, collectionName, options);
  const aliases = primaryAlias(scope);
  const wrapAll = (inner: SQL): SQL =>
    wrapLeading(
      wrapWithRelations([primaryRelation], inner, collectionName, { aliases }),
    );
  // An absent to-one parent must stay UNKNOWN rather than reaching the empty-collection
  // answer, which `all` reads as TRUE and `!exists` inverts into an allow (#309).
  const guardHops = (inner: SQL): SQL =>
    requireLeadingHops(leadingRelations, inner, collectionName, options);

  switch (operator) {
    // filter() yields a list, not a boolean. Reaching it here means the plan used it as a
    // predicate, which CEL evaluates to a no-overload error: denied under both polarities, and
    // absorbed by `||` / `&&` exactly as SQL absorbs UNKNOWN. It is NOT `size(filter(...)) > 0`
    // (cerbos/query-plan-adapters#313); that use is handled by buildFilteredCount before this.
    case "filter":
      return UNKNOWN_CONDITION;
    case "exists": {
      const trueWitness = wrapAll(sql`(${rowCondition}) is true`);
      const unknownWitness = wrapAll(sql`(${rowCondition}) is null`);
      return withPolarity(
        guardHops(
          sql`(case when ${trueWitness} then true when ${unknownWitness} then null else false end)`,
        ),
        negated,
      );
    }
    case "except":
      return withPolarity(guardHops(wrapAll(not(rowCondition))), negated);
    case "all": {
      const falseWitness = wrapAll(sql`(${rowCondition}) is false`);
      const unknownWitness = wrapAll(sql`(${rowCondition}) is null`);
      return withPolarity(
        guardHops(
          sql`(case when ${falseWitness} then false when ${unknownWitness} then null else true end)`,
        ),
        negated,
      );
    }
    case "exists_one": {
      const matchCondition =
        and(relationCorrelation(primaryRelation, scope.alias), rowCondition) ??
        FALSE_CONDITION;
      const countExpr = sql`(select count(*) from ${relationSource(primaryRelation, collectionName, scope.alias)} where ${matchCondition})`;
      const unknownWitness = wrapWithRelations(
        [primaryRelation],
        sql`(${rowCondition}) is null`,
        collectionName,
        { aliases },
      );
      const triState = sql`(case when ${unknownWitness} then null when ${countExpr} = 1 then true else false end)`;
      return withPolarity(guardHops(wrapLeading(triState)), negated);
    }
  }
};
