import { PlanKind } from "@cerbos/core";
import type {
  PlanResourcesResponse,
  PlanExpressionOperand,
  Value,
} from "@cerbos/core";

export { PlanKind };

const CERBOS_TO_PRISMA_OPERATOR: Record<string, string> = {
  eq: "equals",
  ne: "not",
  lt: "lt",
  le: "lte",
  gt: "gt",
  ge: "gte",
};

// Directional operators mirror when their operands swap sides; symmetric operators are unchanged.
const MIRRORED_OPERATOR: Record<string, string> = {
  lt: "gt",
  gt: "lt",
  le: "ge",
  ge: "le",
};

/**
 * Normalize a binary comparison to field-side-first. The planner preserves policy source
 * order, so a constant may precede the field it constrains (`1 < R.attr.x` arrives as
 * `lt(value, variable)`) — swap the operands and mirror directional operators so downstream
 * handlers can assume the field/expression side is first. Without this, value-first
 * comparisons are silently inverted (#256).
 */
function normalizeBinaryOperands(
  operator: string,
  operands: PlanExpressionOperand[]
): { operator: string; operands: PlanExpressionOperand[] } {
  const first = operands[0];
  const second = operands[1];
  if (
    operands.length === 2 &&
    first !== undefined &&
    second !== undefined &&
    isValueOperand(first) &&
    !isValueOperand(second)
  ) {
    return {
      operator: MIRRORED_OPERATOR[operator] ?? operator,
      operands: [second, first],
    };
  }
  return { operator, operands };
}

// Type Definitions
export type PrismaFilter = Record<string, any>;

export type MapperConfig = {
  field?: string;
  /**
   * Marks the mapped column as nullable in the database. Cerbos treats a missing attribute as
   * an evaluation error (deny), which matches SQL three-valued logic for simple predicates —
   * but relation subqueries (some/every/none) collapse UNKNOWN to false at the EXISTS boundary,
   * so collection macros over elements with NULL fields need an explicit guard to stay
   * deny-aligned. Declaring nullability here enables those guards.
   */
  nullable?: boolean;
  relation?: {
    name: string;
    type: "one" | "many";
    /**
     * The Prisma model name of the related record (e.g. "Tag"). Required only for
     * field-to-field comparisons between two columns of the related model, which compile to
     * Prisma field references and need the model name as their container.
     */
    model?: string;
    field?: string;
    fields?: Record<string, MapperConfig>;
  };
};

export type Mapper =
  | Record<string, MapperConfig>
  | ((key: string) => MapperConfig);

export interface QueryPlanToPrismaArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
  /**
   * The Prisma model name the generated filter targets (e.g. "Resource"). Required only for
   * field-to-field comparisons between two root columns, which compile to Prisma field
   * references and need the model name as their container.
   */
  model?: string;
}

export type QueryPlanToPrismaResult =
  | {
      kind: PlanKind.ALWAYS_ALLOWED | PlanKind.ALWAYS_DENIED;
    }
  | {
      kind: PlanKind.CONDITIONAL;
      filters: PrismaFilter;
    };

// Type guards for operands
interface NamedOperand {
  name: string;
}

interface ValueOperand {
  value: Value;
}

interface OperatorOperand {
  operator: string;
  operands: PlanExpressionOperand[];
}

function isNamedOperand(
  operand: PlanExpressionOperand
): operand is NamedOperand {
  return "name" in operand && typeof operand.name === "string";
}

function isValueOperand(
  operand: PlanExpressionOperand
): operand is ValueOperand {
  return "value" in operand && operand.value !== undefined;
}

function isOperatorOperand(
  operand: PlanExpressionOperand
): operand is OperatorOperand {
  return (
    "operator" in operand &&
    typeof operand.operator === "string" &&
    "operands" in operand &&
    Array.isArray(operand.operands)
  );
}

function assertDefined<T>(value: T | undefined, message: string): T {
  if (value === undefined) {
    throw new Error(message);
  }
  return value;
}

function getLeafField(path: string[]): string {
  const fieldName = path[path.length - 1];
  if (!fieldName) {
    throw new Error("Field path cannot be empty");
  }
  return fieldName;
}

function getFilterEntry(filter: Record<string, unknown>): [string, unknown] {
  const entry = Object.entries(filter)[0];
  if (!entry) {
    throw new Error("Filter must contain at least one entry");
  }
  return entry;
}

// Field reference resolution types
type RelationConfig = {
  name: string;
  type: "one" | "many";
  model?: string;
  field?: string;
  nestedMapper?: Record<string, MapperConfig>;
};

type ResolvedFieldReference = {
  path: string[];
  relations?: RelationConfig[];
};

type ResolvedValue = {
  value: any;
};

type ResolvedOperand = ResolvedFieldReference | ResolvedValue;

function isResolvedFieldReference(
  operand: ResolvedOperand
): operand is ResolvedFieldReference {
  return "path" in operand;
}

function isResolvedValue(operand: ResolvedOperand): operand is ResolvedValue {
  return "value" in operand;
}

function getNamedOperand(
  operands: PlanExpressionOperand[],
  message: string
): NamedOperand {
  const operand = operands.find(isNamedOperand);
  if (!operand) {
    throw new Error(message);
  }
  return operand;
}

function getValueOperand(
  operands: PlanExpressionOperand[],
  message: string
): ValueOperand {
  const operand = operands.find(isValueOperand);
  if (!operand) {
    throw new Error(message);
  }
  return operand;
}

function requireResolvedFieldReference(
  operand: ResolvedOperand,
  message: string
): ResolvedFieldReference {
  if (!isResolvedFieldReference(operand)) {
    throw new Error(message);
  }
  return operand;
}

function requireResolvedValue(
  operand: ResolvedOperand,
  message: string
): ResolvedValue {
  if (!isResolvedValue(operand)) {
    throw new Error(message);
  }
  return operand;
}

function wrapRelations(
  relations: RelationConfig[] | undefined,
  filter: PrismaFilter
): PrismaFilter {
  if (!relations || relations.length === 0) {
    return filter;
  }
  return buildNestedRelationFilter(relations, filter);
}

function buildFieldEqualsFilter(
  fieldRef: ResolvedFieldReference,
  value: Value
): PrismaFilter {
  const fieldName = getLeafField(fieldRef.path);
  return wrapRelations(fieldRef.relations, { [fieldName]: { equals: value } });
}

function buildFieldDirectOrInFilter(
  fieldRef: ResolvedFieldReference,
  values: Value[]
): PrismaFilter {
  const fieldName = getLeafField(fieldRef.path);
  const baseFilter =
    values.length === 1
      ? { [fieldName]: values[0] }
      : { [fieldName]: { in: values } };
  return wrapRelations(fieldRef.relations, baseFilter);
}

function buildAlwaysTrueFilter(): PrismaFilter {
  return {};
}

type TernaryBranchPredicate =
  | { kind: "constant"; value: boolean }
  | { kind: "filter"; filter: PrismaFilter };

// Translation-scoped context. Set at queryPlanToPrisma entry; the lambda stack tracks which
// collection variable (and related Prisma model) encloses the expression currently being built,
// so leaf handlers can distinguish element columns from outer columns and collect nullable
// element fields for three-valued-logic guards. Translation is synchronous, so module scope is
// safe.
type LambdaScope = {
  variableName: string;
  relationModel: string | undefined;
  nullableFields: Set<string>;
};
let lambdaScopes: LambdaScope[] = [];
let rootModelName: string | undefined;

/**
 * Converts a Cerbos query plan to a Prisma filter.
 */
export function queryPlanToPrisma({
  queryPlan,
  mapper = {},
  model,
}: QueryPlanToPrismaArgs): QueryPlanToPrismaResult {
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return { kind: PlanKind.ALWAYS_ALLOWED };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL: {
      lambdaScopes = [];
      rootModelName = model;
      const condition = constantFoldExpression(
        hoistOuterScopeReferences(queryPlan.condition, [])
      );
      if (isValueOperand(condition)) {
        // Real PDP plans fold constant conditions to ALWAYS_ALLOWED/ALWAYS_DENIED before
        // they reach the adapter; this can only surface with hand-crafted plans.
        if (condition.value === true) {
          return { kind: PlanKind.CONDITIONAL, filters: {} };
        }
        // Prisma has no model-agnostic always-false `where` shape: empty logical arrays
        // are ignored.
        throw new Error(
          "A constant-false conditional predicate must be folded by the Cerbos planner"
        );
      }
      return {
        kind: PlanKind.CONDITIONAL,
        filters: buildPrismaFilterFromCerbosExpression(condition, mapper),
      };
    }
    default:
      throw Error(`Invalid query plan.`);
  }
}

const COLLECTION_OPERATORS = new Set([
  "exists",
  "exists_one",
  "all",
  "except",
  "filter",
]);

function mentionsVariable(
  expr: PlanExpressionOperand,
  variableName: string
): boolean {
  if (isNamedOperand(expr)) {
    return (
      expr.name === variableName || expr.name.startsWith(variableName + ".")
    );
  }
  if (isOperatorOperand(expr)) {
    return expr.operands.some((operand) =>
      mentionsVariable(operand, variableName)
    );
  }
  return false;
}

function isOuterScopeName(name: string, enclosingVariables: string[]): boolean {
  return !enclosingVariables.some(
    (variable) => name === variable || name.startsWith(variable + ".")
  );
}

/** Replaces every occurrence of the bare named operand `name` with a boolean constant. */
function substituteNamedOperand(
  expr: PlanExpressionOperand,
  name: string,
  value: boolean
): PlanExpressionOperand {
  if (isNamedOperand(expr)) {
    return expr.name === name ? { value } : expr;
  }
  if (isOperatorOperand(expr)) {
    return {
      operator: expr.operator,
      operands: expr.operands.map((operand) =>
        substituteNamedOperand(operand, name, value)
      ),
    };
  }
  return expr;
}

/**
 * Finds a bare boolean reference to an OUTER column (root or enclosing lambda scope) in a
 * boolean position of a lambda body: a direct operand of and/or/not, a ternary condition, or
 * the body itself.
 */
function findOuterBooleanReference(
  expr: PlanExpressionOperand,
  enclosingVariables: string[]
): string | undefined {
  if (isNamedOperand(expr)) {
    return isOuterScopeName(expr.name, enclosingVariables)
      ? expr.name
      : undefined;
  }
  if (!isOperatorOperand(expr)) {
    return undefined;
  }
  const booleanPositions: PlanExpressionOperand[] = [];
  switch (expr.operator) {
    case "and":
    case "or":
    case "not":
      booleanPositions.push(...expr.operands);
      break;
    case "if":
      booleanPositions.push(...expr.operands);
      break;
    default:
      // Comparisons wrapping a ternary keep the outer reference in the ternary condition.
      booleanPositions.push(
        ...expr.operands.filter(
          (operand) =>
            isOperatorOperand(operand) &&
            (operand.operator === "if" ||
              operand.operator === "and" ||
              operand.operator === "or" ||
              operand.operator === "not")
        )
      );
      break;
  }
  for (const operand of booleanPositions) {
    const found = findOuterBooleanReference(operand, enclosingVariables);
    if (found !== undefined) {
      return found;
    }
  }
  return undefined;
}

/**
 * Rewrites lambda bodies that reference columns of an ENCLOSING scope (the root row, or an
 * outer lambda's element) so that every filter lands on the model it belongs to. Without this,
 * an outer column referenced inside `tags.exists(t, ...)` would be emitted as a field of the
 * tag model. Two sound transforms, applied bottom-up:
 *
 * - Conjunct hoisting (exists only): `exists(c, P(c) && Q)` with row-constant Q becomes
 *   `exists(c, P(c)) && Q`. Valid in three-valued logic (AND distributes over the per-element
 *   OR), including the empty-collection case (both sides are FALSE).
 * - Case split (any collection op): a bare outer boolean reference Q in a boolean position
 *   becomes `(Q && op[Q:=true]) || (!Q && op[Q:=false]) || (Q && !Q)`. The contradiction arm
 *   keeps the whole expression UNKNOWN (not FALSE) when Q is NULL-derived, mirroring the
 *   guarded-ternary encoding.
 */
function hoistOuterScopeReferences(
  expr: PlanExpressionOperand,
  enclosingVariables: string[]
): PlanExpressionOperand {
  if (!isOperatorOperand(expr)) {
    return expr;
  }

  if (COLLECTION_OPERATORS.has(expr.operator) && expr.operands.length === 2) {
    const [collection, lambda] = expr.operands;
    if (
      collection !== undefined &&
      lambda !== undefined &&
      isOperatorOperand(lambda) &&
      lambda.operator === "lambda" &&
      lambda.operands.length === 2 &&
      lambda.operands[1] !== undefined &&
      isNamedOperand(lambda.operands[1])
    ) {
      const variable = lambda.operands[1];
      const body = hoistOuterScopeReferences(
        assertDefined(lambda.operands[0], "Lambda requires a condition"),
        [...enclosingVariables, variable.name]
      );

      const rebuild = (newBody: PlanExpressionOperand): OperatorOperand => ({
        operator: expr.operator,
        operands: [
          collection,
          { operator: "lambda", operands: [newBody, variable] },
        ],
      });

      // Conjunct hoisting out of exists().
      if (
        expr.operator === "exists" &&
        isOperatorOperand(body) &&
        body.operator === "and"
      ) {
        const elementConjuncts = body.operands.filter((operand) =>
          mentionsVariable(operand, variable.name)
        );
        const outerConjuncts = body.operands.filter(
          (operand) => !mentionsVariable(operand, variable.name)
        );
        if (outerConjuncts.length > 0 && elementConjuncts.length > 0) {
          const innerBody =
            elementConjuncts.length === 1
              ? elementConjuncts[0]!
              : { operator: "and", operands: elementConjuncts };
          return {
            operator: "and",
            operands: [rebuild(innerBody), ...outerConjuncts],
          };
        }
      }

      // Case split on a bare outer boolean reference.
      const outerRef = findOuterBooleanReference(body, [
        ...enclosingVariables,
        variable.name,
      ]);
      if (outerRef !== undefined) {
        const q: PlanExpressionOperand = { name: outerRef };
        const notQ: PlanExpressionOperand = { operator: "not", operands: [q] };
        return {
          operator: "or",
          operands: [
            {
              operator: "and",
              operands: [
                q,
                rebuild(substituteNamedOperand(body, outerRef, true)),
              ],
            },
            {
              operator: "and",
              operands: [
                notQ,
                rebuild(substituteNamedOperand(body, outerRef, false)),
              ],
            },
            // Contradiction arm: UNKNOWN when the reference is NULL-derived, never TRUE.
            { operator: "and", operands: [q, notQ] },
          ],
        };
      }

      return rebuild(body);
    }
  }

  return {
    operator: expr.operator,
    operands: expr.operands.map((operand) =>
      hoistOuterScopeReferences(operand, enclosingVariables)
    ),
  };
}

const FOLDABLE_COMPARISON_OPERATORS = new Set([
  "eq",
  "ne",
  "lt",
  "le",
  "gt",
  "ge",
]);

/**
 * Bottom-up constant folding over the plan AST. Only shapes the case-split transform can
 * produce need folding (constant ternary conditions, comparisons between two constants,
 * logical operators with constant operands, collection macros with constant bodies); real
 * planner output arrives pre-folded.
 */
function constantFoldExpression(
  expr: PlanExpressionOperand
): PlanExpressionOperand {
  if (!isOperatorOperand(expr)) {
    return expr;
  }
  const operands = expr.operands.map(constantFoldExpression);
  const folded: OperatorOperand = { operator: expr.operator, operands };

  switch (expr.operator) {
    case "if": {
      const [condition, thenBranch, elseBranch] = operands;
      if (condition !== undefined && isValueOperand(condition)) {
        if (typeof condition.value !== "boolean") {
          throw new Error("if (ternary) condition must be a boolean expression");
        }
        return assertDefined(
          condition.value ? thenBranch : elseBranch,
          "if (ternary) requires branch operands"
        );
      }
      return folded;
    }
    case "and": {
      if (operands.some((o) => isValueOperand(o) && o.value === false)) {
        return { value: false };
      }
      const remaining = operands.filter((o) => !isValueOperand(o));
      if (remaining.length === 0) return { value: true };
      if (remaining.length === 1) return remaining[0]!;
      return { operator: "and", operands: remaining };
    }
    case "or": {
      if (operands.some((o) => isValueOperand(o) && o.value === true)) {
        return { value: true };
      }
      const remaining = operands.filter((o) => !isValueOperand(o));
      if (remaining.length === 0) return { value: false };
      if (remaining.length === 1) return remaining[0]!;
      return { operator: "or", operands: remaining };
    }
    case "not": {
      const operand = operands[0];
      if (operand !== undefined && isValueOperand(operand)) {
        return { value: operand.value !== true };
      }
      return folded;
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      const [receiver, needle] = operands;
      if (
        receiver !== undefined &&
        needle !== undefined &&
        isValueOperand(receiver) &&
        isValueOperand(needle) &&
        typeof receiver.value === "string" &&
        typeof needle.value === "string"
      ) {
        const r = receiver.value;
        const n = needle.value;
        return {
          value:
            expr.operator === "contains"
              ? r.includes(n)
              : expr.operator === "startsWith"
                ? r.startsWith(n)
                : r.endsWith(n),
        };
      }
      return folded;
    }
    case "exists":
    case "all": {
      const [collection, lambda] = operands;
      if (
        collection !== undefined &&
        lambda !== undefined &&
        isOperatorOperand(lambda) &&
        lambda.operator === "lambda" &&
        lambda.operands[0] !== undefined &&
        isValueOperand(lambda.operands[0])
      ) {
        const bodyValue = lambda.operands[0].value === true;
        if (expr.operator === "exists" && !bodyValue) return { value: false };
        if (expr.operator === "all" && bodyValue) return { value: true };
        const sizeExpr: PlanExpressionOperand = {
          operator: "size",
          operands: [collection],
        };
        // exists(c, true) is "collection is non-empty"; all(c, false) is "collection is empty".
        return expr.operator === "exists"
          ? { operator: "gt", operands: [sizeExpr, { value: 0 }] }
          : { operator: "eq", operands: [sizeExpr, { value: 0 }] };
      }
      return folded;
    }
    default: {
      if (FOLDABLE_COMPARISON_OPERATORS.has(expr.operator)) {
        const [left, right] = operands;
        if (
          left !== undefined &&
          right !== undefined &&
          isValueOperand(left) &&
          isValueOperand(right)
        ) {
          return {
            value: evaluateConstantComparison(
              expr.operator,
              left.value,
              right.value
            ),
          };
        }
      }
      return folded;
    }
  }
}

/**
 * Resolves a field reference considering relations and nested fields.
 */
function resolveFieldReference(
  reference: string,
  mapper: Mapper
): ResolvedFieldReference {
  const parts = reference.split(".");
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];

  let matchedPrefix = "";
  let matchedConfig: MapperConfig | undefined;

  // If no direct match, look for partial matches
  if (!config) {
    for (let i = parts.length - 1; i >= 0; i--) {
      const prefix = parts.slice(0, i + 1).join(".");
      const prefixConfig =
        typeof mapper === "function" ? mapper(prefix) : mapper[prefix];

      if (prefixConfig) {
        matchedPrefix = prefix;
        matchedConfig = prefixConfig;
        break;
      }
    }
  }

  const activeConfig = config ?? matchedConfig;

  // Handle relation mapping
  if (activeConfig?.relation) {
    const { name, type, fields } = activeConfig.relation;
    const matchedParts = matchedPrefix ? matchedPrefix.split(".") : [];
    const remainingParts = matchedPrefix
      ? parts.slice(matchedParts.length)
      : parts.slice(1);

    let field: string | undefined;
    const relations: RelationConfig[] = [
      {
        name,
        type,
        model: activeConfig.relation.model,
        field: activeConfig.relation.field,
        nestedMapper: fields,
      },
    ];

    // Process nested relations
    if (fields && remainingParts.length > 0) {
      let currentMapper: Record<string, MapperConfig> | undefined = fields;
      let currentParts = remainingParts;

      while (currentParts.length > 0) {
        if (!currentMapper) {
          break;
        }

        const currentPart = currentParts[0];
        if (!currentPart) {
          break;
        }

        const nextConfig: MapperConfig | undefined = currentMapper[currentPart];
        if (nextConfig?.relation) {
          relations.push({
            name: nextConfig.relation.name,
            type: nextConfig.relation.type,
            model: nextConfig.relation.model,
            field: nextConfig.relation.field,
            nestedMapper: nextConfig.relation.fields,
          });
          currentMapper = nextConfig.relation.fields || {};
          currentParts = currentParts.slice(1);
        } else {
          const lastPart = currentParts[currentParts.length - 1];
          if (!lastPart) {
            break;
          }
          field = nextConfig?.field || lastPart;
          break;
        }
      }
    }

    return { path: field ? [field] : remainingParts, relations };
  }

  // Simple field mapping
  return { path: [activeConfig?.field || reference] };
}

/**
 * Determines the appropriate Prisma operator based on relation type.
 */
function getPrismaRelationOperator(relation: {
  name: string;
  type: "one" | "many";
  field?: string;
}): string {
  return relation.type === "one" ? "is" : "some";
}

/**
 * Builds a nested relation filter for Prisma queries.
 */
function buildNestedRelationFilter(
  relations: RelationConfig[],
  fieldFilter: any
): any {
  if (relations.length === 0) return fieldFilter;

  let currentFilter = fieldFilter;

  // Build nested structure from inside out
  for (let i = relations.length - 1; i >= 0; i--) {
    const relation = relations[i];
    if (!relation) {
      throw new Error("Relation mapping is missing");
    }
    const relationOperator = getPrismaRelationOperator(relation);

    // Handle special case for the deepest relation
    if (relation.field && i === relations.length - 1) {
      const [key, filterValue] = getFilterEntry(currentFilter);
      if (key === "NOT") {
        currentFilter = { NOT: { [relation.field]: filterValue } };
      } else {
        currentFilter = { [relation.field]: filterValue };
      }
    }

    currentFilter = { [relation.name]: { [relationOperator]: currentFilter } };
  }

  return currentFilter;
}

/**
 * Resolves a PlanExpressionOperand into a ResolvedOperand.
 */
function resolveOperand(
  operand: PlanExpressionOperand,
  mapper: Mapper
): ResolvedOperand {
  if (isNamedOperand(operand)) {
    return resolveFieldReference(operand.name, mapper);
  } else if (isValueOperand(operand)) {
    return { value: operand.value };
  } else if (isOperatorOperand(operand)) {
    const folded = tryFoldValueExpression(operand, mapper);
    if (folded !== null) return { value: folded };
    const nestedResult = buildPrismaFilterFromCerbosExpression(operand, mapper);
    return { value: nestedResult };
  }
  throw new Error("Operand must have name, value, or be an expression");
}

function tryFoldValueExpression(
  expr: OperatorOperand,
  mapper: Mapper
): Value | null {
  if (!ARITHMETIC_OPERATORS.has(expr.operator)) return null;
  const leftOp = expr.operands[0];
  const rightOp = expr.operands[1];
  if (!leftOp || !rightOp) return null;

  const left = resolveOperand(leftOp, mapper);
  if (!isResolvedValue(left)) return null;
  const right = resolveOperand(rightOp, mapper);
  if (!isResolvedValue(right)) return null;

  try {
    return foldArithmetic(expr.operator, left.value, right.value);
  } catch {
    return null;
  }
}

/**
 * Records that the lambda body being built touches a nullable element column, so the
 * enclosing collection operator can add its three-valued-logic guard.
 */
function recordNullableElementField(
  config: MapperConfig,
  defaultField: string
): void {
  if (!config.nullable) {
    return;
  }
  const scope = lambdaScopes[lambdaScopes.length - 1];
  scope?.nullableFields.add(config.field || defaultField);
}

/**
 * Creates a scoped mapper for collection operations
 */
function createScopedMapper(
  collectionPath: string,
  variableName: string,
  fullMapper: Mapper
): Mapper {
  return (key: string) => {
    // If the key starts with the variable name, it's accessing the collection item
    if (key.startsWith(variableName + ".")) {
      const strippedKey = key.replace(variableName + ".", "");
      const parts = strippedKey.split(".");

      // Get the collection's relation config
      const collectionConfig =
        typeof fullMapper === "function"
          ? fullMapper(collectionPath)
          : fullMapper[collectionPath];

      if (collectionConfig?.relation?.fields) {
        // For nested paths, traverse the fields configuration
        const baseConfig = collectionConfig.relation.fields;
        if (!baseConfig) {
          return { field: strippedKey };
        }

        let currentConfig = baseConfig;
        let field = parts[0] || strippedKey;

        for (let i = 0; i < parts.length - 1; i++) {
          const part = parts[i];
          const nextPart = parts[i + 1];
          if (!part || !nextPart) {
            break;
          }

          const nextConfig = currentConfig[part];
          if (nextConfig?.relation?.fields) {
            currentConfig = nextConfig.relation.fields;
            field = nextPart;
          } else {
            break;
          }
        }

        if (!field) {
          field = strippedKey;
        }

        // Return the field config if it exists, otherwise create a default one
        const fieldConfig = currentConfig[field] || { field };
        recordNullableElementField(fieldConfig, field);
        return fieldConfig;
      }
      return { field: strippedKey };
    }

    // For keys not referencing the collection item, use the full mapper
    if (typeof fullMapper === "function") {
      return fullMapper(key);
    }
    return fullMapper[key] || { field: key };
  };
}

/**
 * Builds a Prisma filter from a Cerbos expression.
 */
function buildPrismaFilterFromCerbosExpression(
  expression: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  // A bare named operand represents a boolean field reference (e.g. `R.attr.booleanAttr`)
  if (isNamedOperand(expression)) {
    const fieldRef = resolveFieldReference(expression.name, mapper);
    return buildFieldEqualsFilter(fieldRef, true);
  }

  if (!isOperatorOperand(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;

  // Process different operator types
  switch (operator) {
    case "and":
      return {
        AND: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };

    case "or":
      return {
        OR: operands.map((operand) =>
          buildPrismaFilterFromCerbosExpression(operand, mapper)
        ),
      };

    case "not": {
      const operand = operands[0];
      if (!operand) {
        throw new Error("not operator requires an operand");
      }
      return buildNegatedFilter(operand, mapper);
    }

    case "if": {
      return handleBooleanTernaryOperator(operands, mapper);
    }

    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      return handleRelationalOperator(operator, operands, mapper);
    }

    case "in": {
      return handleInOperator(operands, mapper);
    }

    case "contains":
    case "startsWith":
    case "endsWith": {
      return handleStringOperator(operator, operands, mapper);
    }

    case "isSet": {
      return handleIsSetOperator(operands, mapper);
    }

    case "hasIntersection": {
      return handleHasIntersectionOperator(operands, mapper);
    }

    case "lambda": {
      return handleLambdaOperator(operands);
    }

    case "exists":
    case "exists_one":
    case "all":
    case "except":
    case "filter": {
      return handleCollectionOperator(operator, operands, mapper);
    }

    case "map": {
      return handleMapOperator(operands, mapper);
    }

    case "overlaps": {
      return handleOverlapsOperator(operands, mapper);
    }

    case "ancestorOf": {
      return handleAncestorDescendantOperator(operands, mapper, "ancestor");
    }

    case "descendentOf": {
      return handleAncestorDescendantOperator(operands, mapper, "descendant");
    }

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

function containsCollectionOperator(expr: PlanExpressionOperand): boolean {
  if (!isOperatorOperand(expr)) {
    return false;
  }
  if (COLLECTION_OPERATORS.has(expr.operator)) {
    return true;
  }
  return expr.operands.some(containsCollectionOperator);
}

/**
 * Builds the filter for `!expr`. Plain field predicates negate correctly with Prisma's NOT
 * (SQL three-valued logic keeps NULL rows excluded under both polarities), but relation
 * subqueries collapse UNKNOWN to false at the EXISTS boundary, so `NOT(some(P))` would
 * wrongly include rows whose elements make P UNKNOWN (a NULL element column is a missing
 * attribute — a CEL error — on the check side, which Cerbos treats as deny). Negation is
 * therefore pushed down to the collection operators, which encode CEL's exact error
 * semantics:
 *
 * - exists(c,P) is TRUE with a true witness, FALSE if every element is definitively false,
 *   and an ERROR otherwise — so `!exists` admits only rows with no P-match AND no
 *   UNKNOWN-element (the nullable-field guard).
 * - all(c,P) is FALSE only with a definitive false witness (a false witness absorbs error
 *   elements) — so `!all` is `some(NOT P)`, which SQL already evaluates definitively.
 */
function buildNegatedFilter(
  operand: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  if (isNamedOperand(operand)) {
    const { relations, ...fieldRef } = resolveFieldReference(
      operand.name,
      mapper
    );
    if (!relations || relations.length === 0) {
      return buildFieldEqualsFilter(fieldRef, false);
    }
    return {
      NOT: buildPrismaFilterFromCerbosExpression(operand, mapper),
    };
  }

  if (isOperatorOperand(operand) && containsCollectionOperator(operand)) {
    switch (operand.operator) {
      case "and":
        return {
          OR: operand.operands.map((o) => buildNegatedFilter(o, mapper)),
        };
      case "or":
        return {
          AND: operand.operands.map((o) => buildNegatedFilter(o, mapper)),
        };
      case "not":
        return buildPrismaFilterFromCerbosExpression(
          assertDefined(
            operand.operands[0],
            "not operator requires an operand"
          ),
          mapper
        );
      case "exists":
      case "all":
      case "except":
      case "exists_one":
      case "filter":
        return buildNegatedCollectionFilter(
          operand.operator,
          operand.operands,
          mapper
        );
    }
  }

  return {
    NOT: buildPrismaFilterFromCerbosExpression(operand, mapper),
  };
}

function getTernaryOperands(operands: PlanExpressionOperand[]): {
  condition: PlanExpressionOperand;
  thenBranch: PlanExpressionOperand;
  elseBranch: PlanExpressionOperand;
} {
  if (operands.length !== 3) {
    throw new Error(
      `if (ternary) requires exactly 3 operands (condition, then, else), got ${operands.length}`
    );
  }

  return {
    condition: assertDefined(
      operands[0],
      "if (ternary) requires a condition operand"
    ),
    thenBranch: assertDefined(
      operands[1],
      "if (ternary) requires a then operand"
    ),
    elseBranch: assertDefined(
      operands[2],
      "if (ternary) requires an else operand"
    ),
  };
}

function getConstantBooleanCondition(
  condition: PlanExpressionOperand
): boolean | undefined {
  if (!isValueOperand(condition)) {
    return undefined;
  }
  if (typeof condition.value !== "boolean") {
    throw new Error("if (ternary) condition must be a boolean expression");
  }
  return condition.value;
}

function buildBooleanBranchFilter(
  branch: PlanExpressionOperand,
  mapper: Mapper
): TernaryBranchPredicate {
  if (!isValueOperand(branch)) {
    return {
      kind: "filter",
      filter: buildPrismaFilterFromCerbosExpression(branch, mapper),
    };
  }
  if (typeof branch.value !== "boolean") {
    throw new Error(
      "if (ternary) branch in boolean position must be a boolean"
    );
  }
  return { kind: "constant", value: branch.value };
}

function buildFalseConditionFilter(
  condition: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  if (isValueOperand(condition)) {
    throw new Error("Constant ternary conditions must be folded before use");
  }

  if (isNamedOperand(condition)) {
    const fieldRef = resolveFieldReference(condition.name, mapper);
    if (!fieldRef.relations || fieldRef.relations.length === 0) {
      return buildFieldEqualsFilter(fieldRef, false);
    }
    return {
      NOT: buildFieldEqualsFilter(fieldRef, true),
    };
  }

  if (isOperatorOperand(condition) && condition.operator === "not") {
    return buildPrismaFilterFromCerbosExpression(
      assertDefined(condition.operands[0], "not operator requires an operand"),
      mapper
    );
  }

  return {
    NOT: buildPrismaFilterFromCerbosExpression(condition, mapper),
  };
}

function buildGuardedTernaryFilter({
  condition,
  thenFilter,
  elseFilter,
  mapper,
}: {
  condition: PlanExpressionOperand;
  thenFilter: TernaryBranchPredicate;
  elseFilter: TernaryBranchPredicate;
  mapper: Mapper;
}): PrismaFilter {
  const conditionTrue = buildPrismaFilterFromCerbosExpression(
    condition,
    mapper
  );
  const conditionFalse = buildFalseConditionFilter(condition, mapper);
  const guardedBranches: PrismaFilter[] = [];

  if (thenFilter.kind === "filter") {
    guardedBranches.push({ AND: [conditionTrue, thenFilter.filter] });
  } else if (thenFilter.value) {
    guardedBranches.push(conditionTrue);
  }

  if (elseFilter.kind === "filter") {
    guardedBranches.push({ AND: [conditionFalse, elseFilter.filter] });
  } else if (elseFilter.value) {
    guardedBranches.push(conditionFalse);
  }

  // For a known condition this contradiction is false. If the condition is
  // NULL/error-derived, both sides are SQL UNKNOWN, which keeps the whole
  // ternary UNKNOWN under an outer NOT instead of authorizing the row.
  guardedBranches.push({ AND: [conditionTrue, conditionFalse] });

  return {
    OR: guardedBranches,
  };
}

function finalizeTernaryBranchPredicate(
  predicate: TernaryBranchPredicate
): PrismaFilter {
  if (predicate.kind === "filter") {
    return predicate.filter;
  }
  if (predicate.value) {
    return buildAlwaysTrueFilter();
  }
  // Prisma has no model-agnostic always-false `where` shape: empty logical
  // arrays are ignored. Real PDP plans fold this case to ALWAYS_DENIED.
  throw new Error(
    "A constant-false conditional predicate must be folded by the Cerbos planner"
  );
}

function handleBooleanTernaryOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const { condition, thenBranch, elseBranch } = getTernaryOperands(operands);
  const constantCondition = getConstantBooleanCondition(condition);
  if (constantCondition !== undefined) {
    return finalizeTernaryBranchPredicate(
      buildBooleanBranchFilter(
        constantCondition ? thenBranch : elseBranch,
        mapper
      )
    );
  }

  return buildGuardedTernaryFilter({
    condition,
    thenFilter: buildBooleanBranchFilter(thenBranch, mapper),
    elseFilter: buildBooleanBranchFilter(elseBranch, mapper),
    mapper,
  });
}

function buildTernaryComparisonBranch({
  operator,
  operands,
  ternaryIndex,
  branch,
  mapper,
}: {
  operator: string;
  operands: PlanExpressionOperand[];
  ternaryIndex: number;
  branch: PlanExpressionOperand;
  mapper: Mapper;
}): TernaryBranchPredicate {
  const substitutedOperands = operands.map((operand, index) =>
    index === ternaryIndex ? branch : operand
  );
  assertDefined(substitutedOperands[0], `${operator} requires a left operand`);
  assertDefined(substitutedOperands[1], `${operator} requires a right operand`);

  const normalized = normalizeBinaryOperands(operator, substitutedOperands);

  const normalizedFirst = normalized.operands[0];
  const normalizedSecond = normalized.operands[1];
  if (
    normalized.operands.length === 2 &&
    normalizedFirst !== undefined &&
    normalizedSecond !== undefined &&
    isValueOperand(normalizedFirst) &&
    isValueOperand(normalizedSecond)
  ) {
    return {
      kind: "constant",
      value: evaluateConstantComparison(
        normalized.operator,
        normalizedFirst.value,
        normalizedSecond.value
      ),
    };
  }

  return {
    kind: "filter",
    filter: buildPrismaFilterFromCerbosExpression(normalized, mapper),
  };
}

function tryHandleTernaryComparison(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter | null {
  if (CERBOS_TO_PRISMA_OPERATOR[operator] === undefined) {
    return null;
  }

  const ternaryIndex = operands.findIndex(
    (operand) => isOperatorOperand(operand) && operand.operator === "if"
  );
  if (ternaryIndex === -1) {
    return null;
  }
  if (operands.length !== 2) {
    throw new Error(
      `${operator} with a ternary requires exactly 2 operands, got ${operands.length}`
    );
  }

  const ternary = assertDefined(
    operands[ternaryIndex],
    "Ternary comparison operand is missing"
  );
  if (!isOperatorOperand(ternary)) {
    throw new Error("Ternary comparison operand must be an expression");
  }

  const { condition, thenBranch, elseBranch } = getTernaryOperands(
    ternary.operands
  );
  const constantCondition = getConstantBooleanCondition(condition);
  if (constantCondition !== undefined) {
    return finalizeTernaryBranchPredicate(
      buildTernaryComparisonBranch({
        operator,
        operands,
        ternaryIndex,
        branch: constantCondition ? thenBranch : elseBranch,
        mapper,
      })
    );
  }

  return buildGuardedTernaryFilter({
    condition,
    thenFilter: buildTernaryComparisonBranch({
      operator,
      operands,
      ternaryIndex,
      branch: thenBranch,
      mapper,
    }),
    elseFilter: buildTernaryComparisonBranch({
      operator,
      operands,
      ternaryIndex,
      branch: elseBranch,
      mapper,
    }),
    mapper,
  });
}

function evaluateConstantComparison(
  operator: string,
  left: Value,
  right: Value
): boolean {
  switch (operator) {
    case "eq":
      return areValuesEqual(left, right);
    case "ne":
      return !areValuesEqual(left, right);
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      if (
        !(
          (typeof left === "number" && typeof right === "number") ||
          (typeof left === "string" && typeof right === "string")
        )
      ) {
        throw new Error(
          `${operator} constant comparison requires two numbers or two strings`
        );
      }
      switch (operator) {
        case "lt":
          return compareConstantValues(left, right) < 0;
        case "le":
          return compareConstantValues(left, right) <= 0;
        case "gt":
          return compareConstantValues(left, right) > 0;
        case "ge":
          return compareConstantValues(left, right) >= 0;
      }
    }
  }

  throw new Error(`Unsupported constant comparison operator: ${operator}`);
}

function compareConstantValues(left: number | string, right: number | string) {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left !== "string" || typeof right !== "string") {
    throw new Error("Cannot order constant values of different types");
  }

  const leftCodePoints = Array.from(left, (character) =>
    character.codePointAt(0)
  );
  const rightCodePoints = Array.from(right, (character) =>
    character.codePointAt(0)
  );
  const sharedLength = Math.min(leftCodePoints.length, rightCodePoints.length);
  for (let index = 0; index < sharedLength; index++) {
    const leftCodePoint = leftCodePoints[index]!;
    const rightCodePoint = rightCodePoints[index]!;
    if (leftCodePoint !== rightCodePoint) {
      return leftCodePoint < rightCodePoint ? -1 : 1;
    }
  }
  return leftCodePoints.length === rightCodePoints.length
    ? 0
    : leftCodePoints.length < rightCodePoints.length
      ? -1
      : 1;
}

function areValuesEqual(left: Value, right: Value): boolean {
  if (Array.isArray(left)) {
    return (
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => areValuesEqual(value, right[index]!))
    );
  }

  if (Array.isArray(right)) {
    return false;
  }

  if (typeof left === "object" && left !== null) {
    if (typeof right !== "object" || right === null) {
      return false;
    }
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key) => key in right && areValuesEqual(left[key]!, right[key]!)
      )
    );
  }

  return left === right;
}

function handleSizeComparison(
  operator: string,
  sizeOperand: OperatorOperand,
  valueOperand: PlanExpressionOperand,
  mapper: Mapper
): PrismaFilter {
  const collectionOperand = sizeOperand.operands[0];
  if (!collectionOperand || !isNamedOperand(collectionOperand)) {
    throw new Error("size operator requires a named collection operand");
  }

  if (!isValueOperand(valueOperand)) {
    throw new Error("size comparison requires a numeric value operand");
  }

  const count = valueOperand.value;
  if (typeof count !== "number") {
    throw new Error("size comparison requires a numeric value");
  }

  const isNonEmpty =
    (operator === "gt" && count === 0) || (operator === "ge" && count === 1);

  const isEmpty =
    (operator === "eq" && count === 0) ||
    (operator === "lt" && count === 1) ||
    (operator === "le" && count === 0);

  if (!isNonEmpty && !isEmpty) {
    throw new Error(
      `Unsupported size comparison: size(...) ${operator} ${count}`
    );
  }

  const { relations } = resolveFieldReference(collectionOperand.name, mapper);
  if (!relations || relations.length === 0) {
    throw new Error("size operator requires a relation mapping");
  }

  const deepest = relations[relations.length - 1];
  if (!deepest) {
    throw new Error("size operator requires a relation mapping");
  }

  const prismaOp = isNonEmpty ? "some" : "none";
  const leafFilter = { [deepest.name]: { [prismaOp]: {} } };

  if (relations.length === 1) {
    return leafFilter;
  }

  return buildNestedRelationFilter(relations.slice(0, -1), leafFilter);
}

/**
 * Helper function to process relational operators (eq, ne, lt, etc.)
 */
function handleRelationalOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const ternaryFilter = tryHandleTernaryComparison(
    operator,
    operands,
    mapper
  );
  if (ternaryFilter) {
    return ternaryFilter;
  }

  ({ operator, operands } = normalizeBinaryOperands(operator, operands));
  const prismaOperator = CERBOS_TO_PRISMA_OPERATOR[operator];

  if (!prismaOperator) {
    throw new Error(`Unsupported operator: ${operator}`);
  }

  const leftOperand = operands.find(
    (o) => isNamedOperand(o) || isOperatorOperand(o)
  );
  if (!leftOperand) throw new Error("No valid left operand found");

  const rightOperand = operands.find((o) => o !== leftOperand);
  if (!rightOperand) throw new Error("No valid right operand found");

  if (isOperatorOperand(leftOperand) && leftOperand.operator === "size") {
    return handleSizeComparison(operator, leftOperand, rightOperand, mapper);
  }

  const arithOperand = [leftOperand, rightOperand].find(
    (o): o is OperatorOperand =>
      isOperatorOperand(o) && ARITHMETIC_OPERATORS.has(o.operator)
  );
  if (arithOperand) {
    const otherOperand =
      arithOperand === leftOperand ? rightOperand : leftOperand;
    return handleArithmeticComparison(
      operator,
      arithOperand,
      otherOperand,
      arithOperand === leftOperand,
      mapper
    );
  }

  const mapOperand = [leftOperand, rightOperand].find(
    (o): o is OperatorOperand =>
      isOperatorOperand(o) && o.operator === "map"
  );
  if (mapOperand) {
    throw new Error(
      `Direct comparison of map(...) to a value is not supported (operator: ${operator}). ` +
        `Wrap the map() expression in hasIntersection(map(...), [...]) instead.`
    );
  }

  const left = resolveOperand(leftOperand, mapper);
  const right = resolveOperand(rightOperand, mapper);

  if (isResolvedFieldReference(left) && isResolvedFieldReference(right)) {
    return buildFieldToFieldFilter(
      operator,
      leftOperand,
      left,
      rightOperand,
      right
    );
  }

  const rightValue = requireResolvedValue(
    right,
    "Right operand must be a value"
  );

  if (isResolvedFieldReference(left)) {
    return buildComparisonFilter(left, operator, rightValue.value);
  }

  return { [prismaOperator]: rightValue.value };
}

/**
 * Builds a field-vs-constant comparison. Fractional constants against ordering (and
 * equality) operators are emitted as a two-clause bracket because Prisma coerces filter
 * values to the column's type: on an Int column, `gte: 1.5` is silently bound as `gte: 1`,
 * inverting rows like aNumber == 1. Each bracket pairs the exact constant with its integer
 * neighbor so the combination is correct BOTH for Float columns (the extra clause is
 * redundant) and Int columns (whichever way Prisma truncates or floors the fraction).
 */
function buildComparisonFilter(
  fieldRef: ResolvedFieldReference,
  operator: string,
  value: Value
): PrismaFilter {
  const prismaOperator = assertDefined(
    CERBOS_TO_PRISMA_OPERATOR[operator],
    `Unsupported operator: ${operator}`
  );

  if (typeof value === "number" && !Number.isInteger(value)) {
    const fieldName = getLeafField(fieldRef.path);
    const lower = Math.floor(value);
    const upper = Math.ceil(value);
    const equalsBracket = {
      AND: [
        { [fieldName]: { equals: value } },
        { [fieldName]: { gt: lower } },
        { [fieldName]: { lt: upper } },
      ],
    };
    let bracket: PrismaFilter | undefined;
    switch (operator) {
      case "gt":
        bracket = {
          OR: [{ [fieldName]: { gt: value } }, { [fieldName]: { gte: upper } }],
        };
        break;
      case "ge":
        bracket = {
          AND: [{ [fieldName]: { gte: value } }, { [fieldName]: { gt: lower } }],
        };
        break;
      case "lt":
        bracket = {
          OR: [{ [fieldName]: { lt: value } }, { [fieldName]: { lte: lower } }],
        };
        break;
      case "le":
        bracket = {
          AND: [{ [fieldName]: { lte: value } }, { [fieldName]: { lt: upper } }],
        };
        break;
      case "eq":
        bracket = equalsBracket;
        break;
      case "ne":
        bracket = { NOT: equalsBracket };
        break;
    }
    if (bracket) {
      return wrapRelations(fieldRef.relations, bracket);
    }
  }

  return buildFieldFilter(fieldRef, prismaOperator, value);
}

/**
 * Builds a column-vs-column comparison as a Prisma field reference. Prisma only supports
 * references between fields of the SAME model, so the container is the root model for
 * root-level comparisons and the relation's model inside a lambda; mixed-scope comparisons
 * (an element column against an outer column) are cross-model and must fail loudly.
 */
function buildFieldToFieldFilter(
  operator: string,
  leftOperand: PlanExpressionOperand,
  left: ResolvedFieldReference,
  rightOperand: PlanExpressionOperand,
  right: ResolvedFieldReference
): PrismaFilter {
  const prismaOperator = assertDefined(
    CERBOS_TO_PRISMA_OPERATOR[operator],
    `Unsupported operator: ${operator}`
  );

  if (!isNamedOperand(leftOperand) || !isNamedOperand(rightOperand)) {
    throw new Error("Field-to-field comparison requires two named operands");
  }

  const scope = lambdaScopes[lambdaScopes.length - 1];
  let container: string | undefined;

  if (scope) {
    const prefix = scope.variableName + ".";
    const leftInScope = leftOperand.name.startsWith(prefix);
    const rightInScope = rightOperand.name.startsWith(prefix);
    if (leftInScope !== rightInScope) {
      throw new Error(
        `Cannot compare a collection element column with an outer column (${leftOperand.name} vs ${rightOperand.name}): ` +
          "Prisma field references only work between fields of the same model"
      );
    }
    if (leftInScope) {
      container = scope.relationModel;
      if (!container) {
        throw new Error(
          "Field-to-field comparison inside a collection requires `relation.model` in the mapper"
        );
      }
    }
  }

  if (container === undefined) {
    if (
      (left.relations && left.relations.length > 0) ||
      (right.relations && right.relations.length > 0)
    ) {
      throw new Error(
        "Cannot compare columns across relations: Prisma field references only work between fields of the same model"
      );
    }
    container = rootModelName;
    if (!container) {
      throw new Error(
        "Field-to-field comparison requires the `model` option (the Prisma model name) to build a field reference"
      );
    }
  }

  const leftField = getLeafField(left.path);
  const rightField = getLeafField(right.path);
  return {
    [leftField]: {
      [prismaOperator]: { _ref: rightField, _container: container },
    },
  };
}

/**
 * Helper function to handle "in" operator
 */
function handleInOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const nameOperand = getNamedOperand(operands, "Name operand is undefined");
  const valueOperand = getValueOperand(operands, "Value operand is undefined");
  const fieldRef = requireResolvedFieldReference(
    resolveOperand(nameOperand, mapper),
    "Name operand must resolve to a field reference"
  );
  const { value } = requireResolvedValue(
    resolveOperand(valueOperand, mapper),
    "Value operand must resolve to a value"
  );
  const values = Array.isArray(value) ? value : [value];
  return buildFieldDirectOrInFilter(fieldRef, values);
}

// Upper bound on the IN-list produced when enumerating a constant receiver's substrings.
const MAX_ENUMERATED_NEEDLES = 1000;

/**
 * Helper function to handle string operators (contains, startsWith, endsWith).
 *
 * These operators are receiver-sensitive and the planner preserves policy source order:
 * operand 0 is the receiver (haystack), operand 1 the needle. Swapping them silently
 * inverts the match direction, so unlike symmetric comparisons they are never normalized.
 */
function handleStringOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error(`${operator} requires exactly two operands`);
  }
  const receiver = resolveOperand(
    assertDefined(operands[0], `${operator} requires a receiver operand`),
    mapper
  );
  const needle = resolveOperand(
    assertDefined(operands[1], `${operator} requires a needle operand`),
    mapper
  );

  // Column receiver, constant needle: Prisma's LIKE-based filter.
  if (isResolvedFieldReference(receiver) && isResolvedValue(needle)) {
    const { value } = needle;
    if (typeof value !== "string") {
      throw new Error(`${operator} operator requires string value`);
    }
    // Prisma emits LIKE without an ESCAPE clause and does not escape wildcard characters,
    // so a needle containing % or _ would match as a pattern instead of literally (e.g.
    // startsWith("100%") wrongly matches "100xdone"). Fail loudly rather than filter wrong.
    if (/[%_]/.test(value)) {
      throw new Error(
        `Cannot translate ${operator} with a needle containing LIKE metacharacters (% or _): ` +
          "Prisma does not escape wildcards in string filters"
      );
    }
    const fieldName = getLeafField(receiver.path);
    return wrapRelations(receiver.relations, {
      [fieldName]: { [operator]: value },
    });
  }

  // Constant receiver, column needle (e.g. `"a-b-c".contains(R.attr.x)`): enumerate every
  // candidate needle the constant admits — prefixes for startsWith, suffixes for endsWith,
  // substrings for contains — into an exact IN filter. No LIKE, so no escaping hazards; a
  // NULL needle column stays excluded (CEL missing-attribute deny).
  if (isResolvedValue(receiver) && isResolvedFieldReference(needle)) {
    if (typeof receiver.value !== "string") {
      throw new Error(`${operator} operator requires a string receiver`);
    }
    const haystack = receiver.value;
    const candidates = new Set<string>([""]);
    if (operator === "startsWith") {
      for (let i = 1; i <= haystack.length; i++) {
        candidates.add(haystack.slice(0, i));
      }
    } else if (operator === "endsWith") {
      for (let i = 0; i < haystack.length; i++) {
        candidates.add(haystack.slice(i));
      }
    } else {
      for (let start = 0; start < haystack.length; start++) {
        for (let end = start + 1; end <= haystack.length; end++) {
          candidates.add(haystack.slice(start, end));
        }
      }
    }
    if (candidates.size > MAX_ENUMERATED_NEEDLES) {
      throw new Error(
        `Cannot translate ${operator} with a constant receiver of this length: ` +
          `enumerating its ${candidates.size} candidate needles exceeds the ${MAX_ENUMERATED_NEEDLES}-entry limit`
      );
    }
    const fieldName = getLeafField(needle.path);
    return wrapRelations(needle.relations, {
      [fieldName]: { in: [...candidates] },
    });
  }

  if (isResolvedFieldReference(receiver) && isResolvedFieldReference(needle)) {
    // A column-valued needle would need its LIKE metacharacters escaped per row, which
    // Prisma's filters cannot do (and Prisma leaks wildcards from field references).
    throw new Error(
      `Cannot translate ${operator} between two columns: Prisma cannot escape LIKE wildcards held in a column`
    );
  }

  throw new Error(
    `${operator} between two constants must be folded by the Cerbos planner`
  );
}

/**
 * Helper function to handle "isSet" operator
 */
function handleIsSetOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const nameOperand = getNamedOperand(operands, "Name operand is undefined");
  const valueOperand = getValueOperand(operands, "Value operand is undefined");
  const fieldRef = requireResolvedFieldReference(
    resolveOperand(nameOperand, mapper),
    "Name operand must resolve to a field reference"
  );
  const resolvedValue = requireResolvedValue(
    resolveOperand(valueOperand, mapper),
    "Value operand must resolve to a value"
  );

  const fieldName = getLeafField(fieldRef.path);
  return wrapRelations(fieldRef.relations, {
    [fieldName]: resolvedValue.value ? { not: null } : { equals: null },
  });
}

/**
 * Helper function to handle "hasIntersection" operator
 */
function handleHasIntersectionOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
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

  // Check if left operand is a map operation
  if (isOperatorOperand(leftOperand) && leftOperand.operator === "map") {
    if (!isValueOperand(rightOperand)) {
      throw new Error("Second operand of hasIntersection must be a value");
    }

    const collection = assertDefined(
      leftOperand.operands[0],
      "Map expression must include a collection reference"
    );
    const lambda = assertDefined(
      leftOperand.operands[1],
      "Map expression must include a lambda expression"
    );

    if (!isNamedOperand(collection)) {
      throw new Error("First operand of map must be a collection reference");
    }

    // Get variable name from lambda
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

    // Create scoped mapper for the collection
    const scopedMapper = createScopedMapper(
      collection.name,
      variable.name,
      mapper
    );

    const { relations } = resolveFieldReference(collection.name, mapper);
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

    // Use scoped mapper for resolving the projection, collecting nullable element columns.
    const scope: LambdaScope = {
      variableName: variable.name,
      relationModel: relations[relations.length - 1]?.model,
      nullableFields: new Set(),
    };
    lambdaScopes.push(scope);
    let resolved: ResolvedFieldReference;
    try {
      resolved = resolveFieldReference(projection.name, scopedMapper);
    } finally {
      lambdaScopes.pop();
    }
    const fieldName = getLeafField(resolved.path);

    const base = buildNestedRelationFilter(relations, {
      [fieldName]: { in: rightOperand.value },
    });
    if (scope.nullableFields.size === 0) {
      return base;
    }
    // CEL map() errors if ANY element is missing the projected attribute — even alongside a
    // matching element — and Cerbos treats that error as deny. Exclude rows holding an
    // element whose projected (nullable) column is NULL.
    return {
      AND: [
        base,
        {
          NOT: buildNestedRelationFilter(
            relations,
            buildNullWitnessFilter(scope.nullableFields)
          ),
        },
      ],
    };
  }

  // Handle regular field reference
  if (!isNamedOperand(leftOperand)) {
    throw new Error(
      "First operand of hasIntersection must be a field reference or map expression"
    );
  }

  if (!isValueOperand(rightOperand)) {
    throw new Error("Second operand of hasIntersection must be a value");
  }

  const { path, relations } = resolveFieldReference(leftOperand.name, mapper);

  if (!Array.isArray(rightOperand.value)) {
    throw new Error("hasIntersection requires an array value");
  }

  if (relations && relations.length > 0) {
    const fieldName = getLeafField(path);
    const fieldFilter = {
      [fieldName]: { in: rightOperand.value },
    };
    return buildNestedRelationFilter(relations, fieldFilter);
  }

  const fieldName = getLeafField(path);
  return { [fieldName]: { some: rightOperand.value } };
}

/**
 * Helper function to handle "lambda" operator
 */
function handleLambdaOperator(operands: PlanExpressionOperand[]): PrismaFilter {
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

  return buildPrismaFilterFromCerbosExpression(condition, (key: string) => ({
    field: key.replace(`${variable.name}.`, ""),
  }));
}

type CollectionLambdaParts = {
  head: RelationConfig;
  restRelations: RelationConfig[];
  /** Element predicate, already wrapped through any relations beyond the first. */
  filterValue: PrismaFilter;
  /** Nullable element columns referenced by the lambda body (for 3VL guards). */
  nullableFields: Set<string>;
};

function buildCollectionLambdaParts(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
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

  // Get variable name from lambda
  const variable = assertDefined(
    lambda.operands[1],
    "Lambda variable must have a name"
  );
  if (!isNamedOperand(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  // Create scoped mapper for the collection
  const scopedMapper = createScopedMapper(
    collection.name,
    variable.name,
    mapper
  );

  const { relations } = resolveFieldReference(collection.name, mapper);
  if (!relations || relations.length === 0) {
    throw new Error(`${operator} operator requires a relation mapping`);
  }

  const head = assertDefined(
    relations[0],
    `${operator} operator requires a relation mapping`
  );
  const restRelations = relations.slice(1);
  const deepest = assertDefined(
    relations[relations.length - 1],
    `${operator} operator requires a relation mapping`
  );

  const lambdaConditionOperand = assertDefined(
    lambda.operands[0],
    "Lambda expression must provide a condition"
  );

  const scope: LambdaScope = {
    variableName: variable.name,
    relationModel: deepest.model,
    nullableFields: new Set(),
  };
  lambdaScopes.push(scope);
  let lambdaCondition: PrismaFilter;
  try {
    lambdaCondition = buildPrismaFilterFromCerbosExpression(
      lambdaConditionOperand, // Use the condition part of the lambda
      scopedMapper
    );
  } finally {
    lambdaScopes.pop();
  }

  let filterValue = lambdaCondition;

  if (restRelations.length > 0) {
    // Chained collection reference (e.g. R.attr.a.b): the lambda's elements live at the END
    // of the chain, so the element predicate must join through every intermediate hop.
    filterValue = buildNestedRelationFilter(restRelations, lambdaCondition);
  } else if (lambdaCondition["AND"] || lambdaCondition["OR"]) {
    // If the lambda condition already has a logical structure, use it as-is
    filterValue = lambdaCondition;
  } else {
    const lambdaKeys = Object.keys(lambdaCondition);
    const defaultKey = lambdaKeys[0];
    if (!defaultKey) {
      throw new Error("Lambda condition must have at least one field");
    }
    const lambdaFieldValue = lambdaCondition[defaultKey];
    if (lambdaFieldValue === undefined) {
      throw new Error("Lambda condition field value cannot be undefined");
    }
    const filterField = head.field || defaultKey;
    filterValue = {
      [filterField]: lambdaFieldValue,
    };
  }

  return {
    head,
    restRelations,
    filterValue,
    nullableFields: scope.nullableFields,
  };
}

/** OR of `field IS NULL` checks for every nullable element column the lambda touched. */
function buildNullWitnessFilter(nullableFields: Set<string>): PrismaFilter {
  const checks = [...nullableFields].map((field) => ({ [field]: null }));
  if (checks.length === 1) {
    return checks[0]!;
  }
  return { OR: checks };
}

/** Wraps an element-level filter through the full relation chain (head + rest). */
function wrapCollectionElementFilter(
  parts: CollectionLambdaParts,
  elementFilter: PrismaFilter
): PrismaFilter {
  const inner =
    parts.restRelations.length > 0
      ? buildNestedRelationFilter(parts.restRelations, elementFilter)
      : elementFilter;
  return { [parts.head.name]: { some: inner } };
}

function throwUnsupportedCollectionOperator(operator: string): never {
  if (operator === "exists_one") {
    throw new Error(
      "exists_one requires counting matching elements, which Prisma where-filters cannot express"
    );
  }
  throw new Error(
    "The filter() collection operator returns a list, not a boolean. " +
      "It cannot be used as a standalone condition. " +
      "Use exists() or combine filter() with size() instead."
  );
}

/**
 * Helper function to handle collection operators (exists, all, except, filter)
 */
function handleCollectionOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  if (operator === "exists_one" || operator === "filter") {
    throwUnsupportedCollectionOperator(operator);
  }

  const parts = buildCollectionLambdaParts(operator, operands, mapper);
  const { head, filterValue, nullableFields } = parts;

  switch (operator) {
    case "exists":
      // A NULL element keeps the per-element predicate UNKNOWN, so it can never create a
      // false positive here; a CEL error (deny) coincides with "no match" — no guard needed.
      return { [head.name]: { some: filterValue } };
    case "except":
      return { [head.name]: { some: { NOT: filterValue } } };
    case "all": {
      if (parts.restRelations.length > 0) {
        throw new Error(
          "all() over a multi-hop relation chain is not supported"
        );
      }
      const base = { [head.name]: { every: filterValue } };
      if (nullableFields.size === 0) {
        return base;
      }
      // CEL all() errors when any element evaluation errors without a false witness; SQL
      // `every` would treat those elements as vacuously passing. Exclude rows holding any
      // element whose referenced nullable column is NULL. (`every` already rejects rows
      // with a definitive false witness, matching error absorption.)
      return {
        AND: [
          base,
          { [head.name]: { none: buildNullWitnessFilter(nullableFields) } },
        ],
      };
    }
    default:
      throw new Error(`Unexpected operator: ${operator}`);
  }
}

/**
 * Builds the filter for a NEGATED collection operator, encoding CEL's error semantics (see
 * buildNegatedFilter).
 */
function buildNegatedCollectionFilter(
  operator: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  if (operator === "exists_one" || operator === "filter") {
    throwUnsupportedCollectionOperator(operator);
  }

  const parts = buildCollectionLambdaParts(operator, operands, mapper);
  const { head, filterValue, nullableFields } = parts;

  switch (operator) {
    case "exists": {
      const base = { NOT: { [head.name]: { some: filterValue } } };
      if (nullableFields.size === 0) {
        return base;
      }
      // !exists is TRUE only when every element is definitively false: no P-match AND no
      // element whose evaluation is UNKNOWN (NULL column = missing attribute = CEL error).
      return {
        AND: [
          base,
          {
            NOT: wrapCollectionElementFilter(
              parts,
              buildNullWitnessFilter(nullableFields)
            ),
          },
        ],
      };
    }
    case "all":
      // !all is TRUE only with a definitive false witness, which also absorbs error
      // elements — exactly `some(NOT P)` in SQL (NULL columns keep NOT P UNKNOWN).
      return { [head.name]: { some: { NOT: filterValue } } };
    case "except": {
      // !except(c,P) = every element definitively matches P.
      const base = { [head.name]: { none: { NOT: filterValue } } };
      if (nullableFields.size === 0) {
        return base;
      }
      return {
        AND: [
          base,
          { [head.name]: { none: buildNullWitnessFilter(nullableFields) } },
        ],
      };
    }
    default:
      throw new Error(`Unexpected operator: ${operator}`);
  }
}

/**
 * Helper function to handle "map" operator
 */
function handleMapOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error("map requires exactly two operands");
  }

  const collection = assertDefined(
    operands[0],
    "map requires a collection operand"
  );
  const lambda = assertDefined(
    operands[1],
    "map requires a lambda operand"
  );

  if (!isNamedOperand(collection)) {
    throw new Error("First operand of map must be a collection reference");
  }

  if (!isOperatorOperand(lambda) || lambda.operator !== "lambda") {
    throw new Error("Second operand of map must be a lambda expression");
  }

  // Get variable name from lambda
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

  // Create scoped mapper for the collection
  const scopedMapper = createScopedMapper(
    collection.name,
    variable.name,
    mapper
  );

  const { relations } = resolveFieldReference(collection.name, mapper);
  if (!relations || relations.length === 0) {
    throw new Error("map operator requires a relation mapping");
  }

  // Use scoped mapper for resolving the projection
  const resolved = resolveFieldReference(projection.name, scopedMapper);
  const fieldName = getLeafField(resolved.path);
  const lastRelation = assertDefined(
    relations[relations.length - 1],
    "Relation mapping must contain at least one relation"
  );

  return buildNestedRelationFilter(relations, {
    [getPrismaRelationOperator(lastRelation)]: {
      select: { [fieldName]: true },
    },
  });
}

function buildImpossibleFilter(fieldRef: ResolvedFieldReference): PrismaFilter {
  return buildFieldFilter(fieldRef, "in", []);
}

const ARITHMETIC_OPERATORS = new Set(["add", "sub", "mult", "div"]);

/**
 * Translates `column ⊕ constant  CMP  value` (and its mirrored forms) by solving for the
 * column: Prisma where-filters cannot express column arithmetic, but linear arithmetic with
 * one constant side always rewrites to a plain comparison. Multiplying/dividing by a
 * negative constant mirrors directional operators. The rewrite happens in IEEE double space,
 * matching CEL: Cerbos attribute numbers are doubles, so this preserves check-time semantics
 * (e.g. `n * 0.1 == 0.3` solves to an unrepresentable fraction and matches no integer row,
 * exactly as CEL evaluates it).
 */
function handleArithmeticComparison(
  operator: string,
  arithExpr: OperatorOperand,
  otherOperand: PlanExpressionOperand,
  arithIsLeft: boolean,
  mapper: Mapper
): PrismaFilter {
  const arithOp = arithExpr.operator;
  const arithLeftOp = assertDefined(
    arithExpr.operands[0],
    `${arithOp} operator requires a left operand`
  );
  const arithRightOp = assertDefined(
    arithExpr.operands[1],
    `${arithOp} operator requires a right operand`
  );

  if (
    isOperatorOperand(otherOperand) &&
    ARITHMETIC_OPERATORS.has(otherOperand.operator) &&
    tryFoldValueExpression(otherOperand, mapper) === null
  ) {
    throw new Error(
      "Arithmetic on both sides of a comparison is not supported: the expression cannot be solved to a plain column filter"
    );
  }

  const arithLeft = resolveOperand(arithLeftOp, mapper);
  const arithRight = resolveOperand(arithRightOp, mapper);
  const other = resolveOperand(otherOperand, mapper);

  // Fully constant arithmetic: fold and compare the other side against the result. The
  // arithmetic side keeps its source position, so when the folded constant was written
  // FIRST (`1 + 2 < f`), directional operators mirror to keep the field on the left.
  if (isResolvedValue(arithLeft) && isResolvedValue(arithRight)) {
    const folded = foldArithmetic(arithOp, arithLeft.value, arithRight.value);
    if (!isResolvedFieldReference(other)) {
      throw new Error(
        `${arithOp} with two values requires a field reference on the other side`
      );
    }
    const effectiveOperator = arithIsLeft
      ? (MIRRORED_OPERATOR[operator] ?? operator)
      : operator;
    return buildComparisonFilter(other, effectiveOperator, folded);
  }

  if (!isResolvedValue(other)) {
    throw new Error(
      `${arithOp} operator with field references requires a value on the other side of the comparison`
    );
  }

  let fieldRef: ResolvedFieldReference;
  let constant: Value;
  let fieldIsLeft: boolean;

  if (isResolvedFieldReference(arithLeft) && isResolvedValue(arithRight)) {
    fieldRef = arithLeft;
    constant = arithRight.value;
    fieldIsLeft = true;
  } else if (
    isResolvedValue(arithLeft) &&
    isResolvedFieldReference(arithRight)
  ) {
    fieldRef = arithRight;
    constant = arithLeft.value;
    fieldIsLeft = false;
  } else {
    throw new Error(
      `${arithOp} operator requires exactly one field reference and one value, or two values`
    );
  }

  // The comparison operator was normalized arithmetic-side-first by the caller; if the
  // arithmetic was written on the RIGHT of the comparison, direction was already mirrored.
  let effectiveOperator = arithIsLeft
    ? operator
    : (MIRRORED_OPERATOR[operator] ?? operator);

  // String concatenation solving (eq/ne only).
  if (arithOp === "add" && typeof constant === "string") {
    if (effectiveOperator !== "eq" && effectiveOperator !== "ne") {
      throw new Error(
        `Operator ${effectiveOperator} is not supported with string concatenation`
      );
    }
    const solvedValue = solveAdd(other.value, constant, fieldIsLeft);
    if (solvedValue === null) {
      if (effectiveOperator === "eq") return buildImpossibleFilter(fieldRef);
      return {};
    }
    return buildComparisonFilter(fieldRef, effectiveOperator, solvedValue);
  }

  if (typeof constant !== "number" || typeof other.value !== "number") {
    throw new Error(`${arithOp} comparison requires numeric operands`);
  }

  let solved: number;
  switch (arithOp) {
    case "add":
      solved = other.value - constant;
      break;
    case "sub":
      if (fieldIsLeft) {
        // f - c CMP v  ⇔  f CMP v + c
        solved = other.value + constant;
      } else {
        // c - f CMP v  ⇔  -f CMP v - c  ⇔  f mirror(CMP) c - v
        solved = constant - other.value;
        effectiveOperator =
          MIRRORED_OPERATOR[effectiveOperator] ?? effectiveOperator;
      }
      break;
    case "mult":
      if (constant === 0) {
        throw new Error(
          "Multiplication by a constant zero must be folded by the Cerbos planner"
        );
      }
      solved = other.value / constant;
      if (constant < 0) {
        effectiveOperator =
          MIRRORED_OPERATOR[effectiveOperator] ?? effectiveOperator;
      }
      break;
    case "div":
      if (!fieldIsLeft) {
        throw new Error(
          "Division by a column is not supported: the comparison cannot be solved to a plain column filter"
        );
      }
      if (constant === 0) {
        throw new Error("Division by a constant zero is not supported");
      }
      solved = other.value * constant;
      if (constant < 0) {
        effectiveOperator =
          MIRRORED_OPERATOR[effectiveOperator] ?? effectiveOperator;
      }
      break;
    default:
      throw new Error(`Unsupported operator: ${arithOp}`);
  }

  return buildComparisonFilter(fieldRef, effectiveOperator, solved);
}

function foldArithmetic(operator: string, left: Value, right: Value): Value {
  if (
    operator === "add" &&
    (typeof left === "string" || typeof right === "string")
  ) {
    return String(left) + String(right);
  }
  if (typeof left !== "number" || typeof right !== "number") {
    throw new Error(`${operator} operator requires string or number operands`);
  }
  switch (operator) {
    case "add":
      return left + right;
    case "sub":
      return left - right;
    case "mult":
      return left * right;
    case "div":
      return left / right;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

function solveAdd(
  comparisonValue: Value,
  addConstant: Value,
  fieldIsLeft: boolean
): Value | null {
  if (typeof comparisonValue === "string" && typeof addConstant === "string") {
    if (fieldIsLeft) {
      if (!comparisonValue.endsWith(addConstant)) return null;
      return comparisonValue.slice(
        0,
        comparisonValue.length - addConstant.length
      );
    }
    if (!comparisonValue.startsWith(addConstant)) return null;
    return comparisonValue.slice(addConstant.length);
  }
  if (typeof comparisonValue === "number" && typeof addConstant === "number") {
    return comparisonValue - addConstant;
  }
  throw new Error("Type mismatch in add comparison");
}

type ConstantSegment = { type: "constant"; value: string };
type FieldSegment = { type: "field"; fieldRef: ResolvedFieldReference };
type HierarchySegment = ConstantSegment | FieldSegment;

type ConstantHierarchy = {
  type: "constant";
  segments: string[];
  raw: string;
  delimiter: string;
};

type FieldHierarchy = {
  type: "field";
  fieldRef: ResolvedFieldReference;
  delimiter: string;
};

type SegmentedHierarchy = {
  type: "segmented";
  segments: HierarchySegment[];
};

type ResolvedHierarchy = ConstantHierarchy | FieldHierarchy | SegmentedHierarchy;

function resolveHierarchy(
  expr: OperatorOperand,
  mapper: Mapper
): ResolvedHierarchy {
  const operands = expr.operands;

  if (operands.length === 2) {
    const strOperand = assertDefined(operands[0], "hierarchy requires operands");
    const delimOperand = assertDefined(
      operands[1],
      "hierarchy requires a delimiter"
    );
    if (!isValueOperand(delimOperand)) {
      throw new Error("hierarchy delimiter must be a value");
    }
    const delimiter = String(delimOperand.value);

    if (isValueOperand(strOperand)) {
      const raw = String(strOperand.value);
      return { type: "constant", segments: raw.split(delimiter), raw, delimiter };
    }
    if (isNamedOperand(strOperand)) {
      return {
        type: "field",
        fieldRef: resolveFieldReference(strOperand.name, mapper),
        delimiter,
      };
    }
    throw new Error("hierarchy(string, delimiter) requires a value or field operand");
  }

  if (operands.length === 1) {
    const inner = assertDefined(operands[0], "hierarchy requires an operand");

    if (isValueOperand(inner)) {
      const raw = String(inner.value);
      return { type: "constant", segments: raw.split("."), raw, delimiter: "." };
    }

    if (isNamedOperand(inner)) {
      return {
        type: "field",
        fieldRef: resolveFieldReference(inner.name, mapper),
        delimiter: ".",
      };
    }

    if (isOperatorOperand(inner) && inner.operator === "list") {
      const segments = inner.operands.map((op): HierarchySegment => {
        const resolved = resolveOperand(op, mapper);
        if (isResolvedValue(resolved)) {
          return { type: "constant", value: String(resolved.value) };
        }
        return { type: "field", fieldRef: resolved };
      });
      return { type: "segmented", segments };
    }

    throw new Error("hierarchy requires a value, field, or list operand");
  }

  throw new Error("hierarchy requires 1 or 2 operands");
}

function toSegments(resolved: ResolvedHierarchy): HierarchySegment[] {
  switch (resolved.type) {
    case "constant":
      return resolved.segments.map((s) => ({ type: "constant" as const, value: s }));
    case "segmented":
      return resolved.segments;
    case "field":
      throw new Error(
        "Cannot get segments from a field-reference hierarchy"
      );
  }
}

function normalizeHierarchy(
  h: ResolvedHierarchy,
  defaultDelimiter = "."
): ResolvedHierarchy {
  if (h.type !== "segmented") return h;
  const allConstant = h.segments.every(
    (s): s is ConstantSegment => s.type === "constant"
  );
  if (!allConstant) return h;
  const segments = (h.segments as ConstantSegment[]).map((s) => s.value);
  return {
    type: "constant",
    segments,
    raw: segments.join(defaultDelimiter),
    delimiter: defaultDelimiter,
  };
}

function checkPrefixConditions(
  shorter: HierarchySegment[],
  longer: HierarchySegment[]
): PrismaFilter | null {
  if (shorter.length > longer.length) return null;

  const conditions: PrismaFilter[] = [];

  for (let i = 0; i < shorter.length; i++) {
    const s = shorter[i]!;
    const l = longer[i]!;

    if (s.type === "constant" && l.type === "constant") {
      if (s.value !== l.value) return null;
    } else if (s.type === "field" && l.type === "constant") {
      conditions.push(buildFieldFilter(s.fieldRef, "equals", l.value));
    } else if (s.type === "constant" && l.type === "field") {
      conditions.push(buildFieldFilter(l.fieldRef, "equals", s.value));
    } else {
      throw new Error(
        "Cannot compare two field references in hierarchy overlap"
      );
    }
  }

  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0]!;
  return { AND: conditions };
}

function handleOverlapsOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper
): PrismaFilter {
  const [left, right] = extractHierarchyOperands("overlaps", operands, mapper);

  if (left.type === "field" || right.type === "field") {
    return handleFieldOverlaps(left, right);
  }

  const leftSegs = toSegments(left);
  const rightSegs = toSegments(right);

  const leftPrefixOfRight = checkPrefixConditions(leftSegs, rightSegs);
  const rightPrefixOfLeft = checkPrefixConditions(rightSegs, leftSegs);

  const validConditions = [leftPrefixOfRight, rightPrefixOfLeft].filter(
    (c): c is PrismaFilter => c !== null
  );

  if (validConditions.length === 0) {
    const allSegs = [...leftSegs, ...rightSegs];
    const fieldSeg = allSegs.find(
      (s): s is FieldSegment => s.type === "field"
    );
    if (fieldSeg) return buildImpossibleFilter(fieldSeg.fieldRef);
    throw new Error("Cannot determine overlap: no field references found");
  }

  if (validConditions.some((c) => Object.keys(c).length === 0)) return {};

  // When both directions are valid (equal-length hierarchies), they produce
  // identical conditions since the same segment pairs are compared in both.
  return validConditions[0]!;
}

function handleFieldOverlaps(
  left: ResolvedHierarchy,
  right: ResolvedHierarchy
): PrismaFilter {
  if (left.type === "field" && right.type === "field") {
    throw new Error("overlaps: cannot compare two field-reference hierarchies");
  }

  const field = (left.type === "field" ? left : right) as FieldHierarchy;
  const other = left.type === "field" ? right : left;

  if (other.type !== "constant") {
    throw new Error("overlaps: segmented hierarchies with field hierarchies are not supported");
  }

  const delimiter = field.delimiter;
  const otherRaw = other.segments.join(delimiter);
  const strictPrefixes = getStrictPrefixes(other.segments, delimiter);

  const conditions: PrismaFilter[] = [];
  if (strictPrefixes.length > 0) {
    conditions.push(buildFieldFilter(field.fieldRef, "in", strictPrefixes));
  }
  conditions.push(buildFieldFilter(field.fieldRef, "equals", otherRaw));
  conditions.push(buildFieldFilter(field.fieldRef, "startsWith", otherRaw + delimiter));

  if (conditions.length === 1) return conditions[0]!;
  return { OR: conditions };
}

function extractHierarchyOperands(
  operatorName: string,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): [ResolvedHierarchy, ResolvedHierarchy] {
  if (operands.length !== 2) {
    throw new Error(`${operatorName} requires exactly two operands`);
  }
  const leftOp = assertDefined(operands[0], `${operatorName} requires a left operand`);
  const rightOp = assertDefined(operands[1], `${operatorName} requires a right operand`);

  if (
    !isOperatorOperand(leftOp) || leftOp.operator !== "hierarchy" ||
    !isOperatorOperand(rightOp) || rightOp.operator !== "hierarchy"
  ) {
    throw new Error(`${operatorName} requires two hierarchy operands`);
  }

  return [
    normalizeHierarchy(resolveHierarchy(leftOp, mapper)),
    normalizeHierarchy(resolveHierarchy(rightOp, mapper)),
  ];
}

function getStrictPrefixes(segments: string[], delimiter: string): string[] {
  if (segments.length <= 1) return [];
  const prefixes: string[] = [];
  let current = segments[0]!;
  prefixes.push(current);
  for (let i = 1; i < segments.length - 1; i++) {
    current = current + delimiter + segments[i]!;
    prefixes.push(current);
  }
  return prefixes;
}

function handleAncestorDescendantOperator(
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  direction: "ancestor" | "descendant"
): PrismaFilter {
  const operatorName = direction === "ancestor" ? "ancestorOf" : "descendentOf";
  const [left, right] = extractHierarchyOperands(operatorName, operands, mapper);

  // ancestorOf(A, B) = A is strict prefix of B
  // descendentOf(A, B) = B is strict prefix of A
  const ancestor = direction === "ancestor" ? left : right;
  const descendant = direction === "ancestor" ? right : left;

  if (ancestor.type === "constant" && descendant.type === "field") {
    const prefix = ancestor.segments.join(descendant.delimiter) + descendant.delimiter;
    return buildFieldFilter(descendant.fieldRef, "startsWith", prefix);
  }

  if (ancestor.type === "field" && descendant.type === "constant") {
    const delimiter = ancestor.delimiter;
    const prefixes = getStrictPrefixes(descendant.segments, delimiter);
    if (prefixes.length === 0) {
      return buildImpossibleFilter(ancestor.fieldRef);
    }
    if (prefixes.length === 1) {
      return buildFieldFilter(ancestor.fieldRef, "equals", prefixes[0]!);
    }
    return buildFieldFilter(ancestor.fieldRef, "in", prefixes);
  }

  if (ancestor.type === "constant" && descendant.type === "constant") {
    const ancestorSegs = ancestor.segments;
    const descendantSegs = descendant.segments;
    if (
      descendantSegs.length > ancestorSegs.length &&
      ancestorSegs.every((seg, i) => seg === descendantSegs[i])
    ) {
      return {};
    }
    throw new Error(`${operatorName}: constants do not satisfy ${direction} relationship`);
  }

  throw new Error(`${operatorName}: unsupported hierarchy type combination`);
}

function buildFieldFilter(
  fieldRef: ResolvedFieldReference,
  prismaOp: string,
  value: Value
): PrismaFilter {
  const fieldName = getLeafField(fieldRef.path);
  return wrapRelations(fieldRef.relations, {
    [fieldName]: { [prismaOp]: value },
  });
}
