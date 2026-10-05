// Comparisons (`eq`, `ne`, `lt`, `le`, `gt`, `ge`) and membership (`in`): against a constant,
// between two columns, over size(), and over linear arithmetic solved for the column.

import type { PlanExpressionOperand, Value } from "@cerbos/core";

import { ARITHMETIC_OPERATORS, evaluateConstantComparison, foldArithmetic } from "./evaluate";
import {
  assertStringField,
  buildComparisonFilter,
  buildFieldFilter,
  buildMembershipFilter,
  rejectConstantFalse,
} from "./fields";
import { literalValue } from "./literals";
import type { PrismaFilter } from "./index";
import {
  currentScope,
  getLeafField,
  isExplicitNullReference,
  isResolvedFieldReference,
  isResolvedValue,
  namesToOneRelation,
  recordErroringNullElement,
  resolveFieldReference,
} from "./mapping";
import type {
  ResolvedFieldReference,
  ResolvedValue,
  TranslationContext,
} from "./mapping";
import {
  CERBOS_TO_PRISMA_OPERATOR,
  assertDefined,
  isNamedOperand,
  isOperatorOperand,
  isValueOperand,
  mirrorOperator,
  normalizeBinaryOperands,
} from "./plan";
import type { OperatorOperand } from "./plan";
import { relationFilter, wrapInRelations } from "./relations";
import { negateInterval, solveMonotone } from "./solve";
import type { DoubleInterval } from "./solve";
import { tryHandleTernaryComparison } from "./ternary";
import { roundSubMillisecond } from "./timestamp";
import { resolveOperand, tryFoldValueExpression } from "./translate";
import { UnsupportedQueryPlanError } from "./errors";

/**
 * Helper function to process relational operators (eq, ne, lt, etc.)
 */
export function handleRelationalOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  const ternaryFilter = tryHandleTernaryComparison(operator, operands, context);
  if (ternaryFilter) {
    return ternaryFilter;
  }
  const lookupFilter =
    tryHandleMapLiteralLookup(operator, operands, context) ??
    tryHandleUpperAsciiComparison(operator, operands, context);
  if (lookupFilter) {
    return lookupFilter;
  }

  ({ operator, operands } = normalizeBinaryOperands(operator, operands));
  if (!CERBOS_TO_PRISMA_OPERATOR[operator]) {
    throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }

  const leftOperand = operands.find(
    (o) => isNamedOperand(o) || isOperatorOperand(o)
  );
  if (!leftOperand) throw new UnsupportedQueryPlanError("No valid left operand found");

  const rightOperand = operands.find((o) => o !== leftOperand);
  if (!rightOperand) throw new UnsupportedQueryPlanError("No valid right operand found");

  if (isOperatorOperand(leftOperand) && leftOperand.operator === "size") {
    return handleSizeComparison(operator, leftOperand, rightOperand, context);
  }

  const arithOperand = [leftOperand, rightOperand].find(
    (o): o is OperatorOperand =>
      isOperatorOperand(o) && ARITHMETIC_OPERATORS.has(o.operator)
  );
  if (arithOperand) {
    const arithIsLeft = arithOperand === leftOperand;
    return handleArithmeticComparison(
      operator,
      arithOperand,
      arithIsLeft ? rightOperand : leftOperand,
      arithIsLeft,
      context
    );
  }

  if (
    [leftOperand, rightOperand].some(
      (o) => isOperatorOperand(o) && o.operator === "map"
    )
  ) {
    throw new UnsupportedQueryPlanError(
      `Direct comparison of map(...) to a value is not supported (operator: ${operator}). ` +
        `Wrap the map() expression in hasIntersection(map(...), [...]) instead.`
    );
  }

  assertNoRawTemporalAgainstTimestamp(leftOperand, rightOperand, context);

  // Only a comparison between a column and one constant can absorb a sub-millisecond instant.
  const oneColumn = isColumnOperand(leftOperand) !== isColumnOperand(rightOperand);
  const left = resolveOperand(leftOperand, context, oneColumn);
  const right = resolveOperand(rightOperand, context, oneColumn);

  if (isResolvedValue(left)) {
    if (isResolvedFieldReference(right)) {
      return buildValueComparisonFilter(
        context,
        right,
        mirrorOperator(operator),
        left
      );
    }
    return evaluateConstantComparison(operator, left.value, right.value)
      ? {}
      : rejectConstantFalse();
  }

  if (isResolvedFieldReference(right)) {
    if (
      left.valueType === "dateTime" &&
      right.valueType === "dateTime" &&
      (!left.timestampWrapped || !right.timestampWrapped)
    ) {
      throw new UnsupportedQueryPlanError(
        "Raw temporal column comparison loses RFC-3339 string spelling; wrap both operands in timestamp()"
      );
    }
    if (
      left.valueType !== undefined &&
      right.valueType !== undefined &&
      left.valueType !== right.valueType
    ) {
      throw new UnsupportedQueryPlanError("Cannot compare fields with different mapped value types");
    }
    return buildFieldToFieldFilter(
      context,
      operator,
      leftOperand,
      left,
      rightOperand,
      right
    );
  }

  return buildValueComparisonFilter(context, left, operator, right);
}

/** `{"k": v, ...}[x]`: an index into a map literal. */
function isMapLiteralLookup(operand: PlanExpressionOperand): operand is OperatorOperand {
  if (!isOperatorOperand(operand) || operand.operator !== "index" || operand.operands.length !== 2) {
    return false;
  }
  const [map] = operand.operands;
  return isOperatorOperand(map!) && map.operator === "struct";
}

/** Whether a map-literal lookup appears anywhere in `operand`. */
export function containsMapLiteralLookup(operand: PlanExpressionOperand): boolean {
  if (!isOperatorOperand(operand)) return false;
  return operand.operands.some(containsMapLiteralLookup) || isMapLiteralLookup(operand);
}

/**
 * `{"k1": v1, "k2": v2}[x] == v` (and `!=`, either side first), with `x` a string column: the keys
 * whose value equals `v` are exactly the values of `x` that make it true, and every other key the
 * values that make it false. Any `x` that is not a key is an error, true under neither polarity,
 * so the comparison and its negation are two membership tests, never one test and its NOT:
 * `negated` asks for the negation's filter, and buildNegatedFilter pushes every `not` above a
 * lookup down to it. Inside a lambda body a macro negates its body with a plain NOT, so the
 * lookup is refused there.
 */
export function tryHandleMapLiteralLookup(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext,
  negated = false
): PrismaFilter | null {
  if ((operator !== "eq" && operator !== "ne") || operands.length !== 2) return null;
  const lookupIndex = operands.findIndex(isMapLiteralLookup);
  if (lookupIndex === -1) return null;
  const lookup = operands[lookupIndex] as OperatorOperand;
  const other = operands[1 - lookupIndex]!;
  const [mapOperand, key] = lookup.operands as [PlanExpressionOperand, PlanExpressionOperand];
  const map = literalValue(mapOperand);
  if (
    map === undefined ||
    map === null ||
    typeof map !== "object" ||
    Array.isArray(map) ||
    !isValueOperand(other) ||
    !isNamedOperand(key)
  ) {
    throw new UnsupportedQueryPlanError(
      "An index into a map literal is translated only as a literal map indexed by a column " +
        "and compared with a literal: a Prisma filter has no map lookup"
    );
  }
  if (context.scopes.length > 0) {
    throw new UnsupportedQueryPlanError(
      "An index into a map literal inside a lambda body is not supported: a missing key is an " +
        "error under both polarities, and the macro negates its body with a plain NOT"
    );
  }
  const fieldRef = resolveFieldReference(key.name, context);
  if (fieldRef.valueType !== "string") {
    throw new UnsupportedQueryPlanError(
      `An index into a map literal requires a string key column, and ${key.name} is not mapped ` +
        'with valueType: "string"'
    );
  }
  const keys = Object.keys(map);
  const matching = keys.filter((k) => evaluateConstantComparison("eq", map[k]!, other.value));
  const holds = (operator === "eq") !== negated;
  return buildFieldFilter(
    fieldRef,
    "in",
    holds ? matching : keys.filter((k) => !matching.includes(k))
  );
}

/** Past this many spellings, `upperAscii(x) == "LIT"` is refused rather than enumerated. */
const MAX_UPPER_ASCII_SPELLINGS = 1024;

/**
 * `x.upperAscii() == "LIT"` (and `!=`, either side first), with `x` a string column: upperAscii
 * maps exactly the ASCII letters a-z to A-Z and leaves every other code point alone, so the
 * strings it maps to "LIT" are its spellings with each ASCII capital in either case — none at all
 * when the literal holds a lowercase ASCII letter. An IN over those spellings compares the bytes
 * exactly, where a store's UPPER would also fold non-ASCII letters. A NULL column stays UNKNOWN,
 * as upperAscii of a missing (or null) value is an error.
 */
function tryHandleUpperAsciiComparison(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter | null {
  if ((operator !== "eq" && operator !== "ne") || operands.length !== 2) return null;
  const callIndex = operands.findIndex(
    (operand) => isOperatorOperand(operand) && operand.operator === "upperAscii"
  );
  if (callIndex === -1) return null;
  const call = operands[callIndex] as OperatorOperand;
  const other = operands[1 - callIndex]!;
  const [subject] = call.operands;
  if (
    call.operands.length !== 1 ||
    subject === undefined ||
    !isNamedOperand(subject) ||
    !isValueOperand(other) ||
    typeof other.value !== "string"
  ) {
    throw new UnsupportedQueryPlanError(
      "upperAscii() is translated only as a column compared with a string literal: a Prisma " +
        "filter has no ASCII-only case mapping"
    );
  }
  const fieldRef = resolveFieldReference(subject.name, context);
  assertStringField(fieldRef, "upperAscii");
  recordErroringNullElement(context, subject, fieldRef);
  let spellings = [""];
  for (const character of other.value) {
    if (character >= "a" && character <= "z") {
      spellings = [];
      break;
    }
    const cases =
      character >= "A" && character <= "Z" ? [character, character.toLowerCase()] : [character];
    spellings = spellings.flatMap((prefix) => cases.map((c) => prefix + c));
    if (spellings.length > MAX_UPPER_ASCII_SPELLINGS) {
      throw new UnsupportedQueryPlanError(
        `Cannot translate upperAscii() == ${JSON.stringify(other.value)}: its spellings exceed ` +
          `the ${MAX_UPPER_ASCII_SPELLINGS}-entry limit`
      );
    }
  }
  const filter = buildFieldFilter(fieldRef, "in", spellings);
  return operator === "eq" ? filter : { NOT: filter };
}

/**
 * Refuses a bare `dateTime` column compared with a `timestamp(...)` operand. The attribute the
 * application sends for that column is its RFC 3339 STRING (a Cerbos attribute has no timestamp
 * type), and CEL has no overload ordering a string against a timestamp: `check()` raises an error,
 * which decides the row under neither polarity the way any column filter would. Wrapping the
 * attribute in timestamp() is what the policy means.
 */
function assertNoRawTemporalAgainstTimestamp(
  first: PlanExpressionOperand,
  second: PlanExpressionOperand,
  context: TranslationContext
): void {
  for (const [column, other] of [
    [first, second],
    [second, first],
  ] as const) {
    if (
      isNamedOperand(column) &&
      isOperatorOperand(other) &&
      other.operator === "timestamp" &&
      resolveFieldReference(column.name, context).valueType === "dateTime"
    ) {
      throw new UnsupportedQueryPlanError(
        `Cannot compare ${column.name} with a timestamp: the attribute holds an RFC 3339 ` +
          "string, and CEL has no overload comparing a string with a timestamp, so check() " +
          "raises an error that no column filter reproduces under both polarities; wrap the " +
          "attribute in timestamp()"
      );
    }
  }
}

/** A column reference, bare or wrapped in timestamp(). */
function isColumnOperand(operand: PlanExpressionOperand): boolean {
  if (isNamedOperand(operand)) return true;
  return (
    isOperatorOperand(operand) &&
    operand.operator === "timestamp" &&
    operand.operands.length === 1 &&
    isNamedOperand(operand.operands[0]!)
  );
}

/** buildComparisonFilter, for a resolved constant that may be a sub-millisecond timestamp. */
function buildValueComparisonFilter(
  context: TranslationContext,
  fieldRef: ResolvedFieldReference,
  operator: string,
  constant: ResolvedValue
): PrismaFilter {
  if (!constant.subMillisecond) {
    return buildComparisonFilter(context, fieldRef, operator, constant.value);
  }
  const rounded = roundSubMillisecond(operator);
  if (rounded !== null) {
    return buildComparisonFilter(context, fieldRef, rounded, constant.value);
  }
  // No whole millisecond equals the instant. Spelled as a contradiction on the column, so a NULL
  // stays UNKNOWN under both polarities — timestamp() of a missing attribute is an error.
  const never: PrismaFilter = {
    AND: [
      buildComparisonFilter(context, fieldRef, "lt", constant.value),
      buildComparisonFilter(context, fieldRef, "ge", constant.value),
    ],
  };
  return operator === "eq" ? never : { NOT: never };
}

/**
 * A size is a non-negative integer, so every `size(x) CMP n` — fractional, negative or huge `n`
 * included — is one of: at least `k`, exactly `k`, or the negation of either. `at least 0` is
 * true whenever `x` can be evaluated at all.
 */
type CountPredicate = {
  kind: "atLeast" | "exactly";
  count: number;
  negated: boolean;
};

function countPredicate(operator: string, n: number): CountPredicate {
  const atLeast = (count: number, negated = false): CountPredicate => ({
    kind: "atLeast",
    count: Math.max(count, 0),
    negated,
  });
  switch (operator) {
    case "gt":
      return atLeast(Math.floor(n) + 1);
    case "ge":
      return atLeast(Math.ceil(n));
    case "lt":
      return atLeast(Math.ceil(n), true);
    case "le":
      return atLeast(Math.floor(n) + 1, true);
    case "eq":
    case "ne": {
      const negated = operator === "ne";
      // A fractional size never equals anything; a negative one never exists.
      return Number.isInteger(n) && n >= 0
        ? { kind: "exactly", count: n, negated }
        : atLeast(0, !negated);
    }
    default:
      throw new UnsupportedQueryPlanError(`Unsupported operator: ${operator}`);
  }
}

/**
 * No store holds a string of 2^32 characters: PostgreSQL caps a value at 1 GB, SQLite at
 * 2^31 - 1 bytes and MySQL's LONGTEXT at 2^32 - 1 bytes. A length threshold at or past it is
 * decided without a pattern.
 */
const STRING_LENGTH_CEILING = 2 ** 32;

/** Past this, a length threshold short of the ceiling is refused rather than spelled out. */
const MAX_LENGTH_PATTERN = 1024;

/**
 * `size(column) CMP n` over a string column, as `LIKE` patterns of `_`: Prisma does not escape
 * the wildcards in a `startsWith` needle, so `startsWith: "_____"` is `LIKE '_____%'`, true exactly
 * when the value holds at least five characters. Each store's `_` matches one character — a code
 * point in a UTF-8 database — which is what CEL's size() counts. `startsWith: ""` is `LIKE '%'`,
 * true for every non-NULL value. Every filter built here is therefore UNKNOWN on a NULL column
 * under both polarities, as size() of a missing attribute is an error in CEL.
 */
function buildStringLengthFilter(
  fieldRef: ResolvedFieldReference,
  predicate: CountPredicate
): PrismaFilter {
  const atLeast = (count: number): PrismaFilter => {
    if (count >= STRING_LENGTH_CEILING) {
      return { NOT: buildFieldFilter(fieldRef, "startsWith", "") };
    }
    if (count > MAX_LENGTH_PATTERN) {
      throw new UnsupportedQueryPlanError(
        `Cannot translate a string length threshold of ${count}: the LIKE pattern it needs ` +
          `exceeds the ${MAX_LENGTH_PATTERN}-character limit`
      );
    }
    return buildFieldFilter(fieldRef, "startsWith", "_".repeat(count));
  };
  const filter =
    predicate.kind === "atLeast"
      ? atLeast(predicate.count)
      : { AND: [atLeast(predicate.count), { NOT: atLeast(predicate.count + 1) }] };
  return predicate.negated ? { NOT: filter } : filter;
}

/**
 * `size(collection) CMP n`. Over a relation, only the thresholds that mean "is empty", "is
 * non-empty" or "the chain reaching it exists" — the only counts a relation filter can express.
 * Over a string column, any threshold (see buildStringLengthFilter).
 */
function handleSizeComparison(
  operator: string,
  sizeOperand: OperatorOperand,
  valueOperand: PlanExpressionOperand,
  context: TranslationContext
): PrismaFilter {
  const collectionOperand = sizeOperand.operands[0];
  if (collectionOperand && isOperatorOperand(collectionOperand)) {
    throw new UnsupportedQueryPlanError(
      `size() of ${collectionOperand.operator}(...) counts the elements of a derived list, and a ` +
        "Prisma relation filter can only test whether some, every or no related row matches, " +
        "never count them"
    );
  }
  if (!collectionOperand || !isNamedOperand(collectionOperand)) {
    throw new UnsupportedQueryPlanError("size operator requires a named collection operand");
  }

  if (!isValueOperand(valueOperand)) {
    throw new UnsupportedQueryPlanError("size comparison requires a numeric value operand");
  }

  const count = valueOperand.value;
  if (typeof count !== "number" || !Number.isFinite(count)) {
    throw new UnsupportedQueryPlanError("size comparison requires a numeric value");
  }
  const predicate = countPredicate(operator, count);

  const fieldRef = resolveFieldReference(collectionOperand.name, context);
  const { relations } = fieldRef;
  if (!relations || relations.length === 0) {
    if (fieldRef.valueType === "string") {
      return buildStringLengthFilter(fieldRef, predicate);
    }
    throw new UnsupportedQueryPlanError("size operator requires a relation mapping");
  }

  // The count applies to the deepest relation; every relation before it is a hop to reach it.
  const hops = relations.slice(0, -1);
  const deepest = relations[relations.length - 1]!;
  const { kind, count: k, negated } = predicate;
  // `size >= 0` holds whenever the collection can be reached. Only a chain can fail to reach it;
  // a direct relation would be an unconditional `{}`, which no negation could then express.
  if (kind === "atLeast" && k === 0 && !negated && hops.length > 0) {
    return wrapInRelations(hops, {});
  }
  const isNonEmpty =
    (kind === "atLeast" && k === 1 && !negated) ||
    (kind === "exactly" && k === 0 && negated);
  const isEmpty =
    (kind === "atLeast" && k === 1 && negated) ||
    (kind === "exactly" && k === 0 && !negated);

  if (!isNonEmpty && !isEmpty) {
    throw new UnsupportedQueryPlanError(
      `Unsupported size comparison: size(...) ${operator} ${count}`
    );
  }

  return wrapInRelations(
    hops,
    relationFilter(deepest, isNonEmpty ? "some" : "none", {})
  );
}

/**
 * Builds a column-vs-column comparison as a Prisma field reference. Prisma only supports
 * references between fields of the SAME model, so the container is the root model for
 * root-level comparisons and the relation's model inside a lambda; mixed-scope comparisons
 * (an element column against an outer column) are cross-model and must fail loudly.
 */
function buildFieldToFieldFilter(
  context: TranslationContext,
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
    throw new UnsupportedQueryPlanError("Field-to-field comparison requires two named operands");
  }

  const container = fieldReferenceContainer(
    context,
    leftOperand.name,
    left,
    rightOperand.name,
    right
  );
  const leftField = getLeafField(left.path);
  const rightField = getLeafField(right.path);
  const fieldReference = { _ref: rightField, _container: container };

  // Two explicit nulls are EQUAL in CEL, and a null against a value is definitely NOT equal.
  // A bare field reference gives UNKNOWN for both, which drops the row under either polarity.
  const leftExplicit = isExplicitNullReference(left);
  const rightExplicit = isExplicitNullReference(right);
  if (operator === "eq" || operator === "ne") {
    // Mixing the two conventions across one comparison has no faithful rendering. The declared
    // side needs a definite answer for its NULL (CEL holds a null VALUE); the undeclared side
    // needs UNKNOWN for its NULL (a missing attribute, which CEL denies under both polarities). A
    // definite predicate returns rows the PDP refuses; a plain one drops rows the PDP allows.
    // Refuse it rather than pick a direction — declare both attributes, or neither.
    // Mixing the two conventions across one comparison: the explicit side's NULL is a null VALUE,
    // which `!=` answers TRUE against any present value, while the other side's NULL is a missing
    // attribute, an error under both polarities. `other = other` is TRUE for a present value and
    // UNKNOWN for a NULL one, so it carries exactly that error into the explicit-null arm:
    //
    //   explicit NULL, other present -> TRUE        explicit NULL, other NULL -> UNKNOWN
    //   explicit present, other NULL -> UNKNOWN     both present -> the plain comparison
    //
    // and `==` is its negation.
    if (leftExplicit !== rightExplicit) {
      const [explicitField, otherField] = leftExplicit
        ? [leftField, rightField]
        : [rightField, leftField];
      const otherReference = { _ref: otherField, _container: container };
      const inequality: PrismaFilter = {
        OR: [
          {
            AND: [
              { [explicitField]: null },
              { [otherField]: { equals: otherReference } },
            ],
          },
          { NOT: { [explicitField]: { equals: otherReference } } },
        ],
      };
      return operator === "ne" ? inequality : { NOT: inequality };
    }
    if (leftExplicit) {
      const equality: PrismaFilter = {
        OR: [
          { AND: [{ [leftField]: null }, { [rightField]: null }] },
          {
            AND: [
              { [leftField]: { not: null } },
              { [rightField]: { not: null } },
              { [leftField]: { equals: fieldReference } },
            ],
          },
        ],
      };
      return operator === "eq" ? equality : { NOT: equality };
    }
  }

  // `ne` is the one operator whose Prisma spelling cannot carry a field reference: `not` nests a
  // NestedStringFilter, which has no `_ref` form, so `{ col: { not: { _ref } } }` is rejected by
  // the client rather than executed. Hoisting the negation to the top-level `NOT` combinator says
  // the same thing with a shape that does accept one — the same rewrite the explicit-null branch
  // above already performs. Three-valued logic is unchanged by the hoist: a NULL on either side
  // makes the inner equality UNKNOWN, and `NOT UNKNOWN` is still UNKNOWN, so the row stays out
  // under both polarities, which is what a missing attribute does on the check side.
  if (operator === "ne") {
    return { NOT: { [leftField]: { equals: fieldReference } } };
  }

  return { [leftField]: { [prismaOperator]: fieldReference } };
}

/**
 * The Prisma model both columns of a field-to-field comparison belong to: the relation model
 * inside a lambda when both are element columns, otherwise the root model.
 */
function fieldReferenceContainer(
  context: TranslationContext,
  leftName: string,
  left: ResolvedFieldReference,
  rightName: string,
  right: ResolvedFieldReference
): string {
  const scope = currentScope(context);
  if (scope) {
    const prefix = scope.variableName + ".";
    const leftInScope = leftName.startsWith(prefix);
    const rightInScope = rightName.startsWith(prefix);
    if (leftInScope !== rightInScope) {
      throw new UnsupportedQueryPlanError(
        `Cannot compare a collection element column with an outer column (${leftName} vs ${rightName}): ` +
          "Prisma field references only work between fields of the same model"
      );
    }
    if (leftInScope) {
      if (!scope.relationModel) {
        throw new Error(
          "Field-to-field comparison inside a collection requires `relation.model` in the mapper"
        );
      }
      return scope.relationModel;
    }
  }

  if (
    (left.relations && left.relations.length > 0) ||
    (right.relations && right.relations.length > 0)
  ) {
    throw new UnsupportedQueryPlanError(
      "Cannot compare columns across relations: Prisma field references only work between fields of the same model"
    );
  }
  if (!context.rootModel) {
    throw new Error(
      "Field-to-field comparison requires the `model` option (the Prisma model name) to build a field reference"
    );
  }
  return context.rootModel;
}

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
  context: TranslationContext
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
    tryFoldValueExpression(otherOperand, context) === null
  ) {
    throw new UnsupportedQueryPlanError(
      "Arithmetic on both sides of a comparison is not supported: the expression cannot be solved to a plain column filter"
    );
  }

  const arithLeft = resolveOperand(arithLeftOp, context);
  const arithRight = resolveOperand(arithRightOp, context);
  const other = resolveOperand(otherOperand, context);

  // Fully constant arithmetic: fold and compare the other side against the result. The
  // arithmetic side keeps its source position, so when the folded constant was written
  // FIRST (`1 + 2 < f`), directional operators mirror to keep the field on the left.
  if (isResolvedValue(arithLeft) && isResolvedValue(arithRight)) {
    const folded = foldArithmetic(arithOp, arithLeft.value, arithRight.value);
    if (!isResolvedFieldReference(other)) {
      throw new UnsupportedQueryPlanError(
        `${arithOp} with two values requires a field reference on the other side`
      );
    }
    return buildComparisonFilter(
      context,
      other,
      arithIsLeft ? mirrorOperator(operator) : operator,
      folded
    );
  }

  if (!isResolvedValue(other)) {
    throw new UnsupportedQueryPlanError(
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
    throw new UnsupportedQueryPlanError(
      `${arithOp} operator requires exactly one field reference and one value, or two values`
    );
  }

  // The comparison operator was normalized arithmetic-side-first by the caller; if the
  // arithmetic was written on the RIGHT of the comparison, direction was already mirrored.
  let effectiveOperator = arithIsLeft ? operator : mirrorOperator(operator);

  // String concatenation solving (eq/ne only).
  if (arithOp === "add" && typeof constant === "string") {
    if (effectiveOperator !== "eq" && effectiveOperator !== "ne") {
      throw new UnsupportedQueryPlanError(
        `Operator ${effectiveOperator} is not supported with string concatenation`
      );
    }
    const solvedValue = solveAdd(other.value, constant, fieldIsLeft);
    if (solvedValue === null) {
      // A contradictory / exhaustive pair remains SQL UNKNOWN for a missing field.
      // An empty IN or an empty filter loses that error when an outer NOT is applied.
      const equality = buildFieldFilter(fieldRef, "equals", other.value);
      const inequality = buildFieldFilter(fieldRef, "not", other.value);
      return effectiveOperator === "eq"
        ? { AND: [equality, inequality] }
        : { OR: [equality, inequality] };
    }
    return buildComparisonFilter(context, fieldRef, effectiveOperator, solvedValue);
  }

  if (typeof constant !== "number" || typeof other.value !== "number") {
    throw new UnsupportedQueryPlanError(`${arithOp} comparison requires numeric operands`);
  }

  if (arithOp === "mult" && constant === 0) {
    throw new UnsupportedQueryPlanError(
      "Multiplication by a constant zero must be folded by the Cerbos planner"
    );
  }
  if (arithOp === "div" && !fieldIsLeft) {
    throw new UnsupportedQueryPlanError(
      "Division by a column is not supported: the comparison cannot be solved to a plain column filter"
    );
  }
  if (arithOp === "div" && constant === 0) {
    throw new UnsupportedQueryPlanError("Division by a constant zero is not supported");
  }
  if (!ARITHMETIC_OPERATORS.has(arithOp)) {
    throw new UnsupportedQueryPlanError(`Unsupported operator: ${arithOp}`);
  }

  // The column side as a non-decreasing function of `y`, where `y` is the column or, when the
  // arithmetic reverses the order (`c - x`, a negative multiplier or divisor), its negation —
  // negating a double is exact, so `c - x` is `c + y` and `x * c` is `y * -c`.
  const c = constant;
  const decreasing =
    (arithOp === "sub" && !fieldIsLeft) || ((arithOp === "mult" || arithOp === "div") && c < 0);
  const f = (y: number): number => {
    switch (arithOp) {
      case "add":
        return y + c;
      case "sub":
        return fieldIsLeft ? y - c : c + y;
      case "mult":
        return decreasing ? y * -c : y * c;
      default:
        return decreasing ? y / -c : y / c;
    }
  };
  const solved = solveMonotone(
    f,
    effectiveOperator === "ne" ? "eq" : (effectiveOperator as "eq" | "lt" | "le" | "gt" | "ge"),
    other.value
  );
  const interval = decreasing ? negateInterval(solved) : solved;
  const filter = buildIntervalFilter(context, fieldRef, interval);
  return effectiveOperator === "ne" ? { NOT: filter } : filter;
}

/**
 * `lo <= column <= hi`. An empty interval is a contradiction and an unbounded one a tautology,
 * both spelled on the column, so a NULL keeps them UNKNOWN as the arithmetic's error requires.
 */
function buildIntervalFilter(
  context: TranslationContext,
  fieldRef: ResolvedFieldReference,
  interval: DoubleInterval
): PrismaFilter {
  if (interval.empty) {
    return {
      AND: [
        buildComparisonFilter(context, fieldRef, "gt", 0),
        buildComparisonFilter(context, fieldRef, "le", 0),
      ],
    };
  }
  const bounds: PrismaFilter[] = [];
  if (interval.lo !== undefined) {
    bounds.push(buildComparisonFilter(context, fieldRef, "ge", interval.lo));
  }
  if (interval.hi !== undefined) {
    bounds.push(buildComparisonFilter(context, fieldRef, "le", interval.hi));
  }
  if (bounds.length === 0) {
    return {
      OR: [
        buildComparisonFilter(context, fieldRef, "gt", 0),
        buildComparisonFilter(context, fieldRef, "le", 0),
      ],
    };
  }
  // Each bound may itself be a bracket (see fractionalBracket); one flat AND reads better.
  const flat = bounds.flatMap((bound) =>
    Object.keys(bound).length === 1 && Array.isArray(bound["AND"]) ? bound["AND"] : [bound]
  );
  return flat.length === 1 ? flat[0]! : { AND: flat };
}

/**
 * The column value that makes `column + addConstant` (or `addConstant + column`) equal
 * `comparisonValue`, or null when no string does.
 */
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
  throw new UnsupportedQueryPlanError("Type mismatch in add comparison");
}

/**
 * Helper function to handle "in" operator
 */
export function handleInOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  if (operands.length !== 2) {
    throw new UnsupportedQueryPlanError("in requires exactly two operands");
  }
  const ternaryFilter = tryHandleTernaryComparison("in", operands, context);
  if (ternaryFilter) {
    return ternaryFilter;
  }
  const member = assertDefined(operands[0], "in requires a member operand");
  const collection = assertDefined(
    operands[1],
    "in requires a collection operand"
  );

  if (isNamedOperand(member) && isValueOperand(collection)) {
    const fieldRef = resolveFieldReference(member.name, context);
    const values = Array.isArray(collection.value)
      ? collection.value
      : [collection.value];
    return buildMembershipFilter(context, fieldRef, values);
  }

  if (isValueOperand(member) && isNamedOperand(collection)) {
    if (namesToOneRelation(context.mapper, collection.name)) {
      throw new UnsupportedQueryPlanError(
        `in over ${collection.name}: it is a to-one relation, which CEL reads as a map, and ` +
          "`in` over a map tests its keys (the attribute names present on the related row); " +
          "a Prisma relation filter tests rows, not attribute names"
      );
    }
    return buildMembershipFilter(
      context,
      resolveFieldReference(collection.name, context),
      [member.value]
    );
  }

  if (isOperatorOperand(collection) && (collection.operator === "list" || collection.operator === "add")) {
    throw new UnsupportedQueryPlanError(
      `Membership in a list built by ${collection.operator}(...) from columns: each element is ` +
        "compared with the member, and a Prisma field reference compares two plain columns of " +
        "one model, never a column with an expression over another column or with the " +
        "elements of a related list"
    );
  }

  if (isNamedOperand(member) && isNamedOperand(collection)) {
    throw new UnsupportedQueryPlanError(
      "Membership between two database attributes is not supported: Prisma cannot compare a related element column with an outer scalar column"
    );
  }

  throw new UnsupportedQueryPlanError(
    "in requires a field member and literal list, or a literal member and field collection"
  );
}
