import type { PlanExpressionOperand } from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import type { Mapper } from "./index";
import { lambdaComponents, operatorFor } from "./evaluate";
import {
  isExpression,
  isMappedReference,
  isValue,
  isVariable,
} from "./operands";

// The refusals made before any filter exists. The invariant is that a shape the adapter cannot
// express must throw at translation, never emit a filter, so every check here runs over the whole
// plan up front rather than at whichever emission site first meets the shape.

/**
 * Every node is well formed and uses an operator the adapter knows, and each operator's own
 * translation-time check (`validate` on its `OPERATORS` entry) passes. Pre-order, so the outermost
 * offending node is the one reported.
 */
export const validateStructure = (expression: PlanExpressionOperand): void => {
  if (isValue(expression) || isVariable(expression)) return;
  if (!isExpression(expression)) {
    throw new UnsupportedQueryPlanError("Invalid Cerbos expression structure");
  }
  const operator = operatorFor(expression.operator);
  if (!operator) {
    throw new UnsupportedQueryPlanError(
      `Unsupported operator: ${expression.operator}`,
    );
  }
  operator.validate?.(expression);
  for (const op of expression.operands) {
    validateStructure(op);
  }
};

// `filter()` and `map()` return a list, not a boolean. CEL raises for a non-boolean condition
// (deny), and the post-filter's asBoolean() reads a list as an evaluation error — which
// happens to deny every row and so to AGREE with the oracle, silently, while still emitting a
// filter for a shape that has no boolean meaning. The conformance contract forbids that:
// translating it means choosing a meaning the policy never stated. Throw instead.
//
// Every boolean POSITION is checked, not just the root. `and(filter(...), aBool)` puts the
// macro one level down, where the recursion dispatches on the operator and the root check
// never runs — the position was deciding rather than the shape (`collection/filter/as-conjunct`,
// cerbos/query-plan-adapters#387). The walk stops at `and`/`or`/`not` because those are the
// only operators whose operands are themselves conditions; inside `size()` a list is exactly
// what is wanted, and both macros stay translatable there.
export const assertNoListValuedCondition = (
  expression: PlanExpressionOperand,
): void => {
  if (!isExpression(expression)) return;
  if (expression.operator === "filter" || expression.operator === "map") {
    throw new UnsupportedQueryPlanError(
      `${expression.operator}() returns a list, not a boolean, so it cannot be a condition ` +
        "on its own; only size() over its result has a boolean meaning",
    );
  }
  if (
    expression.operator === "and" ||
    expression.operator === "or" ||
    expression.operator === "not"
  ) {
    for (const operand of expression.operands) {
      assertNoListValuedCondition(operand);
    }
  }
};

const carriesNullLiteral = (operand: PlanExpressionOperand): boolean =>
  isValue(operand) &&
  (operand.value === null ||
    (Array.isArray(operand.value) && operand.value.includes(null)));

/**
 * Rejects every null literal operand in the plan when the caller omits attributes for NULL
 * fields.
 *
 * Under the `"omitted"` representation a NULL field carries no attribute, so CEL raises a
 * missing-attribute error and `check()` denies the document; matching null would return exactly
 * the documents the PDP refuses. Convex translates a plan down two paths — a pushed-down
 * `q.eq(...)` filter and an in-memory `postFilter` — so the check runs once over the plan tree
 * rather than at each emission site.
 *
 * The scan matches on the OPERAND, never on an allowlist of operators. A null constant reaches a
 * null-selecting predicate through more shapes than the obvious `eq`/`ne`/`in` — `hasIntersection`
 * carries one in its value list too — and any operator added later would silently escape a list
 * that has to be maintained by hand.
 *
 * The rejection is also deliberately wider than the over-granting shapes: `ne(x, null)` on its own
 * is aligned, but negation is applied around the built predicate, so a leaf cannot tell whether an
 * enclosing `not` will flip a not-null predicate back into a null-selecting one. Rejecting every
 * null operand is correct under any nesting; narrowing it requires negation-parity tracking.
 */
export const assertNoNullComparisonOperands = (
  expression: PlanExpressionOperand,
): void => {
  if (!isExpression(expression)) return;

  if (expression.operands.some(carriesNullLiteral)) {
    throw new UnsupportedQueryPlanError(
      `Cannot translate \`${expression.operator}\` against a null operand under ` +
        'nullAttributeRepresentation "omitted": a NULL field sends no attribute, so Cerbos ' +
        "evaluates the comparison as a missing-attribute error (deny) while a null-selecting " +
        "filter would return those documents. Send NULL fields as explicit nulls and use " +
        '"explicit", or keep this shape out of the policy.',
    );
  }

  for (const operand of expression.operands) {
    assertNoNullComparisonOperands(operand);
  }
};

/**
 * Every plan reference outside a lambda's own variable has a mapper entry.
 *
 * An unmapped reference used to be read verbatim as a document path. A plan path such as
 * `request.resource.attr.status` names no field any document stores, and the path is absent from
 * every document, which the pushed-down `q.neq(...)` and a negated comparison both read as a
 * match — so a missing entry turned `R.attr.status != "x"` into a filter returning every document
 * (cerbos/query-plan-adapters#492). Refused up front, over the whole plan, because a reference can
 * reach either half of the output and the post-filter would otherwise only meet it per document.
 * A caller whose documents really are shaped like the plan paths declares each one with an entry
 * that names no `field`.
 */
export const assertEveryReferenceMapped = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
  bound: ReadonlySet<string> = new Set(),
): void => {
  if (isVariable(expression)) {
    const root = expression.name.split(".")[0]!;
    if (bound.has(root) || isMappedReference(expression.name, mapper)) return;
    throw new Error(
      `No mapper entry for ${expression.name}: an unmapped reference is not used verbatim as ` +
        "a document path, because a path absent from every document satisfies a negated " +
        "comparison on every document. Map it to a field, or declare it with an entry to use " +
        "the plan path as-is.",
    );
  }
  if (!isExpression(expression)) return;
  if (expression.operator === "lambda") {
    const { body, variable } = lambdaComponents(expression);
    assertEveryReferenceMapped(
      body,
      mapper,
      new Set([...bound, variable.name]),
    );
    return;
  }
  // `get-field`'s second operand is the field name, not a reference.
  const operands =
    expression.operator === "get-field"
      ? expression.operands.slice(0, 1)
      : expression.operands;
  for (const operand of operands) {
    assertEveryReferenceMapped(operand, mapper, bound);
  }
};
