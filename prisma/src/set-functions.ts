// hasAll, hasAny, hasOnly and hasExactly (CEL list extensions, Cerbos 0.58) rewritten into the
// hasIntersection and isSubset calls the translator already lowers. Each rewrite is exact in CEL,
// errors included; a call without exactly one literal list operand is left for the translator to
// refuse.

import type { PlanExpression, PlanExpressionOperand, Value } from "@cerbos/core";

const SET_FUNCTIONS = new Set(["hasAll", "hasAny", "hasOnly", "hasExactly"]);

const call = (operator: string, operands: PlanExpressionOperand[]): PlanExpression => ({
  operator,
  operands,
});

const literalValues = (operand: PlanExpressionOperand): Value[] | undefined =>
  "value" in operand && Array.isArray(operand.value) ? operand.value : undefined;

export function rewriteSetFunctions(expr: PlanExpressionOperand): PlanExpressionOperand {
  if (!("operator" in expr)) return expr;

  const operands = expr.operands.map(rewriteSetFunctions);

  return setFunction(expr.operator, operands) ?? call(expr.operator, operands);
}

function setFunction(
  called: string,
  operands: PlanExpressionOperand[],
): PlanExpressionOperand | undefined {
  const [first, second] = operands;

  if (!SET_FUNCTIONS.has(called) || operands.length !== 2 || !first || !second) {
    return undefined;
  }

  const firstValues = literalValues(first);
  const secondValues = literalValues(second);

  if ((firstValues === undefined) === (secondValues === undefined)) return undefined;

  const flipped = firstValues !== undefined;
  const list = flipped ? second : first;
  const literal = flipped ? first : second;
  const values = firstValues ?? secondValues ?? [];
  const subset = (): PlanExpression => call("isSubset", [list, literal]);

  const operator =
    flipped && called === "hasAll" ? "hasOnly" : flipped && called === "hasOnly" ? "hasAll" : called;

  switch (operator) {
    case "hasAny":
      return call("hasIntersection", [list, literal]);
    case "hasAll":
      return containsAll(list, values);
    case "hasOnly":
      return subset();
    default:
      return call("and", [containsAll(list, values), subset()]);
  }
}

function containsAll(list: PlanExpressionOperand, values: Value[]): PlanExpressionOperand {
  const intersects = (value: Value[]): PlanExpression =>
    call("hasIntersection", [list, { value }]);

  if (values.length === 0) return call("not", [intersects([])]);

  const tests = values.map((value) => intersects([value]));
  const [only] = tests;

  return tests.length === 1 && only ? only : call("and", tests);
}
