import type {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";

import { UnsupportedQueryPlanError } from "./errors";
import {
  CelDuration,
  EVALUATION_ERROR,
  asBoolean,
  checkedDuration,
  checkedTimestamp,
  compareValues,
  convertToDouble,
  convertToInt,
  convertToString,
  getNestedValue,
  intArithmetic,
  isEvaluationError,
  Hierarchy,
  hierarchiesOverlap,
  isRecord,
  isStrictAncestor,
  parseCelDuration,
  parseRfc3339Timestamp,
  splitHierarchy,
  timeSince,
  valuesEqual,
} from "./cel";
import type { ArithmeticOperator } from "./cel";
import type { Mapper } from "./index";
import {
  isExpression,
  isValue,
  isVariable,
  operandAt,
  resolveField,
} from "./operands";
import type { ComparisonOperator } from "./operands";
import { matchesSafeRegexPattern, parseSafeRegexPattern } from "./regex";

/**
 * The adapter's own CEL evaluator: the post-filter that answers, one document at a time, every
 * part of a plan Convex's filter engine cannot.
 *
 * `OPERATORS` is the whole operator roster. An operator is known to the adapter exactly when it
 * has an entry here — structural validation refuses any other — so adding one is adding an entry:
 * how it evaluates, and, when a plan can carry a form of it the evaluator cannot answer faithfully,
 * the translation-time check that refuses that form before a filter exists.
 */

type Bindings = Record<string, unknown>;

interface Scope {
  doc: Record<string, unknown>;
  mapper: Mapper;
  /** Lambda variables in scope, by name. */
  bindings: Bindings;
  /**
   * Whether a stored `null` reads as a missing attribute: the `"omitted"` convention, under which
   * the caller sends no attribute for a NULL field and CEL raises a missing-attribute error.
   */
  nullIsMissing: boolean;
}

interface Call {
  operator: string;
  operands: PlanExpressionOperand[];
  scope: Scope;
}

interface Operator {
  evaluate: (call: Call) => unknown;
  /** Throws for a node of this operator that has no faithful translation. */
  validate?: (expression: PlanExpression) => void;
}

/** Evaluates `operand` against one document. */
export const evaluate = (
  operand: PlanExpressionOperand,
  scope: Scope,
): unknown => {
  if (isValue(operand)) return operand.value;
  if (isVariable(operand)) return lookUp(operand.name, scope);
  if (!isExpression(operand)) {
    throw new UnsupportedQueryPlanError("Invalid Cerbos expression structure");
  }
  const operator = operatorFor(operand.operator);
  if (!operator) {
    throw new UnsupportedQueryPlanError(
      `Unsupported operator: ${operand.operator}`,
    );
  }
  return operator.evaluate({
    operator: operand.operator,
    operands: operand.operands,
    scope,
  });
};

/**
 * A lambda variable (or a path through one) first, then the mapped document field. A bare lambda
 * variable is a list element, which a caller cannot omit, so only a path reads a null as missing.
 */
const lookUp = (name: string, scope: Scope): unknown => {
  const { doc, mapper, bindings } = scope;
  const dotIdx = name.indexOf(".");
  if (dotIdx !== -1) {
    const root = name.substring(0, dotIdx);
    if (root in bindings) {
      return readPath(
        getNestedValue(bindings[root], name.substring(dotIdx + 1)),
        scope,
      );
    }
  }
  if (name in bindings) return bindings[name];
  return readPath(getNestedValue(doc, resolveField(name, mapper)), scope);
};

const readPath = (value: unknown, { nullIsMissing }: Scope): unknown =>
  nullIsMissing && value === null ? EVALUATION_ERROR : value;

/** Evaluates the operand at `index`, failing with `"<operator> <role>"` when it is missing. */
const arg = (call: Call, index: number, role: string): unknown =>
  evaluate(
    operandAt(call.operands, index, `${call.operator} ${role}`),
    call.scope,
  );

// -- operator families ---------------------------------------------------------------------------

/** CEL's commutative `&&` / `||`: a deciding operand wins over an error in any position. */
const junction =
  (decisive: boolean) =>
  ({ operands, scope }: Call): unknown => {
    let sawError = false;
    for (const operand of operands) {
      const value = asBoolean(evaluate(operand, scope));
      if (value === decisive) return decisive;
      if (isEvaluationError(value)) sawError = true;
    }
    return sawError ? EVALUATION_ERROR : !decisive;
  };

const comparison = (call: Call): unknown =>
  compareValues(
    call.operator as ComparisonOperator,
    arg(call, 0, "left operand"),
    arg(call, 1, "right operand"),
  );

const stringTest =
  (argumentRole: string, test: (receiver: string, arg: string) => boolean) =>
  (call: Call): unknown => {
    const receiver = arg(call, 0, "receiver");
    const argument = arg(call, 1, argumentRole);
    return typeof receiver === "string" && typeof argument === "string"
      ? test(receiver, argument)
      : EVALUATION_ERROR;
  };

const looksLikeLambdaVariable = (
  operand: PlanExpressionOperand,
): operand is PlanExpressionVariable =>
  isVariable(operand) && !operand.name.includes(".");

/**
 * A lambda's body and its variables. With one variable the planner does not fix the order of the
 * two operands: the variable is the bare name. CEL's two-variable comprehensions (`exists(i, v,
 * ...)`, `all(k, v, ...)`) arrive as `lambda(body, first, second)`, the planner appending the
 * variables after the body in declaration order — the index or key first, the element or value
 * second.
 */
export const lambdaComponents = (
  lambda: PlanExpressionOperand,
): { body: PlanExpressionOperand; variables: PlanExpressionVariable[] } => {
  if (!isExpression(lambda) || lambda.operator !== "lambda") {
    throw new UnsupportedQueryPlanError("Expected a lambda operand");
  }
  if (lambda.operands.length === 3) {
    const [body, first, second] = lambda.operands as [
      PlanExpressionOperand,
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];
    if (looksLikeLambdaVariable(first) && looksLikeLambdaVariable(second)) {
      return { body, variables: [first, second] };
    }
    throw new UnsupportedQueryPlanError(
      "A two-variable lambda requires its body followed by two variable operands",
    );
  }
  if (lambda.operands.length !== 2) {
    throw new UnsupportedQueryPlanError(
      "Lambda requires a body and one or two variables",
    );
  }
  const first = operandAt(lambda.operands, 0, "Lambda body is required");
  const second = operandAt(lambda.operands, 1, "Lambda variable is required");
  if (looksLikeLambdaVariable(second)) {
    return { body: first, variables: [second] };
  }
  if (looksLikeLambdaVariable(first)) {
    return { body: second, variables: [first] };
  }
  throw new UnsupportedQueryPlanError("Lambda requires a variable operand");
};

/**
 * Translation-time check of a macro's lambda: well formed, and with as many variables as the macro
 * binds. `exists`, `exists_one` and `all` take one or two; `filter` and `map` one.
 */
const validateMacro =
  (maxVariables: 1 | 2) =>
  ({ operator, operands }: PlanExpression): void => {
    const { variables } = lambdaComponents(
      operandAt(operands, 1, `${operator} lambda`),
    );
    if (variables.length > maxVariables) {
      throw new UnsupportedQueryPlanError(
        `${operator} binds one variable, and the plan's lambda declares ${variables.length}`,
      );
    }
  };

/**
 * The macro's lambda (its second operand) as a function of one collection item: the element, or
 * for two variables the (index, element) or (key, value) pair.
 */
const lambdaOf = (
  call: Call,
): {
  arity: number;
  body: (first: unknown, second?: unknown) => unknown;
} => {
  const { body, variables } = lambdaComponents(
    operandAt(call.operands, 1, `${call.operator} lambda`),
  );
  const { scope } = call;
  return {
    arity: variables.length,
    body: (first, second) => {
      const bindings: Bindings = { ...scope.bindings };
      const [firstVariable, secondVariable] = variables;
      bindings[firstVariable!.name] = first;
      if (secondVariable) bindings[secondVariable.name] = second;
      return evaluate(body, { ...scope, bindings });
    },
  };
};

/**
 * The (first, second) pairs a two-variable comprehension binds: a list's (index, element) pairs,
 * or a map's (key, value) pairs.
 */
const comprehensionPairs = (
  collection: unknown,
): [unknown, unknown][] | undefined => {
  if (Array.isArray(collection)) {
    return collection.map((element, index) => [index, element]);
  }
  if (isRecord(collection)) return Object.entries(collection);
  return undefined;
};

/** What a macro ranges over, and what `in` tests: a list's elements, or a map's keys. */
const macroItems = (collection: unknown): unknown[] | undefined => {
  if (Array.isArray(collection)) return collection;
  if (isRecord(collection)) return Object.keys(collection);
  return undefined;
};

/** `exists`, `exists_one` and `all`, with CEL's error absorption across elements. */
const quantifier = (call: Call): unknown => {
  const { arity, body } = lambdaOf(call);
  const target = arg(call, 0, "collection");
  const items: unknown[][] | undefined =
    arity === 2
      ? comprehensionPairs(target)
      : macroItems(target)?.map((item) => [item]);
  if (items === undefined) return EVALUATION_ERROR;
  let trueCount = 0;
  let sawError = false;
  for (const item of items) {
    const value = asBoolean(body(item[0], item[1]));
    if (value === true) {
      trueCount += 1;
      if (call.operator === "exists") return true;
    } else if (value === false && call.operator === "all") {
      return false;
    } else if (isEvaluationError(value)) {
      sawError = true;
    }
  }
  if (sawError) return EVALUATION_ERROR;
  if (call.operator === "exists") return false;
  if (call.operator === "exists_one") return trueCount === 1;
  return true;
};

// IEEE-754 keeps the sign of a zero, so `n / -0.0` is the OPPOSITE infinity from `n / 0.0`.
// The planner does ship the sign — the wire operand for -0.0 is `-0` — but a plan reaching a
// Convex function is JSON-encoded on the way in, and `JSON.stringify(-0)` is `"0"`, so the sign
// bit is already gone by the time the adapter sees it. Guessing returns rows the PDP denies, so
// fail closed instead (cerbos/query-plan-adapters#312). A zero numerator is unaffected: 0/0 is
// NaN under either sign.
const INDETERMINATE_ZERO_DIVISOR_MESSAGE =
  "division by a constant zero whose sign is indeterminate: a query plan is " +
  "JSON-encoded on its way into a Convex function and JSON has no -0, so the " +
  "adapter cannot tell +Infinity from -Infinity";

const arithmetic = (call: Call): unknown => {
  const { operator } = call;
  if (
    (operator === "add" || operator === "sub") &&
    call.operands.some((operand) => temporalKindOf(operand) !== undefined)
  ) {
    return temporalArithmetic(call);
  }
  const left = arg(call, 0, "left");
  const right = arg(call, 1, "right");
  // Over two int operands CEL does int arithmetic: `int(3) / 2` truncates to 1, where JavaScript
  // divides to 1.5. The plan carries no numeric type, so the mode is chosen from the expression.
  if (isIntExpression({ operator, operands: call.operands })) {
    return intArithmetic(operator as ArithmeticOperator, left, right);
  }
  // CEL has no overload mixing int and double, so `int(x) + R.attr.aDouble` is an error, where
  // JavaScript adds the two numbers and a negation would turn the sum into a grant.
  if (
    call.operands.some(isIntExpression) &&
    call.operands.some((operand) => isDoubleOperand(operand, call.scope))
  ) {
    return EVALUATION_ERROR;
  }
  // CEL overloads `+` on strings, and JavaScript's `+` concatenates identically. Only `add`
  // has the overload — `sub`/`mult`/`div`/`mod` over strings stay a CEL error, which is what
  // falling through to the numeric guard below already produces. Before this, a string `add`
  // was an evaluation error too, so a concatenation the PDP allowed returned no rows at all
  // (cerbos/query-plan-adapters#376).
  if (
    operator === "add" &&
    typeof left === "string" &&
    typeof right === "string"
  ) {
    return left + right;
  }
  // `+` over two lists concatenates them, in order (cerbos/query-plan-adapters#509 ports the
  // shapes: a literal list beside a resource list, and a ternary choosing a list branch).
  if (operator === "add" && Array.isArray(left) && Array.isArray(right)) {
    return [...left, ...right];
  }
  if (typeof left !== "number" || typeof right !== "number") {
    return EVALUATION_ERROR;
  }
  switch (operator) {
    case "add":
      return left + right;
    case "sub":
      return left - right;
    case "mult":
      return left * right;
    case "div":
      if (right === 0 && left !== 0 && !readsDocument(call, 1)) {
        // Backstop for zeros only computed at evaluation time; constant zero divisors are
        // already rejected during translation by `validateDivision`. A zero READ from the
        // document is exempt: Convex stores a float64 with its sign, so IEEE division by it
        // yields the infinity CEL does.
        throw new UnsupportedQueryPlanError(INDETERMINATE_ZERO_DIVISOR_MESSAGE);
      }
      return left / right;
    default:
      return modulo(call, left, right);
  }
};

type TemporalKind = "timestamp" | "duration";

/**
 * Whether the operand is certainly a CEL timestamp or duration, read from the expression: a
 * timestamp is held as a bigint of nanoseconds, which an int beyond the safe range also is, so the
 * type comes from the node that produced the value — `timestamp()`, `duration()`, `timeSince()`,
 * and `+`/`-` over those with the overloads CEL defines — never from the value.
 */
const temporalKindOf = (
  operand: PlanExpressionOperand,
): TemporalKind | undefined => {
  if (!isExpression(operand)) return undefined;
  switch (operand.operator) {
    case "timestamp":
      return "timestamp";
    case "duration":
    case "timeSince":
      return "duration";
    case "add":
    case "sub": {
      const [left, right] = operand.operands;
      if (left === undefined || right === undefined) return undefined;
      return temporalResultKind(
        operand.operator,
        temporalKindOf(left),
        temporalKindOf(right),
      );
    }
    default:
      return undefined;
  }
};

/** The kind CEL's `+`/`-` overloads give two temporal operands; undefined where none exists. */
const temporalResultKind = (
  operator: string,
  left: TemporalKind | undefined,
  right: TemporalKind | undefined,
): TemporalKind | undefined => {
  if (left === "duration" && right === "duration") return "duration";
  if (left === "timestamp" && right === "duration") return "timestamp";
  if (operator === "add" && left === "duration" && right === "timestamp") {
    return "timestamp";
  }
  if (operator === "sub" && left === "timestamp" && right === "timestamp") {
    return "duration";
  }
  return undefined;
};

/**
 * `+`/`-` with a timestamp or duration operand: timestamp ± duration, duration ± duration, and
 * timestamp - timestamp. An overflow, a result outside CEL's timestamp range, and any pairing CEL
 * has no overload for (a duration beside a number or a string attribute) are errors.
 */
const temporalArithmetic = (call: Call): unknown => {
  const [leftOperand, rightOperand] = call.operands;
  const kind = temporalResultKind(
    call.operator,
    leftOperand && temporalKindOf(leftOperand),
    rightOperand && temporalKindOf(rightOperand),
  );
  const left = arg(call, 0, "left");
  const right = arg(call, 1, "right");
  if (kind === undefined) return EVALUATION_ERROR;
  const nanosOf = (value: unknown): bigint | undefined =>
    value instanceof CelDuration
      ? value.nanos
      : typeof value === "bigint"
        ? value
        : undefined;
  const a = nanosOf(left);
  const b = nanosOf(right);
  if (a === undefined || b === undefined) return EVALUATION_ERROR;
  const nanos = call.operator === "add" ? a + b : a - b;
  return kind === "timestamp"
    ? checkedTimestamp(nanos)
    : checkedDuration(nanos);
};

/** Whether the operand at `index` is a document field, rather than a constant or lambda binding. */
const readsDocument = ({ operands, scope }: Call, index: number): boolean => {
  const operand = operands[index];
  return operand !== undefined && isDocumentField(operand, scope);
};

const isDocumentField = (
  operand: PlanExpressionOperand,
  scope: Scope,
): boolean => {
  if (!isVariable(operand)) return false;
  const root = operand.name.split(".")[0] ?? operand.name;
  return !(root in scope.bindings);
};

/**
 * Whether the operand is certainly not a CEL int, so that beside a certain int it is a
 * no-such-overload error: a document field (every number an attribute carries is a double, and
 * any other type is no number at all), a fractional constant, or `double()`.
 */
const isDoubleOperand = (
  operand: PlanExpressionOperand,
  scope: Scope,
): boolean => {
  if (isValue(operand)) {
    return (
      typeof operand.value === "number" && !Number.isInteger(operand.value)
    );
  }
  if (isVariable(operand)) return isDocumentField(operand, scope);
  return isExpression(operand) && operand.operator === "double";
};

// CEL's `%` has int and uint overloads only. Every number an attribute carries is a double (a
// resource or principal attribute reaches the PDP as a protobuf Value), so a field read as either
// operand is a no-such-overload error on every row, and an int modulus by zero is an error too.
// JavaScript's `%` would answer both, and a negation would turn that answer into a grant.
const modulo = (call: Call, left: number, right: number): unknown => {
  if (call.operands.some(isVariable) || right === 0) return EVALUATION_ERROR;
  return left % right;
};

/**
 * Whether the operand is certainly a CEL int: an integral constant (a policy literal: an int
 * literal against a statically typed int operand is the only spelling that type-checks), `int()`,
 * `size()`, or int arithmetic over those. A field read is certainly a double; anything else has a
 * type the plan does not carry.
 */
const isIntOperand = (operand: PlanExpressionOperand): boolean =>
  isValue(operand) ? Number.isInteger(operand.value) : isIntExpression(operand);

/**
 * Whether the operand is an expression whose result is certainly a CEL int: `int()`, `size()`, or
 * arithmetic over ints of which at least one is such an expression. Arithmetic over integral
 * constants alone is not: the planner keeps `0.0 / 0.0` unfolded beside `now()`, and ships it as
 * `0 / 0`.
 */
const isIntExpression = (operand: PlanExpressionOperand): boolean => {
  if (!isExpression(operand)) return false;
  switch (operand.operator) {
    case "int":
    case "size":
      return true;
    case "add":
    case "sub":
    case "mult":
    case "div":
    case "mod":
      return (
        operand.operands.every(isIntOperand) &&
        operand.operands.some(isIntExpression)
      );
    default:
      return false;
  }
};

type NumericType = "int" | "double";

/**
 * The CEL numeric type the plan says the operand certainly has: `int` for a certain int (see
 * `isIntExpression`), `double` for `double()` or a fractional constant, and for a ternary whatever
 * one of its branches fixes, since CEL gives both branches one type. Undefined when the plan does
 * not say: a document field, or an integral constant, which the plan ships as a bare number
 * whether the policy wrote `1000000` or `1000000.0`.
 */
const numericTypeOf = (
  operand: PlanExpressionOperand,
): NumericType | undefined => {
  if (isValue(operand)) {
    return typeof operand.value === "number" && !Number.isInteger(operand.value)
      ? "double"
      : undefined;
  }
  if (isIntExpression(operand)) return "int";
  if (!isExpression(operand)) return undefined;
  if (operand.operator === "double") return "double";
  if (operand.operator !== "if") return undefined;
  const [, whenTrue, whenFalse] = operand.operands;
  return (
    (whenTrue && numericTypeOf(whenTrue)) ??
    (whenFalse && numericTypeOf(whenFalse))
  );
};

/**
 * Whether `string()` over the operand could render an integral constant whose type the plan
 * dropped, where the int and double renderings differ ("1000000" and "1e+06"). `type` is the
 * type an enclosing ternary fixes for its branches.
 */
const rendersUntypedConstant = (
  operand: PlanExpressionOperand,
  type: NumericType | undefined,
): boolean => {
  if (isValue(operand)) {
    const { value } = operand;
    return (
      type === undefined &&
      typeof value === "number" &&
      Number.isInteger(value) &&
      convertToString(value, true) !== convertToString(value, false)
    );
  }
  if (!isExpression(operand) || operand.operator !== "if") return false;
  const branchType = type ?? numericTypeOf(operand);
  return operand.operands
    .slice(1)
    .some((branch) => rendersUntypedConstant(branch, branchType));
};

const validateStringConversion = ({ operands }: PlanExpression): void => {
  const operand = operands[0];
  if (operand !== undefined && rendersUntypedConstant(operand, undefined)) {
    throw new UnsupportedQueryPlanError(
      "string() over an integral constant whose int or double type the plan does not carry: " +
        'CEL renders the int 1000000 as "1000000" and the double as "1e+06", and the plan ' +
        "ships both as the same bare number",
    );
  }
};

const validateModulo = ({ operands }: PlanExpression): void => {
  if (!operands.every((op) => isVariable(op) || isIntOperand(op))) {
    throw new UnsupportedQueryPlanError(
      "modulo requires int operands, and the plan does not carry the numeric type of this one: " +
        "CEL's % is a no-such-overload error on a double, which JavaScript's % would answer",
    );
  }
};

// Reject at translation, not at post-filter evaluation: by the time the evaluator sees the zero
// the filter already exists, and the invariant is that an inexpressible shape must throw before
// its filter can be used. `arithmetic` keeps the same check as a backstop for zeros that are only
// computed at evaluation time.
const validateDivision = ({ operands }: PlanExpression): void => {
  const [numerator, denominator] = operands;
  if (
    denominator !== undefined &&
    isExpression(denominator) &&
    denominator.operator === "div"
  ) {
    throw new UnsupportedQueryPlanError(
      "division requires a constant denominator: the plan does not preserve numeric types needed to distinguish integer errors from floating-point infinity",
    );
  }
  if (
    denominator !== undefined &&
    isValue(denominator) &&
    denominator.value === 0 &&
    !(numerator !== undefined && isValue(numerator) && numerator.value === 0)
  ) {
    throw new UnsupportedQueryPlanError(INDETERMINATE_ZERO_DIVISOR_MESSAGE);
  }
};

const validatePattern = ({ operands }: PlanExpression): void => {
  const pattern = operands[1];
  if (
    !pattern ||
    !isValue(pattern) ||
    typeof pattern.value !== "string" ||
    !parseSafeRegexPattern(pattern.value)
  ) {
    throw new UnsupportedQueryPlanError(
      "matches requires a constant RE2-compatible pattern in the supported " +
        "literal, anchor, and trailing .* subset",
    );
  }
};

const hierarchyRelation = (call: Call): unknown => {
  const left = arg(call, 0, "left");
  const right = arg(call, 1, "right");
  if (!(left instanceof Hierarchy) || !(right instanceof Hierarchy)) {
    return EVALUATION_ERROR;
  }
  switch (call.operator) {
    case "ancestorOf":
      return isStrictAncestor(left, right);
    case "descendentOf":
      return isStrictAncestor(right, left);
    default:
      return hierarchiesOverlap(left, right);
  }
};

/** Cerbos's list operations: both operands lists, or an error. */
const listOperation =
  (operation: (left: unknown[], right: unknown[]) => unknown) =>
  (call: Call): unknown => {
    const left = arg(call, 0, "left");
    const right = arg(call, 1, "right");
    return Array.isArray(left) && Array.isArray(right)
      ? operation(left, right)
      : EVALUATION_ERROR;
  };

const listContains = (list: unknown[], value: unknown): boolean =>
  list.some((member) => valuesEqual(member, value));

/** A struct literal's entries: `set-field` nodes, each a constant string key and a value. */
const validateStruct = ({ operands }: PlanExpression): void => {
  for (const entry of operands) {
    if (
      !isExpression(entry) ||
      entry.operator !== "set-field" ||
      entry.operands.length !== 2 ||
      !isValue(entry.operands[0]!) ||
      typeof entry.operands[0].value !== "string"
    ) {
      throw new UnsupportedQueryPlanError(
        "A map literal's entries must each be a set-field with a constant string key",
      );
    }
  }
};

// -- the roster ----------------------------------------------------------------------------------

const OPERATORS: Record<string, Operator> = {
  and: { evaluate: junction(false) },
  or: { evaluate: junction(true) },
  not: {
    evaluate: (call) => {
      const value = asBoolean(arg(call, 0, "operand"));
      return isEvaluationError(value) ? value : !value;
    },
  },

  eq: { evaluate: comparison },
  ne: { evaluate: comparison },
  lt: { evaluate: comparison },
  le: { evaluate: comparison },
  gt: { evaluate: comparison },
  ge: { evaluate: comparison },

  in: {
    evaluate: (call) => {
      const needle = arg(call, 0, "needle");
      const haystack = arg(call, 1, "haystack");
      if (isEvaluationError(needle) || isEvaluationError(haystack)) {
        return EVALUATION_ERROR;
      }
      // CEL's `in` over a map tests its keys. Only a map-valued attribute reaches here as a map:
      // the planner rewrites a map literal, or a principal map, into its key list first.
      const members = macroItems(haystack);
      if (members === undefined) return EVALUATION_ERROR;
      return members.some((value) => valuesEqual(value, needle));
    },
  },

  contains: {
    evaluate: stringTest("needle", (receiver, needle) =>
      receiver.includes(needle),
    ),
  },
  startsWith: {
    evaluate: stringTest("prefix", (receiver, prefix) =>
      receiver.startsWith(prefix),
    ),
  },
  endsWith: {
    evaluate: stringTest("suffix", (receiver, suffix) =>
      receiver.endsWith(suffix),
    ),
  },
  matches: {
    evaluate: (call) => {
      const receiver = arg(call, 0, "receiver");
      const pattern = arg(call, 1, "pattern");
      if (typeof receiver !== "string" || typeof pattern !== "string") {
        return EVALUATION_ERROR;
      }
      const safePattern = parseSafeRegexPattern(pattern);
      return safePattern
        ? matchesSafeRegexPattern(receiver, safePattern)
        : EVALUATION_ERROR;
    },
    validate: validatePattern,
  },

  // Cerbos's `except`: the elements of the first list absent from the second, duplicates kept.
  except: {
    evaluate: listOperation((left, right) =>
      left.filter((value) => !listContains(right, value)),
    ),
  },
  // Cerbos's `intersect`: the elements of the SHORTER list (the first, on a tie) present in the
  // longer one, in order and with the shorter list's duplicates kept.
  intersect: {
    evaluate: listOperation((left, right) => {
      const [shorter, longer] =
        left.length > right.length ? [right, left] : [left, right];
      return shorter.filter((value) => listContains(longer, value));
    }),
  },
  // `isSubset(a, b)`: every element of `a` is in `b`. An empty `a` is a subset of anything.
  isSubset: {
    evaluate: listOperation((left, right) =>
      left.every((value) => listContains(right, value)),
    ),
  },
  // A list literal holding an expression; any element's error is the list's.
  list: {
    evaluate: (call) => {
      const elements: unknown[] = [];
      for (const operand of call.operands) {
        const value = evaluate(operand, call.scope);
        if (isEvaluationError(value)) return EVALUATION_ERROR;
        elements.push(value);
      }
      return elements;
    },
  },
  // A map literal. A repeated key is an error in CEL, at evaluation.
  struct: {
    evaluate: (call) => {
      const entries: Record<string, unknown> = Object.create(null) as Record<
        string,
        unknown
      >;
      for (const entry of call.operands as PlanExpression[]) {
        const key = (entry.operands[0] as PlanExpressionValue).value as string;
        const value = evaluate(entry.operands[1]!, call.scope);
        if (
          isEvaluationError(value) ||
          Object.prototype.hasOwnProperty.call(entries, key)
        ) {
          return EVALUATION_ERROR;
        }
        entries[key] = value;
      }
      return entries;
    },
    validate: validateStruct,
  },
  "set-field": {
    evaluate: () => {
      throw new UnsupportedQueryPlanError(
        "set-field is only meaningful as a map literal's entry",
      );
    },
  },

  hasIntersection: {
    evaluate: (call) => {
      const left = arg(call, 0, "left");
      const right = arg(call, 1, "right");
      if (!Array.isArray(left) || !Array.isArray(right)) {
        return EVALUATION_ERROR;
      }
      return left.some((leftValue) =>
        right.some((rightValue) => valuesEqual(leftValue, rightValue)),
      );
    },
  },

  exists: { evaluate: quantifier, validate: validateMacro(2) },
  exists_one: { evaluate: quantifier, validate: validateMacro(2) },
  all: { evaluate: quantifier, validate: validateMacro(2) },
  filter: {
    evaluate: (call) => {
      const collection = macroItems(arg(call, 0, "collection"));
      if (collection === undefined) return EVALUATION_ERROR;
      const { body } = lambdaOf(call);
      const filtered: unknown[] = [];
      for (const item of collection) {
        const value = asBoolean(body(item));
        if (isEvaluationError(value)) return EVALUATION_ERROR;
        if (value) filtered.push(item);
      }
      return filtered;
    },
    validate: validateMacro(1),
  },
  map: {
    evaluate: (call) => {
      const collection = macroItems(arg(call, 0, "collection"));
      if (collection === undefined) return EVALUATION_ERROR;
      const { body } = lambdaOf(call);
      const mapped: unknown[] = [];
      for (const item of collection) {
        const value = body(item);
        if (isEvaluationError(value)) return EVALUATION_ERROR;
        mapped.push(value);
      }
      return mapped;
    },
    validate: validateMacro(1),
  },
  lambda: {
    evaluate: () => {
      throw new UnsupportedQueryPlanError(
        "lambda should not be evaluated directly",
      );
    },
  },

  add: { evaluate: arithmetic },
  sub: { evaluate: arithmetic },
  mult: { evaluate: arithmetic },
  div: { evaluate: arithmetic, validate: validateDivision },
  mod: { evaluate: arithmetic, validate: validateModulo },

  index: {
    evaluate: (call) => {
      const collection = arg(call, 0, "collection");
      const index = arg(call, 1, "value");
      // A map is indexed by key; an absent key is an error. Every map the evaluator holds has
      // string keys (a document map, or a literal whose keys `validateStruct` fixed as strings),
      // so a key of any other type is absent too.
      if (isRecord(collection)) {
        return typeof index === "string" &&
          Object.prototype.hasOwnProperty.call(collection, index)
          ? collection[index]
          : EVALUATION_ERROR;
      }
      if (
        !Array.isArray(collection) ||
        typeof index !== "number" ||
        !Number.isInteger(index) ||
        index < 0 ||
        index >= collection.length
      ) {
        return EVALUATION_ERROR;
      }
      return collection[index];
    },
  },
  "get-field": {
    evaluate: (call) => {
      const target = arg(call, 0, "target");
      const fieldOperand = operandAt(call.operands, 1, "get-field name");
      const field = isVariable(fieldOperand)
        ? fieldOperand.name
        : isValue(fieldOperand) && typeof fieldOperand.value === "string"
          ? fieldOperand.value
          : undefined;
      return field
        ? readPath(getNestedValue(target, field), call.scope)
        : EVALUATION_ERROR;
    },
  },
  size: {
    evaluate: (call) => {
      const value = arg(call, 0, "operand");
      if (typeof value === "string") return Array.from(value).length;
      if (Array.isArray(value)) return value.length;
      if (isRecord(value)) return Object.keys(value).length;
      return EVALUATION_ERROR;
    },
  },

  // Each conversion maps an evaluation error to itself, so an error operand needs no special case.
  string: {
    // An int renders "1000000" where a double of the same value renders "1e+06".
    evaluate: (call) =>
      convertToString(
        arg(call, 0, "operand"),
        numericTypeOf(operandAt(call.operands, 0, "string operand")) === "int",
      ),
    validate: validateStringConversion,
  },
  double: { evaluate: (call) => convertToDouble(arg(call, 0, "operand")) },
  int: { evaluate: (call) => convertToInt(arg(call, 0, "operand")) },
  // CEL's strings-extension `upperAscii()`: only the ASCII letters a-z are folded.
  upperAscii: {
    evaluate: (call) => {
      const value = arg(call, 0, "operand");
      return typeof value === "string"
        ? value.replace(/[a-z]/g, (letter) => letter.toUpperCase())
        : EVALUATION_ERROR;
    },
  },
  duration: {
    evaluate: (call) => {
      const value = arg(call, 0, "operand");
      return typeof value === "string"
        ? parseCelDuration(value)
        : EVALUATION_ERROR;
    },
  },
  // Relative to the evaluating clock: the post-filter runs when the query does, as check() does.
  timeSince: {
    evaluate: (call) => {
      const operand = operandAt(call.operands, 0, "timeSince operand");
      const value = arg(call, 0, "operand");
      if (
        temporalKindOf(operand) !== "timestamp" ||
        typeof value !== "bigint"
      ) {
        return EVALUATION_ERROR;
      }
      return timeSince(BigInt(Date.now()) * 1_000_000n, value);
    },
  },
  timestamp: {
    evaluate: (call) => {
      const value = arg(call, 0, "operand");
      if (typeof value !== "string") return EVALUATION_ERROR;
      return parseRfc3339Timestamp(value);
    },
  },

  if: {
    evaluate: (call) => {
      const condition = asBoolean(arg(call, 0, "condition"));
      if (isEvaluationError(condition)) return condition;
      return arg(call, condition ? 1 : 2, "selected branch");
    },
  },

  hierarchy: {
    evaluate: (call) => {
      const value = arg(call, 0, "value");
      const delimiterOperand = call.operands[1];
      if (delimiterOperand) {
        const delimiter = evaluate(delimiterOperand, call.scope);
        return typeof value === "string" && typeof delimiter === "string"
          ? splitHierarchy(value, delimiter)
          : EVALUATION_ERROR;
      }
      // One argument: a string split on ".", a list of string segments, or a hierarchy.
      if (typeof value === "string") return splitHierarchy(value, ".");
      if (value instanceof Hierarchy) return value;
      if (
        Array.isArray(value) &&
        value.every((segment) => typeof segment === "string")
      ) {
        return new Hierarchy(value as string[]);
      }
      return EVALUATION_ERROR;
    },
  },
  ancestorOf: { evaluate: hierarchyRelation },
  descendentOf: { evaluate: hierarchyRelation },
  overlaps: { evaluate: hierarchyRelation },
};

/** The operator's entry, or undefined for one the adapter does not know. */
export const operatorFor = (name: string): Operator | undefined =>
  Object.prototype.hasOwnProperty.call(OPERATORS, name)
    ? OPERATORS[name]
    : undefined;
