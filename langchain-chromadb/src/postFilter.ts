import type {
  PlanExpression,
  PlanExpressionOperand,
  PlanExpressionValue,
  PlanExpressionVariable,
} from "@cerbos/core";

import {
  CelHierarchy,
  CelTimestamp,
  EVALUATION_ERROR,
  asBoolean,
  compareValues,
  convertToDouble,
  convertToInt,
  convertToString,
  getNestedValue,
  intArithmetic,
  isEvaluationError,
  isRecord,
  isSegmentPrefix,
  isStrictAncestor,
  parseGoDuration,
  parseRfc3339Timestamp,
  temporalArithmetic,
  timeSince,
  toHierarchy,
  valuesEqual,
} from "./cel";
import type { ComparisonOperator } from "./cel";
import { UnsupportedOperatorError } from "./errors";
import { matchesSafeRegexPattern, parseSafeRegexPattern } from "./regex";

/**
 * The post-filter: the part of a plan Chroma's `Where` grammar cannot hold, compiled into a
 * predicate over one record's metadata and evaluated with CEL's semantics.
 *
 * Compiling is where every refusal happens. The invariant is that a shape the adapter cannot
 * answer exactly throws before a predicate exists, so `compile` walks the whole expression up
 * front and the predicate it returns never throws. Evaluation itself follows CEL: a metadata key
 * the record does not carry is a missing-attribute error, an error propagates through `not` and
 * through `&&`/`||` unless another operand decides them, and a root that is not `true` denies.
 *
 * The evaluator's CEL semantics are the convex adapter's, ported and copied rather than shared
 * (docs/adr/0007-adapters-share-data-not-code.md).
 */

/** A record's metadata as Chroma returns it. `null` for a record stored without any. */
export type PostFilter = (
  metadata: Readonly<Record<string, unknown>> | null | undefined,
) => boolean;

/** What the post-filter needs from the caller's mapper. */
export interface PostFilterFields {
  /** Whether the mapper declares this reference: an undeclared one is data the record may not store. */
  isMapped: (reference: string) => boolean;
  /** The metadata key a declared reference is stored under. */
  metadataKey: (reference: string) => string;
  /** Whether the mapping declares every value stored under the reference a boolean. */
  isBoolean: (reference: string) => boolean;
}

/** Compiles `condition` into a predicate, or throws `UnsupportedOperatorError`. */
export function compilePostFilter(
  condition: PlanExpressionOperand,
  fields: PostFilterFields,
): PostFilter {
  const compiled = compileCondition(condition, {
    fields,
    bindings: new Map(),
    parent: "eq",
  });
  return (metadata) =>
    compiled.run({ metadata: metadata ?? {}, bindings: {} }) === true;
}

// -- static types --------------------------------------------------------------------------------
//
// A plan carries values, not CEL types, and two of CEL's rules depend on the type: arithmetic has
// no int/double overload (`R.attr.x + 1` is a no-such-overload error, `R.attr.x + 1.0` is not), and
// `string()` renders an int and a double differently past 1e6 ("1000000" against "1e+06"). A
// numeric literal on the wire is only a number, so an integral one could have been written either
// way. The compiler tracks what each operand certainly is and refuses where the answer would turn
// on a type the plan dropped.

type StaticType =
  /** Certainly a CEL int: `int()`, `size()`, or int arithmetic. */
  | "int"
  /** Certainly a CEL double: a fractional literal, `double()`, or double arithmetic. */
  | "double"
  /** An integral numeric literal: written `1` or `1.0`, the plan cannot say which. */
  | "ambiguous"
  /** A metadata read: whatever the record stores, and every stored number is a CEL double. */
  | "attribute"
  /** A timestamp. */
  | "timestamp"
  /** A duration. */
  | "duration"
  /** Certainly not a number: a string, a bool, a list, a map. */
  | "other"
  /** Anything else — a list element, a ternary mixing the above. */
  | "unknown";

// -- the compiled form ---------------------------------------------------------------------------

interface Env {
  metadata: Readonly<Record<string, unknown>>;
  /** Lambda variables in scope, by name. */
  bindings: Record<string, unknown>;
}

interface Compiled {
  type: StaticType;
  run: (env: Env) => unknown;
}

interface Context {
  fields: PostFilterFields;
  /** The static type of each lambda variable in scope. */
  bindings: ReadonlyMap<string, StaticType>;
  /** The operator enclosing the operand being compiled, which a refusal of a leaf reports. */
  parent: string;
}

const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
const isVariable = (e: PlanExpressionOperand): e is PlanExpressionVariable =>
  "name" in e;

const constant = (type: StaticType, value: unknown): Compiled => ({
  type,
  run: () => value,
});

function compile(operand: PlanExpressionOperand, ctx: Context): Compiled {
  if (isVariable(operand)) return compileVariable(operand.name, ctx);
  if (isValue(operand)) return compileValue(operand.value, ctx);
  if (!isExpression(operand)) {
    throw Error(
      `Query plan did not contain an expression for operand ${String(operand)}`,
    );
  }
  const compiler = Object.prototype.hasOwnProperty.call(
    OPERATORS,
    operand.operator,
  )
    ? OPERATORS[operand.operator]
    : undefined;
  if (!compiler) {
    throw new UnsupportedOperatorError(
      operand.operator,
      `Unsupported operator ${operand.operator}: the post-filter has no evaluation for it`,
    );
  }
  return compiler(operand.operands, {
    ...ctx,
    parent: operand.operator,
  });
}

/**
 * An operand in a boolean position. `filter()` and `map()` return a list, which CEL rejects as a
 * condition and the evaluator would read as an error that denies every record — agreeing with the
 * PDP silently while giving the shape a meaning the policy never stated. Refused instead.
 */
function compileCondition(
  operand: PlanExpressionOperand,
  ctx: Context,
): Compiled {
  if (
    isExpression(operand) &&
    (operand.operator === "filter" || operand.operator === "map")
  ) {
    throw new UnsupportedOperatorError(
      operand.operator,
      `${operand.operator}() returns a list, not a boolean, so it cannot be a condition on its own`,
    );
  }
  return compile(operand, ctx);
}

function containsNull(value: unknown): boolean {
  if (value === null) return true;
  if (Array.isArray(value)) return value.some(containsNull);
  if (isRecord(value)) return Object.values(value).some(containsNull);
  return false;
}

function compileValue(value: unknown, ctx: Context): Compiled {
  // Chroma metadata has no null, so a record cannot tell a NULL sent to `check()` as an explicit
  // `null` (which CEL compares) from one left out (a missing-attribute error). The pushdown refuses
  // a null operand for the same reason.
  if (containsNull(value)) {
    throw new UnsupportedOperatorError(
      ctx.parent,
      `${ctx.parent} compares a null literal, and Chroma metadata has no null: a record cannot tell an explicit null attribute from a missing one`,
    );
  }
  if (typeof value === "number") {
    return constant(Number.isInteger(value) ? "ambiguous" : "double", value);
  }
  return constant("other", value);
}

function compileVariable(name: string, ctx: Context): Compiled {
  const dot = name.indexOf(".");
  const root = dot === -1 ? name : name.substring(0, dot);
  const bound = ctx.bindings.get(root);
  if (bound !== undefined) {
    if (dot === -1) {
      return { type: bound, run: ({ bindings }) => bindings[name] };
    }
    const path = name.substring(dot + 1);
    return {
      type: "unknown",
      run: ({ bindings }) => getNestedValue(bindings[root], path),
    };
  }
  if (!ctx.fields.isMapped(name)) {
    throw new UnsupportedOperatorError(
      ctx.parent,
      `${name} has no fieldNameMapper entry, so the post-filter cannot read it: an attribute the mapping does not declare is one a Chroma record may not store (a list, a relation), and reading it as missing would deny records the PDP allows`,
    );
  }
  const key = ctx.fields.metadataKey(name);
  return {
    // A key declared `valueType: "boolean"` never holds a number, so no zero whose sign was lost.
    type: ctx.fields.isBoolean(name) ? "other" : "attribute",
    run: ({ metadata }) => readMetadata(metadata, key),
  };
}

/**
 * A metadata key as CEL reads the attribute. An absent key is the missing-attribute error. A value
 * that is not a string, finite number or boolean — a list or sparse vector the caller stored — is
 * read as missing too: the post-filter reads scalar metadata only, so such a record is denied
 * rather than compared as something the PDP may not have seen
 * (cerbos/query-plan-adapters#475).
 */
function readMetadata(
  metadata: Readonly<Record<string, unknown>>,
  key: string,
): unknown {
  if (!Object.prototype.hasOwnProperty.call(metadata, key)) {
    return EVALUATION_ERROR;
  }
  const value = metadata[key];
  if (
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return value;
  }
  return EVALUATION_ERROR;
}

// -- operand helpers -----------------------------------------------------------------------------

function exactly(
  operands: PlanExpressionOperand[],
  count: number,
  operator: string,
): PlanExpressionOperand[] {
  if (operands.length !== count || operands.some((op) => !op)) {
    throw Error(
      count === 1
        ? "Expected exactly one operand"
        : count === 2
          ? "Expected exactly two operands"
          : `Expected exactly ${count} operands for ${operator}`,
    );
  }
  return operands;
}

function compileAll(
  operands: PlanExpressionOperand[],
  count: number,
  ctx: Context,
): Compiled[] {
  return exactly(operands, count, ctx.parent).map((op) => compile(op, ctx));
}

/** Evaluates each compiled operand, short-circuiting to the error if any is one. */
function runAll(
  compiled: Compiled[],
  env: Env,
): unknown[] | typeof EVALUATION_ERROR {
  const values: unknown[] = [];
  for (const operand of compiled) {
    const value = operand.run(env);
    if (isEvaluationError(value)) return EVALUATION_ERROR;
    values.push(value);
  }
  return values;
}

type Compiler = (operands: PlanExpressionOperand[], ctx: Context) => Compiled;

/** A compiler for an operator of fixed arity whose result is a function of its operand values. */
const strict =
  (
    count: number,
    type: StaticType,
    apply: (values: unknown[]) => unknown,
  ): Compiler =>
  (operands, ctx) => {
    const compiled = compileAll(operands, count, ctx);
    return {
      type,
      run: (env) => {
        const values = runAll(compiled, env);
        return isEvaluationError(values) ? values : apply(values);
      },
    };
  };

// -- operator families ---------------------------------------------------------------------------

/** CEL's commutative `&&` / `||`: a deciding operand wins over an error in any position. */
const junction =
  (decisive: boolean): Compiler =>
  (operands, ctx) => {
    if (operands.length < 2) throw Error("Expected at least 2 operands");
    const compiled = operands.map((op) => compileCondition(op, ctx));
    return {
      type: "other",
      run: (env) => {
        let sawError = false;
        for (const operand of compiled) {
          const value = asBoolean(operand.run(env));
          if (value === decisive) return decisive;
          if (isEvaluationError(value)) sawError = true;
        }
        return sawError ? EVALUATION_ERROR : !decisive;
      },
    };
  };

const comparison = (operator: ComparisonOperator): Compiler =>
  strict(2, "other", ([left, right]) => compareValues(operator, left, right));

const stringTest = (
  test: (receiver: string, argument: string) => boolean,
): Compiler =>
  strict(2, "other", ([receiver, argument]) =>
    typeof receiver === "string" && typeof argument === "string"
      ? test(receiver, argument)
      : EVALUATION_ERROR,
  );

// -- arithmetic ----------------------------------------------------------------------------------

type ArithmeticOperator = "add" | "sub" | "mult" | "div" | "mod";

type Mode =
  /** CEL int arithmetic, exact over bigints. */
  | "int"
  /** Double arithmetic, string and list concatenation; anything else is an error at runtime. */
  | "dynamic"
  /** A no-such-overload error on every record: an int against a double, or `%` on a double. */
  | "error";

/**
 * How CEL evaluates `operator` over operands of these static types, or a refusal where that turns
 * on a type the plan does not carry.
 */
function arithmeticMode(
  operator: ArithmeticOperator,
  left: StaticType,
  right: StaticType,
): Mode {
  const types = [left, right];
  const has = (type: StaticType) => types.includes(type);
  if (has("unknown") || has("timestamp")) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} over an operand whose CEL type the plan does not carry (a list element, a mixed ternary or a timestamp) cannot be evaluated exactly`,
    );
  }
  const intLike = (type: StaticType) => type === "int" || type === "ambiguous";
  if (operator === "mod") {
    // CEL's `%` has int and uint overloads only: over a double or a stored number it is an error.
    return intLike(left) && intLike(right) ? "int" : "error";
  }
  if (has("int")) {
    if (intLike(left) && intLike(right)) return "int";
    if (has("double")) {
      throw new UnsupportedOperatorError(
        operator,
        `${operator} mixes an int with a double, which does not type-check`,
      );
    }
    // A stored number is a double, and anything else is no number at all.
    return "error";
  }
  // Next to a certain double, an integral literal can only be a double: an int would not type-check.
  if (has("double")) return "dynamic";
  if (has("ambiguous") && has("attribute")) {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} meets an integral literal next to a stored number, and the plan does not carry whether it was written as an int or a double: CEL arithmetic has no int/double overload, so \`R.attr.x + 1\` is an error where \`R.attr.x + 1.0\` is not`,
    );
  }
  if (left === "ambiguous" && right === "ambiguous") {
    throw new UnsupportedOperatorError(
      operator,
      `${operator} over two integral literals, and the plan does not carry whether each was written as an int or a double: \`0 / 0\` is an error where \`0.0 / 0.0\` is NaN`,
    );
  }
  return "dynamic";
}

/**
 * The result type of timestamp and duration arithmetic, `undefined` when neither operand is a
 * timestamp or a duration. The overloads are cel-go's: `timestamp ± duration`, `duration +
 * timestamp`, `timestamp - timestamp` and `duration ± duration`. An operand of any other static
 * type next to one is refused, since the plan does not say which overload the policy selected.
 */
function temporalType(
  operator: ArithmeticOperator,
  left: StaticType,
  right: StaticType,
): StaticType | undefined {
  const temporal = (type: StaticType) =>
    type === "timestamp" || type === "duration";
  if (!temporal(left) && !temporal(right)) return undefined;
  const pair = `${left} ${right}`;
  if (operator === "add") {
    if (pair === "timestamp duration" || pair === "duration timestamp") {
      return "timestamp";
    }
    if (pair === "duration duration") return "duration";
  }
  if (operator === "sub") {
    if (pair === "timestamp duration") return "timestamp";
    if (pair === "timestamp timestamp" || pair === "duration duration") {
      return "duration";
    }
  }
  throw new UnsupportedOperatorError(
    operator,
    `${operator} over a ${left} and a ${right}: the post-filter evaluates a timestamp plus or minus a duration, the difference of two timestamps and the sum or difference of two durations only`,
  );
}

/**
 * Why the sign of a stored zero is unknown. The chromadb JS client sends metadata as JSON, and
 * `JSON.stringify(-0)` is `"0"`, so a -0.0 the PDP saw as an attribute reads back from Chroma as 0.
 * Only an operation that can observe a zero's sign is affected: a divisor, and `string()`.
 */
const SIGNED_ZERO =
  "the chromadb client stores metadata through JSON, which writes -0.0 as 0, so a stored zero's sign is lost";

/** Whether the operand's value can depend on a metadata read (a reference that is not a lambda variable). */
function readsMetadata(operand: PlanExpressionOperand, ctx: Context): boolean {
  if (isVariable(operand)) {
    return !ctx.bindings.has(operand.name.split(".")[0]!);
  }
  if (isExpression(operand)) {
    // A reference under a lambda may be the lambda's own variable, which is not in `ctx`; counting
    // it as a read only refuses more.
    return operand.operands.some((op) => readsMetadata(op, ctx));
  }
  return false;
}

const dynamicArithmetic = (
  operator: ArithmeticOperator,
  left: unknown,
  right: unknown,
): unknown => {
  if (typeof left === "number" && typeof right === "number") {
    switch (operator) {
      case "add":
        return left + right;
      case "sub":
        return left - right;
      case "mult":
        return left * right;
      case "div":
        // IEEE division, as CEL's double division: a zero divisor gives a signed infinity or NaN.
        return left / right;
      default:
        return EVALUATION_ERROR;
    }
  }
  if (operator === "add") {
    if (typeof left === "string" && typeof right === "string") {
      return left + right;
    }
    if (Array.isArray(left) && Array.isArray(right)) return [...left, ...right];
  }
  return EVALUATION_ERROR;
};

const arithmetic =
  (operator: ArithmeticOperator): Compiler =>
  (operands, ctx) => {
    const [left, right] = compileAll(operands, 2, ctx) as [Compiled, Compiled];
    const temporal = temporalType(operator, left.type, right.type);
    if (temporal !== undefined) {
      return {
        type: temporal,
        run: (env) => {
          const values = runAll([left, right], env);
          if (isEvaluationError(values)) return values;
          return temporalArithmetic(
            operator as "add" | "sub",
            values[0],
            values[1],
          );
        },
      };
    }
    const mode = arithmeticMode(operator, left.type, right.type);
    if (
      operator === "div" &&
      mode === "dynamic" &&
      readsMetadata(operands[1]!, ctx)
    ) {
      throw new UnsupportedOperatorError(
        "div",
        `div by a value read from metadata: ${SIGNED_ZERO}, and IEEE division by a zero gives the infinity of its sign`,
      );
    }
    const type: StaticType =
      mode === "int"
        ? "int"
        : mode === "error"
          ? "other"
          : left.type === "double" || right.type === "double"
            ? "double"
            : left.type === "attribute" || right.type === "attribute"
              ? "attribute"
              : "other";
    return {
      type,
      run: (env) => {
        const values = runAll([left, right], env);
        if (isEvaluationError(values)) return values;
        const [a, b] = values;
        if (mode === "error") return EVALUATION_ERROR;
        return mode === "int"
          ? intArithmetic(operator, a, b)
          : dynamicArithmetic(operator, a, b);
      },
    };
  };

// -- collections and lambdas ---------------------------------------------------------------------

/** The planner does not fix the order of a lambda's operands: the variable is the bare name. */
function lambdaComponents(lambda: PlanExpressionOperand | undefined): {
  body: PlanExpressionOperand;
  variable: string;
} {
  if (!lambda || !isExpression(lambda) || lambda.operator !== "lambda") {
    throw Error("Expected a lambda operand");
  }
  if (lambda.operands.length === 3) {
    throw new UnsupportedOperatorError(
      "lambda",
      "a two-variable comprehension (index and element, or key and value) has no evaluation in the post-filter",
    );
  }
  const [first, second] = exactly(lambda.operands, 2, "lambda") as [
    PlanExpressionOperand,
    PlanExpressionOperand,
  ];
  const bare = (op: PlanExpressionOperand): op is PlanExpressionVariable =>
    isVariable(op) && !op.name.includes(".");
  if (bare(second)) return { body: first, variable: second.name };
  if (bare(first)) return { body: second, variable: first.name };
  throw Error("Lambda requires a variable operand");
}

/**
 * The static type of an element of `collection`. Only a literal list says anything: an integral
 * number in it makes the element ambiguous, a fractional one a double.
 */
function elementType(collection: PlanExpressionOperand): StaticType {
  if (!isValue(collection) || !Array.isArray(collection.value)) {
    return "unknown";
  }
  const numbers = collection.value.filter((v) => typeof v === "number");
  if (numbers.length === 0) return "other";
  if (numbers.length !== collection.value.length) return "unknown";
  return numbers.every((n) => !Number.isInteger(n)) ? "double" : "ambiguous";
}

type Macro = "exists" | "exists_one" | "all" | "filter" | "map";

const macro =
  (operator: Macro): Compiler =>
  (operands, ctx) => {
    const [collectionOperand, lambda] = exactly(operands, 2, operator) as [
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];
    const collection = compile(collectionOperand, ctx);
    const { body, variable } = lambdaComponents(lambda);
    const bodyCtx: Context = {
      ...ctx,
      bindings: new Map([
        ...ctx.bindings,
        [variable, elementType(collectionOperand)],
      ]),
    };
    const compiledBody =
      operator === "map"
        ? compile(body, bodyCtx)
        : compileCondition(body, bodyCtx);
    const each = (env: Env, element: unknown): unknown =>
      compiledBody.run({
        ...env,
        bindings: { ...env.bindings, [variable]: element },
      });
    return {
      type: "other",
      run: (env) => {
        const iterated = collection.run(env);
        // A CEL macro over a map ranges over its keys.
        const items = isRecord(iterated) ? Object.keys(iterated) : iterated;
        if (!Array.isArray(items)) return EVALUATION_ERROR;
        switch (operator) {
          case "filter":
          case "map": {
            const out: unknown[] = [];
            for (const item of items) {
              const value = each(env, item);
              if (isEvaluationError(value)) return EVALUATION_ERROR;
              if (operator === "map") {
                out.push(value);
                continue;
              }
              const keep = asBoolean(value);
              if (isEvaluationError(keep)) return EVALUATION_ERROR;
              if (keep) out.push(item);
            }
            return out;
          }
          default: {
            // CEL's error absorption across elements: a deciding element wins over an error.
            let trueCount = 0;
            let sawError = false;
            for (const item of items) {
              const value = asBoolean(each(env, item));
              if (value === true) {
                trueCount += 1;
                if (operator === "exists") return true;
              } else if (value === false && operator === "all") {
                return false;
              } else if (isEvaluationError(value)) {
                sawError = true;
              }
            }
            if (sawError) return EVALUATION_ERROR;
            if (operator === "exists") return false;
            if (operator === "exists_one") return trueCount === 1;
            return true;
          }
        }
      },
    };
  };

// -- conversions ---------------------------------------------------------------------------------

/** How a refused conversion names the overload its operand may select. */
const MAY_BE: Readonly<Record<StaticType, string>> = {
  int: "an int",
  ambiguous: "an int",
  double: "a double",
  attribute: "a stored value",
  timestamp: "a timestamp",
  duration: "a duration",
  other: "a string, bool, list or map",
  unknown: "of a type the plan does not carry",
};

/**
 * A one-argument conversion, refused over an operand whose static type selects an overload the
 * evaluator does not model: `int()` and `double()` of a timestamp (seconds since the epoch), and
 * `timestamp()` of an int (the same, the other way).
 */
const conversion =
  (
    operator: string,
    type: StaticType,
    refused: StaticType[],
    convert: (value: unknown) => unknown,
  ): Compiler =>
  (operands, ctx) => {
    const [operand] = compileAll(operands, 1, ctx) as [Compiled];
    if (refused.includes(operand.type)) {
      throw new UnsupportedOperatorError(
        operator,
        `${operator}() over an operand that may be ${MAY_BE[operand.type]}: the post-filter does not evaluate that overload`,
      );
    }
    return {
      type,
      run: (env) => {
        const value = operand.run(env);
        return isEvaluationError(value) ? value : convert(value);
      },
    };
  };

// -- hierarchies ---------------------------------------------------------------------------------

const hierarchyRelation = (
  operator: "ancestorOf" | "descendentOf" | "overlaps",
): Compiler =>
  strict(2, "other", ([left, right]) => {
    if (!(left instanceof CelHierarchy) || !(right instanceof CelHierarchy)) {
      return EVALUATION_ERROR;
    }
    switch (operator) {
      case "ancestorOf":
        return isStrictAncestor(left, right);
      case "descendentOf":
        return isStrictAncestor(right, left);
      default:
        return isSegmentPrefix(left, right) || isSegmentPrefix(right, left);
    }
  });

// -- index ---------------------------------------------------------------------------------------

/** CEL's `[]`: a list by an integral position, a map by a key it holds; anything else is an error. */
function indexValue(collection: unknown, index: unknown): unknown {
  if (Array.isArray(collection)) {
    const position =
      typeof index === "bigint"
        ? Number(index)
        : typeof index === "number" && Number.isInteger(index)
          ? index
          : undefined;
    return position !== undefined &&
      position >= 0 &&
      position < collection.length
      ? collection[position]
      : EVALUATION_ERROR;
  }
  if (isRecord(collection) && typeof index === "string") {
    return Object.prototype.hasOwnProperty.call(collection, index)
      ? collection[index]
      : EVALUATION_ERROR;
  }
  return EVALUATION_ERROR;
}

function indexInto(collection: Compiled, key: Compiled): Compiled {
  return {
    type: "unknown",
    run: (env) => {
      const values = runAll([collection, key], env);
      return isEvaluationError(values)
        ? values
        : indexValue(values[0], values[1]);
    },
  };
}

/**
 * A map literal, `{"a": x, "b": y}`, read only as the target of an index: the plan's `struct` drops
 * the type name, so a message literal is the same node, and only a map can be indexed with `[]`.
 * Each key must be a string literal, since a JavaScript record cannot tell the CEL key `1` from
 * `"1"`, and no two keys may repeat. A value that errors makes the whole map one.
 */
function compileMapLiteral(
  entries: PlanExpressionOperand[],
  ctx: Context,
): Compiled {
  const keys = new Set<string>();
  const fields = entries.map((entry) => {
    if (!isExpression(entry) || entry.operator !== "set-field") {
      throw new UnsupportedOperatorError(
        "struct",
        "a struct literal whose entries are not set-field pairs has no evaluation in the post-filter",
      );
    }
    const [key, value] = exactly(entry.operands, 2, "set-field") as [
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];
    if (!isValue(key) || typeof key.value !== "string" || keys.has(key.value)) {
      throw new UnsupportedOperatorError(
        "struct",
        "a map literal is evaluated only with distinct string literal keys: a JavaScript record cannot tell the CEL key 1 from \"1\"",
      );
    }
    keys.add(key.value);
    return [key.value, compile(value, { ...ctx, parent: "set-field" })] as const;
  });
  return {
    type: "other",
    run: (env) => {
      const values = runAll(
        fields.map(([, value]) => value),
        env,
      );
      if (isEvaluationError(values)) return values;
      return Object.fromEntries(fields.map(([key], i) => [key, values[i]]));
    },
  };
}

// -- the roster ----------------------------------------------------------------------------------

/**
 * Every operator the post-filter evaluates. An operator with no entry is refused at compile time,
 * so adding one is adding an entry.
 */
const OPERATORS: Record<string, Compiler> = {
  and: junction(false),
  or: junction(true),
  not: (operands, ctx) => {
    const [operand] = exactly(operands, 1, "not") as [PlanExpressionOperand];
    const compiled = compileCondition(operand, ctx);
    return {
      type: "other",
      run: (env) => {
        const value = asBoolean(compiled.run(env));
        return isEvaluationError(value) ? value : !value;
      },
    };
  },

  eq: comparison("eq"),
  ne: comparison("ne"),
  lt: comparison("lt"),
  le: comparison("le"),
  gt: comparison("gt"),
  ge: comparison("ge"),

  in: strict(2, "other", ([needle, haystack]) => {
    if (Array.isArray(haystack)) {
      return haystack.some((value) => valuesEqual(value, needle));
    }
    // A map's `in` tests its keys, which a JSON map literal only ever has as strings.
    if (isRecord(haystack)) {
      return (
        typeof needle === "string" &&
        Object.prototype.hasOwnProperty.call(haystack, needle)
      );
    }
    return EVALUATION_ERROR;
  }),

  contains: stringTest((receiver, needle) => receiver.includes(needle)),
  startsWith: stringTest((receiver, prefix) => receiver.startsWith(prefix)),
  endsWith: stringTest((receiver, suffix) => receiver.endsWith(suffix)),
  // cel-go's strings extension: only the 26 ASCII letters are folded, every other code point kept.
  upperAscii: strict(1, "other", ([receiver]) =>
    typeof receiver === "string"
      ? receiver.replace(/[a-z]+/g, (letters) => letters.toUpperCase())
      : EVALUATION_ERROR,
  ),
  matches: (operands, ctx) => {
    const [receiver, pattern] = exactly(operands, 2, "matches") as [
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];
    const safe =
      isValue(pattern) && typeof pattern.value === "string"
        ? parseSafeRegexPattern(pattern.value)
        : undefined;
    if (!safe) {
      throw new UnsupportedOperatorError(
        "matches",
        "matches requires a constant RE2 pattern in the literal, anchor and trailing .* subset the post-filter answers with plain string comparisons, without a regex engine whose dialect differs from RE2",
      );
    }
    const compiled = compile(receiver, ctx);
    return {
      type: "other",
      run: (env) => {
        const value = compiled.run(env);
        if (isEvaluationError(value)) return value;
        return typeof value === "string"
          ? matchesSafeRegexPattern(value, safe)
          : EVALUATION_ERROR;
      },
    };
  },

  hasIntersection: strict(2, "other", ([left, right]) =>
    Array.isArray(left) && Array.isArray(right)
      ? left.some((l) => right.some((r) => valuesEqual(l, r)))
      : EVALUATION_ERROR,
  ),

  exists: macro("exists"),
  exists_one: macro("exists_one"),
  all: macro("all"),
  filter: macro("filter"),
  map: macro("map"),

  add: arithmetic("add"),
  sub: arithmetic("sub"),
  mult: arithmetic("mult"),
  div: arithmetic("div"),
  mod: arithmetic("mod"),

  // A list literal the plan builds from expressions: an element that errors makes the whole list one.
  list: (operands, ctx) => {
    const compiled = operands.map((op) => compile(op, ctx));
    return { type: "other", run: (env) => runAll(compiled, env) };
  },

  index: (operands, ctx) => {
    const [target, key] = exactly(operands, 2, "index") as [
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];
    const collection =
      isExpression(target) && target.operator === "struct"
        ? compileMapLiteral(target.operands, ctx)
        : compile(target, ctx);
    return indexInto(collection, compile(key, ctx));
  },
  "get-field": (operands, ctx) => {
    const [targetOperand, fieldOperand] = exactly(operands, 2, "get-field") as [
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];
    const field = isVariable(fieldOperand)
      ? fieldOperand.name
      : isValue(fieldOperand) && typeof fieldOperand.value === "string"
        ? fieldOperand.value
        : undefined;
    if (field === undefined) throw Error("get-field requires a field name");
    const target = compile(targetOperand, ctx);
    return {
      type: "unknown",
      run: (env) => {
        const value = target.run(env);
        if (isEvaluationError(value)) return value;
        return isRecord(value) &&
          Object.prototype.hasOwnProperty.call(value, field)
          ? value[field]
          : EVALUATION_ERROR;
      },
    };
  },
  size: strict(1, "int", ([value]) => {
    if (typeof value === "string") return Array.from(value).length;
    if (Array.isArray(value)) return value.length;
    if (value instanceof CelHierarchy) return value.segments.length;
    if (isRecord(value)) return Object.keys(value).length;
    return EVALUATION_ERROR;
  }),

  string: (operands, ctx) => {
    const [operand] = compileAll(operands, 1, ctx) as [Compiled];
    if (operand.type === "timestamp" || operand.type === "duration") {
      throw new UnsupportedOperatorError(
        "string",
        `string() over a ${operand.type} renders it in ${operand.type === "timestamp" ? "RFC 3339" : "seconds"}, an overload the post-filter does not evaluate`,
      );
    }
    if (operand.type === "ambiguous" || operand.type === "unknown") {
      throw new UnsupportedOperatorError(
        "string",
        'string() renders an int and a double differently past 1e6 ("1000000" against "1e+06"), and the plan does not carry which this operand is',
      );
    }
    if (
      operand.type !== "int" &&
      operand.type !== "other" &&
      readsMetadata(operands[0]!, ctx)
    ) {
      throw new UnsupportedOperatorError(
        "string",
        `string() over a number read from metadata: ${SIGNED_ZERO}, and string() renders -0.0 as "-0"`,
      );
    }
    const isInt = operand.type === "int";
    return {
      type: "other",
      run: (env) => {
        const value = operand.run(env);
        if (isInt && (typeof value === "number" || typeof value === "bigint")) {
          return BigInt(value).toString();
        }
        return convertToString(value);
      },
    };
  },
  double: conversion(
    "double",
    "double",
    ["timestamp", "duration", "unknown"],
    convertToDouble,
  ),
  int: conversion(
    "int",
    "int",
    ["timestamp", "duration", "unknown"],
    convertToInt,
  ),
  timestamp: conversion(
    "timestamp",
    "timestamp",
    ["int", "ambiguous", "duration", "unknown"],
    (value) =>
      typeof value === "string"
        ? parseRfc3339Timestamp(value)
        : EVALUATION_ERROR,
  ),
  duration: conversion(
    "duration",
    "duration",
    ["int", "ambiguous", "timestamp", "unknown"],
    (value) =>
      typeof value === "string" ? parseGoDuration(value) : EVALUATION_ERROR,
  ),
  timeSince: (operands, ctx) => {
    const [operand] = compileAll(operands, 1, ctx) as [Compiled];
    // Cerbos evaluates `timeSince` against one clock reading per request. The plan carries none, so
    // the post-filter takes its own when the plan is translated, and holds it for every record.
    const now = new CelTimestamp(BigInt(Date.now()) * 1_000_000n);
    return {
      type: "duration",
      run: (env) => {
        const value = operand.run(env);
        return isEvaluationError(value) ? value : timeSince(now, value);
      },
    };
  },

  if: (operands, ctx) => {
    const [condition, then, otherwise] = exactly(operands, 3, "if").map((op) =>
      compile(op, ctx),
    ) as [Compiled, Compiled, Compiled];
    return {
      type: joinTypes(then.type, otherwise.type),
      run: (env) => {
        const chosen = asBoolean(condition.run(env));
        if (isEvaluationError(chosen)) return chosen;
        return (chosen ? then : otherwise).run(env);
      },
    };
  },

  hierarchy: (operands, ctx) => {
    if (operands.length < 1 || operands.length > 2) {
      throw Error("Expected one or two operands for hierarchy");
    }
    const [value, delimiter] = operands.map((op) => compile(op, ctx)) as [
      Compiled,
      Compiled | undefined,
    ];
    return {
      type: "other",
      run: (env) => {
        const path = value.run(env);
        if (isEvaluationError(path)) return path;
        if (!delimiter) return toHierarchy(path);
        const separator = delimiter.run(env);
        return isEvaluationError(separator)
          ? separator
          : toHierarchy(path, separator);
      },
    };
  },
  ancestorOf: hierarchyRelation("ancestorOf"),
  descendentOf: hierarchyRelation("descendentOf"),
  overlaps: hierarchyRelation("overlaps"),
};

/** The static type of a ternary: its branches' when they agree, and a number only when certain. */
function joinTypes(a: StaticType, b: StaticType): StaticType {
  if (a === b) return a;
  const pair = [a, b];
  if (pair.includes("ambiguous")) {
    if (pair.includes("int")) return "int";
    if (pair.includes("double")) return "double";
    return "unknown";
  }
  if (pair.every((t) => t === "attribute" || t === "double" || t === "other")) {
    return "attribute";
  }
  return "unknown";
}
