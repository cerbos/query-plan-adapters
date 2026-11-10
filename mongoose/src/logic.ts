import type { PlanExpression, PlanExpressionOperand } from "@cerbos/core";

import {
  LAMBDA_OPERATORS,
  ORDERINGS,
  buildAggregationExpression,
  buildConstant,
  guardFor,
  isListTyped,
  relationElements,
  temporalMismatch,
} from "./aggregation";
import { FinalUnsupportedQueryPlanError, UnsupportedQueryPlanError } from "./errors";
import type { MapperConfig } from "./index";
import type { ScopedMapper } from "./mapper";
import {
  isNullableReference,
  relationOfReference,
  resolveFieldReference,
  valueTypeOf,
} from "./mapper";
import { getOperandAt, isExpression, isValue, isVariable } from "./operands";

/**
 * CEL's three-valued logic as one aggregation expression: every condition evaluates to true,
 * false, or null for a CEL error. It is what a query filter cannot say. `$elemMatch` rebases every
 * path onto the array element, and MongoDB accepts `$expr` only at the top level, so an element
 * compared with the enclosing document, arithmetic or a ternary inside a macro, and a macro nested
 * over the same collection have no filter form; and a filter has no UNKNOWN, so a negated macro
 * would turn an element's error into a match. Evaluated here instead, a condition is kept where it
 * is `true`, and a negated one where it is `false`: an error is neither, so it denies under both
 * polarities, as it does in CEL.
 *
 * Errors follow CEL: `&&` is false if any operand is false, `||` true if any is true, and an error
 * otherwise wins; exists() is true if any element is, all() false if any element is, and
 * exists_one() raises if any element does. A leaf is an error where one of its operands cannot be
 * evaluated: a nullable field that is null or missing, an absent to-one parent, a field read on an
 * element that is not a document, or any guard the aggregation lowering declares.
 */

/** A macro's elements are bound to `$$cel_<name>`, apart from the internal `$$cerbos_*`. */
const BINDING_PREFIX = "cel_";

type Bound = readonly string[];

const COMPARISONS = new Set(["eq", "ne", "lt", "le", "gt", "ge"]);

const isComparison = (operator: string): boolean => COMPARISONS.has(operator);

const LEAF_OPERATORS = new Set([
  ...COMPARISONS,
  "matches", "contains", "startsWith", "endsWith", "in", "ancestorOf", "descendentOf", "overlaps",
  "hasIntersection", "isSubset",
]);

/** An aggregation expression evaluating to true, false or null. */
export const truth = (node: PlanExpressionOperand, mapper: ScopedMapper, bound: Bound = []): unknown => {
  if (isValue(node)) {
    if (typeof node.value !== "boolean") {
      throw new UnsupportedQueryPlanError("A non-boolean constant in boolean position is an error");
    }

    return node.value;
  }

  if (isVariable(node)) return bareTruth(node.name, mapper, bound);

  if (!isExpression(node)) throw new UnsupportedQueryPlanError("Invalid Cerbos expression structure");

  switch (node.operator) {
    case "and":
      return allOf(node.operands.map((child) => truth(child, mapper, bound)));
    case "or":
      return anyOf(node.operands.map((child) => truth(child, mapper, bound)));
    case "not":
      return negate(truth(getOperandAt(node.operands, 0, "not requires an operand"), mapper, bound));
    case "exists":
    case "all":
    case "exists_one":
      return macro(node, mapper, bound);
    case "if":
      return ternary(node, mapper, bound);
    case "has":
      return presence(node, mapper, bound);
    case "filter":
    case "map":
    case "except":
    case "intersect":
    case "list":
    case "struct":
      // A list or map where CEL needs a boolean is a runtime type error.
      return null;
    default:
      if (LEAF_OPERATORS.has(node.operator)) return leaf(node, mapper, bound);
      throw new UnsupportedQueryPlanError(`${node.operator} has no three-valued aggregation form`);
  }
};

const allOf = (values: unknown[]): unknown =>
  withValues(values, (list) => ({
    $cond: [{ $in: [false, list] }, false, { $cond: [{ $in: [null, list] }, null, true] }],
  }));

const anyOf = (values: unknown[]): unknown =>
  withValues(values, (list) => ({
    $cond: [{ $in: [true, list] }, true, { $cond: [{ $in: [null, list] }, null, false] }],
  }));

const negate = (value: unknown): unknown => ({
  $let: { vars: { cerbos_x: value }, in: { $cond: [{ $eq: ["$$cerbos_x", null] }, null, { $not: ["$$cerbos_x"] }] } },
});

const withValues = (values: unknown, body: (list: string) => unknown): unknown => ({
  $let: { vars: { cerbos_values: values }, in: body("$$cerbos_values") },
});

const ok = (conditions: unknown[]): unknown => {
  const unique = [...new Map(conditions.map((condition) => [JSON.stringify(condition), condition])).values()];

  return unique.length === 0 ? true : { $and: unique };
};

const guarded = (condition: unknown, value: unknown): unknown => ({ $cond: [condition, value, null] });

/** A boolean field in boolean position: anything but a boolean is an error. */
const bareTruth = (name: string, mapper: ScopedMapper, bound: Bound): unknown => {
  const value = buildAggregationExpression({ name }, mapper);

  return guarded(ok([...variableOks([name], mapper, bound), { $eq: [{ $type: value }, "bool"] }]), value);
};

/** A comparison or string predicate: its value where every operand evaluates, else null. */
const leaf = (node: PlanExpression, mapper: ScopedMapper, bound: Bound): unknown => {
  const [left, right] = node.operands;

  if (isComparison(node.operator) && temporalMismatch(left, right, mapper)) {
    // No ordering between the two types (an error on every document); `==` is false.
    if (ORDERINGS.has(node.operator)) return null;

    return guarded(evaluates(node, mapper, bound), node.operator === "ne");
  }

  if (
    isComparison(node.operator) &&
    node.operands.some((op) => isVariable(op) && valueTypeOf(op.name, mapper) === "dateTime") &&
    !node.operands.some((op) => isValue(op) && op.value === null)
  ) {
    throw new FinalUnsupportedQueryPlanError(
      "Bare temporal field comparison cannot preserve CEL string comparison: stored Dates " +
        "discard the original lexical spelling; compare timestamp(...) values instead",
    );
  }

  return guarded(evaluates(node, mapper, bound), buildAggregationExpression(node, mapper));
};

/**
 * Whether `node` evaluates without a CEL error: each operand does, and the node's own guard
 * holds. A ternary evaluates only the branch its condition selects, and only a boolean condition.
 * `||` evaluates where any operand evaluates to true or every operand to a boolean, `&&` likewise
 * with false.
 */
const evaluates = (node: PlanExpressionOperand, mapper: ScopedMapper, bound: Bound): unknown => {
  if (isValue(node)) return true;

  if (isVariable(node)) return ok(variableOks([node.name], mapper, bound));

  if (!isExpression(node)) throw new UnsupportedQueryPlanError("Invalid Cerbos expression structure");

  if (node.operator === "if" && node.operands.length === 3) {
    const [condition, whenTrue, whenFalse] = node.operands as [
      PlanExpressionOperand,
      PlanExpressionOperand,
      PlanExpressionOperand,
    ];

    const selected = buildAggregationExpression(condition, mapper);

    return ok([
      evaluates(condition, mapper, bound),
      { $eq: [{ $type: selected }, "bool"] },
      { $cond: [selected, evaluates(whenTrue, mapper, bound), evaluates(whenFalse, mapper, bound)] },
    ]);
  }

  if (node.operator === "and" || node.operator === "or") {
    const decisive = node.operator === "or";

    const operands = node.operands.map((child) => ({
      evaluates: evaluates(child, mapper, bound),
      value: buildAggregationExpression(child, mapper),
    }));

    return {
      $or: [
        ...operands.map(({ evaluates: e, value }) => ok([e, { $eq: [value, decisive] }])),
        ok(operands.map(({ evaluates: e, value }) => ok([e, { $eq: [{ $type: value }, "bool"] }]))),
      ],
    };
  }

  if (node.operator === "filter") return { $ne: [filterValue(node, mapper, bound), null] };

  if (node.operator === "map") return { $ne: [mapValue(node, mapper, bound), null] };

  if (LAMBDA_OPERATORS.has(node.operator)) {
    throw new UnsupportedQueryPlanError(`${node.operator} inside a value has no three-valued form`);
  }

  const guard = guardFor(node, mapper);

  if (guard && !(Object.keys(guard).length === 1 && "$expr" in guard)) {
    throw new UnsupportedQueryPlanError("A guard with no aggregation form");
  }

  return ok([...node.operands.map((child) => evaluates(child, mapper, bound)), ...(guard ? [guard["$expr"]] : [])]);
};

/**
 * has(x): whether the path is stored (and, where a stored null is a missing attribute, not null),
 * or null, an error, where the selection reads through a missing to-one parent or a macro element
 * that is not a document.
 */
const presence = (node: PlanExpression, mapper: ScopedMapper, bound: Bound): unknown => {
  const [operand] = node.operands;

  if (node.operands.length !== 1 || operand === undefined || !isVariable(operand)) {
    throw new UnsupportedQueryPlanError("has takes exactly one attribute reference");
  }

  const { path, relation } = resolveFieldReference(operand.name, mapper);

  if (relation && relation.type !== "one") {
    throw new UnsupportedQueryPlanError(
      `Cannot translate has(${operand.name}): a presence test over a to-many relation has no filter that matches what the application sends for it`,
    );
  }

  const field = path.join(".");
  const namesRelation = relation !== undefined && field === relation.name;
  let receiver: string | undefined;

  if (relation && !namesRelation) {
    receiver = relation.name;
  } else if (namesRelation) {
    const parent = operand.name.slice(0, Math.max(0, operand.name.lastIndexOf(".")));
    const outer = parent === "" ? undefined : relationOfReference(parent, mapper);
    receiver = outer?.type === "one" ? outer.name : undefined;
  }

  const notNull = namesRelation || isNullableReference(operand.name, mapper);
  const absent = notNull ? ["missing", "null"] : ["missing"];
  const present = { $not: [{ $in: [{ $type: buildAggregationExpression(operand, mapper) }, absent] }] };
  const receivers: unknown[] = [];
  const element = bound.find((variable) => operand.name.startsWith(`${variable}.`));

  if (element !== undefined) receivers.push({ $eq: [{ $type: `$$${BINDING_PREFIX}${element}` }, "object"] });

  if (receiver !== undefined) receivers.push({ $eq: [{ $type: `$${receiver}` }, "object"] });

  return receivers.length === 0 ? present : guarded(ok(receivers), present);
};

const ternary = (node: PlanExpression, mapper: ScopedMapper, bound: Bound): unknown => {
  const [condition, whenTrue, whenFalse] = node.operands;

  if (!condition || !whenTrue || !whenFalse) throw new UnsupportedQueryPlanError("if requires three operands");

  return {
    $let: {
      vars: { cerbos_condition: truth(condition, mapper, bound) },
      in: {
        $switch: {
          branches: [
            { case: { $eq: ["$$cerbos_condition", true] }, then: truth(whenTrue, mapper, bound) },
            { case: { $eq: ["$$cerbos_condition", false] }, then: truth(whenFalse, mapper, bound) },
          ],
          default: null,
        },
      },
    },
  };
};

/**
 * What must hold for each variable to be readable: a nullable one is neither null nor missing, a
 * to-one parent is a document, and a macro element read through a field is a document.
 */
const variableOks = (names: string[], mapper: ScopedMapper, bound: Bound): unknown[] =>
  [...new Set(names)].flatMap((name) => {
    const element = bound.find((variable) => name.startsWith(`${variable}.`));

    if (element !== undefined) {
      return [{ $eq: [{ $type: `$$${BINDING_PREFIX}${element}` }, "object"] }, ...nullableOk(name, mapper)];
    }

    if (bound.includes(name)) {
      // A relation's element read as its projected field is read through a document.
      const value = buildAggregationExpression({ name }, mapper);
      const projected = typeof value === "string" && value.includes(".");

      return [
        ...(projected ? [{ $eq: [{ $type: `$$${BINDING_PREFIX}${name}` }, "object"] }] : []),
        ...nullableOk(name, mapper),
      ];
    }

    const relation = relationOfReference(name, mapper);
    let parent: unknown[] = [];

    if (relation?.type === "one") {
      parent = [{ $eq: [{ $type: `$${relation.name}` }, "object"] }];
    } else if (relation?.requiresParent !== undefined) {
      // The to-one parent, stored as an array, is stored at all.
      parent = [{ $ne: [buildAggregationExpression({ name }, mapper), null] }];
    }

    return [...parent, ...nullableOk(name, mapper)];
  });

const nullableOk = (name: string, mapper: ScopedMapper): unknown[] =>
  isNullableReference(name, mapper)
    ? [{ $not: [{ $in: [{ $type: buildAggregationExpression({ name }, mapper) }, ["missing", "null"]] }] }]
    : [];

const lambdaParts = (node: PlanExpression, what: string) => {
  const [collection, lambda] = node.operands;

  if (
    !collection ||
    !lambda ||
    !isExpression(lambda) ||
    lambda.operator !== "lambda" ||
    !lambda.operands[1] ||
    !isVariable(lambda.operands[1])
  ) {
    throw new UnsupportedQueryPlanError(`${what} requires a collection and a single-variable lambda`);
  }

  return { collection, lambda };
};

/**
 * exists(), all() and exists_one() over a list: the elements' truths, folded as CEL folds them,
 * where the list is an array; an error otherwise.
 */
const macro = (node: PlanExpression, mapper: ScopedMapper, bound: Bound): unknown => {
  const { collection, lambda } = lambdaParts(node, node.operator);

  if (lambda.operands.length === 3) return twoVariableMacro(node, mapper, bound);
  const variable = (lambda.operands[1] as { name: string }).name;
  const { input, listOk, scoped } = elementScope(collection, variable, mapper, bound);

  const values = {
    $map: {
      input,
      as: `${BINDING_PREFIX}${variable}`,
      in: truth(lambda.operands[0]!, scoped, [...bound, variable]),
    },
  };

  return guarded(listOk, foldMacro(node.operator, values));
};

/**
 * exists(i, e, ...), all(i, e, ...) and exists_one(i, e, ...) over a list: CEL binds the first
 * variable to each position and the second to the element there. Over a map CEL binds a key and
 * its value instead, which a stored subdocument does not carry, so only a reference the mapper
 * declares as a list, or a list the plan builds, is iterated.
 */
const twoVariableMacro = (node: PlanExpression, mapper: ScopedMapper, bound: Bound): unknown => {
  const [collection, lambda] = node.operands as [PlanExpressionOperand, PlanExpression];
  const [body, indexVariable, elementVariable] = lambda.operands;

  if (!body || !indexVariable || !elementVariable || !isVariable(indexVariable) || !isVariable(elementVariable)) {
    throw new UnsupportedQueryPlanError(`${node.operator} requires named iteration variables`);
  }

  if (
    isVariable(collection) &&
    mapper.lookup(collection.name) === undefined &&
    relationOfReference(collection.name, mapper) === undefined
  ) {
    throw new UnsupportedQueryPlanError(
      `A two-variable ${node.operator}() over ${collection.name}, which the mapper declares neither a field nor a relation: over a map CEL ranges over its keys and values, which a stored document does not carry as the application sends them`,
    );
  }

  const index = indexVariable.name;
  const element = elementVariable.name;
  const { input, listOk, scoped: elementScoped } = elementScope(collection, element, mapper, bound);
  const scoped = elementMapper(elementScoped, index, `$${BINDING_PREFIX}${index}`, undefined);

  const values = {
    $map: {
      input: { $range: [0, { $size: [input] }] },
      as: `${BINDING_PREFIX}${index}`,
      in: {
        $let: {
          vars: { [`${BINDING_PREFIX}${element}`]: { $arrayElemAt: [input, `$$${BINDING_PREFIX}${index}`] } },
          in: truth(body, scoped, [...bound, element, index]),
        },
      },
    },
  };

  return guarded(listOk, foldMacro(node.operator, values));
};

/** The elements' truths, folded as CEL folds them for `operator`. */
const foldMacro = (operator: string, values: unknown): unknown => {
  let folded: unknown;

  if (operator === "exists") {
    folded = { $cond: [{ $in: [true, "$$cerbos_values"] }, true, { $cond: [{ $in: [null, "$$cerbos_values"] }, null, false] }] };
  } else if (operator === "all") {
    folded = { $cond: [{ $in: [false, "$$cerbos_values"] }, false, { $cond: [{ $in: [null, "$$cerbos_values"] }, null, true] }] };
  } else {
    folded = {
      $cond: [
        { $in: [null, "$$cerbos_values"] },
        null,
        { $eq: [{ $size: { $filter: { input: "$$cerbos_values", cond: { $eq: ["$$this", true] } } } }, 1] },
      ],
    };
  }

  return { $let: { vars: { cerbos_values: values }, in: folded } };
};

/**
 * filter(): the elements whose condition is true, or null (a CEL error) where the list is not an
 * array or any element's condition raises. A relation's projection yields the projected field of
 * each kept element.
 */
export const filterValue = (node: PlanExpression, mapper: ScopedMapper, bound: Bound = mapper.bound): unknown => {
  const { collection, lambda } = lambdaParts(node, "filter");
  const variable = (lambda.operands[1] as { name: string }).name;
  const binding = `${BINDING_PREFIX}${variable}`;
  const { input, listOk, scoped } = elementScope(collection, variable, mapper, bound);
  const condition = truth(lambda.operands[0]!, scoped, [...bound, variable]);
  let kept: unknown = { $filter: { input, as: binding, cond: { $eq: [condition, true] } } };
  const relation = isVariable(collection) ? mapper.lookup(collection.name)?.relation : undefined;

  if (relation?.field) {
    const field = relation.fields?.[relation.field]?.field ?? relation.field;
    kept = { $map: { input: kept, as: binding, in: `$$${binding}.${field}` } };
  }

  const values = { $map: { input, as: binding, in: condition } };

  return guarded(listOk, { $cond: [{ $in: [null, values] }, null, kept] });
};

/**
 * map(): each element's projection, or null (a CEL error) where the list is not an array or any
 * projection raises.
 */
export const mapValue = (node: PlanExpression, mapper: ScopedMapper, bound: Bound = mapper.bound): unknown => {
  const { collection, lambda } = lambdaParts(node, "map");

  if (lambda.operands.length !== 2) {
    throw new UnsupportedQueryPlanError("map requires a collection and a single-variable lambda");
  }

  const variable = (lambda.operands[1] as { name: string }).name;
  const binding = `${BINDING_PREFIX}${variable}`;
  const { input, listOk, scoped } = elementScope(collection, variable, mapper, bound);
  const projection = lambda.operands[0]!;
  const inner = [...bound, variable];
  const projected = { $map: { input, as: binding, in: buildAggregationExpression(projection, scoped) } };
  const evaluated = { $allElementsTrue: [{ $map: { input, as: binding, in: evaluates(projection, scoped, inner) } }] };

  return guarded(listOk, { $cond: [evaluated, projected, null] });
};

/**
 * The array a macro ranges over, what must hold for it to be one, and the mapper its body reads
 * the element with.
 */
const elementScope = (
  collection: PlanExpressionOperand,
  variable: string,
  mapper: ScopedMapper,
  bound: Bound,
): { input: unknown; listOk: unknown; scoped: ScopedMapper } => {
  const binding = `$${BINDING_PREFIX}${variable}`;

  if (isValue(collection)) {
    if (!Array.isArray(collection.value)) {
      throw new UnsupportedQueryPlanError("A macro over a constant that is not a list");
    }

    return { input: buildConstant(collection.value), listOk: true, scoped: elementMapper(mapper, variable, binding, undefined) };
  }

  // A list the plan builds from attributes (`[R.attr.a, R.attr.b].exists(...)`, a
  // concatenation): it errors where any part of it does.
  if (isExpression(collection) && isListTyped(collection)) {
    const input = buildAggregationExpression(collection, mapper);

    return {
      input,
      listOk: ok([evaluates(collection, mapper, bound), { $isArray: [input] }]),
      scoped: elementMapper(mapper, variable, binding, undefined),
    };
  }

  if (!isVariable(collection)) throw new UnsupportedQueryPlanError("A macro's collection must be a field or a list");
  const relation = mapper.lookup(collection.name)?.relation;

  if (relation && relation.type !== "many") {
    throw new UnsupportedQueryPlanError("A macro over a to-one relation ranges over its keys, which have no three-valued form");
  }

  let input: unknown;

  if (relation?.requiresParent !== undefined) {
    input = relationElements(relation, undefined);
  } else if (relation) {
    input = `$${relation.name}`;
  } else {
    input = buildAggregationExpression(collection, mapper);
  }

  const listOk = ok([...variableOks([collection.name], mapper, bound), { $isArray: [input] }]);

  return { input, listOk, scoped: elementMapper(mapper, variable, binding, relation) };
};

/**
 * The mapper a macro's body reads with: the element as `$$cel_<variable>` (its projected field
 * for a relation that names one), each element field under it, and every other key as the
 * enclosing mapper reads it.
 */
const elementMapper = (
  outer: ScopedMapper,
  variable: string,
  binding: string,
  relation: MapperConfig["relation"] | undefined,
): ScopedMapper => {
  const nullableDefault = outer.nullableDefault;

  const lookup = (key: string): MapperConfig | undefined => {
    if (key !== variable && !key.startsWith(`${variable}.`)) return outer.lookup(key);
    const field = key === variable ? relation?.field : key.slice(variable.length + 1);

    if (field === undefined) return { field: binding, nullable: false };
    const config = relation?.fields?.[field];
    const nested = config?.relation && { ...config.relation, name: `${binding}.${config.relation.name}` };

    return {
      field: `${binding}.${config?.field ?? field}`,
      nullable: config?.nullable ?? nullableDefault,
      ...(config?.valueParser ? { valueParser: config.valueParser } : {}),
      ...(config?.valueType ? { valueType: config.valueType } : {}),
      ...(nested ? { relation: nested } : {}),
    };
  };

  return { lookup, nullableDefault, bound: [...outer.bound, variable] };
};
