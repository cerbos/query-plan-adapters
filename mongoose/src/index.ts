import {
  PlanResourcesResponse,
  PlanExpressionOperand,
  PlanExpression,
  PlanExpressionValue,
  PlanExpressionVariable,
  PlanKind,
  Value,
} from "@cerbos/core";

export { PlanKind };

export type MongooseFilter = Record<string, any>;

export type MapperConfig = {
  field?: string;
  /** Treat a stored null as a missing Cerbos attribute and exclude it from comparisons. */
  nullable?: boolean;
  valueParser?: (value: any) => any;
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    /**
     * The document path of an optional to-ONE parent this collection is reached through.
     *
     * CEL cannot dot through a list, so every intermediate segment of `a.b.c` is a to-one
     * parent: absent, the application sends no attribute at all and CEL raises a
     * missing-path error, which denies. A flattened Mongo path cannot see the difference —
     * an absent parent and a childless parent both give an empty array — so
     * `size(chain) == 0` and `size(chain) >= 0` are TRUE for every parentless document and
     * return records the PDP denies. Declaring the parent makes those comparisons yield
     * null, which loses against every number in BSON order and excludes the document
     * (cerbos/query-plan-adapters#309).
     */
    requiresParent?: string;
    fields?: {
      [key: string]: MapperConfig;
    };
  };
};

export type Mapper =
  | {
      [key: string]: MapperConfig;
    }
  | ((key: string) => MapperConfig);

/**
 * How the caller represents a NULL field when building the attributes it sends to `check()`.
 *
 * The planner emits the same `eq(attr, null)` node either way, so the plan cannot reveal which
 * convention is in use and the adapter has to be told.
 *
 * - `"explicit"` (default) — a NULL field is sent as an explicit `null` attribute. CEL compares
 *   `null == null`, so matching null selects exactly the documents `check()` allows.
 * - `"omitted"` — a NULL field sends no attribute at all. CEL then raises a missing-attribute
 *   error, which Cerbos treats as a deny, so a filter that *selects* null documents returns
 *   documents the PDP denies. Null comparison operands are rejected instead of translated.
 *
 * See https://github.com/cerbos/query-plan-adapters/issues/302.
 */
export type NullAttributeRepresentation = "explicit" | "omitted";

export interface QueryPlanToMongooseArgs {
  queryPlan: PlanResourcesResponse;
  mapper?: Mapper;
  /**
   * Which NULL-field representation the caller uses when building `check()` attributes.
   * Defaults to `"explicit"`, preserving the historical null-matching translation.
   */
  nullAttributeRepresentation?: NullAttributeRepresentation;
}

export interface QueryPlanToMongooseResult {
  kind: PlanKind;
  filters?: MongooseFilter;
}

// Helper functions for type checking
const isExpression = (e: PlanExpressionOperand): e is PlanExpression =>
  "operator" in e;
const isValue = (e: PlanExpressionOperand): e is PlanExpressionValue =>
  "value" in e;
const isVariable = (e: PlanExpressionOperand): e is PlanExpressionVariable =>
  "name" in e;

const escapeRegexValue = (value: string): string =>
  value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const normalizeRe2PatternForMongo = (pattern: string): string => {
  const escapedLiterals = new Set("\\.^$*+?()[]{}|".split(""));
  const unsupportedSyntax = new Set("()[]{}|".split(""));
  let normalized = "";
  let canQuantify = false;

  for (let index = 0; index < pattern.length; index += 1) {
    const character = pattern[index];
    if (!character) {
      break;
    }
    if (character === "\\") {
      const escaped = pattern[index + 1];
      if (!escaped || !escapedLiterals.has(escaped)) {
        throw new Error(
          "matches supports only literal escapes in the common RE2/PCRE2 subset"
        );
      }
      normalized += `\\${escaped}`;
      canQuantify = true;
      index += 1;
      continue;
    }
    if (character === "^") {
      if (index !== 0) {
        throw new Error("matches supports ^ only at the start of the pattern");
      }
      normalized += character;
      canQuantify = false;
      continue;
    }
    if (character === "$") {
      if (index !== pattern.length - 1) {
        throw new Error("matches supports $ only at the end of the pattern");
      }
      normalized += "\\z";
      canQuantify = false;
      continue;
    }
    if (character === "*" || character === "+" || character === "?") {
      if (!canQuantify) {
        throw new Error(`matches has an invalid ${character} quantifier`);
      }
      normalized += character;
      canQuantify = false;
      continue;
    }
    if (
      unsupportedSyntax.has(character) ||
      character.charCodeAt(0) < 0x20
    ) {
      throw new Error(
        "matches pattern is outside the supported common RE2/PCRE2 subset"
      );
    }
    normalized += character;
    canQuantify = true;
  }

  return normalized;
};

const RFC3339_TIMESTAMP_PATTERN =
  /^((?!0000)\d{4})-(\d{2})-(\d{2})[Tt](?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
// Same source, but for PCRE2 (Mongo) rather than the JS engine: PCRE2 `$`
// also matches immediately before a final newline, so "…Z\n" would pass as
// RFC 3339 and $convert would silently accept it. `\z` pins the absolute end.
const RFC3339_TIMESTAMP_MONGO_PATTERN =
  RFC3339_TIMESTAMP_PATTERN.source.replace(/\$$/, "\\z");
const MIN_CEL_TIMESTAMP_MILLISECONDS = Date.parse(
  "0001-01-01T00:00:00.000Z"
);
const MAX_CEL_TIMESTAMP_MILLISECONDS = Date.parse(
  "9999-12-31T23:59:59.999Z"
);
const MIN_CEL_TIMESTAMP = new Date(MIN_CEL_TIMESTAMP_MILLISECONDS);
const MAX_CEL_TIMESTAMP = new Date(MAX_CEL_TIMESTAMP_MILLISECONDS);

const isRfc3339Timestamp = (value: string): boolean => {
  const match = RFC3339_TIMESTAMP_PATTERN.exec(value);
  if (!match) {
    return false;
  }
  const yearText = match[1];
  const monthText = match[2];
  const dayText = match[3];
  if (!yearText || !monthText || !dayText) {
    return false;
  }

  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const calendarDate = new Date(0);
  calendarDate.setUTCHours(0, 0, 0, 0);
  calendarDate.setUTCFullYear(year, month - 1, day);
  if (
    calendarDate.getUTCFullYear() !== year ||
    calendarDate.getUTCMonth() !== month - 1 ||
    calendarDate.getUTCDate() !== day
  ) {
    return false;
  }

  const instant = new Date(value).getTime();
  return (
    !Number.isNaN(instant) &&
    instant >= MIN_CEL_TIMESTAMP_MILLISECONDS &&
    instant <= MAX_CEL_TIMESTAMP_MILLISECONDS
  );
};

/**
 * Converts a Cerbos query plan to a Mongoose filter
 */
export function queryPlanToMongoose({
  queryPlan,
  mapper = {},
  nullAttributeRepresentation = "explicit",
}: QueryPlanToMongooseArgs): QueryPlanToMongooseResult {
  nullRepresentation = nullAttributeRepresentation;
  switch (queryPlan.kind) {
    case PlanKind.ALWAYS_ALLOWED:
      return {
        kind: PlanKind.ALWAYS_ALLOWED,
      };
    case PlanKind.ALWAYS_DENIED:
      return { kind: PlanKind.ALWAYS_DENIED };
    case PlanKind.CONDITIONAL:
      return {
        kind: PlanKind.CONDITIONAL,
        filters: buildMongooseFilterFromCerbosExpression(
          queryPlan.condition,
          mapper
        ),
      };
    default:
      throw Error(`Invalid query plan.`);
  }
}

/**
 * Resolves a field reference considering relations
 */
type ResolvedFieldReference = {
  path: string[];
  relation?: {
    name: string;
    type: "one" | "many";
    field?: string;
    requiresParent?: string;
    nestedMapper?: {
      [key: string]: MapperConfig;
    };
  };
};

const resolveFieldReference = (
  reference: string,
  mapper: Mapper
): ResolvedFieldReference => {
  const parts = reference.split(".");
  const lastPart = parts[parts.length - 1];

  if (!lastPart) {
    return { path: [reference] };
  }

  // Try exact match first
  const config =
    typeof mapper === "function" ? mapper(reference) : mapper[reference];

  if (config?.relation) {
    const { name, field, fields, type, requiresParent } = config.relation;
    const path = field
      ? type === "one"
        ? [`${name}.${field}`]
        : [name, field]
      : [name];
    return {
      path,
      relation: {
        name,
        type,
        field,
        requiresParent,
        nestedMapper: fields,
      },
    };
  }

  if (config?.field) {
    return { path: [config.field] };
  }

  // Try parent relation for nested fields
  if (parts.length > 1) {
    const parentPath = parts.slice(0, -1).join(".");
    const parentConfig =
      typeof mapper === "function" ? mapper(parentPath) : mapper[parentPath];

    if (parentConfig?.relation) {
      const { name, fields, type, requiresParent } = parentConfig.relation;
      const fieldConfig = fields?.[lastPart];
      const fieldName = fieldConfig?.field || lastPart;
      return {
        path: fieldName
          ? type === "one"
            ? [`${name}.${fieldName}`]
            : [name, fieldName]
          : [name],
        relation: {
          name,
          type,
          field: fieldName,
          requiresParent,
          nestedMapper: fields,
        },
      };
    }
  }

  return { path: [reference] };
};

const resolveValueParser = (
  fieldReference: string,
  mapper: Mapper
): ((value: any) => any) | undefined => {
  const config =
    typeof mapper === "function" ? mapper(fieldReference) : mapper[fieldReference];

  if (config?.valueParser) {
    return config.valueParser;
  }

  const parts = fieldReference.split(".");
  if (parts.length > 1) {
    const parentPath = parts.slice(0, -1).join(".");
    const lastPart = parts[parts.length - 1] as string;
    const parentConfig =
      typeof mapper === "function" ? mapper(parentPath) : mapper[parentPath];

    if (parentConfig?.relation?.fields?.[lastPart]?.valueParser) {
      return parentConfig.relation.fields[lastPart]!.valueParser;
    }
  }

  return undefined;
};

const applyValueParser = (
  fieldReference: string,
  value: any,
  mapper: Mapper
): any => {
  const parser = resolveValueParser(fieldReference, mapper);
  return parser ? parser(value) : value;
};

const resolveMapperConfig = (
  fieldReference: string,
  mapper: Mapper
): MapperConfig | undefined => {
  const config =
    typeof mapper === "function" ? mapper(fieldReference) : mapper[fieldReference];
  if (config) {
    return config;
  }

  const parts = fieldReference.split(".");
  if (parts.length <= 1) {
    return undefined;
  }

  const parentPath = parts.slice(0, -1).join(".");
  const lastPart = parts[parts.length - 1];
  if (!lastPart) {
    return undefined;
  }
  const parentConfig =
    typeof mapper === "function" ? mapper(parentPath) : mapper[parentPath];
  return parentConfig?.relation?.fields?.[lastPart];
};

const isNullableReference = (fieldReference: string, mapper: Mapper): boolean =>
  resolveMapperConfig(fieldReference, mapper)?.nullable === true;

const collectVariableNames = (operand: PlanExpressionOperand): string[] => {
  if (isVariable(operand)) {
    return [operand.name];
  }
  if (isExpression(operand)) {
    return operand.operands.flatMap(collectVariableNames);
  }
  return [];
};

const referencesNullableField = (
  operand: PlanExpressionOperand,
  mapper: Mapper
): boolean =>
  collectVariableNames(operand).some((name) =>
    isNullableReference(name, mapper)
  );

const buildNestedObject = (path: string[], value: any) =>
  path.reduceRight(
    (acc: any, key: string, index: number) =>
      index === path.length - 1 ? { [key]: value } : { [key]: acc },
    value
  );

const buildFieldFilter = (path: string[], value: any) =>
  path.length === 0 ? value : buildNestedObject(path, value);

// Translation-scoped, set at the queryPlanToMongoose entry. Translation is synchronous, so
// module scope is safe.
let nullRepresentation: NullAttributeRepresentation = "explicit";

/**
 * Guards every site that would emit a null-selecting predicate out of a `null` comparison
 * operand.
 *
 * Under the `"omitted"` representation a NULL field carries no attribute, so CEL raises a
 * missing-attribute error and `check()` denies the document; matching null would return exactly
 * the documents the PDP refuses. The rejection is deliberately wider than the over-granting
 * shapes: `ne(x, null)` on its own is aligned, but negation is applied by wrapping the built
 * filter rather than by pushing it into the leaf, so a leaf cannot tell whether an enclosing
 * `not` will flip a not-null predicate back into a null-selecting one. Rejecting every null
 * operand is correct under any nesting; narrowing it requires negation-parity tracking.
 */
const assertNullOperandTranslatable = (context: string): void => {
  if (nullRepresentation === "omitted") {
    throw new Error(
      `Cannot translate ${context} under nullAttributeRepresentation "omitted": a NULL field ` +
        "sends no attribute, so Cerbos evaluates the comparison as a missing-attribute error " +
        "(deny) while a null-selecting filter would return those documents. Send NULL fields " +
        'as explicit nulls and use "explicit", or keep this shape out of the policy.'
    );
  }
};

/**
 * Whether a comparison operand carries a plan-level `null` — directly, or as an element of an
 * `in` list. This is the guard's own predicate rather than a reuse of `requireExists`: the two
 * happen to coincide today, but `requireExists` means "the field must be present", and a future
 * caller setting it for that reason alone must not trip the null rejection.
 */
const carriesNullOperand = (value: unknown): boolean =>
  value === null || (Array.isArray(value) && value.includes(null));

const buildGuardedFieldFilter = (
  path: string[],
  value: unknown,
  nullable: boolean,
  requireExists = false
): MongooseFilter => {
  const filter = buildFieldFilter(path, value);
  if (!nullable && !requireExists) {
    return filter;
  }
  const guards: MongooseFilter[] = [];
  if (requireExists) {
    guards.push(buildFieldFilter(path, { $exists: true }));
  }
  if (nullable) {
    guards.push(buildFieldFilter(path, { $ne: null }));
  }
  return {
    $and: [...guards, filter],
  };
};

const withNullableGuards = (
  filter: MongooseFilter,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): MongooseFilter => {
  const guardedNames = [
    ...new Set(
      operands
        .flatMap(collectVariableNames)
        .filter((name) => isNullableReference(name, mapper))
    ),
  ];
  if (guardedNames.length === 0) {
    return filter;
  }

  const guards = guardedNames.map((name) => {
    const { path } = resolveFieldReference(name, mapper);
    return buildFieldFilter(path, { $ne: null });
  });
  return { $and: [...guards, filter] };
};

type ComparisonOperator = "eq" | "ne" | "lt" | "le" | "gt" | "ge";

const mirroredComparisonOperator = (
  operator: ComparisonOperator
): ComparisonOperator => {
  switch (operator) {
    case "lt":
      return "gt";
    case "le":
      return "ge";
    case "gt":
      return "lt";
    case "ge":
      return "le";
    default:
      return operator;
  }
};

/**
 * Builds an aggregation-pipeline expression value for use inside `$expr`.
 * - Variables become field paths prefixed with `$` (e.g. `"$aNumber"`).
 * - Values become themselves.
 * - Nested expressions recurse.
 */
const buildAggregationExpression = (
  operand: PlanExpressionOperand,
  mapper: Mapper
): any => {
  if (isVariable(operand)) {
    const { path } = resolveFieldReference(operand.name, mapper);
    return "$" + path.join(".");
  }
  if (isValue(operand)) {
    return operand.value;
  }
  if (isExpression(operand)) {
    return buildAggregationExpressionFromExpression(operand, mapper);
  }
  throw new Error("Invalid operand structure");
};

const aggregationOperatorMap: Record<string, string> = {
  add: "$add",
  sub: "$subtract",
  mult: "$multiply",
  div: "$divide",
  mod: "$mod",
  eq: "$eq",
  ne: "$ne",
  lt: "$lt",
  le: "$lte",
  gt: "$gt",
  ge: "$gte",
  and: "$and",
  or: "$or",
  not: "$not",
};

const buildCheckedConversion = (
  input: unknown,
  allowedTypes: string[],
  targetType: "string" | "double" | "long"
): MongooseFilter => ({
  $cond: {
    if: { $in: [{ $type: input }, allowedTypes] },
    then: {
      $convert: {
        input,
        to: targetType,
        onError: null,
        onNull: null,
      },
    },
    else: null,
  },
});

const buildAggregationExpressionFromExpression = (
  expression: PlanExpression,
  mapper: Mapper
): any => {
  const { operator, operands } = expression;

  switch (operator) {
    case "add":
    case "sub":
    case "mult":
    case "mod":
    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge":
    case "and":
    case "or": {
      return {
        [aggregationOperatorMap[operator] as string]: operands.map((op) =>
          buildAggregationExpression(op, mapper)
        ),
      };
    }
    case "div": {
      const denominator = operands[1];
      if (
        !denominator ||
        !isValue(denominator) ||
        typeof denominator.value !== "number" ||
        denominator.value === 0
      ) {
        throw new Error(
          "div operator requires a non-zero constant denominator"
        );
      }
      return {
        $divide: operands.map((op) => buildAggregationExpression(op, mapper)),
      };
    }
    case "not": {
      const operand = operands[0];
      if (!operand) {
        throw new Error("not operator requires an operand");
      }
      return { $not: [buildAggregationExpression(operand, mapper)] };
    }
    case "string": {
      const operand = operands[0];
      if (!operand) {
        throw new Error("string conversion requires an operand");
      }
      return buildCheckedConversion(
        buildAggregationExpression(operand, mapper),
        ["string", "bool", "int", "long", "double", "decimal"],
        "string"
      );
    }
    // CEL's int()/double() are not $convert. CEL reads a WHOLE string or raises, and an
    // error DENIES the row; $convert parses a leading numeric prefix, so "100%_done"
    // becomes 100 and the filter returns records the PDP denies. The numeric direction is
    // no safer: CEL truncates toward zero while $convert to "long" ROUNDS, so int(-0.6) is
    // 0 to CEL and -1 here. Nothing in the plan says what type the field holds, so no
    // conversion is faithful for every document (cerbos/query-plan-adapters#311).
    case "double":
    case "int":
      throw new Error(
        `'${operator}()' cannot be translated: $convert parses a numeric prefix where CEL ` +
          "requires the whole string and raises otherwise, and rounds where CEL truncates " +
          "toward zero"
      );
    case "if": {
      const [ifOp, thenOp, elseOp] = operands;
      if (!ifOp || !thenOp || !elseOp) {
        throw new Error("if operator requires three operands");
      }
      return {
        $cond: {
          if: buildAggregationExpression(ifOp, mapper),
          then: buildAggregationExpression(thenOp, mapper),
          else: buildAggregationExpression(elseOp, mapper),
        },
      };
    }
    case "index": {
      const [arrOp, index] = parseConstantIndexOperands(operands);
      return {
        $arrayElemAt: [
          buildAggregationExpression(arrOp, mapper),
          index,
        ],
      };
    }
    case "get-field": {
      const [inputOperand, fieldOperand] = operands;
      if (!inputOperand || !fieldOperand || !isVariable(fieldOperand)) {
        throw new Error("get-field requires an input and a field name");
      }
      return {
        $getField: {
          field: fieldOperand.name,
          input: buildAggregationExpression(inputOperand, mapper),
        },
      };
    }
    case "size": {
      const operand = operands[0];
      if (!operand) {
        throw new Error("size operator requires an operand");
      }
      const inner = buildAggregationExpression(operand, mapper);
      // Works for both arrays and strings: $size for arrays, $strLenCP otherwise.
      const size = {
        $cond: [{ $isArray: inner }, { $size: inner }, { $strLenCP: inner }],
      };
      const parentPath = isVariable(operand)
        ? resolveFieldReference(operand.name, mapper).relation?.requiresParent
        : undefined;
      if (parentPath === undefined) {
        return size;
      }
      // An absent to-one parent counts as UNKNOWN, not 0. null loses against every number
      // in BSON order, so both `== 0` and `>= 0` exclude the document (#309).
      return {
        $cond: [
          { $gt: [{ $size: { $ifNull: [`$${parentPath}`, []] } }, 0] },
          size,
          null,
        ],
      };
    }
    case "matches": {
      const [valueOp, patternOp] = operands;
      if (
        !valueOp ||
        !patternOp ||
        !isValue(patternOp) ||
        typeof patternOp.value !== "string"
      ) {
        throw new Error("matches operator requires two operands");
      }
      return {
        $regexMatch: {
          input: buildAggregationExpression(valueOp, mapper),
          regex: normalizeRe2PatternForMongo(patternOp.value),
        },
      };
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      const [receiverOperand, needleOperand] = operands;
      if (!receiverOperand || !needleOperand) {
        throw new Error(`${operator} requires two operands`);
      }
      const receiver = buildAggregationExpression(receiverOperand, mapper);
      const needle = buildAggregationExpression(needleOperand, mapper);
      const index = { $indexOfCP: [receiver, needle] };
      if (operator === "contains") {
        return { $gte: [index, 0] };
      }
      if (operator === "startsWith") {
        return { $eq: [index, 0] };
      }
      const receiverLength = { $strLenCP: receiver };
      const needleLength = { $strLenCP: needle };
      return {
        $cond: {
          if: { $gte: [receiverLength, needleLength] },
          then: {
            $eq: [
              {
                $substrCP: [
                  receiver,
                  { $subtract: [receiverLength, needleLength] },
                  needleLength,
                ],
              },
              needle,
            ],
          },
          else: false,
        },
      };
    }
    case "timestamp": {
      const operand = operands[0];
      if (!operand) {
        throw new Error("timestamp operator requires an operand");
      }
      if (isValue(operand)) {
        if (
          typeof operand.value !== "string" ||
          !isRfc3339Timestamp(operand.value)
        ) {
          throw new Error(
            "timestamp value must be a millisecond-exact RFC 3339 instant in the CEL range"
          );
        }
        return new Date(operand.value);
      }
      const input = buildAggregationExpression(operand, mapper);
      const converted = {
        $cond: {
          if: { $eq: [{ $type: input }, "date"] },
          then: input,
          else: {
            $cond: {
              if: {
                $cond: {
                  if: { $eq: [{ $type: input }, "string"] },
                  then: {
                    $regexMatch: {
                      input,
                      regex: RFC3339_TIMESTAMP_MONGO_PATTERN,
                    },
                  },
                  else: false,
                },
              },
              then: {
                $convert: {
                  input,
                  to: "date",
                  onError: null,
                  onNull: null,
                },
              },
              else: null,
            },
          },
        },
      };
      return {
        $let: {
          vars: { converted },
          in: {
            $cond: {
              if: {
                $and: [
                  { $ne: ["$$converted", null] },
                  { $gte: ["$$converted", MIN_CEL_TIMESTAMP] },
                  { $lte: ["$$converted", MAX_CEL_TIMESTAMP] },
                ],
              },
              then: "$$converted",
              else: null,
            },
          },
        },
      };
    }
    default:
      throw new Error(
        `Unsupported operator inside aggregation expression: ${operator}`
      );
  }
};

const parseConstantIndexOperands = (
  operands: PlanExpressionOperand[]
): readonly [collection: PlanExpressionOperand, index: number] => {
  const [collectionOperand, indexOperand] = operands;
  if (!collectionOperand || !indexOperand) {
    throw new Error("index operator requires two operands");
  }
  if (
    !isValue(indexOperand) ||
    typeof indexOperand.value !== "number" ||
    !Number.isInteger(indexOperand.value) ||
    indexOperand.value < 0
  ) {
    throw new Error("index operator requires a non-negative integer constant");
  }
  return [collectionOperand, indexOperand.value];
};

const collectGuardedExpressions = (
  operand: PlanExpressionOperand
): PlanExpression[] => {
  if (!isExpression(operand)) {
    return [];
  }
  const nested = operand.operands.flatMap(collectGuardedExpressions);
  if (operand.operator === "index") {
    return [operand, ...nested];
  }
  if (
    operand.operator === "string" ||
    operand.operator === "double" ||
    operand.operator === "int" ||
    operand.operator === "matches"
  ) {
    return [operand, ...nested];
  }
  if (
    operand.operator === "timestamp" &&
    operand.operands[0] &&
    !isValue(operand.operands[0])
  ) {
    return [operand, ...nested];
  }
  return nested;
};

const buildEvaluationGuard = (
  expression: PlanExpression,
  mapper: Mapper
): MongooseFilter => {
  if (expression.operator === "timestamp") {
    return {
      $expr: {
        $ne: [buildAggregationExpressionFromExpression(expression, mapper), null],
      },
    };
  }
  if (
    expression.operator === "string" ||
    expression.operator === "double" ||
    expression.operator === "int"
  ) {
    return {
      $expr: {
        $ne: [buildAggregationExpressionFromExpression(expression, mapper), null],
      },
    };
  }
  if (expression.operator === "matches") {
    const inputOperand = expression.operands[0];
    if (!inputOperand) {
      throw new Error("matches operator requires an input operand");
    }
    return {
      $expr: {
        $eq: [
          { $type: buildAggregationExpression(inputOperand, mapper) },
          "string",
        ],
      },
    };
  }
  if (expression.operator === "index") {
    const [collectionOperand, index] = parseConstantIndexOperands(
      expression.operands
    );
    const collection = buildAggregationExpression(collectionOperand, mapper);
    return {
      $expr: {
        $cond: {
          if: { $isArray: collection },
          then: { $gt: [{ $size: collection }, index] },
          else: false,
        },
      },
    };
  }
  throw new Error(`Unsupported guarded expression: ${expression.operator}`);
};

const withEvaluationGuards = (
  filter: MongooseFilter,
  operands: PlanExpressionOperand[],
  mapper: Mapper
): MongooseFilter => {
  const guardedFilter = withNullableGuards(filter, operands, mapper);
  const guards = operands
    .flatMap(collectGuardedExpressions)
    .map((expression) => buildEvaluationGuard(expression, mapper));
  return guards.length === 0
    ? guardedFilter
    : { $and: [...guards, guardedFilter] };
};

const getOperandAt = (
  operands: PlanExpressionOperand[],
  index: number,
  errorMessage: string
): PlanExpressionOperand => {
  const operand = operands[index];
  if (!operand) {
    throw new Error(errorMessage);
  }
  return operand;
};

/**
 * Creates a scoped mapper for collection operations
 */
const createScopedMapper =
  (collectionPath: string, variableName: string, fullMapper: Mapper): Mapper =>
  (key: string) => {
    if (key.startsWith(variableName + ".")) {
      const strippedKey = key.replace(variableName + ".", "");

      // Get the collection's relation config
      const collectionConfig =
        typeof fullMapper === "function"
          ? fullMapper(collectionPath)
          : fullMapper[collectionPath];

      if (collectionConfig?.relation?.fields) {
        const fieldConfig = collectionConfig.relation.fields[strippedKey];
        if (fieldConfig) {
          return fieldConfig;
        }
      }

      // If no specific field mapping found, return default mapping
      return { field: strippedKey };
    }

    // For non-variable keys, use the full mapper
    return typeof fullMapper === "function"
      ? fullMapper(key)
      : fullMapper[key] || { field: key };
  };

const assertCollectionScopedReference = (
  reference: string,
  collectionDepth: number,
  collectionVariable: string | undefined
): void => {
  if (
    collectionDepth > 0 &&
    collectionVariable &&
    reference !== collectionVariable &&
    !reference.startsWith(`${collectionVariable}.`)
  ) {
    throw new Error(
      `Outer reference ${reference} inside a collection predicate is unsupported`
    );
  }
};

type HierarchyOperand =
  | { kind: "field"; name: string; separator: string }
  | { kind: "value"; value: string; separator: string };

const parseHierarchyOperand = (
  operand: PlanExpressionOperand
): HierarchyOperand => {
  if (!isExpression(operand) || operand.operator !== "hierarchy") {
    throw new Error("Hierarchy operators require hierarchy() operands");
  }
  const valueOperand = operand.operands[0];
  const separatorOperand = operand.operands[1];
  if (!valueOperand) {
    throw new Error("hierarchy operator requires a path operand");
  }
  const separator = separatorOperand
    ? isValue(separatorOperand) && typeof separatorOperand.value === "string"
      ? separatorOperand.value
      : undefined
    : ".";
  if (!separator) {
    throw new Error("hierarchy separator must be a non-empty string");
  }
  if (isVariable(valueOperand)) {
    return { kind: "field", name: valueOperand.name, separator };
  }
  if (isValue(valueOperand) && typeof valueOperand.value === "string") {
    return { kind: "value", value: valueOperand.value, separator };
  }
  throw new Error("hierarchy path must be a field or string value");
};

const hierarchyPrefixes = (value: string, separator: string): string[] => {
  const segments = value.split(separator);
  return segments.slice(0, -1).map((_, index) =>
    segments.slice(0, index + 1).join(separator)
  );
};

const buildHierarchyFilter = (
  operator: "ancestorOf" | "descendentOf" | "overlaps",
  operands: PlanExpressionOperand[],
  mapper: Mapper
): MongooseFilter => {
  const leftOperand = operands[0];
  const rightOperand = operands[1];
  if (!leftOperand || !rightOperand) {
    throw new Error(`${operator} requires two hierarchy operands`);
  }
  const left = parseHierarchyOperand(leftOperand);
  const right = parseHierarchyOperand(rightOperand);
  if (left.separator !== right.separator) {
    throw new Error(
      `${operator} requires one field and one value with the same separator`
    );
  }

  let field: Extract<HierarchyOperand, { kind: "field" }>;
  let value: Extract<HierarchyOperand, { kind: "value" }>;
  if (left.kind === "field" && right.kind === "value") {
    field = left;
    value = right;
  } else if (left.kind === "value" && right.kind === "field") {
    field = right;
    value = left;
  } else {
    throw new Error(`${operator} requires one field and one value`);
  }
  const { path, relation } = resolveFieldReference(field.name, mapper);
  if (relation) {
    throw new Error("Hierarchy fields cannot be collection relations");
  }

  const fieldIsLeft = left.kind === "field";
  const fieldMustBeAncestor =
    (operator === "ancestorOf" && fieldIsLeft) ||
    (operator === "descendentOf" && !fieldIsLeft);
  const fieldMustBeDescendent =
    (operator === "descendentOf" && fieldIsLeft) ||
    (operator === "ancestorOf" && !fieldIsLeft);
  const ancestorFilter = buildFieldFilter(path, {
    $in: hierarchyPrefixes(value.value, value.separator),
  });
  const descendentFilter = buildFieldFilter(path, {
    $regex: `^${escapeRegexValue(value.value + value.separator)}`,
  });

  const filter =
    operator === "overlaps"
      ? {
          $or: [
            buildFieldFilter(path, {
              $in: [
                ...hierarchyPrefixes(value.value, value.separator),
                value.value,
              ],
            }),
            descendentFilter,
          ],
        }
      : fieldMustBeAncestor
      ? ancestorFilter
      : fieldMustBeDescendent
      ? descendentFilter
      : undefined;
  if (!filter) {
    throw new Error(`Unable to translate hierarchy operator: ${operator}`);
  }
  return withNullableGuards(filter, operands, mapper);
};

// Operators whose second operand is a lambda that binds an iteration variable.
const LAMBDA_BINDING_OPERATORS = new Set([
  "exists",
  "exists_one",
  "all",
  "filter",
  "map",
  "except",
]);

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
  element: Value
): PlanExpressionOperand => {
  if (isVariable(operand)) {
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
          throw new Error(
            `Cannot resolve "${operand.name}": collection element has no field "${segment}"`
          );
        }
        current = current[segment] as Value;
      }
      return { value: current };
    }
    return operand;
  }

  if (isExpression(operand)) {
    if (
      LAMBDA_BINDING_OPERATORS.has(operand.operator) &&
      operand.operands.length === 2
    ) {
      const [nestedCollection, nestedLambda] = operand.operands;
      if (
        nestedCollection !== undefined &&
        nestedLambda !== undefined &&
        isExpression(nestedLambda) &&
        nestedLambda.operator === "lambda"
      ) {
        const nestedVariable = nestedLambda.operands[1];
        if (
          nestedVariable !== undefined &&
          isVariable(nestedVariable) &&
          nestedVariable.name === variableName
        ) {
          // The nested lambda shadows our variable: substitute only in the
          // collection operand.
          return {
            operator: operand.operator,
            operands: [
              substituteLambdaVariable(nestedCollection, variableName, element),
              nestedLambda,
            ],
          };
        }
      }
    }
    return {
      operator: operand.operator,
      operands: operand.operands.map((o) =>
        substituteLambdaVariable(o, variableName, element)
      ),
    };
  }

  return operand;
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
 * body and combine the per-element filters with `$or` (`exists`) or `$and`
 * (`all`).
 *
 * A literal collection has no relation to `$elemMatch` against, so this is the
 * only translation available — and the only one needed, since every element is
 * already known at plan time.
 */
const handleKnownValueCollectionOperator = (
  operator: string,
  collection: PlanExpressionValue,
  lambda: PlanExpressionOperand,
  mapper: Mapper,
  collectionDepth: number,
  collectionVariable: string | undefined
): MongooseFilter => {
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

  if (!isExpression(lambda) || lambda.operator !== "lambda") {
    throw new Error(`Second operand of ${operator} must be a lambda expression`);
  }
  if (lambda.operands.length !== 2) {
    throw new Error(
      `${operator} over a literal collection supports single-variable lambdas only`
    );
  }

  const body = getOperandAt(
    lambda.operands,
    0,
    "Lambda expression must provide a condition"
  );
  const variable = getOperandAt(
    lambda.operands,
    1,
    "Lambda variable must have a name"
  );
  if (!isVariable(variable)) {
    throw new Error("Lambda variable must have a name");
  }

  if (elements.length === 0) {
    // CEL identity semantics over an empty collection: exists() matches
    // nothing, all() matches everything. MongoDB rejects an empty `$or`/`$and`,
    // so state the constant directly.
    return { $expr: operator === "all" };
  }

  // The enclosing collection scope is carried through unchanged: a fold nested
  // inside another macro's lambda must keep rejecting outer-document
  // references exactly as the equivalent or/and chain would.
  const filters = elements.map((element) =>
    buildMongooseFilterFromCerbosExpression(
      substituteLambdaVariable(body, variable.name, element),
      mapper,
      collectionDepth,
      collectionVariable
    )
  );

  return operator === "exists" ? { $or: filters } : { $and: filters };
};

/**
 * Builds Mongoose conditions from a Cerbos expression
 */
const buildMongooseFilterFromCerbosExpression = (
  expression: PlanExpressionOperand,
  mapper: Mapper,
  collectionDepth = 0,
  collectionVariable?: string
): MongooseFilter => {
  if (isVariable(expression)) {
    assertCollectionScopedReference(
      expression.name,
      collectionDepth,
      collectionVariable
    );
    const { path, relation } = resolveFieldReference(expression.name, mapper);
    if (relation) {
      throw new Error("Bare collection variables are unsupported");
    }
    return buildGuardedFieldFilter(
      path,
      { $eq: true },
      isNullableReference(expression.name, mapper)
    );
  }
  if (!isExpression(expression)) {
    throw new Error("Invalid Cerbos expression structure");
  }

  const { operator, operands } = expression;
  const requireOperandAt = (index: number, message: string) =>
    getOperandAt(operands, index, message);
  const resolveOperand = (operand: PlanExpressionOperand): any => {
    if (isVariable(operand)) {
      return resolveFieldReference(operand.name, mapper);
    } else if (isValue(operand)) {
      return { value: operand.value };
    } else if (isExpression(operand)) {
      const nestedResult = buildMongooseFilterFromCerbosExpression(
        operand,
        mapper
      );
      return { value: nestedResult };
    }
    throw new Error("Invalid operand structure");
  };

  // A literal value list arrives as a macro's collection operand when the
  // planner could not unroll it over a known collection (more than 10
  // elements). Dispatch before the operator switch: every collection macro
  // needs the same treatment, and none of their relation-mapping requirements
  // can be satisfied by a literal.
  if (LAMBDA_BINDING_OPERATORS.has(operator) && operands.length === 2) {
    const [collectionOperand, lambdaOperand] = operands;
    if (
      collectionOperand !== undefined &&
      lambdaOperand !== undefined &&
      isValue(collectionOperand)
    ) {
      return handleKnownValueCollectionOperator(
        operator,
        collectionOperand,
        lambdaOperand,
        mapper,
        collectionDepth,
        collectionVariable
      );
    }
  }

  switch (operator) {
    case "and":
      return {
        $and: operands.map((op) =>
          buildMongooseFilterFromCerbosExpression(
            op,
            mapper,
            collectionDepth,
            collectionVariable
          )
        ),
      };

    case "or":
      return {
        $or: operands.map((op) =>
          buildMongooseFilterFromCerbosExpression(
            op,
            mapper,
            collectionDepth,
            collectionVariable
          )
        ),
      };

    case "not": {
      const operand = requireOperandAt(
        0,
        "not operator requires at least one operand"
      );
      if (
        referencesNullableField(operand, mapper) ||
        (isExpression(operand) &&
          ["exists", "exists_one", "all"].includes(operand.operator))
      ) {
        throw new Error(
          "not over nullable fields or collection macros cannot preserve Cerbos error semantics"
        );
      }
      const negatedFilter = {
        $nor: [
          buildMongooseFilterFromCerbosExpression(
            operand,
            mapper,
            collectionDepth,
            collectionVariable
          ),
        ],
      };
      return withEvaluationGuards(negatedFilter, [operand], mapper);
    }

    case "eq":
    case "ne":
    case "lt":
    case "le":
    case "gt":
    case "ge": {
      const mongoOperators = {
        eq: "$eq",
        ne: "$ne",
        lt: "$lt",
        le: "$lte",
        gt: "$gt",
        ge: "$gte",
      };
      const leftOperand = requireOperandAt(
        0,
        `${operator} operator requires a left operand`
      );
      const rightOperand = requireOperandAt(
        1,
        `${operator} operator requires a right operand`
      );

      // If either operand is a (non-relational) expression, emit a `$expr`
      // with aggregation-pipeline operators. This covers arithmetic, type
      // conversion, ternary, `index`, `size`, `matches` etc. on either side
      // of the comparison (e.g. `aNumber == int(aString)` or `(a + 1) > b`).
      if (
        isExpression(leftOperand) ||
        isExpression(rightOperand) ||
        (isVariable(leftOperand) && isVariable(rightOperand))
      ) {
        if (
          isExpression(leftOperand) &&
          leftOperand.operator === "if" &&
          isExpression(rightOperand) &&
          rightOperand.operator === "if"
        ) {
          throw new Error(
            "Mongoose cannot cast comparisons between two conditional expressions"
          );
        }
        if (collectionDepth > 0) {
          throw new Error(
            `${operator} aggregation expressions inside collection predicates are unsupported`
          );
        }
        const leftAgg = buildAggregationExpression(leftOperand, mapper);
        const rightAgg = buildAggregationExpression(rightOperand, mapper);
        return withEvaluationGuards(
          {
            $expr: { [mongoOperators[operator]]: [leftAgg, rightAgg] },
          },
          [leftOperand, rightOperand],
          mapper
        );
      }

      const variableOperand = isVariable(leftOperand)
        ? leftOperand
        : isVariable(rightOperand)
        ? rightOperand
        : undefined;
      const valueOperand = isValue(leftOperand)
        ? leftOperand
        : isValue(rightOperand)
        ? rightOperand
        : undefined;
      if (!variableOperand || !valueOperand) {
        throw new Error(
          `${operator} requires a field/value pair or aggregation operands`
        );
      }
      assertCollectionScopedReference(
        variableOperand.name,
        collectionDepth,
        collectionVariable
      );

      const effectiveOperator =
        variableOperand === leftOperand
          ? operator
          : mirroredComparisonOperator(operator);
      const { path, relation } = resolveFieldReference(
        variableOperand.name,
        mapper
      );
      const comparison = {
        [mongoOperators[effectiveOperator]]: applyValueParser(
          variableOperand.name,
          valueOperand.value,
          mapper
        ),
      };
      const nullable = isNullableReference(variableOperand.name, mapper);
      const requireExists = carriesNullOperand(valueOperand.value);
      if (requireExists) {
        assertNullOperandTranslatable(
          `\`${effectiveOperator}\` against a null operand`
        );
      }
      if (relation?.type === "many") {
        return {
          [relation.name]: {
            $elemMatch: buildGuardedFieldFilter(
              path.slice(1),
              comparison,
              nullable,
              requireExists
            ),
          },
        };
      }
      return buildGuardedFieldFilter(
        path,
        comparison,
        nullable,
        requireExists
      );
    }

    case "in": {
      const leftOperand = requireOperandAt(0, "in requires a left operand");
      const rightOperand = requireOperandAt(1, "in requires a right operand");

      if (isVariable(leftOperand) && isValue(rightOperand)) {
        if (!Array.isArray(rightOperand.value)) {
          throw new Error("in with a field on the left requires an array value");
        }
        const { path, relation } = resolveFieldReference(
          leftOperand.name,
          mapper
        );
        const comparison = {
          $in: rightOperand.value.map((value) =>
            applyValueParser(leftOperand.name, value, mapper)
          ),
        };
        const nullable = isNullableReference(leftOperand.name, mapper);
        const requireExists = carriesNullOperand(rightOperand.value);
        if (requireExists) {
          assertNullOperandTranslatable("a null element in an `in` list");
        }
        if (relation?.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildGuardedFieldFilter(
                path.slice(1),
                comparison,
                nullable,
                requireExists
              ),
            },
          };
        }
        return buildGuardedFieldFilter(
          path,
          comparison,
          nullable,
          requireExists
        );
      }

      if (isValue(leftOperand) && isVariable(rightOperand)) {
        const { path, relation } = resolveFieldReference(
          rightOperand.name,
          mapper
        );
        const comparison = {
          $eq: applyValueParser(
            rightOperand.name,
            leftOperand.value,
            mapper
          ),
        };
        const nullable = isNullableReference(rightOperand.name, mapper);
        const requireExists = carriesNullOperand(leftOperand.value);
        if (requireExists) {
          assertNullOperandTranslatable(
            "a null needle in a mapped-collection `in`"
          );
        }
        if (relation?.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildGuardedFieldFilter(
                path.slice(1),
                comparison,
                nullable,
                requireExists
              ),
            },
          };
        }
        return buildGuardedFieldFilter(
          path,
          comparison,
          nullable,
          requireExists
        );
      }

      throw new Error(
        "in supports only field-in-value-list or value-in-mapped-collection shapes"
      );
    }

    case "matches": {
      const fieldOperand = requireOperandAt(
        0,
        "matches operator requires a field operand"
      );
      const patternOperand = requireOperandAt(
        1,
        "matches operator requires a regex pattern value"
      );
      if (
        !isVariable(fieldOperand) ||
        !isValue(patternOperand) ||
        typeof patternOperand.value !== "string"
      ) {
        throw new Error("matches operator requires a string regex pattern");
      }

      const { path, relation } = resolveOperand(fieldOperand);
      const regexFilter = {
        $regex: normalizeRe2PatternForMongo(patternOperand.value),
      };

      if (relation) {
        if (relation.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildFieldFilter(path.slice(1), regexFilter),
            },
          };
        }
        return buildFieldFilter(path, regexFilter);
      }
      return buildFieldFilter(path, regexFilter);
    }

    case "contains":
    case "startsWith":
    case "endsWith": {
      const leftOperand = requireOperandAt(
        0,
        `${operator} operator requires a receiver`
      );
      const rightOperand = requireOperandAt(
        1,
        `${operator} operator requires a needle`
      );
      if (
        !isVariable(leftOperand) ||
        !isValue(rightOperand) ||
        typeof rightOperand.value !== "string"
      ) {
        if (collectionDepth > 0) {
          throw new Error(
            `${operator} aggregation expressions inside collection predicates are unsupported`
          );
        }
        return withEvaluationGuards(
          {
            $expr: buildAggregationExpressionFromExpression(
              expression,
              mapper
            ),
          },
          [leftOperand, rightOperand],
          mapper
        );
      }

      const escapedValue = escapeRegexValue(rightOperand.value);
      // Mongo matches $regex with PCRE2, where `$` also matches immediately
      // before a final newline, so "tail\n" would satisfy endsWith("tail")
      // while CEL says false. `\z` is the absolute end of subject — the same
      // rewrite normalizeRe2PatternForMongo applies to a trailing `$`.
      // `^` needs no counterpart: without PCRE2_MULTILINE it already matches
      // only at the start of the subject.
      const regexStr =
        operator === "contains"
          ? escapedValue
          : operator === "startsWith"
          ? `^${escapedValue}`
          : `${escapedValue}\\z`;

      const { path, relation } = resolveFieldReference(
        leftOperand.name,
        mapper
      );
      const nullable = isNullableReference(leftOperand.name, mapper);
      if (relation) {
        const elementPath = path.slice(1);
        if (relation.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildGuardedFieldFilter(
                elementPath,
                { $regex: regexStr },
                nullable
              ),
            },
          };
        }
        return buildGuardedFieldFilter(
          path,
          { $regex: regexStr },
          nullable
        );
      }
      return buildGuardedFieldFilter(
        path,
        { $regex: regexStr },
        nullable
      );
    }

    case "hasIntersection": {
      if (operands.length !== 2) {
        throw new Error("hasIntersection requires exactly two operands");
      }

      const leftOperand = requireOperandAt(
        0,
        "hasIntersection requires a field operand"
      );
      const rightOperand = requireOperandAt(
        1,
        "hasIntersection requires a value operand"
      );

      // A null element in the intersection list lowers to a null-matching disjunct exactly as
      // it does for `in`, so it is subject to the same representation guard.
      if (isValue(rightOperand) && carriesNullOperand(rightOperand.value)) {
        assertNullOperandTranslatable(
          "a null element in a `hasIntersection` list"
        );
      }

      // Handle map expressions specially for hasIntersection
      if (isExpression(leftOperand) && leftOperand.operator === "map") {
        const mapCollectionOperand = getOperandAt(
          leftOperand.operands,
          0,
          "Expected a variable in map expression"
        );
        const mapLambdaOperand = getOperandAt(
          leftOperand.operands,
          1,
          "Expected a lambda in map expression"
        );
        if (!isVariable(mapCollectionOperand)) {
          throw new Error("Expected a variable in map expression");
        }
        if (!isExpression(mapLambdaOperand)) {
          throw new Error("Expected a lambda in map expression");
        }
        const lambdaExpression = mapLambdaOperand;

        if (lambdaExpression.operator !== "lambda") {
          throw new Error("Second operand of map must be a lambda expression");
        }

        const projectionOperand = getOperandAt(
          lambdaExpression.operands,
          0,
          "Map lambda requires a projection operand"
        );
        const variableOperand = getOperandAt(
          lambdaExpression.operands,
          1,
          "Map lambda requires a variable operand"
        );
        if (!isVariable(variableOperand)) {
          throw new Error("Invalid map expression structure");
        }

        if (!isValue(rightOperand) || !Array.isArray(rightOperand.value)) {
          throw new Error("hasIntersection requires an array value");
        }

        const scopedMapper = createScopedMapper(
          mapCollectionOperand.name,
          variableOperand.name,
          mapper
        );

        const collectionResolved = resolveFieldReference(
          mapCollectionOperand.name,
          mapper
        );
        if (!collectionResolved.relation) {
          throw new Error("map operator requires a relation mapping");
        }
        if (collectionResolved.relation.type !== "many") {
          throw new Error("map operator requires a collection relation");
        }

        if (!isVariable(projectionOperand)) {
          throw new Error("Map projection must be a variable reference");
        }

        const projectionResolved = resolveFieldReference(
          projectionOperand.name,
          scopedMapper
        );
        const elementPath = projectionResolved.path;
        const requiresPresentElement = rightOperand.value.includes(null);
        const matchingElement = {
          [collectionResolved.relation.name]: {
            $elemMatch: buildGuardedFieldFilter(
              elementPath,
              { $in: rightOperand.value },
              false,
              requiresPresentElement
            ),
          },
        };
        if (!isNullableReference(projectionOperand.name, scopedMapper)) {
          return matchingElement;
        }
        return {
          $and: [
            {
              [collectionResolved.relation.name]: {
                $not: {
                  $elemMatch: buildFieldFilter(elementPath, { $eq: null }),
                },
              },
            },
            matchingElement,
          ],
        };
      }

      if (!isVariable(leftOperand) || !isValue(rightOperand)) {
        throw new Error("Invalid operands for hasIntersection");
      }

      const { path, relation } = resolveFieldReference(
        leftOperand.name,
        mapper
      );

      if (!Array.isArray(rightOperand.value)) {
        throw new Error("hasIntersection requires an array value");
      }

      if (relation) {
        if (relation.type === "many") {
          return {
            [relation.name]: {
              $elemMatch: buildGuardedFieldFilter(
                path.slice(1),
                { $in: rightOperand.value },
                false,
                rightOperand.value.includes(null)
              ),
            },
          };
        }
        return buildGuardedFieldFilter(
          path,
          { $in: rightOperand.value },
          false,
          rightOperand.value.includes(null)
        );
      }

      return buildGuardedFieldFilter(
        path,
        { $in: rightOperand.value },
        false,
        rightOperand.value.includes(null)
      );
    }

    // Collection operations
    case "exists_one":
      throw new Error(
        "exists_one requires exact match cardinality and is unsupported"
      );

    // filter() yields a list, not a boolean. Reaching it in a boolean position means the
    // plan used it as a predicate, and there is no meaning to pick: `filter(...)` is not
    // `size(filter(...)) > 0`. Fail closed (cerbos/query-plan-adapters#313); the legitimate
    // `size(filter(...))` form is handled by the size operator before this.
    case "filter":
      throw new Error(
        "filter() returns a list, not a boolean, so it cannot be a condition on its own; " +
          "only size(filter(...)) has a boolean meaning"
      );

    case "exists": {
      if (operands.length !== 2) {
        throw new Error(`${operator} requires exactly two operands`);
      }

      const collectionOperand = requireOperandAt(
        0,
        `${operator} operator requires a collection operand`
      );
      const lambdaOperand = requireOperandAt(
        1,
        `${operator} operator requires a lambda operand`
      );

      if (!isVariable(collectionOperand) || !isExpression(lambdaOperand)) {
        throw new Error("Invalid operands for collection operation");
      }

      if (lambdaOperand.operator !== "lambda") {
        throw new Error("Second operand must be a lambda expression");
      }

      const conditionOperand = getOperandAt(
        lambdaOperand.operands,
        0,
        "Lambda operand requires a condition"
      );
      const variableOperand = getOperandAt(
        lambdaOperand.operands,
        1,
        "Lambda operand requires a variable"
      );
      if (!isVariable(variableOperand)) {
        throw new Error("Lambda variable must have a name");
      }

      // Create scoped mapper for the collection
      const scopedMapper = createScopedMapper(
        collectionOperand.name,
        variableOperand.name,
        mapper
      );

      const { relation } = resolveFieldReference(
        collectionOperand.name,
        mapper
      );
      if (!relation) {
        throw new Error(`${operator} operator requires a relation mapping`);
      }
      if (relation.type !== "many") {
        throw new Error(`${operator} operator requires a collection relation`);
      }

      const lambdaCondition = buildMongooseFilterFromCerbosExpression(
        conditionOperand,
        scopedMapper,
        collectionDepth + 1,
        variableOperand.name
      );

      return {
        [relation.name]: {
          $elemMatch: lambdaCondition,
        },
      };
    }

    case "lambda": {
      const conditionOperand = requireOperandAt(
        0,
        "lambda operator requires a condition operand"
      );
      const variableOperand = requireOperandAt(
        1,
        "lambda operator requires a variable operand"
      );
      if (!isVariable(variableOperand)) {
        throw new Error("Lambda variable must have a name");
      }

      // Create a mapper that strips the variable prefix from field references
      return buildMongooseFilterFromCerbosExpression(
        conditionOperand,
        (key: string) => ({
          field: key.replace(`${variableOperand.name}.`, ""),
        }),
        collectionDepth,
        variableOperand.name
      );
    }

    // map() in a BOOLEAN position: the projection is a list, not a predicate. Rendering it
    // as `$elemMatch: { $exists: true }` silently answers "the projection is non-empty",
    // which is not what the policy said (cerbos/query-plan-adapters#313). The legitimate
    // consumer, hasIntersection(map(...), [...]), destructures the map operand itself
    // before reaching this switch.
    case "map":
      throw new Error(
        "map() returns a list, not a boolean, so it cannot be a condition on its own; " +
          "only hasIntersection(map(...), [...]) gives the projection a boolean meaning"
      );

    case "all": {
      if (operands.length !== 2) {
        throw new Error(`${operator} requires exactly two operands`);
      }

      const collectionOperand = requireOperandAt(
        0,
        `${operator} operator requires a collection operand`
      );
      const lambdaOperand = requireOperandAt(
        1,
        `${operator} operator requires a lambda operand`
      );

      if (!isVariable(collectionOperand) || !isExpression(lambdaOperand)) {
        throw new Error("Invalid operands for collection operation");
      }

      if (lambdaOperand.operator !== "lambda") {
        throw new Error("Second operand must be a lambda expression");
      }

      const conditionOperand = getOperandAt(
        lambdaOperand.operands,
        0,
        "Lambda operand requires a condition"
      );
      const variableOperand = getOperandAt(
        lambdaOperand.operands,
        1,
        "Lambda operand requires a variable"
      );
      if (!isVariable(variableOperand)) {
        throw new Error("Lambda variable must have a name");
      }

      // Create scoped mapper for the collection
      const scopedMapper = createScopedMapper(
        collectionOperand.name,
        variableOperand.name,
        mapper
      );

      const { relation } = resolveFieldReference(
        collectionOperand.name,
        mapper
      );
      if (!relation) {
        throw new Error(`${operator} operator requires a relation mapping`);
      }
      if (relation.type !== "many") {
        throw new Error(`${operator} operator requires a collection relation`);
      }

      const lambdaCondition = buildMongooseFilterFromCerbosExpression(
        conditionOperand,
        scopedMapper,
        collectionDepth + 1,
        variableOperand.name
      );

      return {
        [relation.name]: {
          $type: "array",
          $not: {
            $elemMatch: {
              $nor: [lambdaCondition],
            },
          },
        },
      };
    }

    case "if": {
      if (collectionDepth > 0) {
        throw new Error(
          "if aggregation expressions inside collection predicates are unsupported"
        );
      }
      return withEvaluationGuards(
        { $expr: buildAggregationExpressionFromExpression(expression, mapper) },
        operands,
        mapper
      );
    }

    case "ancestorOf":
    case "descendentOf":
    case "overlaps":
      return buildHierarchyFilter(operator, operands, mapper);

    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};
