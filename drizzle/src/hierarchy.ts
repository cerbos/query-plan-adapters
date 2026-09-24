import type { PlanExpressionOperand } from "@cerbos/core";
import { and, or, sql } from "drizzle-orm";
import type { SQL } from "drizzle-orm";

import { UnsupportedQueryPlanError } from "./errors";
import {
  buildColumnExpression,
  columnForOperand,
  isColumn,
  isMappingConfig,
  resolveFieldReference,
} from "./mapper";
import { isNameOperand, isOperatorCall, isValueOperand } from "./operands";
import {
  FALSE_CONDITION,
  TRUE_CONDITION,
  buildStringMatchCondition,
  characterLength,
  columnExpression,
  constantExpression,
} from "./predicates";
import { wrapRelationChain } from "./relations";
import type { BuildFilterOptions, Mapper, ResolvedMapping } from "./types";

/**
 * Cerbos's `hierarchy()` operators — `ancestorOf`, `descendentOf`, `overlaps` — between one
 * hierarchy stored in a column and one the plan carries as a constant, or between a hierarchy
 * built by `list()` from constant and column segments and a constant or another built one.
 */

type HierarchyOperator = "ancestorOf" | "descendentOf" | "overlaps";

type ConstantHierarchy = {
  kind: "constant";
  segments: string[];
  delimiter: string;
};

type FieldHierarchy = {
  kind: "field";
  reference: string;
  resolved: ResolvedMapping;
  delimiter: string;
};

/** One segment of a `list()`-built path: a constant, or a string column read as one segment. */
type Segment = { kind: "constant"; value: string } | { kind: "column"; expr: SQL };

/**
 * `hierarchy(["projects", R.id])`: a path whose segments are listed rather than split out of one
 * string, so its LENGTH is known at translation time even where a segment's value is not.
 */
type SegmentedHierarchy = {
  kind: "segmented";
  segments: Segment[];
};

type ResolvedHierarchy = ConstantHierarchy | FieldHierarchy | SegmentedHierarchy;

/**
 * The segments of a `list()` path operand. A segment must be a string constant or a string column
 * with no relation hop: anything else is refused, and a non-string column is a CEL error, which
 * `null` (UNKNOWN) spells.
 */
const resolveSegments = (
  listOperand: PlanExpressionOperand,
  mapper: Mapper,
): Segment[] | "error" => {
  if (isValueOperand(listOperand) && Array.isArray(listOperand.value)) {
    if (listOperand.value.some((segment) => typeof segment !== "string")) return "error";
    return listOperand.value.map((value) => ({ kind: "constant", value: value as string }));
  }
  if (!isOperatorCall(listOperand, "list")) {
    throw new UnsupportedQueryPlanError(
      "Segmented hierarchy expressions are supported only as a list() of constants and columns",
    );
  }
  let error = false;
  const segments = listOperand.operands.map((segment): Segment => {
    if (isValueOperand(segment)) {
      if (typeof segment.value !== "string") {
        error = true;
        return { kind: "constant", value: "" };
      }
      return { kind: "constant", value: segment.value };
    }
    if (isNameOperand(segment)) {
      const resolved = resolveFieldReference(segment.name, mapper);
      if (resolved.relations.length > 0) {
        throw new UnsupportedQueryPlanError(
          `Hierarchy segment '${segment.name}' behind a relation is not supported`,
        );
      }
      const column = columnForOperand(segment, mapper);
      if (column && column.dataType !== "string") error = true;
      return {
        kind: "column",
        expr: buildColumnExpression(resolved.mapping, segment.name),
      };
    }
    throw new UnsupportedQueryPlanError(
      "Hierarchy segments must be string constants or field references",
    );
  });
  return error ? "error" : segments;
};

const resolveHierarchy = (
  operand: PlanExpressionOperand,
  mapper: Mapper,
): ResolvedHierarchy | "error" => {
  if (!isOperatorCall(operand, "hierarchy")) {
    throw new UnsupportedQueryPlanError("Hierarchy operators require hierarchy(...) operands");
  }
  if (operand.operands.length < 1 || operand.operands.length > 2) {
    throw new UnsupportedQueryPlanError("'hierarchy' operator requires one or two operands");
  }
  const [pathOperand, delimiterOperand] = operand.operands;
  if (!pathOperand) {
    throw new UnsupportedQueryPlanError("'hierarchy' operator is missing its path operand");
  }
  let delimiter = ".";
  if (delimiterOperand) {
    if (
      !isValueOperand(delimiterOperand) ||
      typeof delimiterOperand.value !== "string"
    ) {
      throw new UnsupportedQueryPlanError("Hierarchy delimiter must be a string value");
    }
    delimiter = delimiterOperand.value;
  }

  if (isValueOperand(pathOperand) && !Array.isArray(pathOperand.value)) {
    if (typeof pathOperand.value !== "string") {
      throw new UnsupportedQueryPlanError(
        "Hierarchy path must be a string value or field reference",
      );
    }
    return {
      kind: "constant",
      // Go's strings.Split on an empty separator yields one segment per character (code point),
      // and none at all for an empty string.
      segments: delimiter === "" ? [...pathOperand.value] : pathOperand.value.split(delimiter),
      delimiter,
    };
  }
  if (isNameOperand(pathOperand)) {
    return {
      kind: "field",
      reference: pathOperand.name,
      resolved: resolveFieldReference(pathOperand.name, mapper),
      delimiter,
    };
  }
  if (delimiterOperand) {
    // Cerbos's hierarchy() takes a delimiter only with a string path.
    throw new UnsupportedQueryPlanError(
      "A segmented hierarchy path takes no delimiter",
    );
  }
  const segments = resolveSegments(pathOperand, mapper);
  if (segments === "error") return "error";
  return { kind: "segmented", segments };
};

/** Every proper ancestor path of `segments`, shortest first. */
const hierarchyStrictPrefixes = (
  segments: string[],
  delimiter: string,
): string[] => {
  const prefixes: string[] = [];
  for (let length = 1; length < segments.length; length += 1) {
    prefixes.push(segments.slice(0, length).join(delimiter));
  }
  return prefixes;
};

const buildFieldHierarchyFilter = (
  operator: HierarchyOperator,
  field: FieldHierarchy,
  constant: ConstantHierarchy,
  fieldIsLeft: boolean,
  options: BuildFilterOptions,
): SQL => {
  const { mapping } = field.resolved;
  const fieldColumn = isColumn(mapping)
    ? mapping
    : isMappingConfig(mapping)
      ? mapping.column
      : undefined;
  // A hierarchy over a non-string column is a CEL error: UNKNOWN under both polarities.
  if (fieldColumn && fieldColumn.dataType !== "string") return sql`null`;

  const fieldExpr = buildColumnExpression(mapping, field.reference);
  const constantPath = constant.segments.join(field.delimiter);
  const prefixes = hierarchyStrictPrefixes(constant.segments, field.delimiter);
  const isOneOfPrefixes = () =>
    sql`${fieldExpr} in ${prefixes.map((prefix) => sql`${prefix}`)}`;
  const isDescendantOfConstant = () =>
    buildStringMatchCondition(
      "startsWith",
      columnExpression(fieldExpr),
      constantExpression(sql`${constantPath + field.delimiter}`),
      characterLength([fieldColumn]),
    );

  const fieldIsAncestor =
    (operator === "ancestorOf" && fieldIsLeft) ||
    (operator === "descendentOf" && !fieldIsLeft);

  let filter: SQL;
  if (operator === "overlaps") {
    const combined = or(
      ...(prefixes.length > 0 ? [isOneOfPrefixes()] : []),
      sql`${fieldExpr} = ${constantPath}`,
      isDescendantOfConstant(),
    );
    if (!combined) {
      throw new UnsupportedQueryPlanError("Unable to combine hierarchy overlap conditions");
    }
    filter = combined;
  } else if (fieldIsAncestor) {
    filter = prefixes.length === 0 ? FALSE_CONDITION : isOneOfPrefixes();
  } else {
    filter = isDescendantOfConstant();
  }

  return wrapRelationChain(
    field.resolved.relations,
    filter,
    field.reference,
    options,
  );
};

/**
 * Two paths of known length, at least one built by `list()`. Cerbos's relations reduce to lengths
 * and a shared prefix: `a.ancestorOf(b)` is `len(a) < len(b)` with `a` a prefix of `b`,
 * `descendentOf` the mirror, and `overlaps` a shared prefix of the shorter length (equal, or one an
 * ancestor of the other). The lengths are constants here, so only the segment equalities reach SQL.
 *
 * Every column segment is read even when the relation does not need it: CEL builds the whole list
 * first, and a missing attribute in it is an error that denies. So a NULL in any column segment
 * makes the result NULL, excluded under both polarities.
 */
const buildSegmentedHierarchyFilter = (
  operator: HierarchyOperator,
  left: Segment[],
  right: Segment[],
): SQL => {
  const sharedPrefixLength =
    operator === "ancestorOf"
      ? left.length < right.length
        ? left.length
        : undefined
      : operator === "descendentOf"
        ? left.length > right.length
          ? right.length
          : undefined
        : Math.min(left.length, right.length);

  const segmentSql = (segment: Segment): SQL =>
    segment.kind === "constant" ? sql`${segment.value}` : segment.expr;
  let core: SQL = FALSE_CONDITION;
  if (sharedPrefixLength !== undefined) {
    const equalities: SQL[] = [];
    let contradicted = false;
    for (let index = 0; index < sharedPrefixLength; index += 1) {
      const a = left[index]!;
      const b = right[index]!;
      if (a.kind === "constant" && b.kind === "constant") {
        if (a.value !== b.value) contradicted = true;
        continue;
      }
      equalities.push(sql`${segmentSql(a)} = ${segmentSql(b)}`);
    }
    core = contradicted
      ? FALSE_CONDITION
      : equalities.length === 0
        ? TRUE_CONDITION
        : and(...equalities)!;
  }
  const nullable = [...left, ...right].flatMap((segment) =>
    segment.kind === "column" ? [sql`${segment.expr} is null`] : [],
  );
  return nullable.length === 0
    ? core
    : sql`(case when ${sql.join(nullable, sql` or `)} then null else ${core} end)`;
};

/** A constant hierarchy as segments, for comparison with a `list()`-built one. */
const constantSegments = (hierarchy: ConstantHierarchy): Segment[] =>
  hierarchy.segments.map((value) => ({ kind: "constant", value }));

/**
 * A column hierarchy split on an EMPTY delimiter — one segment per character — against a constant
 * whose segments are single characters. Segment-wise comparison is then plain string-prefix logic
 * over the constant's characters `S`: the column is an ancestor when it is one of `S`'s strict
 * prefixes (the empty string included: zero segments), a descendant when it starts with `S` and is
 * longer, and they overlap when either is a prefix of the other.
 */
const buildCharacterHierarchyFilter = (
  operator: HierarchyOperator,
  field: FieldHierarchy,
  constant: ConstantHierarchy,
  fieldIsLeft: boolean,
  options: BuildFilterOptions,
): SQL => {
  const fieldColumn = isColumn(field.resolved.mapping)
    ? field.resolved.mapping
    : isMappingConfig(field.resolved.mapping)
      ? field.resolved.mapping.column
      : undefined;
  if (fieldColumn && fieldColumn.dataType !== "string") return sql`null`;
  const expr = buildColumnExpression(field.resolved.mapping, field.reference);
  const characters = constant.segments;
  const path = characters.join("");
  const length = characterLength([fieldColumn]);
  const prefixes = (upTo: number): SQL[] =>
    Array.from({ length: upTo + 1 }, (_, size) => sql`${characters.slice(0, size).join("")}`);
  const isPrefixOf = (includeWhole: boolean): SQL =>
    characters.length === 0 && !includeWhole
      ? FALSE_CONDITION
      : sql`${expr} in ${prefixes(includeWhole ? characters.length : characters.length - 1)}`;
  const startsWithPath = buildStringMatchCondition(
    "startsWith",
    columnExpression(expr),
    constantExpression(sql`${path}`),
    length,
  );
  const fieldIsAncestor =
    (operator === "ancestorOf" && fieldIsLeft) ||
    (operator === "descendentOf" && !fieldIsLeft);
  const filter =
    operator === "overlaps"
      ? or(isPrefixOf(true), startsWithPath)!
      : fieldIsAncestor
        ? isPrefixOf(false)
        : sql`(${startsWithPath} and ${length(expr)} > ${characters.length})`;
  return wrapRelationChain(field.resolved.relations, filter, field.reference, options);
};

export const buildHierarchyFilter = (
  operator: HierarchyOperator,
  operands: PlanExpressionOperand[],
  mapper: Mapper,
  options: BuildFilterOptions,
): SQL => {
  if (operands.length !== 2) {
    throw new UnsupportedQueryPlanError(`'${operator}' operator requires exactly two operands`);
  }
  const [leftOperand, rightOperand] = operands;
  if (!leftOperand || !rightOperand) {
    throw new UnsupportedQueryPlanError(`'${operator}' operator is missing operands`);
  }
  const left = resolveHierarchy(leftOperand, mapper);
  const right = resolveHierarchy(rightOperand, mapper);
  // A path CEL cannot build (a non-string segment) is an error: UNKNOWN under both polarities.
  if (left === "error" || right === "error") return sql`null`;
  if (left.kind === "segmented" || right.kind === "segmented") {
    if (left.kind === "field" || right.kind === "field") {
      throw new UnsupportedQueryPlanError(
        `'${operator}' between a list()-built hierarchy and a field-backed one is not supported`,
      );
    }
    return buildSegmentedHierarchyFilter(
      operator,
      left.kind === "segmented" ? left.segments : constantSegments(left),
      right.kind === "segmented" ? right.segments : constantSegments(right),
    );
  }
  const field = left.kind === "field" ? left : right.kind === "field" ? right : undefined;
  const constant = left.kind === "constant" ? left : right.kind === "constant" ? right : undefined;
  if (field?.delimiter === "" || constant?.delimiter === "") {
    if (
      field === undefined || constant === undefined ||
      field.delimiter !== "" ||
      constant.segments.some((segment) => [...segment].length !== 1)
    ) {
      throw new UnsupportedQueryPlanError(
        "An empty hierarchy delimiter splits a path per character; it is supported only for a " +
          "column path split that way against a constant whose segments are single characters",
      );
    }
    return buildCharacterHierarchyFilter(operator, field, constant, field === left, options);
  }
  if (left.kind === "field" && right.kind === "constant") {
    return buildFieldHierarchyFilter(operator, left, right, true, options);
  }
  if (left.kind === "constant" && right.kind === "field") {
    return buildFieldHierarchyFilter(operator, right, left, false, options);
  }
  if (left.kind === "field") {
    throw new UnsupportedQueryPlanError(
      `'${operator}' between two field-backed hierarchies is not supported`,
    );
  }
  throw new UnsupportedQueryPlanError(
    `'${operator}' between two constant hierarchies should be folded by the planner`,
  );
};
