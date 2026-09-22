// The hierarchy operators: `overlaps`, `ancestorOf`, `descendentOf` over `hierarchy(...)`
// operands, lowered to equality, IN-lists of strict prefixes, and `startsWith` on a column.

import type { PlanExpressionOperand } from "@cerbos/core";

import {
  assertStringField,
  buildFieldFilter,
  buildImpossibleFilter,
} from "./fields";
import type { PrismaFilter } from "./index";
import { isResolvedValue, resolveFieldReference } from "./mapping";
import type { ResolvedFieldReference, TranslationContext } from "./mapping";
import {
  assertDefined,
  isNamedOperand,
  isOperatorOperand,
  isValueOperand,
} from "./plan";
import type { OperatorOperand } from "./plan";
import { resolveOperand } from "./translate";

type ConstantSegment = { type: "constant"; value: string };
type FieldSegment = { type: "field"; fieldRef: ResolvedFieldReference };
type HierarchySegment = ConstantSegment | FieldSegment;

type ConstantHierarchy = {
  type: "constant";
  segments: string[];
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
  context: TranslationContext
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
    if (delimiter === "") {
      // Cerbos splits a path on an empty delimiter into one segment per CHARACTER, so the
      // relation becomes a strict string-prefix test. The descendant lowering here is
      // `startsWith(prefix + delimiter)`, which with an empty delimiter matches the path
      // ITSELF (never its own descendant) as well as every string extension of it — the
      // corpus's hier-empty-delim over-granted a2 that way — so the shape is refused rather
      // than emitted with the wrong boundary.
      throw new Error(
        "hierarchy delimiter must be a non-empty string: an empty delimiter splits the path per character, and the startsWith prefix this adapter emits would also match the path itself"
      );
    }

    if (isValueOperand(strOperand)) {
      return {
        type: "constant",
        segments: String(strOperand.value).split(delimiter),
      };
    }
    if (isNamedOperand(strOperand)) {
      return {
        type: "field",
        fieldRef: resolveFieldReference(strOperand.name, context),
        delimiter,
      };
    }
    throw new Error("hierarchy(string, delimiter) requires a value or field operand");
  }

  if (operands.length === 1) {
    const inner = assertDefined(operands[0], "hierarchy requires an operand");

    if (isValueOperand(inner)) {
      return { type: "constant", segments: String(inner.value).split(".") };
    }

    if (isNamedOperand(inner)) {
      return {
        type: "field",
        fieldRef: resolveFieldReference(inner.name, context),
        delimiter: ".",
      };
    }

    if (isOperatorOperand(inner) && inner.operator === "list") {
      const segments = inner.operands.map((op): HierarchySegment => {
        const resolved = resolveOperand(op, context);
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

/** A segmented hierarchy whose segments are all constants is a constant hierarchy. */
function normalizeHierarchy(h: ResolvedHierarchy): ResolvedHierarchy {
  if (h.type !== "segmented") return h;
  const constants: string[] = [];
  for (const segment of h.segments) {
    if (segment.type !== "constant") return h;
    constants.push(segment.value);
  }
  return { type: "constant", segments: constants };
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

export function handleOverlapsOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext,
): PrismaFilter {
  const [left, right] = extractHierarchyOperands("overlaps", operands, context);

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

  // Hierarchy construction evaluates every segment, including a trailing segment that
  // neither prefix comparison needs. Preserve its SQL UNKNOWN under outer negation too.
  const fields = [...leftSegs, ...rightSegs]
    .filter((segment): segment is FieldSegment => segment.type === "field")
    .filter((segment) => segment.fieldRef.nullable !== false);
  const pairs = fields.map(({ fieldRef }) => ({
    equal: buildFieldFilter(fieldRef, "equals", ""),
    unequal: buildFieldFilter(fieldRef, "not", ""),
  }));
  const presence = pairs.map(({ equal, unequal }) => ({
    OR: [equal, unequal],
  }));
  const unknown = pairs.map(({ equal, unequal }) => ({
    AND: [equal, unequal],
  }));

  if (validConditions.length === 0) {
    if (unknown.length > 0) return { OR: unknown };
    const field = [...leftSegs, ...rightSegs].find(
      (segment): segment is FieldSegment => segment.type === "field",
    );
    if (field) return buildImpossibleFilter(field.fieldRef);
    throw new Error("Cannot determine overlap: no field references found");
  }

  if (
    validConditions.some((condition) => Object.keys(condition).length === 0)
  ) {
    return presence.length === 0 ? {} : { AND: presence };
  }

  // Equal-length comparisons are identical in either direction. A contradiction is
  // FALSE for a present field and UNKNOWN for NULL, restoring errors even if the
  // prefix comparison is FALSE (FALSE AND NULL alone would lose that error).
  const condition = validConditions[0]!;
  return presence.length === 0
    ? condition
    : {
        OR: [{ AND: [condition, ...presence] }, ...unknown],
      };
}

/**
 * Guards every hierarchy prefix that is about to become a Prisma `startsWith`.
 *
 * `startsWith` compiles to `LIKE` with no `ESCAPE` clause, so `%` and `_` in the prefix
 * match as wildcards instead of literally and widen the filter past what the PDP allows.
 * `\` is unsafe because PostgreSQL and MySQL treat it as the DEFAULT escape character with no
 * `ESCAPE` clause present, where SQLite has none at all — so the same prefix means different
 * things per provider. `[` is unsafe for a third reason: SQL Server opens a character class on
 * `[` even when an `ESCAPE` clause is declared, so it cannot be made literal at all. Fail loudly
 * rather than emit a filter that admits denied rows.
 */
function assertLikeSafePrefix(prefix: string): void {
  if (/[%_\[\\]/.test(prefix)) {
    throw new Error(
      "Cannot translate hierarchy prefix matching with LIKE metacharacters (%, _, \\ or [): " +
        "Prisma emits LIKE without an ESCAPE clause, \\ is the default escape character on " +
        "PostgreSQL and MySQL, and [ opens a character class on SQL Server even with one"
    );
  }
}

function handleFieldOverlaps(
  left: ResolvedHierarchy,
  right: ResolvedHierarchy
): PrismaFilter {
  if (left.type === "field" && right.type === "field") {
    throw new Error("overlaps: cannot compare two field-reference hierarchies");
  }

  // The caller routes here only when at least one side is a field hierarchy.
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
  const prefix = otherRaw + delimiter;
  assertLikeSafePrefix(prefix);
  conditions.push(buildFieldFilter(field.fieldRef, "startsWith", prefix));

  return { OR: conditions };
}

function extractHierarchyOperands(
  operatorName: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
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
    normalizeHierarchy(resolveHierarchy(leftOp, context)),
    normalizeHierarchy(resolveHierarchy(rightOp, context)),
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

export function handleAncestorDescendantOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext,
  direction: "ancestor" | "descendant",
): PrismaFilter {
  const operatorName = direction === "ancestor" ? "ancestorOf" : "descendentOf";
  const [left, right] = extractHierarchyOperands(operatorName, operands, context);

  // ancestorOf(A, B) = A is strict prefix of B
  // descendentOf(A, B) = B is strict prefix of A
  const ancestor = direction === "ancestor" ? left : right;
  const descendant = direction === "ancestor" ? right : left;
  if (ancestor.type === "field")
    assertStringField(ancestor.fieldRef, operatorName);
  if (descendant.type === "field")
    assertStringField(descendant.fieldRef, operatorName);

  if (ancestor.type === "constant" && descendant.type === "field") {
    const prefix =
      ancestor.segments.join(descendant.delimiter) + descendant.delimiter;
    assertLikeSafePrefix(prefix);
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
