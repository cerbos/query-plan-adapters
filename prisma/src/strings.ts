// The receiver-sensitive string operators: `contains`, `startsWith`, `endsWith`.

import type { PlanExpressionOperand } from "@cerbos/core";

import { assertStringField, buildFieldFilter } from "./fields";
import type { PrismaFilter } from "./index";
import { isResolvedFieldReference, isResolvedValue } from "./mapping";
import type { TranslationContext } from "./mapping";
import { assertDefined } from "./plan";
import { resolveOperand } from "./translate";

// Upper bound on the IN-list produced when enumerating a constant receiver's substrings.
const MAX_ENUMERATED_NEEDLES = 1000;

/**
 * Every needle `haystack.<operator>(needle)` is true for: its prefixes for startsWith, its
 * suffixes for endsWith, its substrings for contains — and the empty string for all three.
 */
function candidateNeedles(operator: string, haystack: string): Set<string> {
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
  return candidates;
}

/**
 * Helper function to handle string operators (contains, startsWith, endsWith).
 *
 * These operators are receiver-sensitive and the planner preserves policy source order:
 * operand 0 is the receiver (haystack), operand 1 the needle. Swapping them silently
 * inverts the match direction, so unlike symmetric comparisons they are never normalized.
 */
export function handleStringOperator(
  operator: string,
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  if (operands.length !== 2) {
    throw new Error(`${operator} requires exactly two operands`);
  }
  const receiver = resolveOperand(
    assertDefined(operands[0], `${operator} requires a receiver operand`),
    context
  );
  const needle = resolveOperand(
    assertDefined(operands[1], `${operator} requires a needle operand`),
    context
  );

  if (isResolvedFieldReference(receiver)) assertStringField(receiver, operator);
  if (isResolvedFieldReference(needle)) assertStringField(needle, operator);

  // Column receiver, constant needle: Prisma's LIKE-based filter.
  if (isResolvedFieldReference(receiver) && isResolvedValue(needle)) {
    const { value } = needle;
    if (typeof value !== "string") {
      throw new Error(`${operator} operator requires string value`);
    }
    // Prisma emits LIKE without an ESCAPE clause and does not escape wildcard characters,
    // so a needle containing % or _ would match as a pattern instead of literally (e.g.
    // startsWith("100%") wrongly matches "100xdone"). Fail loudly rather than filter wrong.
    //
    // A backslash is the same hazard reached from the other direction, and only an executed
    // PostgreSQL leg surfaces it (cerbos/query-plan-adapters#320): SQLite's LIKE has no escape
    // character at all, so `\` is literal there, while PostgreSQL and MySQL treat it as the
    // DEFAULT escape character even with no ESCAPE clause. `endsWith("\\")` is then a pattern
    // ending in an escape character — a hard error on PostgreSQL — and a `\x` anywhere inside
    // silently drops the backslash, so `contains("a\\b")` matches "ab", a row the PDP denies.
    if (/[%_\\]/.test(value)) {
      throw new Error(
        `Cannot translate ${operator} with a needle containing LIKE metacharacters (%, _ or \\): ` +
          "Prisma does not escape wildcards in string filters, and \\ is the default LIKE " +
          "escape character on PostgreSQL and MySQL"
      );
    }
    return buildFieldFilter(receiver, operator, value);
  }

  // Constant receiver, column needle (e.g. `"a-b-c".contains(R.attr.x)`): enumerate every
  // candidate needle the constant admits into an exact IN filter. No LIKE, so no escaping
  // hazards; a NULL needle column stays excluded (CEL missing-attribute deny).
  if (isResolvedValue(receiver) && isResolvedFieldReference(needle)) {
    if (typeof receiver.value !== "string") {
      throw new Error(`${operator} operator requires a string receiver`);
    }
    const candidates = candidateNeedles(operator, receiver.value);
    if (candidates.size > MAX_ENUMERATED_NEEDLES) {
      throw new Error(
        `Cannot translate ${operator} with a constant receiver of this length: ` +
          `enumerating its ${candidates.size} candidate needles exceeds the ${MAX_ENUMERATED_NEEDLES}-entry limit`
      );
    }
    return buildFieldFilter(needle, "in", [...candidates]);
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
