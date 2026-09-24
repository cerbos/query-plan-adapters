// `matches(column, "pattern")` for the RE2 patterns whose language a boolean combination of LIKE
// filters decides exactly. Everything else is refused.

import type { PlanExpressionOperand } from "@cerbos/core";

import { assertStringField, buildFieldFilter } from "./fields";
import type { PrismaFilter } from "./index";
import { isResolvedFieldReference, resolveFieldReference } from "./mapping";
import type { ResolvedFieldReference, TranslationContext } from "./mapping";
import { isNamedOperand, isValueOperand } from "./plan";
import { UnsupportedQueryPlanError } from "./errors";

/** Past this many alternatives, a pattern is refused rather than spelled out. */
const MAX_ALTERNATIVES = 256;

// -- the pattern ---------------------------------------------------------------------------------

/** One position of an expanded pattern: a literal character, `.` (not a newline), or `.*`. */
type Token = { char: string } | { any: true } | { star: true };

type Node =
  | { kind: "alt"; branches: Node[][] }
  | { kind: "char"; chars: string[] }
  | { kind: "any" }
  | { kind: "begin" }
  | { kind: "end" }
  | { kind: "repeat"; node: Node; min: number; max: number };

/** A pattern RE2 itself rejects: evaluating it is a CEL error. */
export class InvalidRegexError extends Error {}

function refuse(pattern: string, why: string): never {
  throw new UnsupportedQueryPlanError(
    `Cannot translate matches() with pattern ${JSON.stringify(pattern)}: ${why}. Prisma has no ` +
      "regex filter; only patterns a combination of LIKE filters decides exactly are translated"
  );
}

const DIGITS = [..."0123456789"];

/** A repetition count, `{n}`, `{n,}` or `{n,m}`; any other brace is a literal to RE2. */
const REPEAT = /^\{(\d+)(,(\d*))?\}/;

/** RE2's simple case folding over ASCII letters: `k` and `s` fold with a non-ASCII sign too. */
function foldOrbit(char: string): string[] {
  const lower = char.toLowerCase();
  if (!/^[a-z]$/.test(lower)) return [char];
  const orbit = [lower, lower.toUpperCase()];
  if (lower === "k") orbit.push("K");
  if (lower === "s") orbit.push("ſ");
  return orbit;
}

/** A recursive-descent parser over the pattern's code points (RE2 matches code points). */
class Parser {
  private index = 0;
  private caseInsensitive = false;
  private readonly chars: string[];

  constructor(private readonly pattern: string) {
    this.chars = Array.from(pattern);
  }

  parse(): Node[][] {
    if (this.pattern.startsWith("(?i)")) {
      this.caseInsensitive = true;
      this.index = 4;
    }
    const branches = this.alternation();
    if (this.index !== this.chars.length) refuse(this.pattern, "unbalanced parenthesis");
    return branches;
  }

  private peek(offset = 0): string | undefined {
    return this.chars[this.index + offset];
  }

  private rest(): string {
    return this.chars.slice(this.index).join("");
  }

  private next(): string | undefined {
    return this.chars[this.index++];
  }

  private alternation(): Node[][] {
    const branches: Node[][] = [this.sequence()];
    while (this.peek() === "|") {
      this.index++;
      branches.push(this.sequence());
    }
    return branches;
  }

  private sequence(): Node[] {
    const nodes: Node[] = [];
    for (let char = this.peek(); char !== undefined && char !== "|" && char !== ")"; char = this.peek()) {
      nodes.push(this.quantified(this.atom()));
    }
    return nodes;
  }

  private quantified(node: Node): Node {
    const char = this.peek();
    let min: number;
    let max: number;
    if (char === "*" || char === "+" || char === "?") {
      this.index++;
      [min, max] = char === "*" ? [0, Infinity] : char === "+" ? [1, Infinity] : [0, 1];
    } else if (char === "{") {
      const match = REPEAT.exec(this.rest());
      // RE2 reads a brace that does not open a repetition as a literal.
      if (match === null) return node;
      this.index += Array.from(match[0]).length;
      min = Number(match[1]);
      max = match[2] === undefined ? min : match[3] === "" ? Infinity : Number(match[3]);
      if (max < min || max > 1000) throw new InvalidRegexError("invalid repeat count");
    } else {
      return node;
    }
    const next = this.peek();
    if (next === "?") refuse(this.pattern, "non-greedy repetition");
    if (next === "*" || next === "+" || (next === "{" && REPEAT.test(this.rest()))) {
      throw new InvalidRegexError("invalid nested repetition");
    }
    if (node.kind === "begin" || node.kind === "end") {
      refuse(this.pattern, "a repeated anchor");
    }
    return { kind: "repeat", node, min, max };
  }

  private atom(): Node {
    const char = this.next()!;
    switch (char) {
      case "^":
        return { kind: "begin" };
      case "$":
        return { kind: "end" };
      case ".":
        return { kind: "any" };
      case "(": {
        if (this.peek() === "?") {
          const rest = this.rest();
          if (/^\?(=|!|<=|<!)/.test(rest)) throw new InvalidRegexError("lookaround");
          if (rest.startsWith("?:")) {
            this.index += 2;
          } else {
            refuse(this.pattern, "a group flag");
          }
        }
        const branches = this.alternation();
        if (this.peek() !== ")") refuse(this.pattern, "unbalanced parenthesis");
        this.index++;
        return { kind: "alt", branches };
      }
      case "[":
        return this.characterClass();
      case "\\":
        return this.escape();
      case "*":
      case "+":
      case "?":
        throw new InvalidRegexError("missing argument to repetition operator");
      default:
        return this.literal(char);
    }
  }

  private literal(char: string): Node {
    if (this.caseInsensitive && /[^\x00-\x7F]/.test(char)) {
      refuse(this.pattern, "case-insensitive matching of a non-ASCII character");
    }
    return { kind: "char", chars: this.caseInsensitive ? foldOrbit(char) : [char] };
  }

  private escape(): Node {
    const char = this.next();
    if (char === undefined) throw new InvalidRegexError("trailing backslash");
    if (char === "d") return { kind: "char", chars: DIGITS };
    if (/[1-9]/.test(char)) throw new InvalidRegexError("backreference");
    if (/[A-Za-z0-9]/.test(char)) refuse(this.pattern, `the escape \\${char}`);
    return this.literal(char);
  }

  private characterClass(): Node {
    if (this.peek() === "^") refuse(this.pattern, "a negated character class");
    const chars = new Set<string>();
    let first = true;
    for (;;) {
      const char = this.next();
      if (char === undefined) throw new InvalidRegexError("missing closing ]");
      if (char === "]" && !first) break;
      first = false;
      if (char === "[" && this.rest().startsWith(":digit:]")) {
        this.index += ":digit:]".length;
        DIGITS.forEach((digit) => chars.add(digit));
        continue;
      }
      if (char === "[" && this.peek() === ":") refuse(this.pattern, "a POSIX class other than digit");
      let from = char;
      if (char === "\\") {
        const escaped = this.next();
        if (escaped === undefined) throw new InvalidRegexError("trailing backslash");
        if (escaped === "d") {
          DIGITS.forEach((digit) => chars.add(digit));
          continue;
        }
        if (/[A-Za-z0-9]/.test(escaped)) refuse(this.pattern, `the escape \\${escaped}`);
        from = escaped;
      }
      if (this.peek() === "-" && this.peek(1) !== "]") {
        this.index++;
        const to = this.next();
        if (to === undefined || to === "\\") refuse(this.pattern, "an escaped class range");
        const start = from.codePointAt(0)!;
        const end = to.codePointAt(0)!;
        if (end < start) throw new InvalidRegexError("invalid character class range");
        if (end - start > 128) refuse(this.pattern, "a wide character range");
        for (let code = start; code <= end; code++) chars.add(String.fromCodePoint(code));
        continue;
      }
      chars.add(from);
    }
    const members = [...chars].flatMap((char) => {
      if (this.caseInsensitive && /[^\x00-\x7F]/.test(char)) {
        refuse(this.pattern, "case-insensitive matching of a non-ASCII character");
      }
      return this.caseInsensitive ? foldOrbit(char) : [char];
    });
    return { kind: "char", chars: [...new Set(members)] };
  }
}

// -- expansion -----------------------------------------------------------------------------------

type Expansion = { begin: boolean; end: boolean; tokens: Token[] };

function product(pattern: string, left: Token[][], right: Token[][]): Token[][] {
  if (left.length * right.length > MAX_ALTERNATIVES) {
    refuse(pattern, `more than ${MAX_ALTERNATIVES} alternatives`);
  }
  return left.flatMap((l) => right.map((r) => [...l, ...r]));
}

/** Every token sequence `nodes` (holding no anchor) can match. */
function expand(pattern: string, nodes: Node[]): Token[][] {
  let sequences: Token[][] = [[]];
  for (const node of nodes) {
    sequences = product(pattern, sequences, expandNode(pattern, node));
  }
  return sequences;
}

function expandNode(pattern: string, node: Node): Token[][] {
  switch (node.kind) {
    case "char":
      return node.chars.map((char) => [{ char }]);
    case "any":
      return [[{ any: true }]];
    case "alt": {
      const all = node.branches.flatMap((branch) => expand(pattern, branch));
      if (all.length > MAX_ALTERNATIVES) refuse(pattern, `more than ${MAX_ALTERNATIVES} alternatives`);
      return all;
    }
    case "repeat": {
      if (node.max === Infinity) {
        if (node.node.kind !== "any") refuse(pattern, "unbounded repetition of anything but .");
        return [[...Array.from({ length: node.min }, (): Token => ({ any: true })), { star: true }]];
      }
      const unit = expandNode(pattern, node.node);
      const results: Token[][] = [];
      let power: Token[][] = [[]];
      for (let count = 0; count <= node.max; count++) {
        if (count >= node.min) results.push(...power);
        if (results.length > MAX_ALTERNATIVES) refuse(pattern, `more than ${MAX_ALTERNATIVES} alternatives`);
        if (count < node.max) power = product(pattern, power, unit);
      }
      return results;
    }
    default:
      refuse(pattern, "an anchor inside a group or away from the pattern's ends");
  }
}

/** The pattern as alternatives, each anchored at its ends or not. */
export function expandPattern(pattern: string): Expansion[] {
  const branches = new Parser(pattern).parse();
  const expansions: Expansion[] = [];
  for (const branch of branches) {
    let nodes = branch;
    const begin = nodes[0]?.kind === "begin";
    if (begin) nodes = nodes.slice(1);
    const end = nodes[nodes.length - 1]?.kind === "end";
    if (end) nodes = nodes.slice(0, -1);
    for (const tokens of expand(pattern, nodes)) {
      expansions.push({ begin, end, tokens });
      if (expansions.length > MAX_ALTERNATIVES) {
        refuse(pattern, `more than ${MAX_ALTERNATIVES} alternatives`);
      }
    }
  }
  return expansions;
}

// -- the filter ----------------------------------------------------------------------------------

const LIKE_METACHARACTERS = /[%_\\]/;

/**
 * One alternative as a filter on the column. Without a wildcard it is `equals`, `startsWith`,
 * `endsWith` or `contains` as its anchors say. With one, only a pattern anchored at both ends is
 * translated: a LIKE of `_` for `.` and `%` for `.*`, and no newline anywhere in the value — the
 * literal characters are not newlines, so every newline in a match falls in a wildcard, which RE2's
 * `.` does not match.
 */
function alternativeFilter(
  pattern: string,
  fieldRef: ResolvedFieldReference,
  { begin, end, tokens }: Expansion
): PrismaFilter {
  const literal = tokens.every((token) => "char" in token)
    ? tokens.map((token) => (token as { char: string }).char).join("")
    : undefined;
  if (literal !== undefined && begin && end) {
    return buildFieldFilter(fieldRef, "equals", literal);
  }
  const likeText = tokens
    .map((token) => ("char" in token ? token.char : "any" in token ? "_" : "%"))
    .join("");
  const literals = tokens.flatMap((token) => ("char" in token ? [token.char] : []));
  if (literals.some((char) => LIKE_METACHARACTERS.test(char))) {
    refuse(pattern, "a literal LIKE metacharacter (%, _ or \\), which Prisma does not escape");
  }
  if (literal !== undefined) {
    return buildFieldFilter(fieldRef, begin ? "startsWith" : end ? "endsWith" : "contains", literal);
  }
  if (!begin || !end) {
    refuse(pattern, "a wildcard in a pattern not anchored at both ends");
  }
  if (literals.includes("\n")) refuse(pattern, "a literal newline beside a wildcard");
  const noNewline: PrismaFilter = { NOT: buildFieldFilter(fieldRef, "contains", "\n") };
  const segments = likeText.split("%");
  if (segments.length === 1) {
    // Exactly the pattern's length: it matches as a prefix, and one more character never fits.
    return {
      AND: [
        buildFieldFilter(fieldRef, "startsWith", likeText),
        { NOT: buildFieldFilter(fieldRef, "startsWith", `${likeText}_`) },
        noNewline,
      ],
    };
  }
  if (segments.length > 2) refuse(pattern, "more than one .* in one alternative");
  // `A.*B`: A at the start, B at the end, and room for both without overlapping.
  const [head, tail] = segments as [string, string];
  const filters: PrismaFilter[] = [];
  if (head !== "") filters.push(buildFieldFilter(fieldRef, "startsWith", head));
  if (tail !== "") filters.push(buildFieldFilter(fieldRef, "endsWith", tail));
  if (head !== "" && tail !== "") {
    filters.push(buildFieldFilter(fieldRef, "startsWith", "_".repeat(Array.from(head).length + Array.from(tail).length)));
  }
  if (filters.length === 0) filters.push(buildFieldFilter(fieldRef, "startsWith", ""));
  return { AND: [...filters, noNewline] };
}

/**
 * Whether the pattern is one RE2 rejects, so that `matches()` raises an error on every row. A
 * pattern this adapter cannot translate is not invalid; it is refused when translated.
 */
export function isInvalidPattern(pattern: string): boolean {
  try {
    expandPattern(pattern);
    return false;
  } catch (error) {
    return error instanceof InvalidRegexError;
  }
}

export function handleMatchesOperator(
  operands: PlanExpressionOperand[],
  context: TranslationContext
): PrismaFilter {
  const [receiver, patternOperand] = operands;
  if (
    operands.length !== 2 ||
    receiver === undefined ||
    !isNamedOperand(receiver) ||
    patternOperand === undefined ||
    !isValueOperand(patternOperand) ||
    typeof patternOperand.value !== "string"
  ) {
    throw new UnsupportedQueryPlanError(
      "matches() is translated only for a column receiver and a literal pattern"
    );
  }
  const pattern = patternOperand.value;
  const fieldRef = resolveFieldReference(receiver.name, context);
  if (!isResolvedFieldReference(fieldRef)) {
    throw new UnsupportedQueryPlanError("matches() requires a column receiver");
  }
  assertStringField(fieldRef, "matches");
  let expansions: Expansion[];
  try {
    expansions = expandPattern(pattern);
  } catch (error) {
    if (error instanceof InvalidRegexError) {
      refuse(pattern, `RE2 rejects it (${error.message})`);
    }
    throw error;
  }
  const filters = expansions.map((expansion) => alternativeFilter(pattern, fieldRef, expansion));
  return filters.length === 1 ? filters[0]! : { OR: filters };
}
