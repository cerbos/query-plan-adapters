import { RE2JS, RE2JSException } from "re2js";

import { UnsupportedQueryPlanError } from "./errors";
import { lookup } from "./narrowing";

/** Escapes every regex metacharacter so the value matches itself literally. */
export const escapeRegexValue = (value: string): string =>
  value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * How a CEL `matches()` pattern reaches MongoDB. CEL compiles the pattern with Go's `regexp`
 * (RE2 syntax) and MongoDB matches `$regex` with PCRE2; the two agree only on a subset, and the
 * pattern is rewritten into that subset's PCRE2 spelling. A pattern Go's parser rejects is a CEL
 * evaluation error, which denies the document under either polarity: it is reported as
 * `invalid` rather than refused, so the caller can answer it as CEL does. Anything else outside
 * the subset is refused.
 */
export type Re2Lowering = { pattern: string } | { invalid: string };

/** Go's `regexp/syntax` cap on a counted repetition, and the budget nested counts share. */
const MAX_REPEAT = 1000;

const POSIX_CLASSES = new Set([
  "alnum",
  "alpha",
  "ascii",
  "blank",
  "cntrl",
  "digit",
  "graph",
  "lower",
  "print",
  "punct",
  "space",
  "upper",
  "word",
  "xdigit",
]);

/** Escapes both engines read as the same one character. */
const CONTROL_ESCAPES = { n: "\\n", t: "\\t", r: "\\r", f: "\\f", a: "\\a" } satisfies Record<string, string>;
const CONTROL_CODE_POINTS = { n: 0x0a, t: 0x09, r: 0x0d, f: 0x0c, a: 0x07 } satisfies Record<string, number>;

/** Perl classes both engines read over ASCII only: `\s` is left out, since PCRE2's includes \v. */
const PERL_CLASSES = new Set(["d", "D", "w", "W"]);

const SPACE_MEMBERS = "\\t\\n\\f\\r ";

type Node =
  | { kind: "atom"; out: string; caseless: boolean }
  | { kind: "anchor"; out: string }
  | { kind: "group"; alternatives: Node[][]; capturing: boolean }
  | { kind: "repeat"; sub: Node; out: string; min: number; max: number; counted: boolean };

class InvalidRe2Pattern extends Error {}

const isWordChar = (c: string): boolean => /^[A-Za-z0-9_]$/.test(c);

type RepeatOperator = { out: string; min: number; max: number; counted: boolean };

class Parser {
  private readonly chars: string[];
  private pos = 0;

  constructor(
    pattern: string,
    private readonly caseInsensitive: boolean,
  ) {
    this.chars = [...pattern];
  }

  parse(): Node[][] {
    const alternatives = this.parseAlternation();

    if (this.pos < this.chars.length) {
      // Only an unmatched `)` stops the top level early.
      throw new UnsupportedQueryPlanError(
        "matches pattern is outside the supported common RE2/PCRE2 subset: unmatched )",
      );
    }

    return alternatives;
  }

  private peek(offset = 0): string | undefined {
    return this.chars[this.pos + offset];
  }

  private parseAlternation(): Node[][] {
    const alternatives: Node[][] = [this.parseConcatenation()];

    while (this.peek() === "|") {
      this.pos += 1;
      alternatives.push(this.parseConcatenation());
    }

    return alternatives;
  }

  private parseConcatenation(): Node[] {
    const nodes: Node[] = [];

    for (;;) {
      const c = this.peek();

      if (c === undefined || c === "|" || c === ")") {
        return nodes;
      }

      const repeat = this.parseRepeatOperator();

      if (repeat !== undefined) {
        const last = nodes[nodes.length - 1];

        if (last === undefined) {
          throw new InvalidRe2Pattern("missing argument to repetition operator");
        }

        if (last.kind === "repeat") {
          throw new InvalidRe2Pattern("invalid nested repetition operator");
        }

        if (last.kind === "anchor") {
          throw new UnsupportedQueryPlanError(
            "matches pattern is outside the supported common RE2/PCRE2 subset: a quantified anchor",
          );
        }

        nodes[nodes.length - 1] = { kind: "repeat", sub: last, ...repeat };
        continue;
      }

      nodes.push(this.parseAtom());
    }
  }

  /**
   * A quantifier at the cursor, consumed: `*`, `+`, `?` or a counted `{n}`, `{n,}`, `{n,m}` as Go
   * reads it (digits without a leading zero). A brace that does not spell one is a literal, left
   * for `parseAtom`. A lazy `?` suffix changes which match is found, never whether one is.
   */
  private parseRepeatOperator(): RepeatOperator | undefined {
    const c = this.peek();
    let result: RepeatOperator;

    if (c === "*" || c === "+" || c === "?") {
      this.pos += 1;
      result = {
        out: c,
        min: c === "+" ? 1 : 0,
        max: c === "?" ? 1 : -1,
        counted: false,
      };
    } else if (c === "{") {
      const counted = this.parseCountedRepeat();

      if (counted === undefined) {
        return undefined;
      }

      result = { ...counted, counted: true };
    } else {
      return undefined;
    }

    if (this.peek() === "?") {
      this.pos += 1;
      result.out += "?";
    }

    return result;
  }

  private parseCountedRepeat(): { out: string; min: number; max: number } | undefined {
    const rest = this.chars.slice(this.pos).join("");
    const match = /^\{(\d+)(?:(,)(\d*))?\}/.exec(rest);

    if (!match || match[1] === undefined) {
      return undefined;
    }

    const [text, minDigits, comma, maxDigits] = match;

    const parseInteger = (digits: string): number | undefined => {
      if (digits.length >= 2 && digits.startsWith("0")) return undefined;

      // Go gives up on an integer of nine or more digits and reports the repeat size invalid.
      return digits.length >= 9 ? -1 : Number(digits);
    };

    const min = parseInteger(minDigits);

    if (min === undefined) {
      return undefined;
    }

    let max: number;

    if (comma === undefined) {
      max = min;
    } else if (maxDigits === undefined || maxDigits === "") {
      max = -1;
    } else {
      const parsed = parseInteger(maxDigits);

      if (parsed === undefined) {
        return undefined;
      }

      max = parsed;
    }

    this.pos += [...text].length;

    if (min < 0 || min > MAX_REPEAT || max > MAX_REPEAT || (max >= 0 && min > max)) {
      throw new InvalidRe2Pattern("invalid repeat count");
    }

    const out =
      comma === undefined ? `{${min}}` : max < 0 ? `{${min},}` : `{${min},${max}}`;

    return { out, min, max };
  }

  private parseAtom(): Node {
    const c = this.peek();

    if (c === undefined) {
      throw new UnsupportedQueryPlanError("matches pattern ended unexpectedly");
    }

    this.pos += 1;

    switch (c) {
      case "(":
        return this.parseGroup();
      case "[":
        return this.parseClass();
      case ".":
        return { kind: "atom", out: ".", caseless: true };
      case "^":
        return { kind: "anchor", out: "^" };
      case "$":
        // PCRE2's `$` also matches before a final newline; `\z` is the end of the subject, as
        // RE2's `$` is.
        return { kind: "anchor", out: "\\z" };
      case "\\":
        return this.parseEscape(false);
      case "*":
      case "+":
      case "?":
        throw new InvalidRe2Pattern("missing argument to repetition operator");
      default:
        return this.literal(c);
    }
  }

  private literal(c: string): Node {
    if (c.charCodeAt(0) < 0x20) {
      throw new UnsupportedQueryPlanError(
        "matches pattern is outside the supported common RE2/PCRE2 subset: a control character",
      );
    }

    this.assertFoldable(c);

    return { kind: "atom", out: isWordChar(c) || c.charCodeAt(0) > 0x7f ? c : `\\${c}`, caseless: true };
  }

  /** Under `(?i)` only ASCII is folded alike: PCRE2's folding of other letters is not proven. */
  private assertFoldable(c: string): void {
    if (this.caseInsensitive && c.charCodeAt(0) > 0x7f) {
      throw new UnsupportedQueryPlanError(
        "matches with (?i) is translated over ASCII only: RE2 and PCRE2 fold other letters " +
          "differently",
      );
    }
  }

  private parseGroup(): Node {
    let capturing = true;

    if (this.peek() === "?") {
      const next = this.peek(1);

      if (next === ":") {
        capturing = false;
        this.pos += 2;
      } else if (next === "=" || next === "!" || (next === "<" && (this.peek(2) === "=" || this.peek(2) === "!"))) {
        throw new InvalidRe2Pattern("invalid or unsupported Perl syntax: lookaround");
      } else {
        throw new UnsupportedQueryPlanError(
          "matches pattern is outside the supported common RE2/PCRE2 subset: a (? group other " +
            "than (?:",
        );
      }
    }

    const alternatives = this.parseAlternation();

    if (this.peek() !== ")") {
      throw new UnsupportedQueryPlanError(
        "matches pattern is outside the supported common RE2/PCRE2 subset: missing )",
      );
    }

    this.pos += 1;

    return { kind: "group", alternatives, capturing };
  }

  /**
   * An escape, outside or inside a bracket class. `\1`–`\9` is a backreference, which RE2 does
   * not have; the escapes passed through are the ones both engines read identically.
   */
  private parseEscape(inClass: boolean): Node {
    const c = this.peek();

    if (c === undefined) {
      throw new UnsupportedQueryPlanError("matches pattern ends in a trailing backslash");
    }

    this.pos += 1;

    if (/^[1-9]$/.test(c)) {
      throw new InvalidRe2Pattern("invalid escape sequence: backreference");
    }

    if (PERL_CLASSES.has(c)) {
      return { kind: "atom", out: `\\${c}`, caseless: false };
    }

    // RE2's `\s` holds no vertical tab, PCRE2's does: spelled as RE2's set.
    if (c === "s") {
      return { kind: "atom", out: inClass ? SPACE_MEMBERS : `[${SPACE_MEMBERS}]`, caseless: false };
    }

    if (c === "S" && !inClass) {
      return { kind: "atom", out: `[^${SPACE_MEMBERS}]`, caseless: false };
    }

    const control = lookup(CONTROL_ESCAPES, c);

    if (control !== undefined) {
      return { kind: "atom", out: control, caseless: false };
    }

    if (c.charCodeAt(0) < 0x80 && !isWordChar(c)) {
      return { kind: "atom", out: `\\${c}`, caseless: false };
    }

    throw new UnsupportedQueryPlanError(
      `matches supports only literal escapes and \\d \\D \\w \\W in the common RE2/PCRE2 subset` +
        (inClass ? " (inside a bracket class)" : ""),
    );
  }

  /**
   * A bracket class, re-spelled member by member: literals escaped, ranges checked the way Go
   * does (`[b-a]` is an error), POSIX classes passed through by name. Both engines read these
   * over ASCII, without Unicode properties.
   */
  private parseClass(): Node {
    let out = "[";

    if (this.peek() === "^") {
      out += "^";
      this.pos += 1;
    }

    let first = true;

    for (;;) {
      const c = this.peek();

      if (c === undefined) {
        throw new UnsupportedQueryPlanError(
          "matches pattern is outside the supported common RE2/PCRE2 subset: missing ]",
        );
      }

      if (c === "]" && !first) {
        this.pos += 1;

        return { kind: "atom", out: `${out}]`, caseless: true };
      }

      first = false;

      if (c === "[" && this.peek(1) === ":") {
        const rest = this.chars.slice(this.pos).join("");
        const match = /^\[:(\^?)([a-z]+):\]/.exec(rest);

        if (!match || match[1] !== "" || !POSIX_CLASSES.has(match[2]!)) {
          throw new UnsupportedQueryPlanError(
            "matches pattern is outside the supported common RE2/PCRE2 subset: a bracket class " +
              "member RE2 and PCRE2 may read differently",
          );
        }

        out += match[0];
        this.pos += match[0].length;
        continue;
      }

      const low = this.parseClassChar();

      if (low.kind === "range-end") {
        throw new UnsupportedQueryPlanError(
          "matches pattern is outside the supported common RE2/PCRE2 subset: a bracket class " +
            "member RE2 and PCRE2 may read differently",
        );
      }

      if (low.kind === "class") {
        out += low.out;
        continue;
      }

      if (this.peek() === "-" && this.peek(1) !== "]" && this.peek(1) !== undefined) {
        this.pos += 1;
        const high = this.parseClassChar();

        if (high.kind !== "char") {
          throw new UnsupportedQueryPlanError(
            "matches pattern is outside the supported common RE2/PCRE2 subset: a range ending " +
              "in a class",
          );
        }

        if (high.codePoint < low.codePoint) {
          throw new InvalidRe2Pattern("invalid character class range");
        }

        out += `${low.out}-${high.out}`;
        continue;
      }

      out += low.out;
    }
  }

  private parseClassChar():
    | { kind: "char"; out: string; codePoint: number }
    | { kind: "class"; out: string }
    | { kind: "range-end" } {
    const c = this.peek();

    if (c === undefined) {
      throw new UnsupportedQueryPlanError(
        "matches pattern is outside the supported common RE2/PCRE2 subset: missing ]",
      );
    }

    this.pos += 1;

    if (c === "\\") {
      const escaped = this.peek() ?? "";
      const node = this.parseEscape(true);

      if (node.kind !== "atom") {
        throw new UnsupportedQueryPlanError("matches pattern has an invalid class escape");
      }

      if (PERL_CLASSES.has(escaped) || escaped === "s") {
        return { kind: "class", out: node.out };
      }

      const codePoint = lookup(CONTROL_CODE_POINTS, escaped) ?? escaped.codePointAt(0)!;

      return { kind: "char", out: node.out, codePoint };
    }

    if (c === "[") {
      // A bare `[` is a member in both engines, but PCRE2 reads `[` before `:` as POSIX syntax.
      return { kind: "char", out: "\\[", codePoint: 0x5b };
    }

    if (c.charCodeAt(0) < 0x20) {
      throw new UnsupportedQueryPlanError(
        "matches pattern is outside the supported common RE2/PCRE2 subset: a control character",
      );
    }

    this.assertFoldable(c);
    const out = c.charCodeAt(0) > 0x7f || isWordChar(c) ? c : `\\${c}`;

    return { kind: "char", out, codePoint: c.codePointAt(0)! };
  }
}

/**
 * Go's `repeatIsValid`: a counted repetition may repeat at most `budget` times, and each one
 * divides the budget its subexpressions get, so `(a{2}){600}` is an error although each count is
 * within 1000 on its own.
 */
const assertRepeatBudget = (nodes: Node[][], budget: number): void => {
  for (const alternative of nodes) {
    for (const node of alternative) {
      assertNodeRepeatBudget(node, budget);
    }
  }
};

const assertNodeRepeatBudget = (node: Node, budget: number): void => {
  if (node.kind === "repeat") {
    let remaining = budget;

    if (node.counted) {
      let m = node.max;

      if (m !== 0) {
        if (m < 0) m = node.min;

        if (m > remaining) {
          throw new InvalidRe2Pattern("invalid repeat count");
        }

        if (m > 0) remaining = Math.floor(remaining / m);
      }
    }

    assertNodeRepeatBudget(node.sub, remaining);
  } else if (node.kind === "group") {
    assertRepeatBudget(node.alternatives, budget);
  }
};

const emit = (alternatives: Node[][]): string =>
  alternatives.map((nodes) => nodes.map(emitNode).join("")).join("|");

const emitNode = (node: Node): string => {
  switch (node.kind) {
    case "atom":
    case "anchor":
      return node.out;
    case "group":
      return `(${node.capturing ? "" : "?:"}${emit(node.alternatives)})`;
    case "repeat":
      return `${emitNode(node.sub)}${node.out}`;
  }
};

/**
 * Rewrites a CEL (RE2) pattern into the subset RE2 and MongoDB's PCRE2 read identically:
 * literals and literal escapes, `.`, `^` and `$` (as `\z`, since PCRE2's `$` also matches before
 * a final newline), the ASCII classes `\d` `\D` `\w` `\W`, bracket classes with ranges and POSIX
 * names, groups, alternation, the `*` `+` `?` quantifiers and counted repetition as Go reads it,
 * and a leading `(?i)`. A pattern RE2 rejects is reported as `invalid`; anything else outside
 * the subset is refused.
 */
export const lowerRe2PatternForMongo = (pattern: string): Re2Lowering => {
  // re2js, a port of RE2, decides validity: whatever it rejects, CEL rejects. The parser below
  // only translates what it accepts, so a rule it misreads can refuse a pattern, never lower one
  // Go would not compile.
  try {
    RE2JS.compile(pattern);
  } catch (error) {
    if (!(error instanceof RE2JSException)) throw error;

    return { invalid: error.message };
  }

  const caseInsensitive = pattern.startsWith("(?i)");
  const body = caseInsensitive ? pattern.slice(4) : pattern;

  try {
    const alternatives = new Parser(body, caseInsensitive).parse();
    assertRepeatBudget(alternatives, MAX_REPEAT);

    return { pattern: `${caseInsensitive ? "(?i)" : ""}${emit(alternatives)}` };
  } catch (error) {
    if (error instanceof InvalidRe2Pattern) {
      throw new UnsupportedQueryPlanError(
        `matches pattern is outside the supported common RE2/PCRE2 subset (${error.message})`,
      );
    }

    throw error;
  }
};
