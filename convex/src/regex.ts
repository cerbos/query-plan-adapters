// `matches()` for the post-filter: an RE2 matcher of its own, so a pattern means exactly what it
// means to CEL (Go's regexp, Perl syntax, no multiline). JavaScript's RegExp is not RE2: its `.`
// skips `\r` and the line separators, `\s` takes Unicode spaces, `$` and case folding differ, and
// it accepts lookaround and backreferences that RE2 reports as errors. Nothing here is handed to it.
//
// The parser follows Go's regexp/syntax: a pattern RE2 rejects throws `InvalidRegexError`, which
// the evaluator turns into the CEL evaluation error `matches()` raises on every row. A pattern RE2
// accepts but this matcher has no tables for (a Unicode class, case folding past ASCII) is refused
// with the adapter's refusal type at translation time, before any filter exists.

import { UnsupportedQueryPlanError } from "./errors";

/** A pattern RE2 itself rejects: evaluating `matches()` with it is a CEL error on every row. */
export class InvalidRegexError extends Error {
  override readonly name = "InvalidRegexError";
}

export interface CompiledRegex {
  /** Whether `input` holds a match anywhere, as Go's `regexp.MatchString` answers. */
  test(input: string): boolean;
}

function refuse(pattern: string, why: string): never {
  throw new UnsupportedQueryPlanError(
    `matches() with pattern ${JSON.stringify(pattern)} is outside the RE2 subset the post-filter ` +
      `evaluates: ${why}`,
  );
}

function invalid(why: string): never {
  throw new InvalidRegexError(`error parsing regexp: ${why}`);
}

// -- the syntax tree -----------------------------------------------------------------------------

type Range = [number, number];

type Assertion =
  | "begin-text"
  | "end-text"
  | "begin-line"
  | "end-line"
  | "word-boundary"
  | "not-word-boundary";

type Node =
  | { kind: "empty" }
  | { kind: "class"; ranges: Range[]; negated: boolean }
  | { kind: "assert"; at: Assertion }
  | { kind: "concat"; nodes: Node[] }
  | { kind: "alt"; branches: Node[] }
  /** `max` is -1 when unbounded, as in Go. */
  | { kind: "repeat"; node: Node; min: number; max: number };

const EMPTY: Node = { kind: "empty" };

const codePoints = (text: string): number[] =>
  Array.from(text, (char) => char.codePointAt(0)!);

const isDigit = (cp: number): boolean => cp >= 0x30 && cp <= 0x39;
const isAlnum = (cp: number): boolean =>
  isDigit(cp) || (cp >= 0x41 && cp <= 0x5a) || (cp >= 0x61 && cp <= 0x7a);
const isWordChar = (cp: number): boolean => isAlnum(cp) || cp === 0x5f;

const PERL_GROUPS: Record<string, Range[]> = {
  d: [[0x30, 0x39]],
  s: [
    [0x09, 0x0a],
    [0x0c, 0x0d],
    [0x20, 0x20],
  ],
  w: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x5f, 0x5f],
    [0x61, 0x7a],
  ],
};

const POSIX_GROUPS: Record<string, Range[]> = {
  alnum: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x61, 0x7a],
  ],
  alpha: [
    [0x41, 0x5a],
    [0x61, 0x7a],
  ],
  ascii: [[0x00, 0x7f]],
  blank: [
    [0x09, 0x09],
    [0x20, 0x20],
  ],
  cntrl: [
    [0x00, 0x1f],
    [0x7f, 0x7f],
  ],
  digit: [[0x30, 0x39]],
  graph: [[0x21, 0x7e]],
  lower: [[0x61, 0x7a]],
  print: [[0x20, 0x7e]],
  punct: [
    [0x21, 0x2f],
    [0x3a, 0x40],
    [0x5b, 0x60],
    [0x7b, 0x7e],
  ],
  space: [
    [0x09, 0x0d],
    [0x20, 0x20],
  ],
  upper: [[0x41, 0x5a]],
  word: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x5f, 0x5f],
    [0x61, 0x7a],
  ],
  xdigit: [
    [0x30, 0x39],
    [0x41, 0x46],
    [0x61, 0x66],
  ],
};

/**
 * RE2's bound on the copies a counted repetition may make, nested ones included: Go's
 * `repeatIsValid`, where each count divides the budget left for the repetitions inside it, an
 * unbounded count spending its minimum.
 */
const MAX_REPEAT = 1000;

const repeatIsValid = (node: Node, budget: number): boolean => {
  let remaining = budget;
  if (node.kind === "repeat") {
    let count = node.max;
    if (count === 0) return true;
    if (count < 0) count = node.min;
    if (count > remaining) return false;
    if (count > 0) remaining = Math.floor(remaining / count);
    return repeatIsValid(node.node, remaining);
  }
  if (node.kind === "concat") return node.nodes.every((n) => repeatIsValid(n, remaining));
  if (node.kind === "alt") return node.branches.every((n) => repeatIsValid(n, remaining));
  return true;
};

// -- the parser ----------------------------------------------------------------------------------

interface Flags {
  foldCase: boolean;
  dotNewline: boolean;
  multiline: boolean;
}

/** Go's `parseInt`: decimal, no leading zero, and -1 past a hundred million. */
const parseCount = (cps: number[], at: number): { value: number; end: number } | undefined => {
  if (at >= cps.length || !isDigit(cps[at]!)) return undefined;
  if (cps[at] === 0x30 && at + 1 < cps.length && isDigit(cps[at + 1]!)) return undefined;
  let end = at;
  let value = 0;
  while (end < cps.length && isDigit(cps[end]!)) {
    if (value >= 1e8) value = -1;
    else value = value * 10 + (cps[end]! - 0x30);
    end++;
  }
  if (value >= 1e8) value = -1;
  return { value, end };
};

class Parser {
  private index = 0;
  private flags: Flags = { foldCase: false, dotNewline: false, multiline: false };
  private depth = 0;
  private readonly cps: number[];

  constructor(private readonly pattern: string) {
    this.cps = codePoints(pattern);
  }

  parse(): Node {
    const node = this.alternation();
    if (this.index < this.cps.length) {
      // Only an unmatched `)` stops the alternation early.
      invalid(`unexpected ): ${JSON.stringify(this.pattern)}`);
    }
    return node;
  }

  private peek(offset = 0): number | undefined {
    return this.cps[this.index + offset];
  }

  private at(cp: string, offset = 0): boolean {
    return this.peek(offset) === cp.codePointAt(0);
  }

  private alternation(): Node {
    const branches: Node[] = [this.sequence()];
    while (this.at("|")) {
      this.index++;
      branches.push(this.sequence());
    }
    return branches.length === 1 ? branches[0]! : { kind: "alt", branches };
  }

  private sequence(): Node {
    const nodes: Node[] = [];
    let lastWasRepeat = false;
    while (this.index < this.cps.length && !this.at("|") && !this.at(")")) {
      const quantifier = this.quantifier();
      if (quantifier !== undefined) {
        if (lastWasRepeat) invalid("invalid nested repetition operator");
        const target = nodes.pop();
        if (target === undefined) invalid("missing argument to repetition operator");
        if (target.kind === "assert") {
          refuse(this.pattern, "a repeated anchor or word boundary");
        }
        const repeat: Node = { kind: "repeat", node: target, ...quantifier };
        if (
          (quantifier.min >= 2 || quantifier.max >= 2) &&
          !repeatIsValid(repeat, MAX_REPEAT)
        ) {
          invalid("invalid repeat count");
        }
        nodes.push(repeat);
        lastWasRepeat = true;
        continue;
      }
      lastWasRepeat = false;
      const atom = this.atom();
      if (Array.isArray(atom)) nodes.push(...atom);
      else if (atom !== undefined) nodes.push(atom);
    }
    if (nodes.length === 0) return EMPTY;
    return nodes.length === 1 ? nodes[0]! : { kind: "concat", nodes };
  }

  /** A `*`, `+`, `?` or counted repetition at the cursor, with Go's reading of a `{`. */
  private quantifier(): { min: number; max: number } | undefined {
    const cp = this.peek();
    let min: number;
    let max: number;
    if (cp === 0x2a || cp === 0x2b || cp === 0x3f) {
      this.index++;
      [min, max] = cp === 0x2a ? [0, -1] : cp === 0x2b ? [1, -1] : [0, 1];
    } else if (cp === 0x7b) {
      const repeat = this.parseRepeat();
      if (repeat === undefined) return undefined;
      [min, max] = [repeat.min, repeat.max];
      if (min < 0 || min > MAX_REPEAT || max > MAX_REPEAT || (max >= 0 && min > max)) {
        invalid("invalid repeat count");
      }
    } else {
      return undefined;
    }
    // Non-greedy: the same language, so the same decision.
    if (this.at("?")) this.index++;
    return { min, max };
  }

  /** `{n}`, `{n,}` or `{n,m}` at the cursor, consumed; any other brace is a literal to RE2. */
  private parseRepeat(): { min: number; max: number } | undefined {
    let at = this.index + 1;
    const min = parseCount(this.cps, at);
    if (min === undefined) return undefined;
    at = min.end;
    let max = min.value;
    if (this.cps[at] === 0x2c) {
      at++;
      if (this.cps[at] === 0x7d) {
        max = -1;
      } else {
        const parsed = parseCount(this.cps, at);
        if (parsed === undefined) return undefined;
        max = parsed.value;
        at = parsed.end;
      }
    }
    if (this.cps[at] !== 0x7d) return undefined;
    this.index = at + 1;
    return { min: min.value, max };
  }

  /**
   * One atom. A flag group such as `(?i)` sets flags and yields nothing; `\Q...\E` yields one
   * literal per character, so a quantifier after it repeats the last character, as in Go.
   */
  private atom(): Node | Node[] | undefined {
    const cp = this.cps[this.index++]!;
    switch (cp) {
      case 0x5e: // ^
        return { kind: "assert", at: this.flags.multiline ? "begin-line" : "begin-text" };
      case 0x24: // $
        return { kind: "assert", at: this.flags.multiline ? "end-line" : "end-text" };
      case 0x2e: // .
        return this.flags.dotNewline
          ? { kind: "class", ranges: [[0, 0x10ffff]], negated: false }
          : { kind: "class", ranges: [[0x0a, 0x0a]], negated: true };
      case 0x28: // (
        return this.group();
      case 0x5b: // [
        return this.characterClass();
      case 0x5c: // \
        return this.escape();
      case 0x2a:
      case 0x2b:
      case 0x3f:
        return invalid("missing argument to repetition operator");
      default:
        return this.literal(cp);
    }
  }

  private group(): Node | undefined {
    const saved = { ...this.flags };
    if (this.at("?")) {
      this.index++;
      if (this.at("P") && this.at("<", 1)) {
        this.index += 2;
        this.captureName();
      } else if (this.at("<")) {
        this.index++;
        this.captureName();
      } else if (!this.perlFlags()) {
        return undefined;
      }
    }
    if (++this.depth > 250) refuse(this.pattern, "more than 250 nested groups");
    const inner = this.alternation();
    this.depth--;
    if (!this.at(")")) invalid(`missing closing ): ${JSON.stringify(this.pattern)}`);
    this.index++;
    this.flags = saved;
    return inner;
  }

  private captureName(): void {
    const start = this.index;
    while (this.index < this.cps.length && !this.at(">")) this.index++;
    if (this.index >= this.cps.length) invalid("invalid named capture");
    const name = this.cps.slice(start, this.index);
    if (name.length === 0 || !name.every(isWordChar)) {
      if (name.some((c) => c > 0x7f)) refuse(this.pattern, "a non-ASCII capture name");
      invalid("invalid named capture");
    }
    this.index++;
  }

  /**
   * Go's `parsePerlFlags` after `(?`: flags, an optional `-` turning the rest off, then `)` (set
   * the flags for the rest of the enclosing group) or `:` (open a group under them). Returns
   * whether a group follows.
   */
  private perlFlags(): boolean {
    let negate = false;
    let sawFlag = false;
    const flags = { ...this.flags };
    for (;;) {
      const cp = this.cps[this.index++];
      if (cp === undefined) invalid("missing closing )");
      switch (cp) {
        case 0x69: // i
          flags.foldCase = !negate;
          sawFlag = true;
          break;
        case 0x73: // s
          flags.dotNewline = !negate;
          sawFlag = true;
          break;
        case 0x6d: // m
          flags.multiline = !negate;
          sawFlag = true;
          break;
        case 0x55: // U
          sawFlag = true;
          break;
        case 0x2d: // -
          if (negate) invalid("invalid or unsupported Perl syntax");
          negate = true;
          sawFlag = false;
          break;
        case 0x3a: // :
        case 0x29: // )
          if (negate && !sawFlag) invalid("invalid or unsupported Perl syntax");
          this.flags = flags;
          return cp === 0x3a;
        default:
          return invalid("invalid or unsupported Perl syntax");
      }
    }
  }

  private literal(cp: number): Node {
    return { kind: "class", ranges: this.fold([[cp, cp]]), negated: false };
  }

  /**
   * The ranges under the current case flag. RE2 folds by Unicode simple case folding orbits; the
   * ASCII letters' orbits are known here (k reaches KELVIN SIGN and s reaches LONG S), and a
   * non-ASCII character under `(?i)` is refused rather than folded by JavaScript's rules.
   */
  private fold(ranges: Range[]): Range[] {
    if (!this.flags.foldCase) return ranges;
    const folded: Range[] = [...ranges];
    for (const [lo, hi] of ranges) {
      if (hi > 0x7f) refuse(this.pattern, "case-insensitive matching of a non-ASCII character");
      for (let cp = lo; cp <= hi; cp++) {
        if (cp >= 0x41 && cp <= 0x5a) folded.push([cp + 0x20, cp + 0x20]);
        if (cp >= 0x61 && cp <= 0x7a) folded.push([cp - 0x20, cp - 0x20]);
        if (cp === 0x6b || cp === 0x4b) folded.push([0x212a, 0x212a]);
        if (cp === 0x73 || cp === 0x53) folded.push([0x017f, 0x017f]);
      }
    }
    return folded;
  }

  private escape(): Node | Node[] {
    const cp = this.cps[this.index];
    if (cp === undefined) invalid("trailing backslash at end of expression");
    switch (cp) {
      case 0x41: // \A
        this.index++;
        return { kind: "assert", at: "begin-text" };
      case 0x7a: // \z
        this.index++;
        return { kind: "assert", at: "end-text" };
      case 0x62: // \b
        this.index++;
        return { kind: "assert", at: "word-boundary" };
      case 0x42: // \B
        this.index++;
        return { kind: "assert", at: "not-word-boundary" };
      case 0x51: {
        // \Q...\E: literal text
        this.index++;
        const nodes: Node[] = [];
        while (this.index < this.cps.length) {
          if (this.at("\\") && this.at("E", 1)) {
            this.index += 2;
            break;
          }
          nodes.push(this.literal(this.cps[this.index++]!));
        }
        return nodes;
      }
      default: {
        const group = this.perlGroup();
        if (group !== undefined) return group;
        return this.literal(this.escapedCodePoint());
      }
    }
  }

  /** `\d`, `\s`, `\w` and their negations at the cursor (after the backslash), or `\p`, refused. */
  private perlGroup(): Node | undefined {
    const cp = this.cps[this.index];
    if (cp === 0x70 || cp === 0x50) {
      refuse(this.pattern, "a Unicode character class (\\p), which this matcher has no tables for");
    }
    if (cp === undefined) return undefined;
    const letter = String.fromCodePoint(cp);
    const ranges = PERL_GROUPS[letter.toLowerCase()];
    if (ranges === undefined) return undefined;
    this.index++;
    return { kind: "class", ranges: this.fold(ranges), negated: letter !== letter.toLowerCase() };
  }

  /** Go's `parseEscape` for a single character, the cursor after the backslash. */
  private escapedCodePoint(): number {
    const cp = this.cps[this.index++];
    if (cp === undefined) invalid("trailing backslash at end of expression");
    // \1 to \7 open an octal escape only when another octal digit follows; alone they are
    // backreferences, which RE2 does not have.
    if (cp >= 0x31 && cp <= 0x37) {
      const next = this.peek();
      if (next === undefined || next < 0x30 || next > 0x37) {
        invalid(`invalid escape sequence: \\${String.fromCodePoint(cp)}`);
      }
    }
    if (cp >= 0x30 && cp <= 0x37) {
      let value = cp - 0x30;
      for (let digits = 1; digits < 3; digits++) {
        const next = this.peek();
        if (next === undefined || next < 0x30 || next > 0x37) break;
        value = value * 8 + (next - 0x30);
        this.index++;
      }
      return value;
    }
    switch (cp) {
      case 0x78: {
        // \x
        const open = this.cps[this.index++];
        if (open === undefined) invalid("invalid escape sequence: \\x");
        if (open === 0x7b) {
          let value = 0;
          let digits = 0;
          for (;;) {
            const next = this.cps[this.index++];
            if (next === undefined) invalid("invalid escape sequence");
            if (next === 0x7d) break;
            const digit = hexValue(next);
            if (digit < 0) invalid("invalid escape sequence");
            value = value * 16 + digit;
            digits++;
            if (value > 0x10ffff) invalid("invalid escape sequence");
          }
          if (digits === 0) invalid("invalid escape sequence");
          return value;
        }
        const second = this.cps[this.index++];
        if (second === undefined || hexValue(open) < 0 || hexValue(second) < 0) {
          invalid("invalid escape sequence");
        }
        return hexValue(open) * 16 + hexValue(second);
      }
      case 0x61:
        return 0x07;
      case 0x66:
        return 0x0c;
      case 0x6e:
        return 0x0a;
      case 0x72:
        return 0x0d;
      case 0x74:
        return 0x09;
      case 0x76:
        return 0x0b;
      default:
        if (cp < 0x80 && !isAlnum(cp)) return cp;
        return invalid(`invalid escape sequence: \\${String.fromCodePoint(cp)}`);
    }
  }

  /** Go's `parseClass`, the cursor after the `[`. */
  private characterClass(): Node {
    let negated = false;
    if (this.at("^")) {
      negated = true;
      this.index++;
    }
    let ranges: Range[] = [];
    let first = true;
    while (first || !this.at("]")) {
      if (this.index >= this.cps.length) {
        invalid(`missing closing ]: ${JSON.stringify(this.pattern)}`);
      }
      first = false;
      if (this.at("[") && this.at(":", 1)) {
        const posix = this.posixClass();
        if (posix !== undefined) {
          ranges.push(...posix);
          continue;
        }
      }
      if (this.at("\\")) {
        this.index++;
        const group = this.perlGroup();
        if (group !== undefined && group.kind === "class") {
          ranges.push(...(group.negated ? negateRanges(group.ranges) : group.ranges));
          continue;
        }
        this.index--;
      }
      const lo = this.classChar();
      let hi = lo;
      if (this.at("-") && this.peek(1) !== undefined && !this.at("]", 1)) {
        this.index++;
        hi = this.classChar();
        if (hi < lo) invalid("invalid character class range");
      }
      ranges.push(...this.fold([[lo, hi]]));
    }
    this.index++;
    if (negated) ranges = negateRanges(ranges);
    return { kind: "class", ranges, negated: false };
  }

  private classChar(): number {
    const cp = this.cps[this.index];
    if (cp === undefined) invalid(`missing closing ]: ${JSON.stringify(this.pattern)}`);
    if (cp === 0x5c) {
      this.index++;
      return this.escapedCodePoint();
    }
    this.index++;
    return cp;
  }

  /** `[:name:]` or `[:^name:]` at the cursor; an unknown name is an error, no `:]` a literal `[`. */
  private posixClass(): Range[] | undefined {
    const rest = this.cps.slice(this.index + 2);
    let end = -1;
    for (let i = 0; i + 1 < rest.length; i++) {
      if (rest[i] === 0x3a && rest[i + 1] === 0x5d) {
        end = i;
        break;
      }
    }
    if (end < 0) return undefined;
    let name = String.fromCodePoint(...rest.slice(0, end));
    const negated = name.startsWith("^");
    if (negated) name = name.slice(1);
    const ranges = POSIX_GROUPS[name];
    if (ranges === undefined) invalid(`invalid character class range: [:${name}:]`);
    this.index += 2 + end + 2;
    const folded = this.fold(ranges);
    return negated ? negateRanges(folded) : folded;
  }
}

const hexValue = (cp: number): number => {
  if (cp >= 0x30 && cp <= 0x39) return cp - 0x30;
  if (cp >= 0x41 && cp <= 0x46) return cp - 0x41 + 10;
  if (cp >= 0x61 && cp <= 0x66) return cp - 0x61 + 10;
  return -1;
};

const negateRanges = (ranges: Range[]): Range[] => {
  const sorted = [...ranges].sort((a, b) => a[0] - b[0]);
  const result: Range[] = [];
  let next = 0;
  for (const [lo, hi] of sorted) {
    if (lo > next) result.push([next, lo - 1]);
    if (hi + 1 > next) next = hi + 1;
  }
  if (next <= 0x10ffff) result.push([next, 0x10ffff]);
  return result;
};

// -- the program ---------------------------------------------------------------------------------

type Instruction =
  | { op: "class"; ranges: Range[]; negated: boolean }
  | { op: "split"; x: number; y: number }
  | { op: "jmp"; x: number }
  | { op: "assert"; at: Assertion }
  | { op: "match" };

/** Past this many instructions a pattern is refused: RE2 has a size limit of its own. */
const MAX_PROGRAM = 20_000;

class Compiler {
  readonly program: Instruction[] = [];

  constructor(private readonly pattern: string) {}

  emit(instruction: Instruction): number {
    if (this.program.length >= MAX_PROGRAM) {
      refuse(this.pattern, "a program of more than 20000 instructions");
    }
    this.program.push(instruction);
    return this.program.length - 1;
  }

  compile(node: Node): void {
    switch (node.kind) {
      case "empty":
        return;
      case "class":
        this.emit({ op: "class", ranges: node.ranges, negated: node.negated });
        return;
      case "assert":
        this.emit({ op: "assert", at: node.at });
        return;
      case "concat":
        for (const child of node.nodes) this.compile(child);
        return;
      case "alt": {
        const jumps: number[] = [];
        node.branches.forEach((branch, i) => {
          if (i < node.branches.length - 1) {
            const split = this.emit({ op: "split", x: 0, y: 0 });
            (this.program[split] as { x: number }).x = this.program.length;
            this.compile(branch);
            jumps.push(this.emit({ op: "jmp", x: 0 }));
            (this.program[split] as { y: number }).y = this.program.length;
          } else {
            this.compile(branch);
          }
        });
        for (const jump of jumps) (this.program[jump] as { x: number }).x = this.program.length;
        return;
      }
      case "repeat": {
        for (let i = 0; i < node.min; i++) this.compile(node.node);
        if (node.max < 0) {
          // x*: split -> (x, jmp back) | out
          const split = this.emit({ op: "split", x: 0, y: 0 });
          (this.program[split] as { x: number }).x = this.program.length;
          this.compile(node.node);
          this.emit({ op: "jmp", x: split });
          (this.program[split] as { y: number }).y = this.program.length;
          return;
        }
        // x{n,m}: (m - n) nested optionals, every one exiting to the same place.
        const splits: number[] = [];
        for (let i = node.min; i < node.max; i++) {
          const split = this.emit({ op: "split", x: 0, y: 0 });
          (this.program[split] as { x: number }).x = this.program.length;
          splits.push(split);
          this.compile(node.node);
        }
        for (const split of splits) (this.program[split] as { y: number }).y = this.program.length;
        return;
      }
    }
  }
}

const inRanges = (ranges: Range[], cp: number): boolean =>
  ranges.some(([lo, hi]) => cp >= lo && cp <= hi);

/** Go decodes each byte of an invalid UTF-8 sequence as U+FFFD; a lone surrogate reads the same. */
const inputCodePoints = (input: string): number[] =>
  codePoints(input).map((cp) => (cp >= 0xd800 && cp <= 0xdfff ? 0xfffd : cp));

const assertionHolds = (at: Assertion, cps: number[], pos: number): boolean => {
  const before = pos > 0 ? cps[pos - 1] : undefined;
  const after = cps[pos];
  switch (at) {
    case "begin-text":
      return pos === 0;
    case "end-text":
      return pos === cps.length;
    case "begin-line":
      return pos === 0 || before === 0x0a;
    case "end-line":
      return pos === cps.length || after === 0x0a;
    case "word-boundary":
    case "not-word-boundary": {
      const boundary =
        (before !== undefined && isWordChar(before)) !==
        (after !== undefined && isWordChar(after));
      return at === "word-boundary" ? boundary : !boundary;
    }
  }
};

/** A Pike VM over code points: every live thread advances together, so no pattern backtracks. */
const runProgram = (program: Instruction[], cps: number[]): boolean => {
  const visited = new Int32Array(program.length).fill(-1);
  let current: number[] = [];
  let next: number[] = [];
  const add = (list: number[], pc: number, pos: number): boolean => {
    if (visited[pc] === pos) return false;
    visited[pc] = pos;
    const inst = program[pc]!;
    switch (inst.op) {
      case "jmp":
        return add(list, inst.x, pos);
      case "split":
        return add(list, inst.x, pos) || add(list, inst.y, pos);
      case "assert":
        return assertionHolds(inst.at, cps, pos) ? add(list, pc + 1, pos) : false;
      case "match":
        return true;
      case "class":
        list.push(pc);
        return false;
    }
  };
  for (let pos = 0; pos <= cps.length; pos++) {
    // Unanchored: a match may start at any position.
    if (add(current, 0, pos)) return true;
    const cp = cps[pos];
    for (const pc of current) {
      const inst = program[pc] as { op: "class"; ranges: Range[]; negated: boolean };
      if (cp !== undefined && inRanges(inst.ranges, cp) !== inst.negated) {
        if (add(next, pc + 1, pos + 1)) return true;
      }
    }
    [current, next] = [next, current];
    next.length = 0;
  }
  return false;
};

const CACHE = new Map<string, CompiledRegex | InvalidRegexError>();

/**
 * Compiles `pattern` as RE2 reads it. Throws `InvalidRegexError` for a pattern RE2 rejects and
 * `UnsupportedQueryPlanError` for one this matcher cannot answer faithfully.
 */
export const compileRe2 = (pattern: string): CompiledRegex => {
  const cached = CACHE.get(pattern);
  if (cached instanceof InvalidRegexError) throw cached;
  if (cached !== undefined) return cached;
  let compiled: CompiledRegex;
  try {
    const compiler = new Compiler(pattern);
    compiler.compile(new Parser(pattern).parse());
    compiler.emit({ op: "match" });
    const program = compiler.program;
    compiled = { test: (input) => runProgram(program, inputCodePoints(input)) };
  } catch (error) {
    if (error instanceof InvalidRegexError && CACHE.size < 1000) CACHE.set(pattern, error);
    throw error;
  }
  if (CACHE.size < 1000) CACHE.set(pattern, compiled);
  return compiled;
};
