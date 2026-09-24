import { UnsupportedQueryPlanError } from "./errors";

/**
 * CEL's `matches()` without a SQL regex engine.
 *
 * CEL matches with RE2 (Go's `regexp`), and no store's regex dialect is RE2: MySQL's ICU engine
 * lets `$` match before a trailing newline, PostgreSQL's ARE and ICU differ from RE2 in classes
 * and flags, and SQLite has no regex operator at all. So a pattern is never handed to the store. Instead a pattern is
 * parsed here, and lowered only when what it matches can be said with the adapter's exact string
 * predicates: equality, `startsWith`, `endsWith` and `contains` over a FINITE set of literals, plus
 * two infinite forms with an exact reading — every character drawn from a small set
 * (`^[ab]+$`), and a prefix and a suffix around a run of non-newline characters (`^a.*b$`).
 *
 * `compileRegex` returns one `RegexPlan` per top-level alternative, which the caller ORs together;
 * `"error"` for a pattern RE2 rejects (CEL raises at evaluation, which denies under both
 * polarities); and throws `UnsupportedQueryPlanError` for anything else.
 */

/** The most literals one pattern may expand to before it is refused rather than enumerated. */
const MAX_LITERALS = 256;
/** The most characters a set may hold for the every-character-in-a-set form. */
const MAX_SET_SIZE = 64;
/** RE2's own bound on a counted repetition. */
const MAX_REPEAT = 1000;

export type RegexPlan =
  /** The string is one of `literals`. */
  | { kind: "equals"; literals: string[] }
  /** Some literal is a prefix / suffix / substring of the string. */
  | { kind: "startsWith" | "endsWith" | "contains"; literals: string[] }
  /** Every character of the string is in `characters`, and there are at least `min` of them. */
  | { kind: "allCharactersIn"; characters: string[]; min: number }
  /** No character is a newline, and there are at least `min` of them. */
  | { kind: "noNewline"; min: number }
  /**
   * For some pair, the string starts with `prefix`, ends with `suffix`, holds at least
   * `prefix + min + suffix` characters, and contains no newline.
   */
  | { kind: "prefixSuffix"; pairs: [string, string][]; min: number };

type Node =
  | { kind: "set"; characters: string[] }
  | { kind: "dot" }
  | { kind: "group"; alternatives: Node[][] }
  | { kind: "repeat"; node: Node; min: number; max: number | undefined }
  | { kind: "begin" }
  | { kind: "end" };

class InvalidPattern extends Error {}

const unsupported = (pattern: string, why: string): UnsupportedQueryPlanError =>
  new UnsupportedQueryPlanError(
    `Cannot translate matches("${pattern}"): ${why}. The adapter lowers only patterns whose ` +
      "matches are a finite set of literals under anchors, every character from a small set, " +
      "or a prefix and suffix around non-newline characters, because no store's regex dialect " +
      "is RE2",
  );

const range = (from: string, to: string): string[] => {
  const characters: string[] = [];
  for (let code = from.codePointAt(0)!; code <= to.codePointAt(0)!; code += 1) {
    characters.push(String.fromCodePoint(code));
  }
  return characters;
};

const DIGITS = range("0", "9");
const LOWER = range("a", "z");
const UPPER = range("A", "Z");
const WORD = [...DIGITS, ...UPPER, ...LOWER, "_"];
const SPACE = ["\t", "\n", "\f", "\r", " "];

/** RE2's ASCII POSIX classes, `[[:name:]]`. */
const POSIX_CLASSES: Record<string, string[]> = {
  alnum: [...DIGITS, ...UPPER, ...LOWER],
  alpha: [...UPPER, ...LOWER],
  blank: [" ", "\t"],
  digit: DIGITS,
  lower: LOWER,
  space: [...SPACE, "\v"],
  upper: UPPER,
  word: WORD,
  xdigit: [...DIGITS, ...range("A", "F"), ...range("a", "f")],
};

/** Perl classes RE2 accepts, outside and inside brackets. */
const PERL_CLASSES: Record<string, string[]> = { d: DIGITS, s: SPACE, w: WORD };

const LITERAL_ESCAPES: Record<string, string> = {
  a: "\x07",
  f: "\f",
  n: "\n",
  r: "\r",
  t: "\t",
  v: "\v",
};

/**
 * Every character RE2's `(?i)` treats as the same as `character`. Its case folding follows Unicode
 * simple folding, so beyond the two ASCII cases `k` also matches KELVIN SIGN and `s` matches LONG S.
 * A non-ASCII letter's orbit is not tabulated here, so it is refused.
 */
const foldCase = (character: string, pattern: string): string[] => {
  if (character === "k" || character === "K") return ["k", "K", "K"];
  if (character === "s" || character === "S") return ["s", "S", "ſ"];
  if (/^[a-zA-Z]$/.test(character)) {
    return [character.toLowerCase(), character.toUpperCase()];
  }
  if (character.codePointAt(0)! > 0x7f && character.toLowerCase() !== character.toUpperCase()) {
    throw unsupported(pattern, `case-insensitive matching of '${character}' is not tabulated`);
  }
  return [character];
};

const unique = (values: string[]): string[] => [...new Set(values)];

/** A recursive-descent parser for the RE2 syntax the lowering can use. */
class Parser {
  private readonly characters: string[];
  private position = 0;
  private caseInsensitive = false;

  constructor(private readonly pattern: string) {
    this.characters = [...pattern];
  }

  parse(): Node[][] {
    if (this.pattern.startsWith("(?i)")) {
      this.caseInsensitive = true;
      this.position = 4;
    }
    const alternatives = this.alternation();
    if (this.position < this.characters.length) {
      // Only an unbalanced `)` stops the top level early.
      throw new InvalidPattern();
    }
    return alternatives;
  }

  private peek(offset = 0): string | undefined {
    return this.characters[this.position + offset];
  }

  private alternation(): Node[][] {
    const alternatives = [this.sequence()];
    while (this.peek() === "|") {
      this.position += 1;
      alternatives.push(this.sequence());
    }
    return alternatives;
  }

  private sequence(): Node[] {
    const nodes: Node[] = [];
    for (;;) {
      const next = this.peek();
      if (next === undefined || next === "|" || next === ")") return nodes;
      const atom = this.atom();
      nodes.push(this.quantified(atom));
    }
  }

  private quantified(atom: Node): Node {
    let node = atom;
    for (;;) {
      const next = this.peek();
      let bounds: [number, number | undefined] | undefined;
      if (next === "*") bounds = [0, undefined];
      else if (next === "+") bounds = [1, undefined];
      else if (next === "?") bounds = [0, 1];
      else if (next === "{") bounds = this.countedRepeat();
      if (bounds === undefined) return node;
      if (next !== "{") this.position += 1;
      // RE2 rejects a repeated repetition (`a**`, "invalid nested repetition operator").
      if (node.kind === "repeat") throw new InvalidPattern();
      if (node.kind === "begin" || node.kind === "end") {
        throw unsupported(this.pattern, "a repeated anchor is not read");
      }
      // A trailing `?` makes the repetition lazy, which changes WHERE it matches, not WHETHER.
      if (this.peek() === "?") this.position += 1;
      node = { kind: "repeat", node, min: bounds[0], max: bounds[1] };
    }
  }

  /** `{n}`, `{n,}` or `{n,m}`; anything else starting with `{` is a literal brace in RE2. */
  private countedRepeat(): [number, number | undefined] | undefined {
    const rest = this.characters.slice(this.position).join("");
    const match = /^\{(\d+)(,(\d*))?\}/.exec(rest);
    if (!match) return undefined;
    const min = Number(match[1]);
    const max = match[2] === undefined ? min : match[3] === "" ? undefined : Number(match[3]);
    if (min > MAX_REPEAT || (max !== undefined && (max > MAX_REPEAT || max < min))) {
      throw new InvalidPattern();
    }
    this.position += [...match[0]].length;
    return [min, max];
  }

  private literal(character: string): Node {
    return {
      kind: "set",
      characters: this.caseInsensitive ? foldCase(character, this.pattern) : [character],
    };
  }

  private atom(): Node {
    const character = this.peek()!;
    this.position += 1;
    switch (character) {
      case "^":
        return { kind: "begin" };
      case "$":
        return { kind: "end" };
      case ".":
        return { kind: "dot" };
      case "(":
        return this.group();
      case "[":
        return { kind: "set", characters: this.characterClass() };
      case "\\":
        return this.escape();
      case "*":
      case "+":
      case "?":
        throw new InvalidPattern(); // "missing argument to repetition operator"
      case "{":
        // A brace that opens a valid count with nothing before it is the same error; any other
        // brace is a literal in RE2.
        this.position -= 1;
        if (this.countedRepeat() !== undefined) throw new InvalidPattern();
        this.position += 1;
        return this.literal(character);
      default:
        return this.literal(character);
    }
  }

  private group(): Node {
    if (this.peek() === "?") {
      const rest = this.characters.slice(this.position).join("");
      // Lookaround is not RE2 syntax: `regexp.Compile` rejects it, so CEL raises at evaluation.
      if (/^\?(=|!|<=|<!)/.test(rest)) throw new InvalidPattern();
      if (rest.startsWith("?:")) {
        this.position += 2;
      } else {
        throw unsupported(this.pattern, "only (?i) at the start and (?:...) groups are read");
      }
    }
    const alternatives = this.alternation();
    if (this.peek() !== ")") throw new InvalidPattern(); // "missing closing )"
    this.position += 1;
    return { kind: "group", alternatives };
  }

  private escape(): Node {
    const character = this.peek();
    if (character === undefined) throw new InvalidPattern(); // "trailing backslash"
    this.position += 1;
    if (character === "A") return { kind: "begin" };
    if (character === "z") return { kind: "end" };
    const perl = PERL_CLASSES[character];
    if (perl) return { kind: "set", characters: perl };
    const control = LITERAL_ESCAPES[character];
    if (control) return this.literal(control);
    if (/^[!-/:-@[-`{-~]$/.test(character)) return this.literal(character);
    throw unsupported(this.pattern, `the escape \\${character} is not read`);
  }

  /** `[...]`, just after its `[`. */
  private characterClass(): string[] {
    if (this.peek() === "^") {
      throw unsupported(this.pattern, "a negated class matches an unbounded set of characters");
    }
    const members: string[] = [];
    let first = true;
    for (;;) {
      const character = this.peek();
      if (character === undefined) throw new InvalidPattern(); // "missing closing ]"
      if (character === "]" && !first) {
        this.position += 1;
        break;
      }
      first = false;
      if (character === "[" && this.peek(1) === ":") {
        const rest = this.characters.slice(this.position).join("");
        const match = /^\[:([a-z]+):\]/.exec(rest);
        const posix = match ? POSIX_CLASSES[match[1]!] : undefined;
        if (!match || !posix) {
          throw unsupported(this.pattern, "only ASCII POSIX classes are read");
        }
        members.push(...posix);
        this.position += match[0].length;
        continue;
      }
      const start = this.classCharacter();
      if (Array.isArray(start)) {
        members.push(...start);
        continue;
      }
      if (this.peek() === "-" && this.peek(1) !== "]" && this.peek(1) !== undefined) {
        this.position += 1;
        const end = this.classCharacter();
        if (Array.isArray(end) || end < start) throw new InvalidPattern();
        const span = range(start, end);
        if (span.length > MAX_SET_SIZE * 4) {
          throw unsupported(this.pattern, "a class range is too wide to enumerate");
        }
        members.push(...span);
      } else {
        members.push(start);
      }
    }
    const folded = this.caseInsensitive
      ? members.flatMap((member) => foldCase(member, this.pattern))
      : members;
    return unique(folded);
  }

  /** One class member: a character, or a Perl class's characters. */
  private classCharacter(): string | string[] {
    const character = this.peek()!;
    this.position += 1;
    if (character !== "\\") return character;
    const escaped = this.peek();
    if (escaped === undefined) throw new InvalidPattern();
    this.position += 1;
    const perl = PERL_CLASSES[escaped];
    if (perl) return perl;
    const control = LITERAL_ESCAPES[escaped];
    if (control) return control;
    if (/^[!-/:-@[-`{-~]$/.test(escaped)) return escaped;
    throw unsupported(this.pattern, `the escape \\${escaped} is not read`);
  }
}

/** Every string a node sequence matches, when that is a finite set of at most MAX_LITERALS. */
const finiteSequence = (nodes: Node[]): string[] | undefined => {
  let strings = [""];
  for (const node of nodes) {
    const next = finiteNode(node);
    if (next === undefined) return undefined;
    const product: string[] = [];
    for (const head of strings) {
      for (const tail of next) {
        product.push(head + tail);
        if (product.length > MAX_LITERALS) return undefined;
      }
    }
    strings = unique(product);
  }
  return strings;
};

const finiteNode = (node: Node): string[] | undefined => {
  switch (node.kind) {
    case "set":
      return node.characters;
    case "dot":
    case "begin":
    case "end":
      return undefined;
    case "group": {
      const union: string[] = [];
      for (const alternative of node.alternatives) {
        const strings = finiteSequence(alternative);
        if (strings === undefined) return undefined;
        union.push(...strings);
        if (union.length > MAX_LITERALS) return undefined;
      }
      return unique(union);
    }
    case "repeat": {
      if (node.max === undefined) return undefined;
      const union: string[] = [];
      for (let count = node.min; count <= node.max; count += 1) {
        const strings = finiteSequence(Array<Node>(count).fill(node.node));
        if (strings === undefined) return undefined;
        union.push(...strings);
        if (union.length > MAX_LITERALS) return undefined;
      }
      return unique(union);
    }
  }
};

/**
 * One top-level alternative, its anchors removed. An end without an anchor is free: a match may
 * start (or stop) anywhere, so a repetition there needs only its minimum number of copies —
 * `a+b` somewhere is exactly `ab` somewhere — and `.*` there needs none.
 */
const planAlternative = (pattern: string, sequence: Node[]): RegexPlan | "never" => {
  const nodes = [...sequence];
  let anchoredStart = false;
  let anchoredEnd = false;
  while (nodes[0]?.kind === "begin") {
    anchoredStart = true;
    nodes.shift();
  }
  while (nodes[nodes.length - 1]?.kind === "end") {
    anchoredEnd = true;
    nodes.pop();
  }
  if (nodes.some((node) => node.kind === "begin" || node.kind === "end")) {
    throw unsupported(pattern, "an anchor inside the pattern is not read");
  }
  const atLeast = (node: Node): Node =>
    node.kind === "repeat" ? { ...node, max: node.min } : node;
  if (!anchoredStart) {
    while (nodes[0]?.kind === "repeat" && nodes[0].min === 0) nodes.shift();
    if (nodes[0]) nodes[0] = atLeast(nodes[0]);
  }
  if (!anchoredEnd) {
    for (let last = nodes[nodes.length - 1]; last?.kind === "repeat" && last.min === 0; ) {
      nodes.pop();
      last = nodes[nodes.length - 1];
    }
    if (nodes.length > 0) nodes[nodes.length - 1] = atLeast(nodes[nodes.length - 1]!);
  }

  const literals = finiteSequence(nodes);
  if (literals !== undefined) {
    if (literals.length === 0) return "never";
    const kind = anchoredStart
      ? anchoredEnd
        ? "equals"
        : "startsWith"
      : anchoredEnd
        ? "endsWith"
        : "contains";
    return { kind, literals };
  }

  if (anchoredStart && anchoredEnd) {
    // `^X*$`, `^X+$`, `^X{n,}$` over a set or `.`.
    const [only] = nodes;
    if (nodes.length === 1 && only?.kind === "repeat" && only.max === undefined) {
      if (only.node.kind === "dot") return { kind: "noNewline", min: only.min };
      if (only.node.kind === "set" && only.node.characters.length <= MAX_SET_SIZE) {
        return { kind: "allCharactersIn", characters: only.node.characters, min: only.min };
      }
    }
    // `^P.*S$`: finite prefix and suffix around one unbounded run of `.`.
    const runs = nodes.flatMap((node, index) =>
      node.kind === "repeat" && node.node.kind === "dot" && node.max === undefined ? [index] : [],
    );
    if (runs.length === 1) {
      const run = runs[0]!;
      const prefixes = finiteSequence(nodes.slice(0, run));
      const suffixes = finiteSequence(nodes.slice(run + 1));
      if (prefixes !== undefined && suffixes !== undefined) {
        const pairs = prefixes.flatMap((prefix) =>
          suffixes.map((suffix): [string, string] => [prefix, suffix]),
        );
        if (pairs.length === 0) return "never";
        if (
          pairs.length <= MAX_LITERALS &&
          pairs.every(([prefix, suffix]) => !`${prefix}${suffix}`.includes("\n"))
        ) {
          return { kind: "prefixSuffix", pairs, min: (nodes[run] as { min: number }).min };
        }
      }
    }
  }
  throw unsupported(pattern, "what it matches is not a finite set of literals");
};

export const compileRegex = (pattern: string): RegexPlan[] | "error" => {
  let alternatives: Node[][];
  try {
    alternatives = new Parser(pattern).parse();
  } catch (error) {
    if (error instanceof InvalidPattern) return "error";
    throw error;
  }
  return alternatives.flatMap((alternative) => {
    const plan = planAlternative(pattern, alternative);
    return plan === "never" ? [] : [plan];
  });
};
