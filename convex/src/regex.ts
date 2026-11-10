import { RE2JS, RE2JSException } from "re2js";

/**
 * CEL's `matches`: an unanchored RE2 search. The engine is re2js, a port of RE2 with its syntax
 * and its limits (no lookaround or backreferences, a repeat count of at most 1000, `$` at the
 * absolute end, `.` excluding only `\n`, `(?i)` by Unicode simple case folding), so a pattern
 * means what it means to the PDP. JavaScript's RegExp is not RE2 and is never handed a pattern.
 */
export interface CompiledRegex {
  test(input: string): boolean;
}

const CACHE = new Map<string, CompiledRegex | null>();

/**
 * `pattern` compiled as RE2 reads it, or null for one RE2 itself rejects: evaluating `matches()`
 * with it is a CEL error on every row.
 */
export const compileRe2 = (pattern: string): CompiledRegex | null => {
  const cached = CACHE.get(pattern);

  if (cached !== undefined) return cached;
  let result: CompiledRegex | null;

  try {
    const compiled = RE2JS.compile(pattern);
    result = { test: (input) => compiled.matcher(input).find() };
  } catch (error) {
    if (!(error instanceof RE2JSException)) throw error;
    result = null;
  }

  if (CACHE.size < 1000) CACHE.set(pattern, result);

  return result;
};
