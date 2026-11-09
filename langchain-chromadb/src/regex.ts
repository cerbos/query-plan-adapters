import { RE2JS } from "re2js";

import { EVALUATION_ERROR } from "./cel";
import type { EvaluationError } from "./cel";

/**
 * CEL's `matches`: an unanchored RE2 search. The engine is re2js, a port of RE2 with its syntax
 * and its limits (no lookaround or backreferences, a repeat count of at most 1000, `$` at the
 * absolute end, `(?i)` by Unicode simple case folding), so the pattern means what it means to the
 * PDP. A pattern RE2 rejects is an evaluation error, as it is in Cerbos.
 */
export interface CompiledRegex {
  matches: (input: string) => boolean;
}

export const compileRegex = (pattern: string): CompiledRegex | EvaluationError => {
  let compiled: RE2JS;
  try {
    compiled = RE2JS.compile(pattern);
  } catch {
    return EVALUATION_ERROR;
  }
  return { matches: (input) => compiled.matcher(input).find() };
};
