/**
 * A well-formed plan asks for something a Chroma metadata filter cannot express. Thrown so a caller
 * can route that case — a broader search, a per-document `check()`, a deny — without matching on
 * the message, which is not a stable contract
 * (cerbos/query-plan-adapters#228). A malformed plan or a mapper misconfiguration is a plain `Error`.
 *
 * `operator` is the plan operator the refusal is about: the one the message names, after mirroring
 * and negation (`not(eq)` over an optional key reports `ne`); a computed operand's own inside a
 * comparison (`add`, `size`), and the enclosing operator anywhere else (`exists`, not its lambda);
 * the comparison itself for two keys or two literals; `if` for a ternary.
 *
 * Under `allowPostFilter`, the shapes the post-filter cannot evaluate exactly throw it too, and
 * `operator` names the operator the post-filter refused (an unmapped reference reports the
 * operator reading it).
 */
export class UnsupportedOperatorError extends Error {
  readonly operator: string;

  constructor(operator: string, message: string) {
    super(message);
    this.name = "UnsupportedOperatorError";
    this.operator = operator;
  }
}
