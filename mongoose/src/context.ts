import type { Mapper, NullAttributeRepresentation } from "./index";

/** What every translation step can see: the caller's choices, and whether it is inside a lambda. */
export type TranslateContext = {
  readonly mapper: Mapper;
  readonly nullRepresentation: NullAttributeRepresentation;
  readonly scope:
    | { readonly kind: "root" }
    | { readonly kind: "collection"; readonly variable: string };
};

/**
 * Guards every site that would emit a null-selecting predicate out of a `null` comparison
 * operand.
 *
 * Under the `"omitted"` representation a NULL field carries no attribute, so CEL raises a
 * missing-attribute error and `check()` denies the document; matching null would return exactly
 * the documents the PDP refuses. The rejection is deliberately wider than the over-granting
 * shapes: `ne(x, null)` on its own is aligned, but negation is applied by wrapping the built
 * filter rather than by pushing it into the leaf, so a leaf cannot tell whether an enclosing
 * `not` will flip a not-null predicate back into a null-selecting one. Rejecting every null
 * operand is correct under any nesting; narrowing it requires negation-parity tracking.
 */
export const assertNullOperandTranslatable = (
  ctx: TranslateContext,
  context: string,
): void => {
  if (ctx.nullRepresentation === "omitted") {
    throw new Error(
      `Cannot translate ${context} under nullAttributeRepresentation "omitted": a NULL field ` +
        "sends no attribute, so Cerbos evaluates the comparison as a missing-attribute error " +
        "(deny) while a null-selecting filter would return those documents. Send NULL fields " +
        'as explicit nulls and use "explicit", or keep this shape out of the policy.',
    );
  }
};

/** Inside a collection predicate, only the iteration variable can be expressed per element. */
export const assertCollectionScopedReference = (
  reference: string,
  ctx: TranslateContext,
): void => {
  if (
    ctx.scope.kind === "collection" &&
    reference !== ctx.scope.variable &&
    !reference.startsWith(`${ctx.scope.variable}.`)
  ) {
    throw new Error(
      `Outer reference ${reference} inside a collection predicate is unsupported`,
    );
  }
};
