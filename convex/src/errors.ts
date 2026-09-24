/**
 * The error `queryPlanToConvex` raises when a plan holds a shape this adapter cannot translate
 * without returning documents the PDP would deny: an operator it does not know, a regular
 * expression outside the subset its post-filter answers, a division whose result it cannot
 * reproduce, a list-valued condition, a null operand under `nullAttributeRepresentation:
 * "omitted"`, or a plan that is malformed.
 *
 * It extends `Error`, so existing `catch` blocks keep working. Two failures are deliberately NOT
 * refusals and stay a plain `Error`: a reference with no mapper entry (a bug in the caller's
 * mapping, not a limitation of the adapter) and a plan that needs the post-filter when the caller
 * has not passed `allowPostFilter: true` (a caller opt-in, not a shape the adapter cannot express).
 */
export class UnsupportedQueryPlanError extends Error {
  override readonly name = "UnsupportedQueryPlanError";
}
