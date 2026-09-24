/**
 * The error `queryPlanToDrizzle` raises when a plan holds a shape this adapter cannot translate
 * without returning rows the PDP would deny: a CEL operator with no faithful SQL spelling, a
 * literal it cannot bind exactly, a construct CEL itself rejects, or a plan that is malformed.
 *
 * It extends `Error`, so existing `catch` blocks keep working. Mapper misconfiguration — a
 * reference with no mapping, a column of the wrong type for its declaration — is not a refusal and
 * stays a plain `Error`: that is a bug in the caller's mapping, not a limitation of the adapter.
 */
export class UnsupportedQueryPlanError extends Error {
  override readonly name = "UnsupportedQueryPlanError";
}
