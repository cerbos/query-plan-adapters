/**
 * The error `queryPlanToMongoose` raises when a plan holds a shape this adapter cannot translate
 * without returning documents the PDP would deny: a CEL operator with no faithful MongoDB
 * spelling, a literal it cannot bind exactly, a construct CEL itself rejects, or a plan that is
 * malformed.
 *
 * It extends `Error`, so existing `catch` blocks keep working. Mapper misconfiguration — a
 * reference with no mapping, a macro over a reference not mapped as a collection relation — is not
 * a refusal and stays a plain `Error`: that is a bug in the caller's mapping, not a limitation of
 * the adapter.
 */
export class UnsupportedQueryPlanError extends Error {
  override readonly name = "UnsupportedQueryPlanError";
}
