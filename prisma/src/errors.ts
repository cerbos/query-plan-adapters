/**
 * The error `queryPlanToPrisma` raises when a plan holds a shape this adapter cannot translate
 * without returning rows the PDP would deny: a CEL operator with no faithful Prisma filter, a
 * literal it cannot bind exactly, a construct CEL itself rejects, or a plan that is malformed.
 *
 * It extends `Error`, so existing `catch` blocks keep working. Mapper misconfiguration — a
 * missing `model` option or `relation.model`, an empty field path — is not a refusal and stays a
 * plain `Error`: that is a bug in the caller's mapping, not a limitation of the adapter.
 */
export class UnsupportedQueryPlanError extends Error {
  override readonly name = "UnsupportedQueryPlanError";
}
