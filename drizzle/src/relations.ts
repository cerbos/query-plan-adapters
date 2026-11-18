import {
  aliasedTableColumn,
  and,
  Column,
  eq,
  exists,
  getTableName,
  is,
  SQL,
  sql,
} from "drizzle-orm";
import type { AnyColumn, SQLChunk, Table } from "drizzle-orm";

import { columnTable } from "./compat";
import { FALSE_CONDITION, TRUE_CONDITION } from "./predicates";
import type { BuildFilterOptions, RelationMapping } from "./types";

/**
 * Correlated subqueries over relation chains. Every subquery the adapter builds over a relation
 * goes through `relationCorrelation` or `wrapWithRelations`, so a declared `subqueryFilter`
 * reaches the EXISTS forms, the COUNT forms and the hop-existence guard alike. Adding a new
 * subquery shape means routing it through one of the two, or the declaration silently stops
 * applying to it.
 */

const ORIGINAL_NAME = Symbol.for("drizzle:OriginalName");
const SCHEMA = Symbol.for("drizzle:Schema");

/** The table's own name, also for a drizzle `alias()` of it. */
export const resolveTableName = (table: Table, reference: string): string => {
  const original: unknown = Reflect.get(table, ORIGINAL_NAME);

  if (typeof original === "string") return original;

  try {
    return getTableName(table);
  } catch {
    throw new Error(`Unable to resolve table name for relation: ${reference}`);
  }
};

const sameTable = (a: Table, b: Table): boolean =>
  Reflect.get(a, ORIGINAL_NAME) === Reflect.get(b, ORIGINAL_NAME) &&
  Reflect.get(a, SCHEMA) === Reflect.get(b, SCHEMA);

/** `column` read from the row `alias` names, when it is a column of `table`. */
export const rebindColumn = (column: AnyColumn, table: Table, alias: string): AnyColumn =>
  sameTable(columnTable(column), table) ? aliasedTableColumn(column, alias) : column;

const rebindChunk = (chunk: SQLChunk, table: Table, alias: string): SQLChunk => {
  if (Array.isArray(chunk)) return chunk.map((item: SQLChunk) => rebindChunk(item, table, alias));
  if (is(chunk, Column)) return rebindColumn(chunk, table, alias);
  if (is(chunk, SQL)) return rebindColumns(chunk, table, alias);

  return chunk;
};

/** `query` with every column of `table` read from the row `alias` names. */
export const rebindColumns = (query: SQL, table: Table, alias: string): SQL =>
  new SQL(query.queryChunks.map((chunk) => rebindChunk(chunk, table, alias)));

/**
 * Each relation reached from the root ranges under its own alias, named by its path, so two
 * relations into one table, and a relation from a table to itself, each read their own row
 * rather than the innermost subquery's.
 */
const aliases = new WeakMap<RelationMapping, string>();
const bound = new WeakMap<RelationMapping, Map<RelationMapping | undefined, RelationMapping>>();

export const relationAlias = (relation: RelationMapping): string | undefined =>
  aliases.get(relation);

const MAX_ALIAS = 60;

const aliasFor = (source: RelationMapping | undefined, segment: string): string => {
  const name = `${(source && aliases.get(source)) ?? "cerbos"}_${segment.replace(/[^A-Za-z0-9_]/g, "_")}`;

  if (name.length <= MAX_ALIAS) return name;

  let hash = 0x811c9dc5;

  for (const char of name) hash = Math.imul(hash ^ char.charCodeAt(0), 0x01000193) >>> 0;

  return `${name.slice(0, MAX_ALIAS - 9)}_${hash.toString(16).padStart(8, "0")}`;
};

/** `relation` reached through `source` (the root when undefined) as path `segment`, aliased. */
export const bindRelation = (
  relation: RelationMapping,
  source: RelationMapping | undefined,
  segment: string,
): RelationMapping => {
  const bySource = bound.get(relation) ?? new Map<RelationMapping | undefined, RelationMapping>();

  bound.set(relation, bySource);

  const cached = bySource.get(source);

  if (cached !== undefined) return cached;

  const alias = aliasFor(source, segment);
  const sourceAlias = source && aliases.get(source);

  const binding: RelationMapping = {
    ...relation,
    sourceColumn:
      source === undefined || sourceAlias === undefined
        ? relation.sourceColumn
        : rebindColumn(relation.sourceColumn, source.table, sourceAlias),
    targetColumn: aliasedTableColumn(relation.targetColumn, alias),
  };

  if (relation.position !== undefined) binding.position = aliasedTableColumn(relation.position, alias);

  if (relation.subqueryFilter !== undefined) {
    binding.subqueryFilter = rebindColumns(relation.subqueryFilter, relation.table, alias);
  }

  aliases.set(binding, alias);
  bySource.set(source, binding);

  return binding;
};

/**
 * The correlation predicate of a subquery over `relation`: the join, narrowed by whatever
 * store-side predicate the caller declared on the mapping.
 */
export const relationCorrelation = (
  relation: RelationMapping,
  alias?: string,
): SQL => {
  const targetColumn =
    alias === undefined
      ? relation.targetColumn
      : aliasedTableColumn(relation.targetColumn, alias);

  const joinCondition = eq(targetColumn, relation.sourceColumn);

  if (relation.subqueryFilter === undefined) {
    return joinCondition;
  }

  return and(joinCondition, relation.subqueryFilter) ?? joinCondition;
};

/**
 * The FROM item of a subquery over `relation`: the table under the relation's own alias, or
 * under `alias` when an enclosing subquery already ranges over the same path (see
 * `CollectionScope.alias`).
 */
export const relationSource = (
  relation: RelationMapping,
  reference: string,
  alias: string | undefined = relationAlias(relation),
): SQL => {
  const table = sql.identifier(resolveTableName(relation.table, reference));

  return alias === undefined ? sql`${table}` : sql`${table} ${sql.identifier(alias)}`;
};

/**
 * Nest `filter` inside one correlated `EXISTS` per relation, the first relation outermost.
 * Relations in `skipRelations` are already correlated by an enclosing subquery and are passed
 * through; a relation in `aliases` takes that alias. An empty chain returns `filter` itself.
 */
export const wrapWithRelations = (
  relations: RelationMapping[],
  filter: SQL,
  reference: string,
  options?: {
    skipRelations?: Set<RelationMapping>;
    aliases?: ReadonlyMap<RelationMapping, string>;
  },
): SQL =>
  relations
    .slice()
    .reverse()
    .reduce((currentFilter, relation) => {
      if (options?.skipRelations?.has(relation)) {
        return currentFilter;
      }

      // The caller-declared store-side predicate goes in alongside the join, not around the
      // EXISTS, so it narrows the rows the subquery examines rather than the rows it returns.
      // That is what makes it correct under negation too: `all` compiles to `NOT EXISTS (… AND
      // NOT P)`, and restricting the scan is what turns that into "every VISIBLE row satisfies
      // P" instead of "every row in the table does".
      const alias = options?.aliases?.get(relation);
      const condition = and(relationCorrelation(relation, alias), currentFilter);

      return exists(
        sql`(select 1 from ${relationSource(relation, reference, alias)} where ${condition})`,
      );
    }, filter);

/**
 * The WHERE clause of a subquery over `primary`, reached THROUGH every leading hop rather than
 * straight off the root.
 */
export const chainCorrelation = (
  primary: RelationMapping,
  leading: RelationMapping[],
  reference: string,
  options: BuildFilterOptions,
  alias?: string,
): SQL =>
  wrapWithRelations(
    leading,
    relationCorrelation(primary, alias),
    reference,
    options,
  );

/**
 * Make `inner` UNKNOWN (SQL NULL) unless every intermediate to-one hop of a dotted path
 * exists.
 *
 * CEL cannot dot through a list, so every intermediate segment of `a.b.c` is a to-ONE
 * parent: when it is absent the caller sends no attribute at all and CEL raises a
 * missing-path error, which denies. A join chain rooted at the resource row cannot see
 * that — an absent parent and a childless parent both produce zero correlated rows — so
 * `all` goes vacuously TRUE, `!exists` goes TRUE and the count goes 0, each returning rows
 * the PDP denies (cerbos/query-plan-adapters#309).
 *
 * The CASE has no ELSE on purpose: a missing hop yields NULL, and `NOT NULL` is still
 * NULL, so the row stays excluded under BOTH polarities. Hops listed in `skipRelations`
 * are already correlated by an enclosing subquery and exist there by construction, so they
 * must not be re-required off the root.
 */
export const requireLeadingHops = (
  leadingRelations: RelationMapping[],
  inner: SQL,
  reference: string,
  options: BuildFilterOptions,
): SQL => {
  const required = leadingRelations.filter(
    (relation) => !options.skipRelations?.has(relation),
  );

  if (required.length === 0) {
    return inner;
  }

  const hopsExist = wrapWithRelations(required, TRUE_CONDITION, reference);

  return sql`(case when ${hopsExist} then ${inner} end)`;
};

/**
 * Every to-ONE relation on the path is a hop that must EXIST — including a TRAILING one,
 * which a chain of to-many relations never produces. `parent.aBool` ends AT its hop, and a
 * bare `NOT EXISTS` over it is TRUE for a row with no parent, so the negation returns rows
 * the PDP denies (cerbos/query-plan-adapters#375). A trailing to-MANY relation is the
 * collection being iterated rather than a hop, and keeps its empty-collection semantics:
 * `!exists` over zero rows is TRUE in CEL as well.
 */
const requiredRelationHops = (
  relations: RelationMapping[],
): RelationMapping[] =>
  relations[relations.length - 1]?.type === "one"
    ? relations
    : relations.slice(0, -1);

/**
 * Evaluate `filter` over a whole relation chain reached from the root: the correlated
 * EXISTS of `wrapWithRelations`, made UNKNOWN rather than FALSE when an intermediate to-one
 * hop is absent. An empty chain returns `filter` itself.
 *
 * Every operator that reaches a collection through a dotted path must go through here
 * rather than calling `wrapWithRelations` directly. The collection macros used to carry the
 * hop guard themselves, which left the sibling operators built on the same chain — plain
 * membership and `hasIntersection` — unguarded: a bare `NOT EXISTS` over an absent parent is
 * TRUE, so `!("x" in R.attr.parent.names)` returned every parentless row
 * (cerbos/query-plan-adapters#315). Guarding the chain construction instead of each operator
 * is what ent and pgx already do, and is why they never had the hole.
 */
export const wrapRelationChain = (
  relations: RelationMapping[],
  filter: SQL,
  reference: string,
  options: BuildFilterOptions,
): SQL => {
  const wrapped = relations.filter((relation) => !options.skipRelations?.has(relation));

  if (wrapped.length > 0 && wrapped.every((relation) => relation.type === "one")) {
    return wrapToOneChain(relations, filter, reference, options);
  }

  return requireLeadingHops(
    requiredRelationHops(relations),
    wrapWithRelations(relations, filter, reference, options),
    reference,
    options,
  );
};

/**
 * `filter` read through a chain of to-ONE hops, keeping all three of its values.
 *
 * `EXISTS (… WHERE filter)` is two-valued: a related row whose filter is UNKNOWN is simply not
 * selected, so the EXISTS is FALSE and a `NOT` over it is TRUE. That is wrong exactly where the
 * leaf is UNKNOWN on a present row — a NULL leaf column on the omitted convention, which CEL
 * reads as a missing attribute and denies under both polarities. `!(parent.aOptionalString ==
 * null)` returned every row whose parent had a NULL `aOptionalString`
 * (cerbos/query-plan-adapters#553).
 *
 * So the chain asks twice: TRUE when a related row satisfies `filter`, FALSE when one refutes it,
 * and otherwise NULL. The otherwise covers both a missing hop at any depth (no related row, so
 * neither EXISTS holds) and a present row whose leaf is UNKNOWN, which is the missing-path error
 * CEL raises in either case. That subsumes the leading-hop guard of `requireLeadingHops`.
 */
const wrapToOneChain = (
  relations: RelationMapping[],
  filter: SQL,
  reference: string,
  options: BuildFilterOptions,
): SQL => {
  const holds = wrapWithRelations(relations, filter, reference, options);
  const refuted = wrapWithRelations(relations, sql`not (${filter})`, reference, options);

  return sql`(case when ${holds} then ${TRUE_CONDITION} when ${refuted} then ${FALSE_CONDITION} end)`;
};

/**
 * Wrap a filter with two operands' relation chains (deduplicated by identity) so both
 * sides' columns are in scope. skipRelations (enclosing lambda scopes) are honoured.
 *
 * The hops the two chains share go outermost, then the secondary chain's own hops, then the
 * primary's, so every hop is correlated inside the subquery of the hop it hangs off.
 * `parent.aBool == parent.inner.aBool` used to nest `inner` OUTSIDE `parent`, correlating it to
 * a table not yet in scope, which every store rejected.
 *
 * A chain of to-ONE hops goes through `wrapToOneChain`, as in `wrapRelationChain`: a bare
 * `NOT EXISTS (parent WHERE col LIKE ...)` is TRUE for a parent whose column is NULL, which CEL
 * reads as a missing attribute and denies, so `!parent.aOptionalString.endsWith("ror")` returned
 * those rows.
 */
export const wrapCombinedRelations = (
  filter: SQL,
  primary: RelationMapping[],
  secondary: RelationMapping[],
  reference: string,
  options: BuildFilterOptions,
): SQL => {
  let shared = 0;

  while (
    shared < primary.length &&
    shared < secondary.length &&
    primary[shared] === secondary[shared]
  ) {
    shared++;
  }

  const seen = new Set(primary);

  const chain = [
    ...primary.slice(0, shared),
    ...secondary.slice(shared).filter((relation) => !seen.has(relation)),
    ...primary.slice(shared),
  ];

  const wrapped = chain.filter((relation) => !options.skipRelations?.has(relation));

  if (wrapped.length > 0 && wrapped.every((relation) => relation.type === "one")) {
    return wrapToOneChain(chain, filter, reference, options);
  }

  return requireLeadingHops(
    requiredRelationHops(secondary),
    requireLeadingHops(
      requiredRelationHops(primary),
      wrapWithRelations(chain, filter, reference, options),
      reference,
      options,
    ),
    reference,
    options,
  );
};
