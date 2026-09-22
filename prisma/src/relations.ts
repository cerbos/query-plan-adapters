// Nesting a filter through the relations a reference crosses, and the hop-existence guards that
// keep an absent to-one parent denied under negation.

import type { PlanExpressionOperand } from "@cerbos/core";

import type { PrismaFilter } from "./index";
import { resolveFieldReference } from "./mapping";
import type { RelationConfig, TranslationContext } from "./mapping";
import { isNamedOperand, isOperatorOperand } from "./plan";

/** The Prisma operator that reaches into a relation for an existence test. */
export function relationOperator(relation: RelationConfig): string {
  return relation.type === "one" ? "is" : "some";
}

/**
 * One relation-scoped filter (`{ tags: { some: … } }`), narrowed by the store-side predicate the
 * caller declared on the mapping.
 *
 * Every nested relation filter the adapter emits goes through here, so a declared
 * `subqueryFilter` reaches the collection macros, the membership shapes, the emptiness checks and
 * the hop-existence guard alike. Adding a new relation shape means routing it through here, or
 * the declaration silently stops applying to it.
 *
 * `every` is the one operator that cannot simply absorb the predicate. `{ every: AND(W, P) }`
 * would REQUIRE every record to satisfy W, which is the opposite of "ignore the records W hides",
 * so it is rewritten to `{ none: AND(W, NOT P) }` — no visible record violates P. Both agree with
 * `every` when nothing is declared, and both stay vacuously true over an empty relation.
 */
export function relationFilter(
  relation: RelationConfig,
  operator: string,
  inner: PrismaFilter
): PrismaFilter {
  const declared = relation.subqueryFilter;
  if (declared === undefined) {
    return { [relation.name]: { [operator]: inner } };
  }
  const isEmpty = Object.keys(inner).length === 0;
  if (operator === "every") {
    return isEmpty
      ? { [relation.name]: { every: inner } }
      : { [relation.name]: { none: { AND: [declared, { NOT: inner }] } } };
  }
  return {
    [relation.name]: {
      [operator]: isEmpty ? declared : { AND: [declared, inner] },
    },
  };
}

/**
 * `filter` nested through every relation in `relations`, outermost first, each reached with its
 * existence operator (`is` for to-one, `some` for to-many). No relations leaves it unchanged.
 */
export function wrapInRelations(
  relations: RelationConfig[] | undefined,
  filter: PrismaFilter
): PrismaFilter {
  let current = filter;
  for (const relation of [...(relations ?? [])].reverse()) {
    current = relationFilter(relation, relationOperator(relation), current);
  }
  return current;
}

/**
 * "Every one of these relations exists", as a nested Prisma filter with an empty innermost
 * predicate — `{ parent: { is: { inner: { is: {} } } } }`.
 */
function buildHopsExistFilter(
  hops: RelationConfig[]
): PrismaFilter | undefined {
  return hops.length === 0 ? undefined : wrapInRelations(hops, {});
}

/**
 * "Every intermediate hop of a dotted path exists", as a Prisma filter.
 *
 * CEL cannot dot through a list, so each intermediate segment of `a.b.c` is a to-ONE
 * parent: absent, the caller sends no attribute and CEL raises a missing-path error, which
 * denies. `NOT { categories: { some: { subCategories: { some: P } } } }` is TRUE for a row
 * with no category at all, so the negation returns rows the PDP denies — the parent must be
 * required separately (cerbos/query-plan-adapters#309).
 *
 * Returns undefined when the reference has no intermediate hop, so a direct relation keeps
 * its empty-collection semantics (`!tags.exists(...)` over zero tags is still TRUE).
 */
export function buildLeadingHopsExistFilter(
  head: RelationConfig,
  restRelations: RelationConfig[]
): PrismaFilter | undefined {
  if (restRelations.length === 0) {
    return undefined;
  }
  // The last entry is the collection being iterated; everything before it is a hop.
  return buildHopsExistFilter([head, ...restRelations.slice(0, -1)]);
}

/**
 * "Every to-one hop this expression dots through exists", as a Prisma filter, or undefined
 * when it dots through none.
 *
 * Prisma has no UNKNOWN: a chained relation filter is exact under the POSITIVE polarity —
 * `categories: { some: { subCategories: { some: P } } }` cannot hold without the category —
 * but `NOT` inverts the hop requirement along with the predicate, so every operator built on
 * a chain readmits the parentless rows once negated. #309 fixed that inside the collection
 * macros, which left the sibling operators reached through the same chain unguarded:
 * membership and `hasIntersection` (cerbos/query-plan-adapters#315), and the negated count
 * spelling `!(size(chain) > 0)` (#316), all of which the macros' guard never saw.
 *
 * Requiring the hops OUTSIDE the negation is what fixes all of them at once, and it is
 * unconditionally faithful to CEL rather than a per-operator patch: a missing hop is a
 * missing-path error, which denies under BOTH polarities, so the requirement never depends
 * on which operator sits above the chain.
 *
 * Lambda bodies are deliberately not walked — their references resolve against a scoped
 * mapper, and a negated macro carries its own guard through the negated collection operators.
 */
function buildExpressionHopsExistFilter(
  operand: PlanExpressionOperand,
  context: TranslationContext
): PrismaFilter | undefined {
  const hops: PrismaFilter[] = [];
  const seen = new Set<string>();

  const visit = (expr: PlanExpressionOperand): void => {
    if (isNamedOperand(expr)) {
      if (seen.has(expr.name)) {
        return;
      }
      seen.add(expr.name);
      const { relations } = resolveFieldReference(expr.name, context);
      if (!relations || relations.length === 0) {
        return;
      }
      const last = relations[relations.length - 1]!;
      // Every to-ONE relation on the path is a hop that must EXIST — including a TRAILING one,
      // which is the case a chain of to-many relations never produces. `parent.aBool` resolves
      // to a single to-one relation, and `NOT { parent: { is: { aBool: true } } }` is TRUE for
      // a row with no parent at all, so the negation returns rows the PDP denies. A trailing
      // to-MANY relation is the collection being iterated rather than a hop, and keeps its
      // empty-collection semantics (`!tags.exists(...)` over zero tags is still TRUE).
      const hopFilter = buildHopsExistFilter(
        last.type === "one" ? relations : relations.slice(0, -1)
      );
      if (hopFilter !== undefined) {
        hops.push(hopFilter);
      }
      return;
    }
    if (!isOperatorOperand(expr) || expr.operator === "lambda") {
      return;
    }
    expr.operands.forEach(visit);
  };

  visit(operand);

  if (hops.length === 0) {
    return undefined;
  }
  return hops.length === 1 ? hops[0]! : { AND: hops };
}

/**
 * `{ NOT: filter }`, with every to-one hop `operand` dots through required OUTSIDE the
 * negation so an absent parent stays denied (see buildExpressionHopsExistFilter).
 */
export function negateRequiringHops(
  operand: PlanExpressionOperand,
  filter: PrismaFilter,
  context: TranslationContext
): PrismaFilter {
  const hops = buildExpressionHopsExistFilter(operand, context);
  return hops === undefined ? { NOT: filter } : { AND: [hops, { NOT: filter }] };
}

/**
 * Whether `expr` dots through at least one intermediate to-one hop, so negating it needs the
 * hop requirement of buildExpressionHopsExistFilter.
 */
export function referencesChainedRelation(
  expr: PlanExpressionOperand,
  context: TranslationContext
): boolean {
  if (isNamedOperand(expr)) {
    const { relations } = resolveFieldReference(expr.name, context);
    return relations !== undefined && relations.length > 1;
  }
  if (!isOperatorOperand(expr) || expr.operator === "lambda") {
    return false;
  }
  return expr.operands.some((operand) =>
    referencesChainedRelation(operand, context)
  );
}
