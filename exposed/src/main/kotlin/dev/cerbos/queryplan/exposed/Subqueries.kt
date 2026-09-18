package dev.cerbos.queryplan.exposed

/**
 * THE ONE CORRELATED-SUBQUERY SEAM. Every subquery this adapter emits is built here and nowhere
 * else: the fresh alias, the correlation `alias[to] = parent[from]`, and the caller's
 * `visibleWhen` predicate ANDed INTO that correlation. Routing a new relation shape around this
 * class is how `visibleWhen` silently stops applying, and the subquery then examines rows the
 * application never serialised into the resource attributes.
 *
 * Nothing in here reads a plan. It is also the only class that touches `exposed-jdbc`
 * (`alias.select(...).where(...)`), which is what keeps a future R2DBC transport a contained
 * change.
 *
 * OWNED BY THE RELATION SIDE. Stub.
 */
internal class Subqueries(@Suppress("unused") private val translation: Translation)
