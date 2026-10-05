package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.sql.ScoreGuard
import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.ColumnSet
import org.jetbrains.exposed.v1.core.Count
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.JoinType
import org.jetbrains.exposed.v1.core.LongColumnType
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.exists
import org.jetbrains.exposed.v1.core.intLiteral
import org.jetbrains.exposed.v1.core.longLiteral
import org.jetbrains.exposed.v1.core.wrapAsExpression
import org.jetbrains.exposed.v1.jdbc.select

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
 */
internal class Subqueries(private val translation: Translation) {

    /**
     * One subquery's scan over a chain of hops: an alias per hop, the flattened join across them,
     * and the correlation that ties the first hop to the owning row.
     *
     * A multi-hop chain is FLATTENED into one scan rather than nested, so an aggregate over it —
     * a count, a macro score — ranges over exactly the tail elements an `EXISTS` over it sees.
     */
    class Chain internal constructor(
        val aliases: List<Alias<Table>>,
        val source: ColumnSet,
        /** The joins, every `visibleWhen`, and the tie back to the owning row. */
        val correlation: Op<Boolean>,
    ) {
        val tail: Alias<Table> get() = aliases.last()
    }

    /**
     * The scan over [hops], correlated against [owner]'s own row.
     *
     * Each hop's `visibleWhen` goes in beside the join rather than around the subquery, so it
     * narrows the rows the subquery EXAMINES rather than the rows it returns. That is what keeps
     * it right under `all` and under negation: a universal over a filtered association has to mean
     * "every VISIBLE element satisfies the body", not "every row in the table does".
     */
    fun chain(owner: Scope, hops: List<AttributeMapping.Relation>): Chain {
        require(hops.isNotEmpty()) { "a chain needs at least one hop" }
        val aliases = hops.map { translation.aliases.allocate(it.table) }
        val terms = mutableListOf<Op<Boolean>>(EqOp(aliases[0][hops[0].to], owner.read(hops[0].from)))
        hops[0].visibleWhen?.let { terms += it(aliases[0]) }
        var source: ColumnSet = aliases[0]
        for (index in 1 until hops.size) {
            source = source.join(
                aliases[index],
                JoinType.INNER,
                // `from` is a column on the PREVIOUS hop's table, so it is read through that hop's
                // alias: the bare column would correlate out of the subquery instead of joining.
                onColumn = aliases[index - 1][hops[index].from],
                otherColumn = aliases[index][hops[index].to],
            )
            hops[index].visibleWhen?.let { terms += it(aliases[index]) }
        }
        return Chain(aliases, source, TriLogic.and(terms))
    }

    /**
     * A two-valued `EXISTS` over an already-built scan, with [body] ANDed into its `WHERE`.
     *
     * Correct on its own only where there is no leading hop to require: `NOT EXISTS` over an
     * absent to-one parent is TRUE, which readmits every parentless row. Everything else goes
     * through [chainContains] or [chainAggregate].
     */
    fun existsOver(chain: Chain, body: Op<Boolean>? = null): Op<Boolean> {
        val where = if (body == null) chain.correlation else TriLogic.and(chain.correlation, body)
        return exists(chain.source.select(intLiteral(1)).where(where))
    }

    /**
     * "Some element of the chain satisfies [body]", as a THREE-valued predicate: UNKNOWN rather
     * than FALSE when an intermediate to-one hop is absent.
     *
     * Every operator whose whole answer is an existence test over a chain builds it here rather
     * than calling [existsOver] directly. `EXISTS` is two-valued, so the guard cannot live on the
     * predicate — counting instead and guarding the count EXPRESSION is what makes both polarities
     * inherit it (cerbos/query-plan-adapters#315).
     */
    fun chainContains(collection: Resolution.Collection, body: (Alias<Table>) -> Op<Boolean>): Op<Boolean> {
        val chain = chain(collection.owner, collection.hops)
        if (collection.leadingHops.isEmpty()) {
            return existsOver(chain, body(chain.tail))
        }
        val matches = wrapAsExpression<Long>(
            chain.source.select(countRows()).where(TriLogic.and(chain.correlation, body(chain.tail))),
        )
        return GreaterOp(requireLeadingHops(collection, matches, LongColumnType()), longLiteral(0))
    }

    /**
     * A correlated aggregate over the flattened chain, made UNKNOWN when a leading hop is absent.
     *
     * [select] receives the TAIL alias, because the lambda body an aggregate scores has to be
     * translated against the element the aggregate ranges over. It runs after the scan is built
     * and before the guard, so alias numbering follows the walk.
     */
    fun <T : Any> chainAggregate(
        collection: Resolution.Collection,
        columnType: IColumnType<T>,
        select: (Alias<Table>) -> Expression<*>,
    ): Expression<T?> {
        val chain = chain(collection.owner, collection.hops)
        val aggregate = wrapAsExpression<T>(chain.source.select(select(chain.tail)).where(chain.correlation))
        return requireLeadingHops(collection, aggregate, columnType)
    }

    /** The flattened element count of the chain, made UNKNOWN when a leading hop is absent. */
    fun chainCount(collection: Resolution.Collection): Expression<Long?> =
        chainAggregate(collection, LongColumnType()) { countRows() }

    /**
     * "Every intermediate hop of the chain has a row", or `null` when there is no hop to require.
     *
     * CEL cannot dot through a list, so each intermediate segment of `a.b.c` is a to-ONE parent:
     * absent, the caller sends no attribute at all and CEL raises a missing-path error, which
     * denies. A subquery rooted at the resource row cannot see that — an absent parent and a
     * childless parent both return nothing — so `all` reads TRUE, `!exists` reads TRUE and the
     * count reads 0, each admitting rows the PDP denies (cerbos/query-plan-adapters#309).
     */
    fun leadingHopsExist(collection: Resolution.Collection): Op<Boolean>? {
        val leading = collection.leadingHops
        if (leading.isEmpty()) return null
        return existsOver(chain(collection.owner, leading))
    }

    /** [value], made SQL NULL unless every leading hop exists. */
    fun <T> requireLeadingHops(
        collection: Resolution.Collection,
        value: Expression<T>,
        columnType: IColumnType<T & Any>,
    ): Expression<T> {
        val guard = leadingHopsExist(collection) ?: return value
        return ScoreGuard(guard, value, columnType)
    }

    /**
     * One column of a to-ONE chain, read as a correlated SCALAR subquery.
     *
     * No hop guard is wrapped around it, unlike the collection chains: a subquery with no
     * correlated row IS NULL, which is already CEL's missing-path error, and NULL propagates
     * through every comparison and negation built on top of it. The guard the collection macros
     * need exists because `EXISTS` is two-valued and collapses "absent parent" onto "no matching
     * child"; a scalar projection never makes that collapse.
     *
     * Nothing makes the database enforce the single row the mapping promises — `to` needs a unique
     * index — and a multi-row scalar subquery errors on PostgreSQL, MySQL and H2 while SQLite
     * silently takes the first row.
     */
    fun scalarThroughHops(
        owner: Scope,
        hops: List<AttributeMapping.Relation>,
        column: Column<*>,
    ): Expression<*> {
        val chain = chain(owner, hops)
        return wrapAsExpression<Any>(chain.source.select(chain.tail[column]).where(chain.correlation))
    }

    /**
     * `COUNT(1)`: the number of rows the scan reaches, never a column's non-NULL count. The chain
     * is joined with INNER joins, so a row of the scan IS a flattened tail element.
     */
    private fun countRows(): Count = Count(intLiteral(1), false)
}
