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
 * Builds every correlated subquery: the alias, the correlation `alias[to] = parent[from]`, and the
 * caller's `visibleWhen` ANDed into it. The only class that touches `exposed-jdbc`.
 */
internal class Subqueries(private val translation: Translation) {

    /** One flattened scan over a chain of hops, correlated to the owning row. */
    class Chain internal constructor(
        val aliases: List<Alias<Table>>,
        val source: ColumnSet,
        /** The joins, every `visibleWhen`, and the tie back to the owning row. */
        val correlation: Op<Boolean>,
    ) {
        val tail: Alias<Table> get() = aliases.last()
    }

    /** The scan over [hops], correlated against [owner]; `visibleWhen` narrows the rows examined. */
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

    /** A two-valued `EXISTS` over [chain]. Only for chains with no leading hop; otherwise use [chainContains]. */
    fun existsOver(chain: Chain, body: Op<Boolean>? = null): Op<Boolean> {
        val where = if (body == null) chain.correlation else TriLogic.and(chain.correlation, body)
        return exists(chain.source.select(intLiteral(1)).where(where))
    }

    /** "Some element satisfies [body]", UNKNOWN when an intermediate to-one hop is absent (#315). */
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

    /** A correlated aggregate over the chain's tail alias, UNKNOWN when a leading hop is absent. */
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

    /** "Every intermediate hop has a row", or `null` when there is none to require (#309). */
    fun leadingHopsExist(collection: Resolution.Collection): Op<Boolean>? {
        val leading = collection.leadingHops
        if (leading.isEmpty()) return null
        return hopsExist(collection.owner, leading)
    }

    /** "Every hop of [hops] has a row for [owner]'s row": a two-valued `EXISTS` over the chain. */
    fun hopsExist(owner: Scope, hops: List<AttributeMapping.Relation>): Op<Boolean> = existsOver(chain(owner, hops))

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
     * One column of a to-one chain as a correlated scalar subquery. No hop guard: an uncorrelated scalar
     * subquery is already NULL. `to` needs a unique index; SQLite takes the first of several rows.
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
