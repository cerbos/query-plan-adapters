package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.alias

/**
 * Hands every subquery instance its own table alias: `cerbos_1`, `cerbos_2`, …
 *
 * Always aliased, never "only on collision": one relation can be entered twice in one plan (nested
 * lambdas over the same table, a self-reference), and an unaliased inner table would capture the
 * outer correlation. Numbered in walk order, so the emitted SQL, and with it the golden asset, is
 * deterministic. Prefixed, so an alias cannot collide with one the caller's own query declares.
 */
internal class AliasAllocator {
    private var next = 0

    /**
     * How many times [allocate] has been called: how many subquery instances this translation asked
     * for an alias, counted INDEPENDENTLY of the numbering.
     *
     * The independence is why this is a second counter rather than [next]. The translator unit test
     * asserts it against the number of DISTINCT aliases the rendered statement carries, and that
     * comparison is what catches an allocator that handed one alias per TABLE rather than per
     * subquery: such an allocator would still number densely from 1 and still bind each name to one
     * table, so every weaker check passes, and only "asked 3 times, rendered 2" says a subquery lost
     * its own alias. A counter derived from the numbering would collapse with it and report the two
     * as equal.
     *
     * What the counter CANNOT catch on its own: a regression at the CALL SITE. `Subqueries` deciding
     * to enter one table once where it entered it twice lowers [calls] in step with the rendered
     * count, and the two still agree. The bite comes from the anti-vacuity clause beside that
     * assertion, which names `p-hasintersection-map` — the one corpus shape that reads a table
     * through two independent subqueries — and pins that it still asks for 2 aliases and still
     * renders `cerbos_1` and `cerbos_2` over `adversarial_tags`. Without that clause the comparison
     * would hold vacuously for a corpus in which nothing entered a table twice.
     */
    var calls: Int = 0
        private set

    fun <T : Table> allocate(table: T): Alias<T> {
        calls++
        return table.alias("$PREFIX${++next}")
    }

    companion object {
        const val PREFIX = "cerbos_"
    }
}
