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
     * The independence is the whole point, and it is why this is a second counter rather than
     * [next]. The translator unit test asserts this against the number of DISTINCT aliases the
     * rendered statement carries, and that comparison is what catches the capture bug this class
     * exists to prevent: an allocator that handed one alias per TABLE rather than per subquery would
     * still number densely from 1 and still bind each name to one table — every weaker check passes
     * — and only "asked 3 times, rendered 2" says a subquery lost its own alias. A counter derived
     * from the numbering would collapse with it and report the two as equal.
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
