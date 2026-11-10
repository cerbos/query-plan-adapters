package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.alias

/**
 * Hands every subquery instance its own alias, `cerbos_1`, `cerbos_2`, …, in walk order: one table
 * can be entered twice in a plan, and an unaliased inner table would capture the outer correlation.
 */
internal class AliasAllocator {
    private var next = 0

    fun <T : Table> allocate(table: T): Alias<T> = table.alias("$PREFIX${++next}")

    companion object {
        const val PREFIX = "cerbos_"
    }
}
