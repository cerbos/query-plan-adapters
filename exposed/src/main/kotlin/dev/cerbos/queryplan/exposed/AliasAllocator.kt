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

    fun <T : Table> allocate(table: T): Alias<T> = table.alias("$PREFIX${++next}")

    companion object {
        const val PREFIX = "cerbos_"
    }
}
