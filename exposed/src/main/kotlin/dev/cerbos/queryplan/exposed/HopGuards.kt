package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Op

/**
 * Keeps an absent to-one row apart from a null value. A scalar read through to-one hops is SQL NULL
 * both when the row is absent (a CEL missing-path error) and when its column is NULL; any reading that
 * treats NULL as a value is made UNKNOWN on a row where a hop is absent.
 */
internal class HopGuards(private val translation: Translation) {
    private val frames = ArrayDeque<MutableList<Resolution.Scalar>>()

    fun note(resolution: Resolution) {
        if (resolution is Resolution.Scalar && translation.leaf.isExplicitNull(resolution)) require(resolution)
    }

    fun require(scalar: Resolution.Scalar) {
        if (scalar.hops.isEmpty()) return
        val frame = frames.lastOrNull() ?: throw Refusals.internal("${scalar.variable} was read outside an operator")
        if (frame.none { it === scalar }) frame += scalar
    }

    fun guarded(build: () -> Op<Boolean>): Op<Boolean> {
        val frame = mutableListOf<Resolution.Scalar>()
        frames.addLast(frame)
        val built = try {
            build()
        } finally {
            frames.removeLast()
        }
        if (frame.isEmpty()) return built
        val absent = frame.map { TriLogic.not(translation.subqueries.hopsExist(it.owner, it.hops)) }
        return TriLogic.baseUnlessUnknown(built, TriLogic.or(absent))
    }
}
