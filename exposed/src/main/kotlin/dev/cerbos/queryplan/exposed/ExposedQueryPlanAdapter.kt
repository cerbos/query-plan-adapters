package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse
import dev.cerbos.sdk.PlanResourcesResult

/**
 * Translates a Cerbos `PlanResources` response into an Exposed predicate.
 *
 * ```kotlin
 * val filter = ExposedQueryPlanAdapter.toFilter(plan, Options.of(mapping))
 * Documents.selectAll().where { filter.toOp() and (Documents.archived eq false) }
 * ```
 *
 * The invariant: **a shape this adapter cannot express raises, it never emits a filter.** A wrong
 * filter returns rows the PDP denies; a refusal is a bug report. See [UnsupportedPlanShapeException],
 * [UnmappedAttributeException] and [MalformedPlanException].
 *
 * The predicate is a value: build it once per request and hand it to `where { }`, `andWhere { }`
 * or a DAO `find { }`. It renders inside the caller's transaction, for that transaction's dialect.
 */
public object ExposedQueryPlanAdapter {

    /** Translates the result the Cerbos SDK client returns. */
    @JvmStatic
    public fun toFilter(plan: PlanResourcesResult, options: Options): QueryPlanFilter {
        if (plan.isAlwaysAllowed) return QueryPlanFilter.AlwaysAllowed
        if (plan.isAlwaysDenied) return QueryPlanFilter.AlwaysDenied
        val condition = plan.condition.orElseThrow { Refusals.malformed("Conditional plan has no condition") }
        return conditional(condition, options)
    }

    /** Translates the raw protobuf response, for callers that did not go through the SDK client. */
    @JvmStatic
    public fun toFilter(response: PlanResourcesResponse, options: Options): QueryPlanFilter = toFilter(response.filter, options)

    /** Translates the filter of a plan response. */
    @JvmStatic
    public fun toFilter(filter: PlanResourcesFilter, options: Options): QueryPlanFilter = when (filter.kind) {
        PlanResourcesFilter.Kind.KIND_ALWAYS_ALLOWED -> QueryPlanFilter.AlwaysAllowed
        PlanResourcesFilter.Kind.KIND_ALWAYS_DENIED -> QueryPlanFilter.AlwaysDenied
        PlanResourcesFilter.Kind.KIND_CONDITIONAL -> {
            val condition = filter.condition
            if (condition.nodeCase == Operand.NodeCase.NODE_NOT_SET) {
                throw Refusals.malformed("Conditional plan has no condition")
            }
            conditional(condition, options)
        }
        else -> throw Refusals.malformed("Unknown filter kind: ${filter.kind}")
    }

    private fun conditional(condition: Operand, options: Options): QueryPlanFilter {
        NullOperandScan.assertTranslatable(condition, options)
        val translation = Translation(options)
        return QueryPlanFilter.Conditional(translation.walker.traverse(condition, translation.rootScope()))
    }
}
