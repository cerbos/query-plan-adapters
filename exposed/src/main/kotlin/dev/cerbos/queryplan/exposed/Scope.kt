package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Expression

/**
 * Where a plan variable is looked up: the root resource, or the element of an enclosing lambda.
 *
 * [resolve] is TOTAL: every variable lands in one of [Resolution]'s two arms or throws naming why.
 * There is no null return and no second classifier.
 */
internal sealed interface Scope {
    fun resolve(variable: String): Resolution

    /** [resolve], for a position that needs one value per row. */
    fun scalar(variable: String): Resolution.Scalar = when (val resolution = resolve(variable)) {
        is Resolution.Scalar -> resolution
        is Resolution.Collection -> throw Refusals.unmapped(
            "$variable is mapped as a relation, but this position needs a scalar column",
        )
    }

    /** [resolve], for a position that needs a collection. */
    fun collection(variable: String): Resolution.Collection = when (val resolution = resolve(variable)) {
        is Resolution.Collection -> resolution
        is Resolution.Scalar -> throw Refusals.unmapped(
            "$variable is mapped as a column, but this position needs a relation",
        )
    }
}

internal sealed interface Resolution {
    /**
     * One value per row of the resolving scope.
     *
     * [expression] is what goes into SQL: the column itself, the same column read through a
     * subquery alias, or a correlated scalar subquery when the variable reaches through a to-one
     * relation (NULL when no row correlates, which is already CEL's missing-attribute error).
     *
     * [column] is the DECLARED column whatever [expression] is. It carries the type knowledge the
     * plan does not: `column.columnType` says boolean, text, numeric or temporal.
     */
    class Scalar(
        val variable: String,
        val expression: Expression<*>,
        val column: Column<*>,
        val field: AttributeMapping.Field,
    ) : Resolution

    /**
     * A collection reached through one or more relations, outermost first; the last hop's table is
     * the element table.
     *
     * [owner] is the scope that OWNS the first hop, which is not always the scope that resolved
     * the variable: `request.resource.attr.tags` inside a `categories` lambda belongs to the root.
     * Anchoring the subquery to the wrong scope still builds and still returns rows, the wrong
     * ones, so the owner travels with the resolution rather than being re-derived.
     */
    class Collection(
        val variable: String,
        val owner: Scope,
        val hops: List<AttributeMapping.Relation>,
    ) : Resolution {
        init {
            require(hops.isNotEmpty()) { "a collection resolution needs at least one hop" }
        }

        val tail: AttributeMapping.Relation get() = hops.last()
    }
}

/**
 * The root resource. OWNED BY THE RELATION SIDE: this minimal form resolves an exactly-mapped
 * reference only. Still to come: longest-prefix matching with the remainder walked through nested
 * `fields`, to-one paths lowered to correlated scalar subqueries, and the lambda scope.
 */
internal class RootScope(private val translation: Translation) : Scope {
    override fun resolve(variable: String): Resolution =
        when (val mapping = translation.options.mapping.resolve(variable)) {
            null -> throw Refusals.unknownAttribute(variable)
            is AttributeMapping.Field -> Resolution.Scalar(variable, mapping.column, mapping.column, mapping)
            is AttributeMapping.Relation -> when (mapping.cardinality) {
                AttributeMapping.Relation.Cardinality.MANY -> Resolution.Collection(variable, this, listOf(mapping))
                AttributeMapping.Relation.Cardinality.ONE ->
                    throw Refusals.notYetImplemented("a to-one relation used as a value ($variable)")
            }
        }
}
