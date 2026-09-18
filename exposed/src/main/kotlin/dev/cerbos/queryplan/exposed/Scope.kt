package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Table

/**
 * Where a plan variable is looked up: the root resource, or the element of an enclosing lambda.
 *
 * [resolve] is TOTAL: every variable lands in one of [Resolution]'s two arms or throws naming why.
 * There is no null return and no second classifier.
 *
 * Three rules live here and only here:
 *  - **longest-prefix chain walking** — a dotted variable is matched against the longest MAPPED
 *    prefix and the remainder walked through nested [AttributeMapping.Relation.fields];
 *  - **lambda delegation** — a [LambdaScope] resolves its own variable against the element alias
 *    and delegates everything else OUTWARD, keeping the outer scope as the owner;
 *  - **owner anchoring** — a [Resolution.Collection] carries the scope that owns its first hop,
 *    which is where a subquery over it must correlate.
 */
internal sealed interface Scope {
    fun resolve(variable: String): Resolution

    /**
     * A column of THIS scope's own table, as SQL reads it here: bare at the root, through the
     * element alias inside a lambda.
     *
     * [AttributeMapping.Relation.from] is a column on the PARENT scope's table, so a subquery over
     * a relation declared inside a lambda has to correlate against the alias rather than the bare
     * table. Reading it bare still builds and still returns rows — the wrong ones, from whichever
     * row of the outer table the planner's optimiser happens to pair it with.
     */
    fun read(column: Column<*>): Column<*>

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

        /**
         * The hops that are merely TRAVERSED rather than iterated. Each is a to-one parent on the
         * check side — CEL cannot dot through a list — so an absent one is a missing-path error,
         * and a subquery rooted at the resource row cannot tell it apart from a childless parent.
         */
        val leadingHops: List<AttributeMapping.Relation> get() = hops.subList(0, hops.size - 1)
    }
}

/** The root resource: mapped columns are read bare, and relations correlate against its own row. */
internal class RootScope(private val translation: Translation) : Scope {
    private val resolved = HashMap<String, Resolution>()

    override fun read(column: Column<*>): Column<*> = column

    override fun resolve(variable: String): Resolution = resolved.getOrPut(variable) { classify(variable) }

    private fun classify(variable: String): Resolution {
        when (val direct = translation.options.mapping.resolve(variable)) {
            is AttributeMapping.Field -> return Resolution.Scalar(variable, read(direct.column), direct.column, direct)
            is AttributeMapping.Relation ->
                return ChainWalk(listOf(direct), null).resolveAt(translation, this, variable)
            null -> Unit
        }
        // Not mapped under its whole name, so it is a dotted suffix off a mapped relation. The
        // resolver may be a FUNCTION rather than a table, so the prefixes are probed one by one,
        // longest first: the mapping cannot be enumerated and asked which keys are prefixes.
        val parts = variable.split('.')
        for (length in parts.size - 1 downTo 1) {
            val prefix = parts.subList(0, length).joinToString(".")
            val head = translation.options.mapping.resolve(prefix) as? AttributeMapping.Relation ?: continue
            val walked = ChainWalk.from(head, parts.subList(length, parts.size)) ?: continue
            // The mapped prefix is itself the first hop: `from` walks the nested fields BEYOND the
            // relation it starts at, so dropping the head here would correlate the remainder
            // straight off the root and skip the intermediate table entirely.
            return ChainWalk(listOf(head) + walked.hops, walked.leaf).resolveAt(translation, this, variable)
        }
        throw Refusals.unknownAttribute(variable)
    }
}

/**
 * Inside a collection lambda. The lambda's own variable resolves against the element alias;
 * everything else delegates OUTWARD, which is what keeps `request.resource.attr.tags` inside a
 * `categories` lambda owned by the root and therefore correlated against the root's row.
 */
internal class LambdaScope(
    private val translation: Translation,
    private val alias: Alias<Table>,
    private val relation: AttributeMapping.Relation,
    private val lambdaVariable: String,
    private val outer: Scope,
) : Scope {
    private val resolved = HashMap<String, Resolution>()

    override fun read(column: Column<*>): Column<*> {
        if (column.table !== alias.delegate) {
            throw Refusals.internal(
                "Column ${column.table.tableName}.${column.name} is not a column of " +
                    "${alias.delegate.tableName}, which this lambda iterates",
            )
        }
        return alias[column]
    }

    override fun resolve(variable: String): Resolution = resolved.getOrPut(variable) { classify(variable) }

    private fun classify(variable: String): Resolution {
        if (variable == lambdaVariable) {
            val element = relation.element ?: throw RelationRefusals.noElementColumn(variable, relation)
            return Resolution.Scalar(variable, read(element.column), element.column, element)
        }
        val prefix = "$lambdaVariable."
        if (!variable.startsWith(prefix)) {
            return outer.resolve(variable)
        }
        val walked = ChainWalk.from(relation, variable.substring(prefix.length).split('.'))
            ?: throw Refusals.unknownAttribute(variable)
        if (walked.hops.isEmpty()) {
            val leaf = walked.leaf ?: throw Refusals.internal("A member walk ended at neither a column nor a relation")
            return Resolution.Scalar(variable, read(leaf.column), leaf.column, leaf)
        }
        return walked.resolveAt(translation, this, variable)
    }
}

/**
 * A dotted reference walked onto storage: the relation [hops] it passes through, and the [leaf]
 * column it ends at when it ends at one rather than at a relation.
 *
 * [hops] is relative to the scope that walked it — [ChainWalk.from] never includes the relation it
 * started from, and [RootScope] prepends the mapped head itself.
 */
internal class ChainWalk(
    val hops: List<AttributeMapping.Relation>,
    val leaf: AttributeMapping.Field?,
) {
    /**
     * The resolution this walk denotes, anchored at [owner].
     *
     * A path that reaches a to-many relation is a COLLECTION; one that ends at a column having
     * passed only through to-one relations is a scalar read through a correlated subquery. The
     * two are not interchangeable: an aggregate over a to-one hop and a scalar projection through
     * it differ exactly where a corpus action discriminates them.
     */
    fun resolveAt(translation: Translation, owner: Scope, variable: String): Resolution {
        if (leaf == null) {
            val tail = hops.last()
            if (tail.cardinality == AttributeMapping.Relation.Cardinality.MANY) {
                return Resolution.Collection(variable, owner, hops)
            }
            // A to-one relation named as a value denotes its element column, the same way the bare
            // lambda variable of a scalar collection does.
            val element = tail.element ?: throw RelationRefusals.noElementColumn(variable, tail)
            return scalarThroughHops(translation, owner, variable, element)
        }
        val plural = hops.firstOrNull { it.cardinality == AttributeMapping.Relation.Cardinality.MANY }
        if (plural != null) {
            throw RelationRefusals.memberOfToMany(variable, plural)
        }
        return scalarThroughHops(translation, owner, variable, leaf)
    }

    private fun scalarThroughHops(
        translation: Translation,
        owner: Scope,
        variable: String,
        field: AttributeMapping.Field,
    ): Resolution.Scalar = Resolution.Scalar(
        variable,
        translation.subqueries.scalarThroughHops(owner, hops, field.column),
        field.column,
        field,
    )

    companion object {
        /**
         * Walk [parts] through [start]'s nested fields, or `null` as soon as a segment is unmapped
         * or a non-final segment is a column. The returned hops do NOT include [start].
         */
        fun from(start: AttributeMapping.Relation, parts: List<String>): ChainWalk? {
            val hops = mutableListOf<AttributeMapping.Relation>()
            var current = start
            parts.forEachIndexed { index, part ->
                when (val next = current.fields[part]) {
                    null -> return null
                    is AttributeMapping.Relation -> {
                        hops += next
                        current = next
                    }
                    is AttributeMapping.Field ->
                        return if (index == parts.lastIndex) ChainWalk(hops, next) else null
                }
            }
            return ChainWalk(hops, null)
        }
    }
}

/**
 * The column standing for one member of [relation]'s elements: the named one, or the element
 * column itself when the member is unnamed (`t` rather than `t.name`).
 *
 * A member the mapping does not declare is refused rather than guessed at a column name: guessing
 * is how a subquery ends up comparing the wrong column and returning rows the PDP denies.
 */
internal fun relationMemberColumn(
    relation: AttributeMapping.Relation,
    member: String,
    reference: String,
): Column<*> {
    if (member.isEmpty()) {
        return (relation.element ?: throw RelationRefusals.noElementColumn(reference, relation)).column
    }
    return when (val mapped = relation.fields[member]) {
        is AttributeMapping.Field -> mapped.column
        is AttributeMapping.Relation -> throw Refusals.unmapped(
            "$reference names ${relation.table.tableName}'s '$member' relation, but this position needs a column",
        )
        null -> throw Refusals.unknownAttribute(reference)
    }
}
