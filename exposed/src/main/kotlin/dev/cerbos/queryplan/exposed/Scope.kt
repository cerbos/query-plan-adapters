package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table

/**
 * Where a plan variable is looked up: the root resource, or the element of an enclosing lambda.
 * [path] places a variable on storage or throws; [resolve] is total. Owns longest-prefix chain
 * walking, lambda delegation outward, and anchoring a collection to the scope owning its first hop.
 */
internal sealed interface Scope {
    fun resolve(variable: String): Resolution

    /**
     * Where [variable] lands on storage, before it is read as a value. `has()` asks this rather
     * than [resolve], because a presence test over a to-one relation needs the relation itself, not
     * the element column a value reading of it would denote.
     */
    fun path(variable: String): AttributePath

    /** A column of this scope's table: bare at the root, through the element alias inside a lambda. */
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
     * One value per row. [expression] goes into SQL (possibly a correlated subquery through a to-one
     * relation); [column] is the declared column, for its type.
     */
    class Scalar(
        val variable: String,
        val expression: Expression<*>,
        val column: Column<*>,
        val field: AttributeMapping.Field,
        val owner: Scope,
        val hops: List<AttributeMapping.Relation> = emptyList(),
        /** Whether the value exists at all, where that is not [hops] existing: a list position in range. */
        val presence: (() -> Op<Boolean>)? = null,
    ) : Resolution

    /**
     * A collection reached through one or more relations, outermost first. [owner] owns the first hop,
     * which is where its subquery correlates.
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

    override fun resolve(variable: String): Resolution =
        resolved.getOrPut(variable) {
            PositionalRead.parse(variable, translation.options.mapping)?.resolve(translation, this, variable)
                ?: path(variable).resolve(translation, variable)
        }.also(translation.hopGuards::note)

    override fun path(variable: String): AttributePath {
        if (PositionalRead.parse(variable, translation.options.mapping) != null) {
            throw Refusals.unsupported("$variable reads a list element by position, which has() cannot test")
        }
        when (val direct = translation.options.mapping.resolve(variable)) {
            is AttributeMapping.Field -> return AttributePath(this, emptyList(), direct)
            is AttributeMapping.Relation -> return AttributePath(this, listOf(direct), null)
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
            return AttributePath(this, listOf(head) + walked.hops, walked.leaf)
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

    override fun resolve(variable: String): Resolution = resolved.getOrPut(variable) {
        if (owns(variable)) path(variable).resolve(translation, variable) else outer.resolve(variable)
    }.also(translation.hopGuards::note)

    private fun owns(variable: String): Boolean =
        variable == lambdaVariable || variable.startsWith("$lambdaVariable.")

    override fun path(variable: String): AttributePath {
        if (!owns(variable)) {
            return outer.path(variable)
        }
        if (variable == lambdaVariable) {
            val element = relation.element ?: throw RelationRefusals.noElementColumn(variable, relation)
            // The lambda variable IS a list element, and CEL has no missing element: a NULL element
            // column is an explicit null VALUE — `null == "x"` is a definite false, so its negation
            // is true — whatever convention the element field was built with. It is the same
            // reading `null in tagNames` already takes. Reading it as a missing attribute instead
            // dropped exactly those rows from a negated body (`projection-exists-not-eq`).
            val asValue = AttributeMapping.Field(element.column, NullAttributeRepresentation.EXPLICIT)
            return AttributePath(this, emptyList(), asValue)
        }
        val walked = ChainWalk.from(relation, variable.substring(lambdaVariable.length + 1).split('.'))
            ?: throw Refusals.unknownAttribute(variable)
        if (walked.hops.isEmpty() && walked.leaf == null) {
            throw Refusals.internal("A member walk ended at neither a column nor a relation")
        }
        return AttributePath(this, walked.hops, walked.leaf)
    }
}

/**
 * A variable placed on storage: the [owner] scope its first hop correlates against, the relation
 * [hops] it passes through, and the [leaf] column it ends at, or `null` when it ends at the last
 * hop's relation itself.
 */
internal class AttributePath(
    val owner: Scope,
    val hops: List<AttributeMapping.Relation>,
    val leaf: AttributeMapping.Field?,
) {
    init {
        require(leaf != null || hops.isNotEmpty()) { "a path ends at a column or a relation" }
    }

    /** This place read as a value: the column itself, or a walk through [hops] anchored at [owner]. */
    fun resolve(translation: Translation, variable: String): Resolution {
        if (hops.isEmpty()) {
            val field = leaf ?: throw Refusals.internal("a path with no hop ends at a column")
            return Resolution.Scalar(variable, owner.read(field.column), field.column, field, owner)
        }
        return ChainWalk(hops, leaf).resolveAt(translation, owner, variable)
    }
}

/** A dotted reference walked onto storage: its relation [hops], relative to the walking scope, and [leaf]. */
internal class ChainWalk(
    val hops: List<AttributeMapping.Relation>,
    val leaf: AttributeMapping.Field?,
) {
    /** The resolution anchored at [owner]: a collection if any hop is to-many, else a scalar subquery. */
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
        owner,
        hops,
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

/** The column for one member of [relation]'s elements. An undeclared member is refused. */
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
