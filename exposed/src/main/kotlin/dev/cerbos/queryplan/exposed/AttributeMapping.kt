package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Alias
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table

/**
 * Resolves a plan's attribute reference, e.g. `request.resource.attr.owner`, onto storage.
 *
 * Resolution is fail-closed: returning `null` makes the translation raise
 * [UnmappedAttributeException] rather than guess a column name. [cerbosMapping] builds the usual
 * static table; implement this interface directly to resolve references by rule.
 *
 * A dotted reference is matched against the LONGEST mapped prefix, and the remainder is walked
 * through that relation's nested [AttributeMapping.Relation.fields]. A function-style resolver is
 * therefore asked for progressively shorter prefixes of the same reference.
 */
public fun interface AttributeResolver {
    public fun resolve(reference: String): AttributeMapping?
}

/** What an attribute reference resolves to: a column, or a relation reaching another table. */
public sealed interface AttributeMapping {

    /** One column on the table of the scope that declares it. */
    public class Field internal constructor(
        public val column: Column<*>,
        /**
         * This attribute's NULL convention, or `null` when nothing is declared. Declaring it
         * asserts two things at once: that the column can be NULL, and how that NULL reaches
         * `check()`.
         *
         * - [NullAttributeRepresentation.EXPLICIT]: the NULL is sent as a null VALUE, so the
         *   equality family (`eq`, `ne`, `in`) renders DEFINITELY and a negation includes the NULL
         *   rows CEL allows.
         * - [NullAttributeRepresentation.OMITTED]: the NULL sends no attribute, so a null operand
         *   against this attribute is refused whatever the call-level option says.
         * - `null`: the column renders the way an undeclared column always has, as if NOT NULL, and
         *   [Options.nullAttributeRepresentation] decides only whether a null OPERAND is refused.
         *
         * `null` does NOT mean "inherit the call-level option" for rendering, and must not: the
         * call-level default is EXPLICIT, so inheriting it would hand definite equality to every
         * undeclared column and return NULL rows the PDP denies for an attribute the caller omits.
         * The fix is opt-in per column (docs/adr/0004).
         */
        public val nullAttributeRepresentation: NullAttributeRepresentation?,
    ) : AttributeMapping {
        override fun toString(): String =
            "Field(${column.table.tableName}.${column.name}, nulls=$nullAttributeRepresentation)"
    }

    /**
     * A relation to another table, lowered to a correlated subquery over that table read BARE.
     *
     * The correlation is always `<table>.<to> = <parent>.<from>`, where the parent is the scope
     * that declares the relation: the root table at the top level, the enclosing relation's table
     * when nested under [fields]. Both keys are single columns, so a composite association key is
     * not expressible here: that is a compile error rather than a wrong join.
     */
    public class Relation internal constructor(
        public val cardinality: Cardinality,
        /** The related table: the one a lambda body, or a to-one path, reads its fields from. */
        public val table: Table,
        /** The column on the PARENT scope's table. */
        public val from: Column<*>,
        /** The matching column on [table]. */
        public val to: Column<*>,
        /**
         * The column that stands for the element itself: the value of a scalar collection such as
         * `tagNames`, or the default member when an object collection is used as a bare value.
         */
        public val element: Field?,
        /** The element's own attributes. An entry may itself be a [Relation], giving a chain. */
        public val fields: Map<String, AttributeMapping>,
        /**
         * The predicate the APPLICATION applies whenever it reads [table]: a soft-delete flag, a
         * tenant column, a subtype discriminator. The subquery reads the table bare and cannot see
         * any of those, so without this it examines rows the application never serialised into the
         * resource attributes. It is built against the alias each subquery instance reads through.
         */
        internal val visibleWhen: ((Alias<Table>) -> Op<Boolean>)?,
    ) : AttributeMapping {

        public enum class Cardinality {
            /**
             * At most one related row. Read as a scalar; an absent row is a missing attribute.
             * Nothing makes the database enforce the single row: [to] needs a unique index.
             */
            ONE,

            /** Any number of related rows: a collection. */
            MANY,
        }

        /** Whether the caller declared a [visibleWhen] predicate. */
        public val hasVisibilityPredicate: Boolean get() = visibleWhen != null

        init {
            require(to.table === table) {
                "Relation target column ${to.table.tableName}.${to.name} is not a column of ${table.tableName}"
            }
            element?.let {
                require(it.column.table === table) {
                    "Relation element column ${it.column.table.tableName}.${it.column.name} " +
                        "is not a column of ${table.tableName}"
                }
            }
            fields.forEach { (name, mapping) ->
                val owner = when (mapping) {
                    is Field -> mapping.column.table
                    is Relation -> mapping.from.table
                }
                require(owner === table) {
                    "Relation field '$name' reads ${owner.tableName}, not the relation's table ${table.tableName}"
                }
            }
        }

        override fun toString(): String =
            "Relation($cardinality ${table.tableName}: ${to.name} = ${from.table.tableName}.${from.name})"
    }

    public companion object {
        /**
         * A column that declares NO null convention, and so renders as if it were NOT NULL. See
         * [Field.nullAttributeRepresentation]: the call-level [Options.nullAttributeRepresentation]
         * decides only whether a null OPERAND against it is refused, never how it renders.
         */
        @JvmStatic
        public fun field(column: Column<*>): Field = Field(column, null)

        /** A column that declares its own NULL convention. */
        @JvmStatic
        public fun field(column: Column<*>, nulls: NullAttributeRepresentation): Field = Field(column, nulls)
    }
}

/** A static reference-to-mapping table, as built by [cerbosMapping]. */
public class AttributeMappings internal constructor(entries: Map<String, AttributeMapping>) : AttributeResolver {
    /** The mapped references. A defensive copy: later changes to the source map never reach it. */
    public val entries: Map<String, AttributeMapping> = LinkedHashMap(entries)

    override fun resolve(reference: String): AttributeMapping? = entries[reference]

    override fun toString(): String = "AttributeMappings(${entries.keys})"

    public companion object {
        /** For callers that assemble the table without the Kotlin DSL. */
        @JvmStatic
        public fun of(entries: Map<String, AttributeMapping>): AttributeMappings = AttributeMappings(entries)
    }
}

@DslMarker
public annotation class CerbosMappingDsl

/**
 * Builds a static mapping:
 *
 * ```kotlin
 * val mapping = cerbosMapping {
 *     "request.resource.id" to Documents.id
 *     "request.resource.attr.owner" to field(Documents.owner, nulls = NullAttributeRepresentation.EXPLICIT)
 *     "request.resource.attr.tags" to many(Tags, from = Documents.id, to = Tags.documentId) {
 *         "name" to Tags.name
 *         visibleWhen { t -> t[Tags.deletedAt].isNull() }
 *     }
 * }
 * ```
 */
public fun cerbosMapping(block: MappingBuilder.() -> Unit): AttributeMappings =
    AttributeMappings(MappingBuilder().apply(block).entries)

/** Shared by the top-level mapping and every nested relation: the entries and their factories. */
@CerbosMappingDsl
public open class MappingBuilder internal constructor() {
    internal val entries: MutableMap<String, AttributeMapping> = LinkedHashMap()

    /**
     * Maps [this] reference onto a column that declares no null convention, and so renders as if
     * it were NOT NULL ([AttributeMapping.Field.nullAttributeRepresentation]).
     */
    public infix fun String.to(column: Column<*>) {
        put(this, AttributeMapping.Field(column, null))
    }

    /** Maps [this] reference onto a [field], [one] or [many] mapping. */
    public infix fun String.to(mapping: AttributeMapping) {
        put(this, mapping)
    }

    /** A column that declares its own NULL convention. */
    public fun field(column: Column<*>, nulls: NullAttributeRepresentation? = null): AttributeMapping.Field =
        AttributeMapping.Field(column, nulls)

    /** A to-one relation: `table.to = parent.from`, at most one row. */
    public fun <T : Table> one(
        table: T,
        from: Column<*>,
        to: Column<*>,
        block: RelationBuilder<T>.() -> Unit = {},
    ): AttributeMapping.Relation =
        RelationBuilder<T>().apply(block).build(AttributeMapping.Relation.Cardinality.ONE, table, from, to, null)

    /**
     * A to-many relation: `table.to = parent.from`. Pass [element] for a scalar collection, where
     * each element IS a value (`tagNames`), or as the default member of an object collection.
     */
    public fun <T : Table> many(
        table: T,
        from: Column<*>,
        to: Column<*>,
        element: Column<*>? = null,
        block: RelationBuilder<T>.() -> Unit = {},
    ): AttributeMapping.Relation =
        RelationBuilder<T>().apply(block).build(AttributeMapping.Relation.Cardinality.MANY, table, from, to, element)

    private fun put(reference: String, mapping: AttributeMapping) {
        require(reference.isNotEmpty()) { "An attribute reference must not be empty" }
        require(entries.put(reference, mapping) == null) { "'$reference' is mapped twice" }
    }
}

/** The body of a [MappingBuilder.one] or [MappingBuilder.many] block. */
@CerbosMappingDsl
public class RelationBuilder<T : Table> internal constructor() : MappingBuilder() {
    private var visibleWhen: ((Alias<T>) -> Op<Boolean>)? = null

    /**
     * Declares the predicate the application itself applies when it reads this relation's table.
     * It receives the alias the subquery reads through: resolve columns with `alias[Table.column]`.
     * It is ANDed into the subquery's correlation, so it narrows the rows the subquery EXAMINES,
     * which is what keeps it right under negation.
     */
    public fun visibleWhen(predicate: (Alias<T>) -> Op<Boolean>) {
        require(visibleWhen == null) { "visibleWhen is declared twice" }
        visibleWhen = predicate
    }

    internal fun build(
        cardinality: AttributeMapping.Relation.Cardinality,
        table: T,
        from: Column<*>,
        to: Column<*>,
        element: Column<*>?,
    ): AttributeMapping.Relation {
        val predicate = visibleWhen
        return AttributeMapping.Relation(
            cardinality = cardinality,
            table = table,
            from = from,
            to = to,
            element = element?.let { AttributeMapping.Field(it, null) },
            fields = LinkedHashMap(entries),
            // The alias a subquery hands in is always an alias of `table`, so the narrowing is safe;
            // the type parameter only exists to give the caller `alias[Table.column]` with no cast.
            visibleWhen = predicate?.let { typed ->
                { alias -> @Suppress("UNCHECKED_CAST") typed(alias as Alias<T>) }
            },
        )
    }
}
