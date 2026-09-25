package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * The relational shape the shared `../conformance/` corpus is persisted into, and the mapping the
 * corpus is read back through.
 *
 * The tables are declared BARE — no `references`, no Exposed relation metadata — because that is
 * what this adapter's subqueries read. An association the ORM knows about would carry predicates
 * the correlated subquery cannot see, which is the class-1 mapping hazard
 * `conformance/README.md` describes; here there is nothing to carry.
 *
 * Both the mapping and the tables live in one file on purpose: the adapter resolves a reference
 * onto a COLUMN, so which column a reference names and which table owns it are one statement. The
 * conformance harness executes the predicates this mapping produces, and the translator unit test
 * pins the SQL they render to — two statements about one query, which they only are while both are
 * built from here.
 */

// -- tables ------------------------------------------------------------------------------------

/**
 * The resource rows. Every nullable column is nullable BECAUSE the corpus discriminates on it: a
 * NULL `a_bool`, `a_string`, `a_number`, `a_optional_string`, `scope`, `a_double`, `created_at` or
 * `updated_at` is a missing attribute on the check side, so both the predicate and its negation must
 * exclude the row.
 */
internal object Resources : Table("adversarial_resources") {
    val id = varchar("id", 64)
    val aBool = bool("a_bool").nullable()
    val aString = varchar("a_string", 255).nullable()
    val aNumber = integer("a_number").nullable()
    val aDouble = double("a_double").nullable()
    val aOptionalString = varchar("a_optional_string", 255).nullable()
    val createdBy = varchar("created_by", 64)
    val scope = varchar("scope", 255).nullable()
    val createdAt = timestamp("created_at").nullable()
    val updatedAt = timestamp("updated_at").nullable()
    override val primaryKey = PrimaryKey(id)
}

/**
 * The first hop of the corpus's one REAL to-one chain (`conformance/README.md`, "The real to-one
 * relation"). `resource_id` carries a UNIQUE index rather than merely being documented as unique:
 * nothing else makes the database enforce the single row a to-one mapping promises, and a
 * multi-row correlated scalar subquery errors on PostgreSQL, MySQL and H2 but silently takes the
 * first row on SQLite.
 */
internal object Parents : Table("adversarial_parents") {
    val id = varchar("id", 64)
    val aBool = bool("a_bool").nullable()
    val aString = varchar("a_string", 255).nullable()
    val aNumber = integer("a_number").nullable()
    val aOptionalString = varchar("a_optional_string", 255).nullable()
    val resourceId = varchar("resource_id", 64).uniqueIndex()
    override val primaryKey = PrimaryKey(id)
}

/** The second hop of the chain. It is cut here: there is no `parent.inner.inner`. */
internal object Inners : Table("adversarial_inners") {
    val id = varchar("id", 64)
    val aBool = bool("a_bool").nullable()
    val aString = varchar("a_string", 255).nullable()
    val aNumber = integer("a_number").nullable()
    val aOptionalString = varchar("a_optional_string", 255).nullable()
    val parentId = varchar("parent_id", 64).uniqueIndex()
    override val primaryKey = PrimaryKey(id)
}

/**
 * The object-valued collection. `name` is NULLABLE on purpose: a NULL tag name is a missing element
 * attribute on the check side (a CEL error, so a deny) and must stay UNKNOWN — never FALSE — in
 * SQL, or a negation readmits the row.
 */
internal object Tags : Table("adversarial_tags") {
    val tagId = varchar("tag_id", 64)
    val name = varchar("name", 255).nullable()
    val resourceId = varchar("resource_id", 64)

    /** The tag's index in the seed's `tags` list, for positional reads (`tags[0]`). */
    val position = integer("position")
    override val primaryKey = PrimaryKey(tagId)
}

internal object Categories : Table("adversarial_categories") {
    val id = varchar("id", 64)
    val name = varchar("name", 255)
    val resourceId = varchar("resource_id", 64)
    override val primaryKey = PrimaryKey(id)
}

internal object SubCategories : Table("adversarial_sub_categories") {
    val id = varchar("id", 64)
    val name = varchar("name", 255)
    val categoryId = varchar("category_id", 64)
    override val primaryKey = PrimaryKey(id)
}

/** The third macro level, for the `macro-depth3-*` actions. A NULL name is a missing attribute. */
internal object Labels : Table("adversarial_labels") {
    val id = varchar("id", 64)
    val name = varchar("name", 255).nullable()
    val subCategoryId = varchar("sub_category_id", 64)
    override val primaryKey = PrimaryKey(id)
}

/**
 * `aNumberList` as one row per element. A NULL [element] is a null list element, which CEL
 * compares as a value.
 */
internal object NumberListElements : Table("adversarial_number_list") {
    val id = varchar("id", 64)
    val element = double("element").nullable()
    val resourceId = varchar("resource_id", 64)
    val position = integer("position")
    override val primaryKey = PrimaryKey(id)
}

/** `aBoolList` as one row per element, as [NumberListElements]. */
internal object BoolListElements : Table("adversarial_bool_list") {
    val id = varchar("id", 64)
    val element = bool("element").nullable()
    val resourceId = varchar("resource_id", 64)
    val position = integer("position")
    override val primaryKey = PrimaryKey(id)
}

/** Creation order for `SchemaUtils.create`, and the order the harness seeds and asserts in. */
internal val ADVERSARIAL_TABLES: List<Table> = listOf(
    Resources, Parents, Inners, Tags, NumberListElements, BoolListElements, Categories, SubCategories,
    Labels,
)

// -- the corpus mapped onto those tables ---------------------------------------------------------

/**
 * The corpus's attribute mapping, built with the adapter's own DSL so the harness proves the
 * surface a consumer writes rather than an internal shortcut.
 */
internal val MAPPING: AttributeMappings = cerbosMapping {
    // The primary key, reached as `request.resource.id` rather than through `attr` (the `id-*`
    // actions). An adapter that resolves references by stripping a `request.resource.attr.` prefix
    // never sees this name.
    "request.resource.id" to Resources.id
    "request.resource.attr.aBool" to omitted(Resources.aBool)
    "request.resource.attr.aString" to omitted(Resources.aString)
    "request.resource.attr.aNumber" to omitted(Resources.aNumber)
    "request.resource.attr.aDouble" to omitted(Resources.aDouble)
    "request.resource.attr.aOptionalString" to omitted(Resources.aOptionalString)
    // ISO-date string column; the `p-timestamp` probe asks timestamp() of it, which is why it is
    // deliberately a varchar rather than a temporal column.
    "request.resource.attr.createdBy" to Resources.createdBy
    // Delimited hierarchy path column for the hier-* actions.
    "request.resource.attr.scope" to omitted(Resources.scope)
    // Instant column for the ts-* timestamp() comparison actions.
    "request.resource.attr.createdAt" to omitted(Resources.createdAt)
    // A second instant column, for `temporal-raw-eq`: two instants compared WITHOUT timestamp(),
    // which CEL answers over their strings.
    "request.resource.attr.updatedAt" to omitted(Resources.updatedAt)
    // `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map, under the
    // OTHER null convention: the oracle sends a real null attribute for them rather than omitting
    // it. Declaring it here is what makes the equality family definite for these two attributes
    // and leaves it untouched for every other mapping
    // (https://github.com/cerbos/query-plan-adapters/issues/308).
    "request.resource.attr.owner" to
        field(Resources.aOptionalString, nulls = NullAttributeRepresentation.EXPLICIT)
    "request.resource.attr.coOwner" to
        field(Resources.scope, nulls = NullAttributeRepresentation.EXPLICIT)
    // A FLAT column wearing a dotted name, the same trick every reference harness uses for the
    // `p-struct` probe. `parent.inner` below is the opposite — a genuine two-level join — and the
    // two are kept side by side so a reader can tell which dotted name emits a subquery.
    "request.resource.attr.obj.inner" to omitted(Resources.aString)
    // The corpus's one REAL to-one chain (the `rel-*` actions). ONE is what tells the adapter the
    // hop can be ABSENT, which the negated shapes discriminate: a row with no parent sends no
    // attribute, so CEL raises a missing-path error and the PDP denies, while an unguarded
    // two-valued read of the hop is TRUE for exactly those rows.
    "request.resource.attr.parent" to one(Parents, from = Resources.id, to = Parents.resourceId) {
        "aBool" to omitted(Parents.aBool)
        "aString" to omitted(Parents.aString)
        "aNumber" to omitted(Parents.aNumber)
        "aOptionalString" to omitted(Parents.aOptionalString)
        "inner" to one(Inners, from = Parents.id, to = Inners.parentId) {
            "aBool" to omitted(Inners.aBool)
            "aString" to omitted(Inners.aString)
            "aNumber" to omitted(Inners.aNumber)
            "aOptionalString" to omitted(Inners.aOptionalString)
        }
    }
    // Scalar lists, one related row per element; a NULL element column is a null element.
    // Each carries its seed-list index as a position column, for positional reads (`list[0]`).
    "request.resource.attr.aNumberList" to
        many(NumberListElements, from = Resources.id, to = NumberListElements.resourceId, element = NumberListElements.element) {
            position(NumberListElements.position)
        }
    "request.resource.attr.aBoolList" to
        many(BoolListElements, from = Resources.id, to = BoolListElements.resourceId, element = BoolListElements.element) {
            position(BoolListElements.position)
        }
    "request.resource.attr.tags" to many(Tags, from = Resources.id, to = Tags.resourceId) {
        "id" to Tags.tagId
        "name" to Tags.name
        position(Tags.position)
    }
    // The scalar projection of the same relation, for `null in R.attr.tagNames`: each element IS a
    // value, and a NULL name column is an explicit null list element on the check side.
    "request.resource.attr.tagNames" to
        many(Tags, from = Resources.id, to = Tags.resourceId, element = Tags.name) {
            position(Tags.position)
        }
    "request.resource.attr.categories" to
        many(Categories, from = Resources.id, to = Categories.resourceId) {
            "name" to Categories.name
            "subCategories" to
                many(SubCategories, from = Categories.id, to = SubCategories.categoryId) {
                    "name" to SubCategories.name
                    // Third macro level, for the macro-depth3-* actions.
                    "labels" to many(Labels, from = SubCategories.id, to = Labels.subCategoryId) {
                        "name" to Labels.name
                    }
                }
        }
    // Multi-hop chain probe (W1): mainCategory is a SINGLE nested object on the check side (every
    // seed holds at most one category), so CEL evaluates dotted chains like
    // R.attr.mainCategory.subCategories naturally — while the ADAPTER maps the same path through
    // TWO collection hops, pinning that chained variables join through every intermediate hop,
    // never off the root.
    "request.resource.attr.mainCategory" to
        many(Categories, from = Resources.id, to = Categories.resourceId) {
            "name" to Categories.name
            "subCategories" to
                many(SubCategories, from = Categories.id, to = SubCategories.categoryId) {
                    "name" to SubCategories.name
                }
            // subNames: the same 2-hop chain but with an element column, so plain `in` membership
            // compares the flattened tail's name.
            "subNames" to many(
                SubCategories,
                from = Categories.id,
                to = SubCategories.categoryId,
                element = SubCategories.name,
            )
        }
}

/**
 * A column `resources.json` omits when it is NULL: the attribute is missing, not null, so a null
 * literal compared against it is refused (docs/adr/0004).
 */
private fun omitted(column: Column<*>): AttributeMapping.Field =
    AttributeMapping.field(column, NullAttributeRepresentation.OMITTED)

/**
 * The same mapping with every per-attribute null convention stripped, so the call-level option is
 * the only thing governing null operands.
 *
 * The #302 completeness guard is a statement about that option: every corpus action carrying a null
 * literal must be rejected under OMITTED. Declaring `owner`/`coOwner` as explicit-null (#308)
 * deliberately overrides the option for those two attributes — which would otherwise read as the
 * guard going quiet, when in fact it is the per-attribute declaration doing exactly its job.
 * Stripping the declarations keeps the guard testing what it was written to test.
 */
internal val MAPPING_WITHOUT_NULL_CONVENTIONS: AttributeMappings = AttributeMappings.of(
    MAPPING.entries.mapValues { (_, mapping) ->
        when (mapping) {
            is AttributeMapping.Field -> AttributeMapping.field(mapping.column)
            is AttributeMapping.Relation -> mapping
        }
    },
)
