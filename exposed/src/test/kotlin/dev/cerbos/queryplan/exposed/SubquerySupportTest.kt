package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * The relation side's shared fixture, and the assertions about the SHAPE of what it emits rather
 * than about which rows come back.
 *
 * The schema is the smallest one that reaches every hazard the relation side has to answer for: a
 * to-many with NULLable member names, a two-hop chain whose intermediate rows some resources do not
 * have, a real to-one parent with a unique foreign key, and a second to-one hop beyond it. Which
 * rows the PDP actually allows is the conformance harness's question; these suites hand-pick data
 * so each CEL state — empty, undetermined, absent parent — is reached by a named row.
 */
class SubquerySupportTest {

    @Test
    fun `a universal over a chain emits one guarded scoring scan`() {
        // Pinned whole, because the SHAPE is what is being asserted and every part of it is
        // load-bearing: the guard with no ELSE, the scores that keep the three CEL states apart,
        // NULLIF turning "only undetermined elements" into SQL NULL, COALESCE folding the empty
        // collection to 0, and the INNER JOIN that makes the scan range over the flattened tail.
        assertEquals(
            "(CASE WHEN EXISTS (SELECT 1 FROM REL_CATEGORIES cerbos_3 " +
                "WHERE cerbos_3.RESOURCE_ID = REL_RESOURCES.ID) " +
                "THEN (SELECT NULLIF(COALESCE(MAX(CASE WHEN (cerbos_2.\"name\" = 'finance') THEN 0 " +
                "WHEN NOT (cerbos_2.\"name\" = 'finance') THEN 2 ELSE 1 END), 0), 1) " +
                "FROM REL_CATEGORIES cerbos_1 INNER JOIN REL_SUB_CATEGORIES cerbos_2 " +
                "ON cerbos_1.ID = cerbos_2.CATEGORY_ID " +
                "WHERE cerbos_1.RESOURCE_ID = REL_RESOURCES.ID) END) = 0",
            render(translate("relation/all/to-one-chain")),
        )
    }

    @Test
    fun `every subquery is aliased, and the aliases are numbered in walk order`() {
        val sql = render(translate("relation/all/to-one-chain"))
        // The scan the score ranges over, then the guard that requires the leading hop: two scans
        // over two tables each, numbered as the walk reached them.
        assertTrue(
            sql.contains("REL_CATEGORIES cerbos_1") &&
                sql.contains("REL_SUB_CATEGORIES cerbos_2") &&
                sql.contains("REL_CATEGORIES cerbos_3"),
            sql,
        )
        assertEquals(3, Regex("cerbos_\\d+ ").findAll(sql).count(), sql)
        // Deterministic: the same plan under a fresh translation numbers identically.
        assertEquals(sql, render(translate("relation/all/to-one-chain")))
    }

    @Test
    fun `an unaliased table never reaches the emitted SQL`() {
        // The bare table name may only appear as the thing an alias is declared on. An inner scan
        // that read it unaliased would capture the outer correlation instead of joining.
        val sql = render(translate("relation/exists/to-one-chain"))
        assertEquals(
            Regex("REL_SUB_CATEGORIES").findAll(sql).count(),
            Regex("REL_SUB_CATEGORIES cerbos_\\d+").findAll(sql).count(),
            sql,
        )
    }

    @Test
    fun `visibleWhen is inside the correlation of every subquery a universal builds`() {
        // `all` compiles to a scan whose scores decide the macro, so a predicate applied OUTSIDE
        // the subquery would leave the universal quantifying over rows the application hides.
        val sql = render(translate("collection/all/empty-collection", VISIBILITY_FILTERED))
        val subqueries = Regex("SELECT ").findAll(sql).count()
        assertEquals(subqueries, Regex("DELETED = FALSE").findAll(sql).count(), sql)
    }

    @Test
    fun `visibleWhen reaches the hop guard as well as the scan it guards`() {
        val sql = render(translate("relation/all/to-one-chain", VISIBILITY_FILTERED))
        // One term per categories alias: the scan's own, and the guard's.
        assertEquals(2, Regex("VISIBLE = TRUE").findAll(sql).count(), sql)
    }

    companion object {
        object RelResources : Table("rel_resources") {
            val id = varchar("id", 32)
            val aString = varchar("a_string", 64)
            val aBool = bool("a_bool")

            /** The explicit-null attribute: a NULL here is sent to `check()` as a null VALUE. */
            val owner = varchar("owner", 64).nullable()

            /**
             * NOT NULL on purpose. The corpus's principal-value folds compare an attribute against
             * each element of a literal list, and this suite has to pin the FOLD rather than the
             * scalar side's null convention, which is another seam entirely.
             */
            val team = varchar("team", 32)
            override val primaryKey = PrimaryKey(id)
        }

        object RelTags : Table("rel_tags") {
            val id = varchar("id", 32)
            val resourceId = varchar("resource_id", 32)

            /** NULLable on purpose: a NULL member is what makes a lambda body UNDETERMINED. */
            val name = varchar("name", 64).nullable()

            /** The store-side predicate the application applies and the bare table does not. */
            val deleted = bool("deleted")
            override val primaryKey = PrimaryKey(id)
        }

        object RelCategories : Table("rel_categories") {
            val id = varchar("id", 32)
            val resourceId = varchar("resource_id", 32)
            val name = varchar("name", 64)
            val visible = bool("visible")
            override val primaryKey = PrimaryKey(id)
        }

        object RelSubCategories : Table("rel_sub_categories") {
            val id = varchar("id", 32)
            val categoryId = varchar("category_id", 32)
            val name = varchar("name", 64).nullable()
            override val primaryKey = PrimaryKey(id)
        }

        object RelLabels : Table("rel_labels") {
            val id = varchar("id", 32)
            val subCategoryId = varchar("sub_category_id", 32)
            val name = varchar("name", 64)
            override val primaryKey = PrimaryKey(id)
        }

        object RelParents : Table("rel_parents") {
            val id = varchar("id", 32)

            /** Unique: nothing else makes the database enforce the single row a to-one promises. */
            val resourceId = varchar("resource_id", 32).uniqueIndex()
            val aString = varchar("a_string", 64).nullable()
            val aNumber = double("a_number")
            val aBool = bool("a_bool")
            override val primaryKey = PrimaryKey(id)
        }

        object RelInners : Table("rel_inners") {
            val id = varchar("id", 32)
            val parentId = varchar("parent_id", 32).uniqueIndex()
            val aString = varchar("a_string", 64)
            val aBool = bool("a_bool")
            override val primaryKey = PrimaryKey(id)
        }

        /**
         * The mapping every suite here translates against.
         *
         * `mainCategory` and `categories` are the same two tables reached differently: a policy
         * dots through `mainCategory` as a single object, and the adapter walks it as two
         * collection hops, which is the shape the absent-to-one-parent hazard lives in.
         */
        val MAPPING = cerbosMapping {
            "request.resource.id" to RelResources.id
            "request.resource.attr.aString" to RelResources.aString
            "request.resource.attr.aBool" to RelResources.aBool
            "request.resource.attr.aOptionalString" to RelResources.team
            "request.resource.attr.owner" to field(RelResources.owner, NullAttributeRepresentation.EXPLICIT)
            "request.resource.attr.tags" to many(
                RelTags,
                from = RelResources.id,
                to = RelTags.resourceId,
                element = RelTags.name,
            ) {
                "id" to RelTags.id
                "name" to RelTags.name
            }
            "request.resource.attr.tagNames" to many(
                RelTags,
                from = RelResources.id,
                to = RelTags.resourceId,
                element = RelTags.name,
            )
            "request.resource.attr.categories" to categories()
            "request.resource.attr.mainCategory" to categories()
        }

        /** The same mapping, with the predicate the application applies to both related tables. */
        val VISIBILITY_FILTERED = cerbosMapping {
            "request.resource.attr.tags" to many(
                RelTags,
                from = RelResources.id,
                to = RelTags.resourceId,
                element = RelTags.name,
            ) {
                "name" to RelTags.name
                visibleWhen { tags -> tags[RelTags.deleted] eq false }
            }
            "request.resource.attr.mainCategory" to many(
                RelCategories,
                from = RelResources.id,
                to = RelCategories.resourceId,
                element = RelCategories.name,
            ) {
                "name" to RelCategories.name
                "subCategories" to many(
                    RelSubCategories,
                    from = RelCategories.id,
                    to = RelSubCategories.categoryId,
                    element = RelSubCategories.name,
                ) {
                    "name" to RelSubCategories.name
                }
                visibleWhen { c -> c[RelCategories.visible] eq true }
            }
        }

        /** The to-one chain, kept out of [MAPPING] so a suite can prove it is a separate shape. */
        val WITH_PARENT = cerbosMapping {
            "request.resource.attr.aString" to RelResources.aString
            "request.resource.attr.aBool" to RelResources.aBool
            "request.resource.attr.categories" to categories()
            "request.resource.attr.parent" to one(
                RelParents,
                from = RelResources.id,
                to = RelParents.resourceId,
            ) {
                "aString" to RelParents.aString
                "aNumber" to RelParents.aNumber
                "aBool" to RelParents.aBool
                "inner" to one(RelInners, from = RelParents.id, to = RelInners.parentId) {
                    "aString" to RelInners.aString
                    "aBool" to RelInners.aBool
                }
            }
        }

        private fun MappingBuilder.categories(): AttributeMapping.Relation = many(
            RelCategories,
            from = RelResources.id,
            to = RelCategories.resourceId,
            element = RelCategories.name,
        ) {
            "name" to RelCategories.name
            "subCategories" to many(
                RelSubCategories,
                from = RelCategories.id,
                to = RelSubCategories.categoryId,
                element = RelSubCategories.name,
            ) {
                "name" to RelSubCategories.name
                "labels" to many(
                    RelLabels,
                    from = RelSubCategories.id,
                    to = RelLabels.subCategoryId,
                    element = RelLabels.name,
                ) {
                    "name" to RelLabels.name
                }
            }
            "subNames" to many(
                RelSubCategories,
                from = RelCategories.id,
                to = RelSubCategories.categoryId,
                element = RelSubCategories.name,
            )
        }

        private val database by lazy {
            val db = Database.connect("jdbc:h2:mem:relations;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(db) { seed() }
            db
        }

        /** Translate a corpus wire fixture, and fail loudly rather than returning a bare predicate. */
        fun translate(action: String, mapping: AttributeResolver = MAPPING): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(wireFixture(action), Options.of(mapping)).toOp()

        /** The emitted SQL with its constants inlined, so an assertion can read the shape. */
        fun render(op: Op<Boolean>): String = transaction(database) { op.toString() }

        /** The resource ids the predicate selects, sorted. */
        fun ids(op: Op<Boolean>): List<String> = transaction(database) {
            RelResources.selectAll().where(op).map { it[RelResources.id] }.sorted()
        }

        /** A golden `PlanResources` response from the shared corpus, decoded the way the PDP wrote it. */
        fun wireFixture(action: String): PlanResourcesFilter =
            Corpus.plan(action).filter

        /**
         * Rows chosen so every CEL state is reached by a named one:
         *
         * | id | tags                     | categories               | parent                  |
         * |----|--------------------------|--------------------------|-------------------------|
         * | r1 | none                     | none                     | none                    |
         * | r2 | public                   | business -> finance      | "One", 2.0, no inner    |
         * | r3 | private                  | other -> tech            | none                    |
         * | r4 | NULL name                | none                     | NULL, 1.0, inner        |
         * | r5 | public, NULL             | business -> finance,tech | none                    |
         * | r6 | public, public, private* | none                     | none                    |
         * | r7 | none                     | other, no children       | none                    |
         * | r8 | none                     | none                     | "Two", 5.0, inner       |
         *
         * `*` is deleted, so it is in the table and not in the attributes the application sent.
         */
        private fun seed() {
            SchemaUtils.create(
                RelResources,
                RelTags,
                RelCategories,
                RelSubCategories,
                RelLabels,
                RelParents,
                RelInners,
            )
            listOf(
                Resource("r1", "one", true, "same", "set"),
                Resource("r2", "two", false, "public", "public"),
                Resource("r3", "three", true, null, "same"),
                Resource("r4", "four", false, null, "none"),
                Resource("r5", "five", true, "zzz", "X"),
                Resource("r6", "six", false, "public", "none"),
                Resource("r7", "seven", true, "public", "none"),
                Resource("r8", "eight", false, "public", "none"),
            ).forEach { row ->
                RelResources.insert {
                    it[id] = row.id
                    it[aString] = row.aString
                    it[aBool] = row.aBool
                    it[owner] = row.owner
                    it[team] = row.team
                }
            }
            listOf(
                Tag("t2", "r2", "public", false),
                Tag("t3", "r3", "private", false),
                Tag("t4", "r4", null, false),
                Tag("t5a", "r5", "public", false),
                Tag("t5b", "r5", null, false),
                Tag("t6a", "r6", "public", false),
                Tag("t6b", "r6", "public", false),
                // Hidden by the application's own read, so a universal must not examine it.
                Tag("t6c", "r6", "private", true),
            ).forEach { row ->
                RelTags.insert {
                    it[id] = row.id
                    it[resourceId] = row.resourceId
                    it[name] = row.name
                    it[deleted] = row.deleted
                }
            }
            listOf(
                Category("c2", "r2", "business", true),
                Category("c3", "r3", "other", true),
                Category("c5", "r5", "business", true),
                Category("c7", "r7", "other", true),
            ).forEach { row ->
                RelCategories.insert {
                    it[id] = row.id
                    it[resourceId] = row.resourceId
                    it[name] = row.name
                    it[visible] = row.visible
                }
            }
            listOf(
                SubCategory("s2", "c2", "finance"),
                SubCategory("s3", "c3", "tech"),
                SubCategory("s5a", "c5", "finance"),
                SubCategory("s5b", "c5", "tech"),
            ).forEach { row ->
                RelSubCategories.insert {
                    it[id] = row.id
                    it[categoryId] = row.categoryId
                    it[name] = row.name
                }
            }
            // s5a carries none, so a universal one level up has a determined-false element.
            listOf(Label("l2", "s2", "gold"), Label("l3", "s3", "silver"), Label("l5", "s5b", "gold"))
                .forEach { row ->
                    RelLabels.insert {
                        it[id] = row.id
                        it[subCategoryId] = row.subCategoryId
                        it[name] = row.name
                    }
                }
            listOf(
                Parent("p2", "r2", "One", 2.0, true),
                Parent("p4", "r4", null, 1.0, false),
                Parent("p8", "r8", "Two", 5.0, false),
            ).forEach { row ->
                RelParents.insert {
                    it[id] = row.id
                    it[resourceId] = row.resourceId
                    it[aString] = row.aString
                    it[aNumber] = row.aNumber
                    it[aBool] = row.aBool
                }
            }
            // p2 deliberately has no inner: "the parent exists, the level beyond it does not".
            listOf(Inner("i4", "p4", "inner4", true), Inner("i8", "p8", "inner8", false)).forEach { row ->
                RelInners.insert {
                    it[id] = row.id
                    it[parentId] = row.parentId
                    it[aString] = row.aString
                    it[aBool] = row.aBool
                }
            }
        }

        private class Resource(
            val id: String,
            val aString: String,
            val aBool: Boolean,
            val owner: String?,
            val team: String,
        )
        private class Tag(val id: String, val resourceId: String, val name: String?, val deleted: Boolean)
        private class Category(val id: String, val resourceId: String, val name: String, val visible: Boolean)
        private class SubCategory(val id: String, val categoryId: String, val name: String)
        private class Label(val id: String, val subCategoryId: String, val name: String)
        private class Parent(
            val id: String,
            val resourceId: String,
            val aString: String?,
            val aNumber: Double,
            val aBool: Boolean,
        )
        private class Inner(val id: String, val parentId: String, val aString: String, val aBool: Boolean)
    }
}
