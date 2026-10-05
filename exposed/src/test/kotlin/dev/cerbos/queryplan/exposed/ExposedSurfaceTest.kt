package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Exists
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ReferenceOption
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.alias
import org.jetbrains.exposed.v1.core.booleanParam
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.dao.id.IntIdTable
import org.jetbrains.exposed.v1.core.stringParam
import org.jetbrains.exposed.v1.dao.IntEntity
import org.jetbrains.exposed.v1.dao.IntEntityClass
import org.jetbrains.exposed.v1.exceptions.ExposedSQLException
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.andWhere
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

/** The root resource. An `IntIdTable` so the DAO shape below has an entity to bind to. */
internal object SurfaceDocs : IntIdTable("surface_docs") {
    val key = varchar("doc_key", 32)
    val aString = varchar("a_string", 64)
    val region = varchar("region", 32)
    val archived = bool("archived")
}

/** A second table, for the join shape and for a correlated subquery over something else. */
internal object SurfaceTags : Table("surface_tags") {
    // Cascading, so the `deleteWhere` shape below measures what the PREDICATE does rather than
    // tripping over a foreign key the application would have declared this way anyway.
    val doc = reference("doc_id", SurfaceDocs, onDelete = ReferenceOption.CASCADE)
    val label = varchar("label", 32)
}

internal class SurfaceDoc(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<SurfaceDoc>(SurfaceDocs)

    var key by SurfaceDocs.key
    var aString by SurfaceDocs.aString
    var region by SurfaceDocs.region
    var archived by SurfaceDocs.archived
}

/**
 * The emitted `Op<Boolean>` in the call shapes a consumer actually writes.
 *
 * This is plumbing, not semantics. Which rows a filter must return is the conformance harness's
 * question and what it emits is the translator unit test's; what neither of them asks is whether the
 * value this adapter hands back survives `andWhere`, `count()`, pagination, a join, a DAO lookup, an
 * aliased root table or a `DELETE`. Every harness in this repository runs exactly one flat
 * `SELECT … WHERE`, so every one of those shapes is executed nowhere else.
 *
 * H2 in memory, in process. Plans come from `conformance/wire-fixtures/` (ADR 0006): `cs-eq` is
 * `request.resource.attr.aString == "one"`, which is the shape the translator handles today, and
 * `p-has` and `in-empty` are the two the planner folds to a constant.
 */
class ExposedSurfaceTest {

    companion object {
        private lateinit var database: Database

        /** Rows chosen so every shape below has something to include AND something to exclude. */
        @JvmStatic
        @BeforeAll
        fun seed() {
            database = Database.connect("jdbc:h2:mem:surface;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(database) {
                SchemaUtils.create(SurfaceDocs, SurfaceTags)
                listOf(
                    // key, aString, region, archived, labels
                    Row("a", "one", "emea", false, listOf("public")),
                    Row("b", "one", "amer", false, listOf("public", "internal")),
                    Row("c", "one", "emea", true, emptyList()),
                    Row("d", "One", "emea", false, listOf("public")),
                    Row("e", "two", "emea", false, listOf("internal")),
                ).forEach { row ->
                    val id = SurfaceDocs.insert {
                        it[key] = row.key
                        it[aString] = row.aString
                        it[region] = row.region
                        it[archived] = row.archived
                    } get SurfaceDocs.id
                    row.labels.forEach { label ->
                        SurfaceTags.insert {
                            it[doc] = id
                            it[SurfaceTags.label] = label
                        }
                    }
                }
            }
        }

        private class Row(
            val key: String,
            val aString: String,
            val region: String,
            val archived: Boolean,
            val labels: List<String>,
        )
    }

    private val mapping = cerbosMapping { "request.resource.attr.aString" to SurfaceDocs.aString }

    private fun filter(action: String): QueryPlanFilter =
        ExposedQueryPlanAdapter.toFilter(wireFixture(action), Options.of(mapping))

    private fun <T> read(body: () -> T): T = transaction(database) { body() }

    // -- the query shapes -------------------------------------------------------------------------

    @Test
    fun `where`() {
        val keys = read {
            SurfaceDocs.selectAll().where { filter("string/equals/case-sensitive").toOp() }.map { it[SurfaceDocs.key] }.sorted()
        }
        // CEL string equality is exact: "One" is a different string from "one".
        assertEquals(listOf("a", "b", "c"), keys)
    }

    @Test
    fun `andWhere`() {
        val keys = read {
            SurfaceDocs.selectAll()
                .where { filter("string/equals/case-sensitive").toOp() }
                .andWhere { EqOp(SurfaceDocs.archived, booleanParam(false)) }
                .map { it[SurfaceDocs.key] }
                .sorted()
        }
        assertEquals(listOf("a", "b"), keys)
    }

    @Test
    fun `composed with a predicate the application owns`() {
        // The load-bearing shape: the adapter's predicate is one conjunct among the application's
        // own, and an adapter that returned something only usable alone would fail here.
        val keys = read {
            SurfaceDocs.selectAll()
                .where {
                    TriLogic.and(
                        filter("string/equals/case-sensitive").toOp(),
                        EqOp(SurfaceDocs.archived, booleanParam(false)),
                        EqOp(SurfaceDocs.region, stringParam("emea")),
                    )
                }
                .map { it[SurfaceDocs.key] }
        }
        assertEquals(listOf("a"), keys)
    }

    @Test
    fun `count`() {
        assertEquals(3L, read { SurfaceDocs.selectAll().where { filter("string/equals/case-sensitive").toOp() }.count() })
        // `count()` rewrites the query into a COUNT projection, so the predicate has to survive a
        // rewrite rather than only the SELECT it was first attached to.
        assertEquals(2L, read { SurfaceDocs.select(SurfaceDocs.key).where { filter("string/equals/case-sensitive").toOp() }.andWhere { EqOp(SurfaceDocs.region, stringParam("emea")) }.count() })
    }

    @Test
    fun `orderBy limit offset`() {
        val page = read {
            SurfaceDocs.selectAll()
                .where { filter("string/equals/case-sensitive").toOp() }
                .orderBy(SurfaceDocs.key, SortOrder.ASC)
                .limit(2)
                .offset(1)
                .map { it[SurfaceDocs.key] }
        }
        assertEquals(listOf("b", "c"), page)
    }

    @Test
    fun `a DAO find takes the same predicate`() {
        val keys = read { SurfaceDoc.find(filter("string/equals/case-sensitive").toOp()).map { it.key }.sorted() }
        assertEquals(listOf("a", "b", "c"), keys)
    }

    @Test
    fun `a query that joins another table`() {
        val keys = read {
            (SurfaceDocs innerJoin SurfaceTags)
                .select(SurfaceDocs.key)
                .where { filter("string/equals/case-sensitive").toOp() }
                .andWhere { EqOp(SurfaceTags.label, stringParam("internal")) }
                .map { it[SurfaceDocs.key] }
                .sorted()
        }
        assertEquals(listOf("b"), keys)
    }

    // -- the aliased root table ---------------------------------------------------------------------

    @Test
    fun `an aliased root table works when the caller maps the alias's columns`() {
        // The adapter has no way to learn about an alias the caller will introduce later: the
        // mapping names columns, and `alias[Table.column]` is a different column from
        // `Table.column`. So the caller maps through the alias, and this is the shape that says so.
        val aliased = SurfaceDocs.alias("d")
        val aliasedMapping = cerbosMapping { "request.resource.attr.aString" to aliased[SurfaceDocs.aString] }
        val aliasedFilter = ExposedQueryPlanAdapter.toFilter(wireFixture("string/equals/case-sensitive"), Options.of(aliasedMapping))

        val keys = read {
            aliased.selectAll().where { aliasedFilter.toOp() }.map { it[aliased[SurfaceDocs.key]] }.sorted()
        }
        assertEquals(listOf("a", "b", "c"), keys)
        assertTrue(
            OfflineRenderer.renderOn(OfflineRenderer.POSTGRESQL, aliasedFilter.toOp()).sql.startsWith("d.a_string"),
        )
    }

    @Test
    fun `an aliased root table with the un-aliased columns mapped fails loudly`() {
        // The hazard the README has to name. The predicate names `surface_docs`, the query renames
        // it to `d`, and the base table is then out of scope — so the failure is the database's, at
        // the point of execution, rather than a silently wrong row set. What must NOT happen is the
        // predicate binding to some other copy of the table and returning rows the PDP denies.
        val aliased = SurfaceDocs.alias("d")
        val error = assertThrows(ExposedSQLException::class.java) {
            read { aliased.selectAll().where { filter("string/equals/case-sensitive").toOp() }.toList() }
        }
        assertTrue(
            error.message!!.contains("\"SURFACE_DOCS.A_STRING\" not found"),
            "the failure must name the column that is out of scope: ${error.message}",
        )
    }

    // -- the folded kinds ---------------------------------------------------------------------------

    @Test
    fun `an always-allowed plan filters nothing and an always-denied one filters everything`() {
        assertEquals(QueryPlanFilter.AlwaysAllowed, filter("null/has/missing-attribute"))
        assertEquals(QueryPlanFilter.AlwaysDenied, filter("membership/in/empty-list"))
        assertEquals(5L, read { SurfaceDocs.selectAll().where { filter("null/has/missing-attribute").toOp() }.count() })
        assertEquals(0L, read { SurfaceDocs.selectAll().where { filter("membership/in/empty-list").toOp() }.count() })
        // …and they still compose, which is the whole point of `toOp()` existing beside the sealed
        // result: a caller who always runs the query writes one expression either way.
        assertEquals(
            4L,
            read {
                SurfaceDocs.selectAll()
                    .where { TriLogic.and(filter("null/has/missing-attribute").toOp(), EqOp(SurfaceDocs.region, stringParam("emea"))) }
                    .count()
            },
        )
    }

    // -- writes ---------------------------------------------------------------------------------------

    @Test
    fun `the predicate works in deleteWhere and update, on H2 and on SQLite`() {
        // Both stores, because this is the one shape where the statement the predicate lands in is
        // not a SELECT, and a dialect is free to restrict what a DELETE's WHERE may reference.
        listOf(Store.H2, Store.SQLITE).forEach { store ->
            store.fresh { deleted ->
                assertEquals(3, SurfaceDocs.deleteWhere { deleted.toOp() }, "$store deleteWhere")
                assertEquals(listOf("d", "e"), SurfaceDocs.selectAll().map { it[SurfaceDocs.key] }.sorted())
            }
            store.fresh { updated ->
                assertEquals(
                    3,
                    SurfaceDocs.update({ updated.toOp() }) { it[region] = "moved" },
                    "$store update",
                )
                assertEquals(
                    listOf("a", "b", "c"),
                    SurfaceDocs.selectAll()
                        .where { EqOp(SurfaceDocs.region, stringParam("moved")) }
                        .map { it[SurfaceDocs.key] }
                        .sorted(),
                )
            }
        }
    }

    @Test
    fun `a correlated subquery survives deleteWhere and update, on H2 and on SQLite`() {
        // Hand-built, because the translator does not emit relation subqueries yet — but shaped like
        // the ones it will, so the answer this records ("both stores allow a correlated subquery in
        // a DELETE") is the answer the README needs. MySQL's own restriction is on a subquery over
        // the table being deleted from, which this is not.
        val tags = SurfaceTags.alias("cerbos_1")
        val hasInternalLabel = Exists(
            tags.select(tags[SurfaceTags.label])
                .where(
                    TriLogic.and(
                        EqOp(tags[SurfaceTags.doc], SurfaceDocs.id),
                        EqOp(tags[SurfaceTags.label], stringParam("internal")),
                    ),
                ),
        )
        listOf(Store.H2, Store.SQLITE).forEach { store ->
            store.fresh {
                assertEquals(2, SurfaceDocs.deleteWhere { hasInternalLabel }, "$store deleteWhere")
                assertEquals(listOf("a", "c", "d"), SurfaceDocs.selectAll().map { it[SurfaceDocs.key] }.sorted())
            }
            store.fresh {
                assertEquals(
                    2,
                    SurfaceDocs.update({ hasInternalLabel }) { it[region] = "moved" },
                    "$store update",
                )
            }
        }
    }

    /**
     * A store to run a mutating shape against: a database nobody else is using, created, seeded and
     * mutated inside ONE transaction.
     *
     * One transaction is not tidiness. SQLite's in-memory database lives only while a connection is
     * open, so a schema created in one transaction is gone by the next.
     */
    private enum class Store(private val url: (Int) -> String, private val driver: String) {
        H2({ "jdbc:h2:mem:surface_write_$it" }, "org.h2.Driver"),
        SQLITE({ "jdbc:sqlite:file:surface_write_$it?mode=memory&cache=shared" }, "org.sqlite.JDBC"),
        ;

        fun fresh(body: (QueryPlanFilter) -> Unit) {
            val database = Database.connect(url(NEXT.incrementAndGet()), driver = driver)
            transaction(database) {
                SchemaUtils.create(SurfaceDocs, SurfaceTags)
                listOf(
                    Triple("a", "one", emptyList<String>()),
                    Triple("b", "one", listOf("internal")),
                    Triple("c", "one", emptyList()),
                    Triple("d", "One", emptyList()),
                    Triple("e", "two", listOf("internal")),
                ).forEach { (key, aString, labels) ->
                    val id = SurfaceDocs.insert {
                        it[SurfaceDocs.key] = key
                        it[SurfaceDocs.aString] = aString
                        it[region] = "emea"
                        it[archived] = false
                    } get SurfaceDocs.id
                    labels.forEach { label ->
                        SurfaceTags.insert {
                            it[doc] = id
                            it[SurfaceTags.label] = label
                        }
                    }
                }
                val mapping = cerbosMapping { "request.resource.attr.aString" to SurfaceDocs.aString }
                body(ExposedQueryPlanAdapter.toFilter(wireFixture("string/equals/case-sensitive"), Options.of(mapping)))
            }
        }

        private companion object {
            /** Each call gets its own database name, so one test's writes cannot reach another's. */
            val NEXT = AtomicInteger()
        }
    }
}

/**
 * A golden `PlanResources` response from the shared corpus, decoded the way the PDP wrote it
 * (ADR 0006). Duplicated per suite on purpose: adapters share data, not code.
 */
private fun wireFixture(action: String): PlanResourcesFilter =
    Corpus.plan(action).filter
