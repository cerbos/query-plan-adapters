package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.append
import org.jetbrains.exposed.v1.javatime.timestamp
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
import java.time.Instant

/**
 * The scalar side's shared fixture, and the sanity checks that the fixture itself discriminates.
 *
 * Every plan comes from `conformance/wire-fixtures/`, decoded the way the PDP wrote it
 * (docs/adr/0006). Hand-built plans appear only where a shape is malformed or CEL cannot reach it,
 * and each of those says so at the test.
 *
 * What these suites prove is narrow on purpose: that the emitted predicate returns the rows CEL
 * would allow over THIS data. Which rows the PDP actually allows over the corpus is the
 * conformance harness's question, and no unit test substitutes for it.
 */
internal object Scalars {

    object Docs : Table("scalar_docs") {
        val id = varchar("id", 32)
        val aString = varchar("a_string", 64)

        /** Nullable, and mapped TWICE — under both NULL conventions, as the corpus does. */
        val aOptionalString = varchar("a_optional_string", 64).nullable()
        val scope = varchar("scope", 64).nullable()
        val aNumber = integer("a_number")
        val aDouble = double("a_double")
        val aBool = bool("a_bool")
        val createdAt = timestamp("created_at")
        override val primaryKey = PrimaryKey(id)
    }

    /**
     * The same double mapping the corpus carries: `owner` and `coOwner` declare that the caller
     * sends their NULLs as explicit nulls, while `aOptionalString` and `scope` — the SAME two
     * columns — declare nothing and so keep the omitted convention.
     */
    val MAPPING: AttributeMappings = cerbosMapping {
        "request.resource.id" to Docs.id
        "request.resource.attr.aString" to Docs.aString
        "request.resource.attr.aOptionalString" to Docs.aOptionalString
        "request.resource.attr.scope" to Docs.scope
        "request.resource.attr.owner" to field(Docs.aOptionalString, NullAttributeRepresentation.EXPLICIT)
        "request.resource.attr.coOwner" to field(Docs.scope, NullAttributeRepresentation.EXPLICIT)
        "request.resource.attr.aNumber" to Docs.aNumber
        "request.resource.attr.aDouble" to Docs.aDouble
        "request.resource.attr.aBool" to Docs.aBool
        "request.resource.attr.createdAt" to Docs.createdAt
        "request.resource.attr.createdBy" to Docs.aString
    }

    private class Row(
        val id: String,
        val aString: String,
        val aOptionalString: String?,
        val scope: String?,
        val aNumber: Int,
        val aDouble: Double,
        val aBool: Boolean,
        val createdAt: String,
    )

    /**
     * Rows chosen so that a wrong translation shows up as a different id set, never as the same
     * one: `r5` holds the LIKE metacharacters an unescaped needle would match, `r2`/`r4`/`r7` hold
     * the NULLs both polarities have to exclude, `r2` differs from `r1` by case alone, and `f1`
     * and `c1` carry the identity and concatenation witnesses.
     */
    private val ROWS = listOf(
        Row("r1", "one", "one", "dept.eng", 1, 0.5, true, "2024-06-01T00:00:00Z"),
        Row("r2", "One", null, null, 2, -0.6, false, "2024-07-01T00:00:00Z"),
        Row("r3", "100%_x", "100%", "dept.eng.platform", 0, 0.0, true, "2024-05-01T00:00:00Z"),
        Row("r4", "[SEC]top", null, "dept", -5, 1.5, false, "2023-01-01T00:00:00Z"),
        Row("r5", "xaXby", "%", "other.team", 3, 2.0, true, "2024-06-01T00:00:00Z"),
        Row("r6", "100abc", "x", "dept.eng", 4, 3.5, false, "2024-08-01T00:00:00Z"),
        Row("r7", "tail\\", null, null, 10, 10.0, true, "2025-01-01T00:00:00Z"),
        Row("f1", "f1", "prefix:f1", "dept.eng", 1, 7.0, true, "2024-09-01T00:00:00Z"),
        Row("c1", "one", "set", "dept.eng", 1, 0.5, false, "2024-06-01T00:00:00Z"),
    )

    /** Every seeded id, for the assertions whose expectation is "no row is filtered out". */
    val ALL: List<String> = ROWS.map { it.id }.sorted()

    private var seeded = false

    private val database: Database by lazy {
        Database.connect("jdbc:h2:mem:cerbos_scalar;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
    }

    /**
     * Runs [body] inside a transaction on a seeded in-memory H2, which needs nothing external.
     *
     * The seeding is a transaction OF ITS OWN, and deliberately so: half these suites assert a
     * refusal, and a body that throws rolls its transaction back. Seeding inside it would undo the
     * rows while leaving the table behind — H2 commits DDL on its own — and every later assertion
     * would then read an empty table and agree with any filter that returns nothing.
     */
    fun <T> onH2(body: () -> T): T {
        seed()
        return transaction(database) { body() }
    }

    private fun seed() {
        if (seeded) return
        transaction(database) {
            SchemaUtils.create(Docs)
            ROWS.forEach { row ->
                Docs.insert {
                    it[id] = row.id
                    it[aString] = row.aString
                    it[aOptionalString] = row.aOptionalString
                    it[scope] = row.scope
                    it[aNumber] = row.aNumber
                    it[aDouble] = row.aDouble
                    it[aBool] = row.aBool
                    it[createdAt] = Instant.parse(row.createdAt)
                }
            }
        }
        seeded = true
    }

    /** The ids a wire fixture's translated filter selects, sorted. */
    fun ids(action: String, options: Options = Options.of(MAPPING)): List<String> = onH2 {
        val filter = ExposedQueryPlanAdapter.toFilter(wireFixture(action), options)
        Docs.selectAll().where { filter.toOp() }.map { it[Docs.id] }.sorted()
    }

    /** The ids an already-built predicate selects, sorted — for hand-built plans. */
    fun idsOf(op: Op<Boolean>): List<String> = onH2 {
        Docs.selectAll().where { op }.map { it[Docs.id] }.sorted()
    }

    /** The translated predicate, for the assertions whose subject is the emitted SQL. */
    fun op(action: String, options: Options = Options.of(MAPPING)): Op<Boolean> = onH2 {
        ExposedQueryPlanAdapter.toFilter(wireFixture(action), options).toOp()
    }

    /** The predicate as prepared SQL, with the bound arguments beside it. */
    fun rendered(op: Op<Boolean>): Rendered = onH2 {
        val builder = QueryBuilder(true)
        builder.append(op)
        Rendered(builder.toString(), builder.args.map { it.second })
    }

    class Rendered(val sql: String, val args: List<Any?>)

    /** A golden `PlanResources` response from the shared corpus, decoded the way the PDP wrote it. */
    fun wireFixture(action: String): PlanResourcesFilter {
        val file = Path.of(System.getProperty("user.dir"), "..", "conformance", "wire-fixtures", "$action.json")
        val filter = ObjectMapper().readTree(Files.readString(file)).get("filter")
        return PlanResourcesFilter.newBuilder()
            .also { JsonFormat.parser().merge(filter.toString(), it) }
            .build()
    }
}

/**
 * Asserts which ids a corpus wire fixture's filter selects. The expectation is written out per
 * action, hand-derived from what CEL would decide over [Scalars]'s rows — never recomputed from
 * the seed data, which would only re-state the translation and pass whatever it emitted.
 */
internal fun assertSelects(action: String, vararg expected: String) {
    assertEquals(expected.toList().sorted(), Scalars.ids(action), action)
}

/** [assertSelects] under caller-supplied options. */
internal fun assertSelects(action: String, options: Options, vararg expected: String) {
    assertEquals(expected.toList().sorted(), Scalars.ids(action, options), action)
}

class ScalarFixtureTest {

    @Test
    fun `the fixture discriminates - no seeded column is constant across the rows`() {
        Scalars.onH2 {
            val rows = Scalars.Docs.selectAll().toList()
            assertEquals(9, rows.size)
            assertTrue(rows.map { it[Scalars.Docs.aOptionalString] }.contains(null), "a NULL witness is required")
            assertTrue(rows.count { it[Scalars.Docs.aBool] } in 1 until rows.size, "aBool must split the rows")
            assertEquals(
                setOf("one", "One"),
                rows.map { it[Scalars.Docs.aString] }.filter { it.equals("one", ignoreCase = true) }.toSet(),
                "a case-only pair is what proves CEL string equality is exact",
            )
        }
    }

    @Test
    fun `the two null conventions are mapped onto the same columns, as the corpus maps them`() {
        val owner = Scalars.MAPPING.resolve("request.resource.attr.owner") as AttributeMapping.Field
        val omitted = Scalars.MAPPING.resolve("request.resource.attr.aOptionalString") as AttributeMapping.Field
        assertEquals(owner.column, omitted.column, "one column, two attribute names, two conventions")
        assertEquals(NullAttributeRepresentation.EXPLICIT, owner.nullAttributeRepresentation)
        assertEquals(null, omitted.nullAttributeRepresentation)
    }
}
