package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.AndOp
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Exists
import org.jetbrains.exposed.v1.core.GreaterOp
import org.jetbrains.exposed.v1.core.LikeEscapeOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.OrOp
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.alias
import org.jetbrains.exposed.v1.core.booleanParam
import org.jetbrains.exposed.v1.core.doubleParam
import org.jetbrains.exposed.v1.core.stringParam
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.wait.strategy.WaitStrategy
import java.sql.DatabaseMetaData
import java.sql.DriverManager

/**
 * Proves the offline renderer renders what a real server would.
 *
 * Two halves. The first runs everywhere and pins that the four dialects are genuinely four — that
 * the stubbed PostgreSQL and MySQL connections are not quietly re-rendering somebody else's SQL.
 * The second runs only where Docker is, and is the honesty check the first cannot be: it starts the
 * pinned PostgreSQL and MySQL images, renders the SAME predicates through the real drivers, and
 * asserts the stub's SQL is byte-identical and every metadata answer it gives is the real server's.
 *
 * The predicates here are hand-built rather than translated, and that is not the exception ADR 0006
 * carves out for plans, because none of these is a plan. What is under test is Exposed's renderer
 * and this module's connection stubs, so the input is chosen to exercise the rendering decisions the
 * adapter depends on: identifier case folding, keyword quoting, `LIKE … ESCAPE`, a correlated
 * subquery over an alias, the boolean literal, and a bound double.
 */

/**
 * Tags the cases that start a real database server to cross-check the offline stub. The build
 * excludes it on the store legs; see the banner above those cases.
 */
internal const val SERVER_CROSS_CHECK: String = "server-cross-check"

class OfflineRendererTest {

    private object Resources : Table("offline_resources") {
        val id = varchar("id", 32)
        val aString = varchar("a_string", 64)
        val aNumber = double("a_number")
        val archived = bool("archived")
    }

    private object Tags : Table("offline_tags") {
        val resourceId = varchar("resource_id", 32)
        val name = varchar("name", 64)
    }

    /**
     * Two identifiers whose rendering is decided by metadata rather than by the dialect class.
     *
     * `MixedCase` discriminates the four case-folding flags; `csv` is a keyword to PostgreSQL's
     * server and to nothing else, so it discriminates the driver-supplied keyword list — the one
     * stubbed value that is a property of the SERVER build rather than of the driver, and therefore
     * the one most likely to drift under an image bump.
     */
    private object Awkward : Table("offline_awkward") {
        val mixedCase = varchar("MixedCase", 32)
        val csv = varchar("csv", 32)
    }

    /** The predicates both halves render, named so a divergence says which shape moved. */
    private fun predicates(): Map<String, Op<Boolean>> {
        val tags = Tags.alias("cerbos_1")
        return linkedMapOf(
            "like-escape" to LikeEscapeOp(Resources.aString, stringParam("a\\%b%"), true, '\\'),
            "bound-double" to GreaterOp(Resources.aNumber, doubleParam(1.5)),
            "bound-boolean" to EqOp(Resources.archived, booleanParam(false)),
            "boolean-literal" to OrOp(listOf(Op.TRUE, Op.FALSE)),
            "correlated-subquery" to Exists(
                tags.select(tags[Tags.name]).where(EqOp(tags[Tags.resourceId], Resources.id)),
            ),
            "mixed-case-identifier" to EqOp(Awkward.mixedCase, stringParam("x")),
            "keyword-identifier" to EqOp(Awkward.csv, stringParam("x")),
            "representative" to AndOp(
                listOf(
                    LikeEscapeOp(Resources.aString, stringParam("a\\%b%"), true, '\\'),
                    GreaterOp(Resources.aNumber, doubleParam(1.5)),
                    EqOp(Resources.archived, booleanParam(false)),
                    OrOp(
                        listOf(
                            Exists(
                                tags.select(tags[Tags.name])
                                    .where(EqOp(tags[Tags.resourceId], Resources.id)),
                            ),
                            Op.TRUE,
                        ),
                    ),
                ),
            ),
        )
    }

    private fun renderAll(dialect: String): Map<String, Rendered> =
        predicates().mapValues { (_, op) -> OfflineRenderer.renderOn(dialect, op) }

    private fun renderAll(database: Database): Map<String, Rendered> =
        predicates().mapValues { (_, op) -> OfflineRenderer.renderOn(database, op) }

    // -- no server ------------------------------------------------------------------------------

    @Test
    fun `every dialect renders, with no server and no transaction of the caller's`() {
        val rendered = OfflineRenderer.render { predicates().getValue("representative") }
        assertEquals(OfflineRenderer.DIALECTS.toSet(), rendered.keys)
        rendered.forEach { (dialect, one) ->
            assertTrue(one.sql.contains("LIKE ? ESCAPE ?"), "$dialect dropped the ESCAPE clause: ${one.sql}")
            assertTrue(one.sql.contains("EXISTS (SELECT"), "$dialect dropped the subquery: ${one.sql}")
            assertEquals(
                listOf("TextColumnType", "TextColumnType", "DoubleColumnType", "BooleanColumnType"),
                one.params.map { it.type },
                "$dialect bound the constants through different column types",
            )
        }
    }

    @Test
    fun `translation happens outside any transaction`() {
        // The check is the assertion: a translator that reached for `currentDialect` would need a
        // transaction here, and the four renderings below would then all be one dialect's.
        var transactionsSeen = 0
        OfflineRenderer.render {
            if (TransactionManager.currentOrNull() != null) {
                transactionsSeen++
            }
            predicates().getValue("bound-double")
        }
        assertEquals(0, transactionsSeen)
    }

    @Test
    fun `the stubs render their own dialect, not a stand-in`() {
        val postgres = renderAll(OfflineRenderer.POSTGRESQL)
        val mysql = renderAll(OfflineRenderer.MYSQL)
        val h2 = renderAll(OfflineRenderer.H2)
        val sqlite = renderAll(OfflineRenderer.SQLITE)

        // MySQL quotes with a backtick and nothing else does. A stub that had fallen back to some
        // other dialect's identifier manager would lose this.
        assertTrue(mysql.getValue("correlated-subquery").sql.contains("`name`"))
        assertFalse(postgres.getValue("correlated-subquery").sql.contains("`"))

        // PostgreSQL folds an unquoted identifier to lower case, so a mixed-case one has to be
        // quoted to survive; MySQL stores mixed case as written and leaves it bare.
        assertTrue(postgres.getValue("mixed-case-identifier").sql.contains("\"MixedCase\""))
        assertTrue(mysql.getValue("mixed-case-identifier").sql.contains(".MixedCase "))

        // `csv` is a keyword to the PostgreSQL server alone: this is the driver-supplied keyword
        // list deciding the SQL, which is the stubbed value with the weakest provenance.
        assertTrue(postgres.getValue("keyword-identifier").sql.contains("\"csv\""))
        assertTrue(mysql.getValue("keyword-identifier").sql.contains(".csv "))

        // H2 folds to UPPER case, which no other dialect here does.
        assertTrue(h2.getValue("bound-double").sql.contains("OFFLINE_RESOURCES.A_NUMBER"))

        // SQLite has no boolean literal; every other dialect spells one.
        assertEquals("1 OR 0", sqlite.getValue("boolean-literal").sql)
        assertEquals("TRUE OR FALSE", postgres.getValue("boolean-literal").sql)

        // …and, as a whole, no two dialects agree on the representative predicate.
        val representative = OfflineRenderer.DIALECTS.map { OfflineRenderer.renderOn(it, predicates().getValue("representative")).sql }
        assertEquals(representative.size, representative.toSet().size, "two dialects rendered identically: $representative")
    }

    @Test
    fun `a bind is recorded with the Exposed column type it went through`() {
        val rendered = OfflineRenderer.renderOn(OfflineRenderer.POSTGRESQL, predicates().getValue("bound-double"))
        assertEquals(1, rendered.params.size)
        assertEquals("DoubleColumnType", rendered.params[0].type)
        assertEquals(1.5, rendered.params[0].value)
        assertEquals(emptyList<Int>(), rendered.normalisedParams)
    }

    @Test
    fun `a double JSON cannot hold is recorded as its own text and listed`() {
        val nonJson = listOf(-0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
        val op = OrOp(nonJson.map { GreaterOp(Resources.aNumber, doubleParam(it)) })
        val rendered = OfflineRenderer.renderOn(OfflineRenderer.POSTGRESQL, op)
        assertEquals(listOf("-0.0", "NaN", "Infinity", "-Infinity"), rendered.params.map { it.value })
        assertEquals(listOf("-0.0", "NaN", "Infinity", "-Infinity"), rendered.params.map { it.normalisedFrom })
        assertEquals(listOf(0, 1, 2, 3), rendered.normalisedParams)
        // The point of recording the text rather than a nearby number: `0.0` and `-0.0` are
        // different constants, and IEEE division is the place that notices.
        assertNotEquals(
            RenderedParam.of("DoubleColumnType", 0.0).value,
            RenderedParam.of("DoubleColumnType", -0.0).value,
        )
    }

    @Test
    fun `a metadata question the stub was not built for fails loudly`() {
        val error = assertThrowsUnsupported { StubDatabase.POSTGRESQL.connection().metaData.supportsUnion() }
        assertTrue(error.message!!.contains("DatabaseMetaData.supportsUnion()"), error.message)
        assertTrue(error.message!!.contains("StubDatabase"), error.message)
    }

    // -- against the real servers ---------------------------------------------------------------
    //
    // The two cases below start the pinned PostgreSQL and MySQL images, read from the same
    // `*_IMAGE` files and through the same reader the conformance harness uses, and assert the
    // stub's SQL and every metadata answer it gives against the real driver. They are tagged
    // [SERVER_CROSS_CHECK] and the build excludes that tag on the store legs (ADAPTER_TEST_DB set):
    // the question is a property of the Exposed release and the server image, not of which store
    // the harness runs on, so asking it once per Exposed leg is enough. They skip themselves where
    // there is no Docker at all.

    @Test
    @Tag(SERVER_CROSS_CHECK)
    fun `the PostgreSQL stub renders what PostgreSQL renders`() {
        assertStubMatchesServer(
            stub = StubDatabase.POSTGRESQL,
            dialect = OfflineRenderer.POSTGRESQL,
            image = DatabaseTestImages.POSTGRES,
            port = 5432,
            environment = mapOf("POSTGRES_USER" to "test", "POSTGRES_PASSWORD" to "test", "POSTGRES_DB" to "test"),
            ready = Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2),
            url = { host, port -> "jdbc:postgresql://$host:$port/test" },
        )
    }

    @Test
    @Tag(SERVER_CROSS_CHECK)
    fun `the MySQL stub renders what MySQL renders`() {
        assertStubMatchesServer(
            stub = StubDatabase.MYSQL,
            dialect = OfflineRenderer.MYSQL,
            image = DatabaseTestImages.MYSQL,
            port = 3306,
            environment = mapOf(
                "MYSQL_ROOT_PASSWORD" to "root",
                "MYSQL_DATABASE" to "test",
                "MYSQL_USER" to "test",
                "MYSQL_PASSWORD" to "test",
            ),
            ready = Wait.forLogMessage(".*ready for connections.*port: 3306.*\\n", 1),
            url = { host, port -> "jdbc:mysql://$host:$port/test" },
        )
    }

    /**
     * Metadata this suite deliberately does not compare, each for a reason.
     *
     * Everything else a stub answers IS compared, so an answer cannot be added without a decision
     * about where its value came from.
     */
    private val notComparedToTheServer = mapOf(
        // The stub's URL is deliberately its own: `JdbcDatabaseMetadataImpl` caches identifier
        // managers in a process-wide map keyed by it, and sharing a key with a real connection
        // would hand one of them the other's manager.
        "getURL" to "the stub's URL is deliberately distinct",
        // A patch-level image rebuild moves these, and nothing on the rendering path reads past the
        // major version, so comparing them would fail a suite that has nothing to say about SQL.
        "getDatabaseProductVersion" to "patch level, not read while rendering",
        "getDatabaseMinorVersion" to "patch level, not read while rendering",
    )

    private fun assertStubMatchesServer(
        stub: StubDatabase,
        dialect: String,
        image: DockerImageName,
        port: Int,
        environment: Map<String, String>,
        ready: WaitStrategy,
        url: (String, Int) -> String,
    ) {
        assumeTrue(dockerAvailable(), "Docker is not available")
        val container = GenericContainer(image).withExposedPorts(port).waitingFor(ready)
        environment.forEach { (key, value) -> container.withEnv(key, value) }
        container.start()
        try {
            val jdbcUrl = url(container.host, container.getMappedPort(port))
            DriverManager.getConnection(jdbcUrl, "test", "test").use { connection ->
                assertMetadataMatches(stub, connection.metaData)
            }
            val real = Database.connect(jdbcUrl, user = "test", password = "test")
            val realRenderings = renderAll(real)
            val stubRenderings = renderAll(dialect)
            predicates().keys.forEach { name ->
                assertEquals(
                    realRenderings.getValue(name).sql,
                    stubRenderings.getValue(name).sql,
                    "the ${stub.key} stub renders '$name' differently from a real ${stub.key} server",
                )
                assertEquals(
                    realRenderings.getValue(name).params.map { it.type to it.value },
                    stubRenderings.getValue(name).params.map { it.type to it.value },
                    "the ${stub.key} stub binds '$name' differently from a real ${stub.key} server",
                )
            }
        } finally {
            container.stop()
        }
    }

    /**
     * Every answer the stub gives, read back off the real driver.
     *
     * This is the assertion that keeps the stub a RECORD of a real server rather than a guess about
     * one. Comparing the rendered SQL alone would pass while an answer that happens not to move the
     * SQL for these predicates drifted, and the next corpus action is what would find it.
     */
    private fun assertMetadataMatches(stub: StubDatabase, metadata: DatabaseMetaData) {
        val compared = stub.answeredMetadataMethods - notComparedToTheServer.keys
        assertTrue(compared.isNotEmpty())
        compared.forEach { name ->
            val actual = DatabaseMetaData::class.java.getMethod(name).invoke(metadata)
            assertEquals(stub.answer(name), actual, "${stub.key} stub answers $name() with a value the server does not")
        }
        assertEquals(
            stub.answer("getDatabaseMajorVersion"),
            metadata.databaseMajorVersion,
            "${stub.key} stub was pinned against another major version",
        )
    }

    private fun assertThrowsUnsupported(body: () -> Unit): UnsupportedOperationException =
        assertThrows(UnsupportedOperationException::class.java) { body() }
}
