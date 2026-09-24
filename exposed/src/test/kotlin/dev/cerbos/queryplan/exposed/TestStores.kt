package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.mysql.MySQLContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import java.nio.file.Files

/**
 * The database the conformance corpus is replayed against, selected by `adapter.test.db`
 * (forwarded from `ADAPTER_TEST_DB` by the build).
 *
 * Four stores, because the translator's output is dialect-dependent in ways only execution shows:
 * collation governs `=`, a per-connection pragma governs SQLite's `LIKE`, cast targets and
 * parameter typing differ per engine, and MySQL's client-side prepared statements can turn a double
 * bind into a decimal. A store the workflow does not execute is a store this adapter does not
 * cover.
 *
 * An unknown value FAILS rather than falling back: a typo that quietly ran H2 would report a
 * PostgreSQL leg green without ever starting PostgreSQL.
 */
internal class TestStore(
    /** The `adapter.test.db` value that selected this store. */
    val name: String,
    val database: Database,
    /**
     * The engine's own banner query, and the first word its answer must carry. Each spelling is a
     * syntax error or a different answer on the other three engines, so the anti-vacuity assertion
     * fails in both directions rather than only when a container failed to start.
     */
    private val bannerQuery: String,
    val expectedEngine: String,
    /** The MySQL server collation this leg pinned, for the run summary. Null off MySQL. */
    val collation: String?,
    private val container: JdbcDatabaseContainer<*>?,
    private val onStop: () -> Unit = {},
) {
    /** The banner, asked of the connection the suite actually queries through. */
    fun serverBanner(): String = transaction(database) {
        exec(bannerQuery) { rs -> if (rs.next()) rs.getString(1) else null }
    } ?: error("$name: the server banner query returned no row")

    fun stop() {
        onStop()
        container?.stop()
    }

    companion object {
        const val PROPERTY: String = "adapter.test.db"
        const val COLLATION_PROPERTY: String = "adapter.test.mysql.collation"
        const val SERVER_PREP_STMTS_PROPERTY: String = "adapter.test.mysql.serverPrepStmts"

        /**
         * MySQL's own default, `utf8mb4_0900_ai_ci`, makes `=` itself case- and accent-insensitive
         * while CEL's is byte-exact. That is a store misconfiguration, not an adapter limitation,
         * so the leg pins the byte-exact NO PAD collation; overriding it with the default
         * reproduces the documented over-grant and measures what it costs. Case- and
         * accent-sensitive is not enough: `utf8mb4_0900_as_cs` gives a soft hyphen no weight, so
         * seed h6 (`"o\u00ADne"`) equals `"one"` under it (cerbos/query-plan-adapters#474).
         */
        const val DEFAULT_MYSQL_COLLATION: String = "utf8mb4_0900_bin"

        fun selected(): String = System.getProperty(PROPERTY, "h2")

        fun open(): TestStore = when (val store = selected()) {
            "h2" -> h2()
            "sqlite" -> sqlite()
            "postgres" -> postgres()
            "mysql" -> mysql()
            else -> throw IllegalArgumentException(
                "Unknown $PROPERTY '$store' (expected h2, sqlite, postgres or mysql)",
            )
        }

        private fun h2(): TestStore = TestStore(
            name = "h2",
            // DB_CLOSE_DELAY=-1 because Exposed opens a connection per transaction and an in-memory
            // H2 database is dropped when its last connection closes.
            database = Database.connect("jdbc:h2:mem:adversarial;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver"),
            bannerQuery = "select 'H2 ' || h2version()",
            expectedEngine = "H2",
            collation = null,
            container = null,
        )

        /**
         * A temp FILE rather than `:memory:`: Exposed opens a connection per transaction, and each
         * connection to `:memory:` gets a database of its own.
         *
         * `PRAGMA case_sensitive_like` is set on every connection because it is per-connection and
         * this adapter lowers string matching to `LIKE`. SQLite's `LIKE` is case-insensitive for
         * ASCII whatever collation the column was created with, so without it `cs-contains` and its
         * siblings return a row the PDP denies — the whole-lever point
         * `conformance/README.md` makes under "Case sensitivity is two invariants, not one".
         */
        private fun sqlite(): TestStore {
            val file = Files.createTempFile("cerbos-exposed-adversarial", ".db")
            Files.deleteIfExists(file)
            return TestStore(
                name = "sqlite",
                database = Database.connect(
                    "jdbc:sqlite:$file",
                    driver = "org.sqlite.JDBC",
                    setupConnection = { connection ->
                        connection.createStatement().use { it.execute("PRAGMA case_sensitive_like = ON") }
                    },
                ),
                bannerQuery = "select 'SQLite ' || sqlite_version()",
                expectedEngine = "SQLite",
                collation = null,
                container = null,
                onStop = { Files.deleteIfExists(file) },
            )
        }

        private fun postgres(): TestStore {
            val container = PostgreSQLContainer(DatabaseTestImages.POSTGRES)
            container.start()
            return TestStore(
                name = "postgres",
                database = connect(container),
                bannerQuery = "select version()",
                expectedEngine = "PostgreSQL",
                collation = null,
                container = container,
            )
        }

        private fun mysql(): TestStore {
            val collation = System.getProperty(COLLATION_PROPERTY, DEFAULT_MYSQL_COLLATION)
            val container = MySQLContainer(DatabaseTestImages.MYSQL)
                .withCommand("--character-set-server=utf8mb4", "--collation-server=$collation")
            // Connector/J's DEFAULT client-side prepared statements interpolate a double bind as a
            // DECIMAL literal, which evaluates the adapter's double arithmetic in exact decimal:
            // `3 * 0.1 == 0.3` becomes TRUE where CEL's IEEE semantics say FALSE. Running
            // client-side by default makes the oracle pin whatever fix the translator carries;
            // setting this property replays the same corpus under server-side statements, and both
            // modes must agree with the check() oracle.
            if (java.lang.Boolean.getBoolean(SERVER_PREP_STMTS_PROPERTY)) {
                container.withUrlParam("useServerPrepStmts", "true")
            }
            container.start()
            return TestStore(
                name = "mysql",
                database = connect(container),
                // `@@version_comment` rather than `version()`: the latter answers `8.4.x` here and
                // would make the assertion a version check, while this one names the engine and is
                // a syntax error on the other three.
                bannerQuery = "select @@version_comment",
                expectedEngine = "MySQL",
                collation = collation,
                container = container,
            )
        }

        private fun connect(container: JdbcDatabaseContainer<*>): Database = Database.connect(
            url = container.jdbcUrl,
            driver = container.driverClassName,
            user = container.username,
            password = container.password,
        )
    }
}
