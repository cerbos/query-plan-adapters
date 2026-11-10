package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

/**
 * Renders an `Op<Boolean>` to prepared SQL on in-memory SQLite and H2. Exposed needs a connection
 * to render; neither needs a server.
 */
internal object OfflineRenderer {

    const val SQLITE: String = "sqlite"
    const val H2: String = "h2"

    val DIALECTS: List<String> = listOf(SQLITE, H2)

    private val databases: Map<String, Database> by lazy {
        mapOf(
            SQLITE to Database.connect("jdbc:sqlite::memory:", driver = "org.sqlite.JDBC"),
            H2 to Database.connect("jdbc:h2:mem:cerbos_offline_render;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver"),
        )
    }

    /** Runs [build] with no transaction open: translation must not read the dialect. */
    fun <T> translate(build: () -> T): T {
        check(TransactionManager.currentOrNull() == null) {
            "translation must happen outside any transaction: it must not read the dialect"
        }
        return build()
    }

    fun render(op: Op<Boolean>): Map<String, Rendered> = DIALECTS.associateWith { renderOn(it, op) }

    fun renderOn(dialect: String, op: Op<Boolean>): Rendered = transaction(databases.getValue(dialect)) {
        val builder = QueryBuilder(prepared = true)
        builder.append(op)
        Rendered(builder.toString(), builder.args.map { (type, value) -> RenderedParam(type::class.simpleName!!, value) })
    }
}

internal data class Rendered(val sql: String, val params: List<RenderedParam>)

internal data class RenderedParam(val type: String, val value: Any?)
