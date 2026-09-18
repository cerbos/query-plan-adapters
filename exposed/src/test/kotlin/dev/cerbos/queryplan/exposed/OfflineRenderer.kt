package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.vendors.MysqlDialect
import org.jetbrains.exposed.v1.core.vendors.PostgreSQLDialect
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.util.concurrent.ConcurrentHashMap

/**
 * Renders an `Op<Boolean>` to SQL and typed bind arguments for every dialect this adapter claims,
 * with **no database server**.
 *
 * The translator unit test needs this because Exposed makes rendering a transaction-scoped act:
 * `Column.toQueryBuilder` reaches for `currentTransaction()`, and the identifiers it emits are
 * quoted and case-folded through `db.identifierManager`, which the JDBC `Database` derives from
 * `DatabaseMetaData` on a live connection. JetBrains' own FAQ says a connection is required to
 * generate SQL. So "offline" here means *no server*, not *no connection*:
 *
 * - **SQLite and H2** get real in-process connections. Both drivers are test dependencies and the
 *   databases live in memory, so nothing is started and nothing is reached over a socket — the
 *   same thing activerecord's translator spec does with in-memory SQLite.
 * - **PostgreSQL and MySQL** get a stub [Connection] and [DatabaseMetaData] ([StubDatabase]) that
 *   answer the handful of metadata questions Exposed asks with the values the real drivers give,
 *   and the real `PostgreSQLDialect` / `MysqlDialect` selected through
 *   `DatabaseConfig { explicitDialect = … }` — which short-circuits the only other metadata read on
 *   the rendering path. `OfflineRendererTest` proves the stubs honest against real servers.
 *
 * H2's compatibility modes are deliberately NOT used as a stand-in: `MODE=PostgreSQL` still renders
 * through `H2Dialect`, so the asset would pin SQL that no database in this repository executes.
 *
 * Rendering is **prepared** (`QueryBuilder(prepared = true)`), so every constant is recorded as a
 * bind argument with the Exposed column type it is bound through rather than inlined into the text.
 * That type is the point: a double bound as a decimal and a double bound as a double render to the
 * same `?`, and only one of them compares the way CEL does.
 */
internal object OfflineRenderer {

    const val SQLITE: String = "sqlite"
    const val H2: String = "h2"
    const val POSTGRESQL: String = "postgresql"
    const val MYSQL: String = "mysql"

    /**
     * The dialect keys a golden entry carries, in the order it records them: the two that render
     * through a real driver first, then the two that render through a stub.
     */
    val DIALECTS: List<String> = listOf(SQLITE, H2, POSTGRESQL, MYSQL)

    /**
     * Translates once with [build] and renders the result under every dialect.
     *
     * [build] is called with **no transaction open**, which is an assertion and not an accident:
     * translation must not read the dialect, because the predicate it returns renders later, inside
     * the caller's transaction, for whatever dialect that transaction has. A translator that
     * reached for `currentDialect` would fail here rather than quietly pinning four identical
     * renderings taken from whichever dialect happened to be open.
     */
    fun render(build: () -> Op<Boolean>): Map<String, Rendered> {
        check(TransactionManager.currentOrNull() == null) {
            "OfflineRenderer.render must translate outside any transaction: translation must not read the dialect"
        }
        return render(build())
    }

    /** [render], for a predicate that has already been translated. */
    fun render(op: Op<Boolean>): Map<String, Rendered> =
        DIALECTS.associateWith { renderOn(it, op) }

    /** [op] as SQL and bind arguments under one [dialect] of [DIALECTS]. */
    fun renderOn(dialect: String, op: Op<Boolean>): Rendered = renderOn(database(dialect), op)

    /**
     * [op] as SQL and bind arguments inside a transaction on [database], whichever database that is.
     *
     * Taking the database rather than a dialect key is what lets `OfflineRendererTest` point the
     * same renderer at a real server and compare the bytes.
     */
    fun renderOn(database: Database, op: Op<Boolean>): Rendered = transaction(database) {
        val builder = QueryBuilder(prepared = true)
        builder.append(op)
        Rendered(
            sql = builder.toString(),
            params = builder.args.map { (type, value) ->
                RenderedParam.of(type::class.simpleName ?: type::class.java.name, value)
            },
        )
    }

    /**
     * One `Database` per dialect, reused across renderings.
     *
     * Reuse is not only about speed: `Database.identifierManager` is a `lazy`, so a fresh `Database`
     * per rendering would re-read `DatabaseMetaData` for every action.
     */
    private val databases = ConcurrentHashMap<String, Database>()

    private fun database(dialect: String): Database = databases.computeIfAbsent(dialect) {
        when (it) {
            SQLITE -> Database.connect("jdbc:sqlite::memory:", driver = "org.sqlite.JDBC")
            H2 -> Database.connect("jdbc:h2:mem:cerbos_offline_render;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            POSTGRESQL -> StubDatabase.POSTGRESQL.connect()
            MYSQL -> StubDatabase.MYSQL.connect()
            else -> throw IllegalArgumentException("Unknown dialect '$it', expected one of $DIALECTS")
        }
    }
}

/** One rendering of one predicate: the SQL text, and the arguments bound into it, in order. */
internal class Rendered(val sql: String, val params: List<RenderedParam>) {
    /**
     * The positions in [params] whose value JSON cannot hold, so a suite can pin *where* the asset
     * is a stand-in rather than the bytes. See [RenderedParam.normalisedFrom].
     */
    val normalisedParams: List<Int>
        get() = params.indices.filter { params[it].normalisedFrom != null }

    override fun toString(): String = "$sql  $params"
}

/**
 * One bound argument: the simple name of the Exposed column type it was bound through, and its
 * value in a form JSON round-trips.
 */
internal class RenderedParam(
    val type: String,
    val value: Any?,
    /**
     * The `Double` [value] stands in for, when the encoding cannot hold it — `-0.0`, `NaN` and the
     * infinities are all outside JSON's number grammar, and `-0.0` in particular is a value IEEE
     * division depends on and a reader would never guess from a recorded `0.0`. The stand-in is the
     * double's own text, so the asset stays lossless, and [Rendered.normalisedParams] is what lets a
     * test pin the list of places it happens rather than trusting the encoder.
     */
    val normalisedFrom: String?,
) {
    override fun equals(other: Any?): Boolean =
        other is RenderedParam && other.type == type && other.value == value

    override fun hashCode(): Int = 31 * type.hashCode() + value.hashCode()

    override fun toString(): String = "$type=$value"

    companion object {
        fun of(type: String, value: Any?): RenderedParam = when {
            value == null -> RenderedParam(type, null, null)
            value is Double && !value.isFinite() -> RenderedParam(type, value.toString(), value.toString())
            value is Double && value == 0.0 && 1.0 / value < 0.0 -> RenderedParam(type, "-0.0", "-0.0")
            value is Double -> RenderedParam(type, value, null)
            value is Float -> of(type, value.toDouble())
            value is String || value is Boolean -> RenderedParam(type, value, null)
            value is Long -> RenderedParam(type, value, null)
            value is Byte || value is Short || value is Int -> RenderedParam(type, (value as Number).toLong(), null)
            // Anything else — a temporal, a BigDecimal, an EntityID — is recorded as its own text.
            // The type name beside it is what says which, so the asset never has to guess.
            else -> RenderedParam(type, value.toString(), null)
        }
    }
}

/**
 * A `Connection` and `DatabaseMetaData` that answer only what Exposed asks on the rendering path,
 * with the values a real server gives, and raise on everything else.
 *
 * Raising is the design. An Exposed upgrade that starts asking a new metadata question would
 * otherwise get a plausible default and quietly move the SQL this asset pins; instead the suite
 * fails naming the method, and someone decides what the real answer is.
 *
 * Every value below was read from the pinned images — `exposed/POSTGRES_IMAGE` and
 * `exposed/MYSQL_IMAGE` — through the real JDBC drivers. `OfflineRendererTest` re-reads them from
 * those same images under `@Tag("docker")` and asserts both that each answer still matches and that
 * the SQL the stub renders is byte-identical to the SQL the real connection renders.
 */
internal class StubDatabase private constructor(
    val key: String,
    /**
     * Distinct per stub, and distinctive.
     *
     * `JdbcDatabaseMetadataImpl` caches identifier managers in a **process-wide** map keyed by the
     * connection URL, and an `IdentifierManagerApi`'s keyword set is computed once, under whichever
     * dialect reached it first. Two stubs sharing a URL would therefore share one manager and one
     * dialect's keywords.
     */
    val url: String,
    val catalog: String,
    private val metadataAnswers: Map<String, Any?>,
    private val newDialect: () -> org.jetbrains.exposed.v1.core.vendors.DatabaseDialect,
) {
    /** The metadata methods this stub answers, for the honesty check in `OfflineRendererTest`. */
    val answeredMetadataMethods: Set<String> get() = metadataAnswers.keys

    fun answer(method: String): Any? = metadataAnswers.getValue(method)

    fun connect(): Database = Database.connect(
        getNewConnection = ::connection,
        // Short-circuits `Database.dialect`, which would otherwise read `databaseDialectName` off
        // the driver name. It is also what makes this a PostgreSQL/MySQL rendering rather than a
        // rendering of whatever dialect a stubbed driver name resolved to.
        databaseConfig = DatabaseConfig { explicitDialect = newDialect() },
    )

    fun connection(): Connection {
        val state = ConnectionState()
        lateinit var connection: Connection
        val metadata = proxy(DatabaseMetaData::class.java, "DatabaseMetaData") { method, _ ->
            when (val name = method.name) {
                "getConnection" -> connection
                else -> if (metadataAnswers.containsKey(name)) metadataAnswers[name] else refuse("DatabaseMetaData", name)
            }
        }
        connection = proxy(Connection::class.java, "Connection") { method, args ->
            when (val name = method.name) {
                "getMetaData" -> metadata
                "getCatalog" -> catalog
                "getSchema" -> null
                "getTransactionIsolation" -> state.isolation
                "setTransactionIsolation" -> state.isolation = args[0] as Int
                "isReadOnly" -> state.readOnly
                "setReadOnly" -> state.readOnly = args[0] as Boolean
                "getAutoCommit" -> state.autoCommit
                "setAutoCommit" -> state.autoCommit = args[0] as Boolean
                // Nothing is ever executed on this connection, so there is nothing to commit or
                // roll back; Exposed calls both around the block that does the rendering.
                "commit", "rollback" -> Unit
                "isClosed" -> state.closed
                "close" -> state.closed = true
                else -> refuse("Connection", name)
            }
        }
        return connection
    }

    private class ConnectionState {
        var isolation: Int = Connection.TRANSACTION_READ_COMMITTED
        var readOnly: Boolean = false
        var autoCommit: Boolean = true
        var closed: Boolean = false
    }

    private fun <T> proxy(type: Class<T>, label: String, body: (Method, Array<out Any?>) -> Any?): T {
        val handler = InvocationHandler { self, method, args ->
            when (method.name) {
                "toString" -> "$label stub for $key"
                "hashCode" -> System.identityHashCode(self)
                "equals" -> self === args?.get(0)
                else -> body(method, args ?: emptyArray())
            }
        }
        @Suppress("UNCHECKED_CAST")
        return Proxy.newProxyInstance(StubDatabase::class.java.classLoader, arrayOf(type), handler) as T
    }

    private fun refuse(type: String, method: String): Nothing = throw UnsupportedOperationException(
        "The offline $key stub does not answer $type.$method(). Exposed asked a metadata question" +
            " this stub was not built for: find the value a real $key server gives, add it to" +
            " StubDatabase, and let OfflineRendererTest's Docker leg check it.",
    )

    companion object {
        /**
         * PostgreSQL 16 through `org.postgresql:postgresql`.
         *
         * `sqlKeywords` is the server's own `pg_get_keywords()` list, not a driver constant, which
         * is why the Docker leg re-reads it: a PostgreSQL major bump can move it, and the list
         * decides which identifiers Exposed quotes.
         */
        val POSTGRESQL: StubDatabase = StubDatabase(
            key = "postgresql",
            url = "jdbc:postgresql://cerbos-offline-renderer/postgres",
            catalog = "test",
            metadataAnswers = linkedMapOf(
                "getURL" to "jdbc:postgresql://cerbos-offline-renderer/postgres",
                "getIdentifierQuoteString" to "\"",
                "storesUpperCaseIdentifiers" to false,
                "storesUpperCaseQuotedIdentifiers" to false,
                "storesLowerCaseIdentifiers" to true,
                "storesLowerCaseQuotedIdentifiers" to false,
                "supportsMixedCaseIdentifiers" to false,
                "supportsMixedCaseQuotedIdentifiers" to true,
                "getExtraNameCharacters" to "",
                "getDatabaseProductName" to "PostgreSQL",
                "getDatabaseProductVersion" to "16",
                "getDatabaseMajorVersion" to 16,
                "getDatabaseMinorVersion" to 0,
                "getMaxColumnNameLength" to 63,
                "getSQLKeywords" to POSTGRES_KEYWORDS,
            ),
            newDialect = { PostgreSQLDialect() },
        )

        /** MySQL 8.4 through `com.mysql:mysql-connector-j`. `sqlKeywords` is a driver constant. */
        val MYSQL: StubDatabase = StubDatabase(
            key = "mysql",
            url = "jdbc:mysql://cerbos-offline-renderer/mysql",
            catalog = "test",
            metadataAnswers = linkedMapOf(
                "getURL" to "jdbc:mysql://cerbos-offline-renderer/mysql",
                "getIdentifierQuoteString" to "`",
                "storesUpperCaseIdentifiers" to false,
                "storesUpperCaseQuotedIdentifiers" to false,
                // `lower_case_table_names=0`, which is the default on the Linux image this
                // repository pins and the only setting under which `=` on an identifier is
                // case-exact the way CEL is.
                "storesLowerCaseIdentifiers" to false,
                "storesLowerCaseQuotedIdentifiers" to false,
                "supportsMixedCaseIdentifiers" to true,
                "supportsMixedCaseQuotedIdentifiers" to true,
                "getExtraNameCharacters" to "$",
                "getDatabaseProductName" to "MySQL",
                "getDatabaseProductVersion" to "8.4",
                "getDatabaseMajorVersion" to 8,
                "getDatabaseMinorVersion" to 4,
                "getMaxColumnNameLength" to 64,
                "getSQLKeywords" to MYSQL_KEYWORDS,
            ),
            newDialect = { MysqlDialect() },
        )

        /** Both stubs, for the suites that sweep them. */
        val ALL: List<StubDatabase> = listOf(POSTGRESQL, MYSQL)
    }
}

/** `DatabaseMetaData.getSQLKeywords()` from PostgreSQL 16 — the server's `pg_get_keywords()` list. */
private const val POSTGRES_KEYWORDS: String =
    "abort,absent,access,aggregate,also,analyse,analyze,attach,backward,bit,cache,checkpoint,class," +
        "cluster,columns,comment,comments,compression,concurrently,configuration,conflict,connection," +
        "content,conversion,copy,cost,csv,current_catalog,current_schema,database,delimiter,delimiters," +
        "depends,detach,dictionary,disable,discard,do,document,enable,encoding,encrypted,enum,event," +
        "exclusive,explain,expression,extension,family,finalize,force,format,forward,freeze,functions," +
        "generated,greatest,groups,handler,header,if,ilike,immutable,implicit,import,include,indent," +
        "index,indexes,inherit,inherits,inline,instead,isnull,json,json_array,json_arrayagg,json_object," +
        "json_objectagg,keys,label,leakproof,least,limit,listen,load,location,lock,locked,logged,mapping," +
        "materialized,mode,move,nfc,nfd,nfkc,nfkd,nothing,notify,notnull,nowait,off,offset,oids,operator," +
        "owned,owner,parallel,parser,passing,password,plans,policy,prepared,procedural,procedures,program," +
        "publication,quote,reassign,recheck,refresh,reindex,rename,replace,replica,reset,restrict," +
        "returning,routines,rule,scalar,schemas,sequences,server,setof,share,show,skip,snapshot,stable," +
        "standalone,statistics,stdin,stdout,storage,stored,strict,strip,subscription,support,sysid,tables," +
        "tablespace,temp,template,text,truncate,trusted,types,unencrypted,unlisten,unlogged,until,vacuum," +
        "valid,validate,validator,variadic,verbose,version,views,volatile,whitespace,wrapper,xml," +
        "xmlattributes,xmlconcat,xmlelement,xmlexists,xmlforest,xmlnamespaces,xmlparse,xmlpi,xmlroot," +
        "xmlserialize,xmltable,yes"

/** `DatabaseMetaData.getSQLKeywords()` from MySQL Connector/J against MySQL 8.4. */
private const val MYSQL_KEYWORDS: String =
    "ACCESSIBLE,ADD,ANALYZE,ASC,BEFORE,CASCADE,CHANGE,CONTINUE,DATABASE,DATABASES,DAY_HOUR," +
        "DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DELAYED,DESC,DISTINCTROW,DIV,DUAL,ELSEIF,EMPTY,ENCLOSED," +
        "ESCAPED,EXIT,EXPLAIN,FIRST_VALUE,FLOAT4,FLOAT8,FORCE,FULLTEXT,GENERATED,GROUPS,HIGH_PRIORITY," +
        "HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,INDEX,INFILE,INT1,INT2,INT3,INT4,INT8," +
        "IO_AFTER_GTIDS,IO_BEFORE_GTIDS,ITERATE,JSON_TABLE,KEY,KEYS,KILL,LAG,LAST_VALUE,LEAD,LEAVE,LIMIT," +
        "LINEAR,LINES,LOAD,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MAXVALUE,MEDIUMBLOB,MEDIUMINT," +
        "MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,NO_WRITE_TO_BINLOG,NTH_VALUE,NTILE," +
        "OPTIMIZE,OPTIMIZER_COSTS,OPTION,OPTIONALLY,OUTFILE,PURGE,QUALIFY,READ,READ_WRITE,REGEXP,RENAME," +
        "REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RLIKE,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SEPARATOR,SHOW," +
        "SIGNAL,SPATIAL,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STORED," +
        "STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,UNDO,UNLOCK,UNSIGNED,USAGE,USE,UTC_DATE," +
        "UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,VIRTUAL,WHILE,WRITE,XOR,YEAR_MONTH,ZEROFILL"
