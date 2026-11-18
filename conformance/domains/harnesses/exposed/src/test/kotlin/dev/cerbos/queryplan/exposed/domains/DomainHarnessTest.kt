package dev.cerbos.queryplan.exposed.domains

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse
import dev.cerbos.queryplan.exposed.ExposedQueryPlanAdapter
import dev.cerbos.queryplan.exposed.MalformedPlanException
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.QueryPlanFilter
import dev.cerbos.queryplan.exposed.UnmappedAttributeException
import dev.cerbos.queryplan.exposed.UnsupportedPlanShapeException
import org.jetbrains.exposed.v1.core.BooleanColumnType
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.DecimalColumnType
import org.jetbrains.exposed.v1.core.DoubleColumnType
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.LongColumnType
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.time.Instant
import kotlin.io.path.extension
import kotlin.io.path.readText

internal interface ExposedDomain {
    val name: String
    val tables: List<Table>
    val root: Table
    val id: Column<String>
    fun options(): Options
}

internal class DomainHarnessTest {
    private val json = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    private val store = System.getenv("ADAPTER_DOMAIN_STORE")?.ifBlank { null } ?: "h2"
    private val domainsDir = Path.of(System.getProperty("user.dir"), "..", "..").normalize()
    private val domains: List<ExposedDomain> = listOf(
        Healthcare, Fintech, Logistics, Education, Legal, Retail, Hr, Media, Iot, Government,
    )

    @Test
    fun replayDomains() {
        val only = (System.getenv("ADAPTER_DOMAIN_ONLY") ?: "").split(",").filter { it.isNotBlank() }.toSet()
        val results = mutableListOf<ObjectNode>()
        for (d in domains) {
            if (only.isNotEmpty() && d.name !in only) continue
            val db = load(d)
            for (golden in goldens(d.name)) results += replay(db, d, golden)
        }
        results.sortWith(compareBy({ it["domain"].asText() }, { it["pdp"].asText() }, { it["principal"].asText() }, { it["action"].asText() }))
        val out = json.createObjectNode()
        out.put("adapter", "exposed")
        out.put("store", store)
        val arr = out.putArray("results")
        results.forEach { arr.add(it) }
        val dir = domainsDir.resolve("results/exposed")
        Files.createDirectories(dir)
        json.writeValue(dir.resolve(store + (if (only.isEmpty()) "" else ".partial") + ".json").toFile(), out)
        results.groupingBy { it["verdict"].asText() }.eachCount().forEach { (k, v) -> println("==> $store $k: $v") }
    }

    private fun replay(db: Database, d: ExposedDomain, golden: JsonNode): ObjectNode {
        val r = json.createObjectNode()
        r.put("domain", d.name)
        r.put("pdp", golden["pdp"].asText())
        r.put("principal", golden["principalName"].asText())
        r.put("action", golden["action"].asText())
        val allowed = golden["allowed"].map { it.asText() }.toSortedSet()
        val returned = sortedSetOf<String>()
        var sql: String? = null
        val (verdict, detail) = try {
            val filter = ExposedQueryPlanAdapter.toFilter(plan(golden), d.options())
            transaction(db) {
                val query = when (filter) {
                    QueryPlanFilter.AlwaysDenied -> null
                    QueryPlanFilter.AlwaysAllowed -> d.root.select(d.id)
                    is QueryPlanFilter.Conditional -> d.root.select(d.id).where(filter.op)
                }
                if (query != null) {
                    sql = query.prepareSQL(this, prepared = false)
                    query.forEach { returned += it[d.id] }
                }
            }
            when {
                !allowed.containsAll(returned) -> "over-grant" to "extra ${returned - allowed}; sql: $sql"
                returned == allowed -> "pass" to ""
                else -> "under-grant" to "missing ${allowed - returned}; sql: $sql"
            }
        } catch (e: UnsupportedPlanShapeException) {
            "refused" to "${e.javaClass.simpleName}: ${e.message}"
        } catch (e: UnmappedAttributeException) {
            "refused" to "${e.javaClass.simpleName}: ${e.message}"
        } catch (e: MalformedPlanException) {
            "refused" to "${e.javaClass.simpleName}: ${e.message}"
        } catch (e: Exception) {
            var root: Throwable = e
            while (root.cause != null && root.cause !== root) root = root.cause!!
            "error" to "${e.javaClass.simpleName}: ${e.message}" +
                (if (root !== e) " | cause ${root.javaClass.simpleName}: ${root.message}" else "") +
                (sql?.let { "; sql: $it" } ?: "")
        }
        r.put("verdict", verdict)
        r.putArray("returned").also { a -> returned.forEach { a.add(it) } }
        r.putArray("allowed").also { a -> allowed.forEach { a.add(it) } }
        r.put("detail", detail)
        return r
    }

    private fun plan(golden: JsonNode): PlanResourcesResponse {
        val filter = PlanResourcesFilter.newBuilder()
        JsonFormat.parser().merge(golden["plan"].toString(), filter)
        return PlanResourcesResponse.newBuilder().setFilter(filter).build()
    }

    private fun goldens(domain: String): List<JsonNode> =
        Files.walk(domainsDir.resolve(domain).resolve("golden")).use { s ->
            s.filter { it.extension == "json" }.sorted().map { json.readTree(it.toFile()) }.toList()
        }

    private fun load(d: ExposedDomain): Database {
        val sqlFile = domainsDir.resolve(d.name).resolve("generated/sql/$store.sql")
        return when (store) {
            "h2" -> {
                val url = "jdbc:h2:mem:${d.name};DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE"
                runSql(url, "sa", "", sqlFile)
                Database.connect(url, driver = "org.h2.Driver", user = "sa")
            }
            "postgres" -> {
                val base = "jdbc:postgresql://localhost:${env("ADAPTER_DOMAIN_PG_PORT", "45432")}/postgres"
                DriverManager.getConnection(base, "postgres", "postgres").use { c ->
                    c.createStatement().use { s ->
                        s.execute("DROP SCHEMA IF EXISTS ${d.name} CASCADE")
                        s.execute("CREATE SCHEMA ${d.name}")
                    }
                }
                val url = "$base?currentSchema=${d.name}"
                runSql(url, "postgres", "postgres", sqlFile)
                Database.connect(url, driver = "org.postgresql.Driver", user = "postgres", password = "postgres")
            }
            "mysql" -> {
                val base = "jdbc:mysql://localhost:${env("ADAPTER_DOMAIN_MYSQL_PORT", "43306")}/"
                fun create(collation: String?) = DriverManager.getConnection(base, "root", "root").use { c ->
                    c.createStatement().use { s ->
                        s.execute("DROP DATABASE IF EXISTS ${d.name}")
                        s.execute("CREATE DATABASE ${d.name}" + (collation?.let { " COLLATE $it" } ?: ""))
                    }
                }
                val url = "$base${d.name}?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true"
                create(null)
                try {
                    runSql(url, "root", "root", sqlFile)
                } catch (e: java.sql.SQLIntegrityConstraintViolationException) {
                    // Keys differing only by case need a binary collation on MySQL (government's DSS/dss).
                    println("==> ${d.name}: ${e.message}; reloading under utf8mb4_0900_bin")
                    create("utf8mb4_0900_bin")
                    runSql(url, "root", "root", sqlFile)
                }
                Database.connect(url, driver = "com.mysql.cj.jdbc.Driver", user = "root", password = "root")
            }
            "sqlite" -> {
                // Exposed has its own SQLite datetime text format, so the rows go through Exposed.
                val file = Files.createTempFile("domain-${d.name}", ".db")
                val db = Database.connect(
                    "jdbc:sqlite:$file", driver = "org.sqlite.JDBC",
                    setupConnection = { c -> c.createStatement().use { it.execute("PRAGMA case_sensitive_like = ON") } },
                )
                loadRows(db, d)
                db
            }
            else -> error("unknown store $store")
        }
    }

    private fun env(name: String, fallback: String) = System.getenv(name)?.ifBlank { null } ?: fallback

    private fun runSql(url: String, user: String, password: String, file: Path) {
        DriverManager.getConnection(url, user, password).use { c ->
            c.createStatement().use { s ->
                for (stmt in file.readText().split(Regex(";\\s*\\n"))) {
                    val body = stmt.lines().filterNot { it.startsWith("--") }.joinToString("\n").trim()
                    if (body.isNotEmpty()) s.execute(body)
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun loadRows(db: Database, d: ExposedDomain) {
        val rows = json.readTree(domainsDir.resolve(d.name).resolve("generated/rows.json").toFile())
        transaction(db) {
            SchemaUtils.create(*d.tables.toTypedArray())
            for (t in rows["tables"]) {
                val table = d.tables.first { it.tableName.trim('"', '`') == t["name"].asText() }
                for (row in t["rows"]) {
                    table.insert { st ->
                        row.fields().forEach { (name, v) ->
                            val col = table.columns.first { it.name.trim('"', '`') == name } as Column<Any?>
                            st[col] = convert(col, v)
                        }
                    }
                }
            }
        }
    }

    private fun convert(col: Column<*>, v: JsonNode): Any? {
        if (v.isNull) return null
        val type = col.columnType
        return when {
            type is IntegerColumnType -> v.asInt()
            type is LongColumnType -> v.asLong()
            type is DoubleColumnType -> v.asDouble()
            type is DecimalColumnType -> BigDecimal(v.asText())
            type is BooleanColumnType -> v.asBoolean()
            type.javaClass.simpleName.contains("Instant") || type.javaClass.simpleName.contains("Timestamp") -> Instant.parse(v.asText())
            v.isContainerNode -> v.toString()
            else -> v.asText()
        }
    }
}
