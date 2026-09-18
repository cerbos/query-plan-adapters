package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * The thinnest end-to-end slice: a planner wire fixture in, an `Op<Boolean>` out, real rows back
 * from H2. It exists to prove the module's seams hold together, and nothing else: which rows a
 * filter must return is the conformance harness's question, and what it emits is the translator
 * unit test's.
 */
class ContractSmokeTest {
    private object Docs : Table("smoke_docs") {
        val id = varchar("id", 32)
        val aString = varchar("a_string", 64)
        val archived = bool("archived")
        override val primaryKey = PrimaryKey(id)
    }

    private val mapping = cerbosMapping {
        "request.resource.attr.aString" to Docs.aString
    }

    @Test
    fun `a wire fixture translates to a predicate that composes with the application's own`() {
        val db = Database.connect("jdbc:h2:mem:smoke;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
        transaction(db) {
            SchemaUtils.create(Docs)
            listOf(Triple("a", "one", false), Triple("b", "One", false), Triple("c", "one", true)).forEach { row ->
                Docs.insert {
                    it[id] = row.first
                    it[aString] = row.second
                    it[archived] = row.third
                }
            }

            val filter = ExposedQueryPlanAdapter.toFilter(wireFixture("cs-eq"), Options.of(mapping))

            val all = Docs.selectAll().where { filter.toOp() }.map { it[Docs.id] }.sorted()
            assertEquals(listOf("a", "c"), all, "CEL string equality is exact: 'One' must not match 'one'")

            val composed = Docs.selectAll()
                .where { filter.toOp() and (Docs.archived eq false) }
                .map { it[Docs.id] }
            assertEquals(listOf("a"), composed)
        }
    }

    @Test
    fun `an unmapped attribute is refused, never guessed`() {
        val empty = cerbosMapping { }
        val error = assertThrows(UnmappedAttributeException::class.java) {
            ExposedQueryPlanAdapter.toFilter(wireFixture("cs-eq"), Options.of(empty))
        }
        assertEquals("Unknown attribute: request.resource.attr.aString", error.message)
    }

    /** A golden `PlanResources` response from the shared corpus, decoded the way the PDP wrote it. */
    private fun wireFixture(action: String): PlanResourcesFilter {
        val file = Path.of(System.getProperty("user.dir"), "..", "conformance", "wire-fixtures", "$action.json")
        val filter = ObjectMapper().readTree(Files.readString(file)).get("filter")
        return PlanResourcesFilter.newBuilder()
            .also { JsonFormat.parser().merge(filter.toString(), it) }
            .build()
    }
}
