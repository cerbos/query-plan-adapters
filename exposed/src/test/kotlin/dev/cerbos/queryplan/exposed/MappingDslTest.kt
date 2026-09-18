package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.NullValue
import com.google.protobuf.Value
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.alias
import org.jetbrains.exposed.v1.core.booleanParam
import org.jetbrains.exposed.v1.core.stringParam
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * The mapping the caller writes: both forms it can take, and everything the DSL refuses to guess at.
 *
 * All of it is kind 2 material (`CLAUDE.md`, "What a translator unit test may pin") — the corpus
 * classifies each action against exactly one mapping per adapter, so a second mapper form, a
 * per-attribute NULL convention and a `visibleWhen` predicate have no corpus spelling at all.
 *
 * The refusals matter more than the successes. A relation is declared as a table plus two columns,
 * with no association metadata behind it, so a column from the wrong table produces a subquery that
 * builds, runs, and returns the wrong rows. Every one of those is rejected where it is declared.
 */
class MappingDslTest {

    private object Docs : Table("mapping_docs") {
        val id = varchar("id", 32)
        val aString = varchar("a_string", 64)
        val owner = varchar("owner", 32).nullable()
    }

    private object Tags : Table("mapping_tags") {
        val docId = varchar("doc_id", 32)
        val name = varchar("name", 64)
        val deleted = bool("deleted")
    }

    private object Parents : Table("mapping_parents") {
        val id = varchar("id", 32)
        val docId = varchar("doc_id", 32)
        val label = varchar("label", 64)
    }

    private object Inners : Table("mapping_inners") {
        val parentId = varchar("parent_id", 32)
        val note = varchar("note", 64)
    }

    private object Elsewhere : Table("mapping_elsewhere") {
        val id = varchar("id", 32)
        val stray = varchar("stray", 32)
    }

    // -- the two mapper forms ---------------------------------------------------------------------

    @Test
    fun `a static table and a resolver function translate the same plan identically`() {
        val declared: AttributeResolver = cerbosMapping { "request.resource.attr.aString" to Docs.aString }
        val byRule = AttributeResolver { reference ->
            if (reference == "request.resource.attr.aString") AttributeMapping.field(Docs.aString) else null
        }

        val fromTable = translate(declared)
        val fromFunction = translate(byRule)
        OfflineRenderer.DIALECTS.forEach { dialect ->
            assertEquals(
                OfflineRenderer.renderOn(dialect, fromTable).sql,
                OfflineRenderer.renderOn(dialect, fromFunction).sql,
            )
        }
    }

    @Test
    fun `a resolver that knows nothing is a refusal, never a guessed column`() {
        val error = assertThrows(UnmappedAttributeException::class.java) { translate(AttributeResolver { null }) }
        assertEquals("Unknown attribute: request.resource.attr.aString", error.message)
    }

    @Test
    fun `the declared table is a defensive copy`() {
        val source = linkedMapOf<String, AttributeMapping>("request.resource.id" to AttributeMapping.field(Docs.id))
        val mappings = AttributeMappings.of(source)
        source["request.resource.attr.aString"] = AttributeMapping.field(Docs.aString)
        // A caller who keeps a handle on the map they passed must not be able to widen what an
        // in-flight translation resolves.
        assertEquals(setOf("request.resource.id"), mappings.entries.keys)
        assertNull(mappings.resolve("request.resource.attr.aString"))
    }

    // -- a column entry ---------------------------------------------------------------------------

    @Test
    fun `a bare column declares no NULL convention, and a declared one says which`() {
        val mapping = cerbosMapping {
            "request.resource.attr.aString" to Docs.aString
            "request.resource.attr.owner" to field(Docs.owner, nulls = NullAttributeRepresentation.OMITTED)
        }
        val bare = mapping.resolve("request.resource.attr.aString") as AttributeMapping.Field
        assertNull(bare.nullAttributeRepresentation)
        assertSame(Docs.aString, bare.column)

        val declared = mapping.resolve("request.resource.attr.owner") as AttributeMapping.Field
        assertEquals(NullAttributeRepresentation.OMITTED, declared.nullAttributeRepresentation)
    }

    @Test
    fun `an undeclared attribute is read under BOTH conventions in one call, and that is accepted`() {
        // KIND 2 — a caller-supplied argument the corpus structurally cannot vary: actions.json
        // classifies each action against ONE mapping per adapter, and the corpus declares `owner`
        // EXPLICIT, so nothing in `conformance/` reaches this combination.
        //
        // A DECISION, recorded rather than fixed. Under the default call-level EXPLICIT, an
        // attribute that declares nothing gets:
        //
        //  - `== null` rendered as a definite `IS NULL`, because the pre-walk NullOperandScan takes
        //    the call-level option at its word about what the caller SENDS for a null operand;
        //  - `!= "x"` rendered as the plain three-valued `<>`, because ADR 0004 says an undeclared
        //    column RENDERS as if NOT NULL.
        //
        // So one attribute is read under both conventions within one call. It is left alone because
        // it errs the safe way: `<>` EXCLUDES the NULL rows that a declared EXPLICIT attribute would
        // include, so the asymmetry under-grants. Making `== null` follow the undeclared rendering
        // instead would have to answer UNKNOWN for every row, which is a filter no caller asked for;
        // making `!=` follow the option would hand definite equality to every column a caller never
        // thought about, which is exactly what ADR 0004 exists to prevent. Declaring the attribute
        // makes both halves definite.
        val mapping = cerbosMapping { "request.resource.attr.aOptionalString" to Docs.owner }
        val options = Options.of(mapping)

        val isNull = render(options, "eq", Operand.newBuilder().setValue(NULL_VALUE).build())
        assertTrue(isNull.contains("IS NULL"), isNull)

        val notEqual = render(options, "ne", Operand.newBuilder().setValue(STRING_VALUE).build())
        assertFalse(notEqual.contains("IS NULL"), notEqual)
        assertFalse(notEqual.contains("IS NOT NULL"), notEqual)
    }

    @Test
    fun `a reference mapped twice is refused`() {
        // Last-one-wins would make the mapping depend on declaration order, and the two entries a
        // caller wrote by accident are rarely interchangeable.
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping {
                "request.resource.attr.aString" to Docs.aString
                "request.resource.attr.aString" to Docs.owner
            }
        }
        assertEquals("'request.resource.attr.aString' is mapped twice", error.message)
    }

    @Test
    fun `an empty reference is refused`() {
        val error = assertThrows(IllegalArgumentException::class.java) { cerbosMapping { "" to Docs.aString } }
        assertEquals("An attribute reference must not be empty", error.message)
    }

    // -- a relation entry ---------------------------------------------------------------------------

    @Test
    fun `a relation declares its cardinality, its keys and its element`() {
        val mapping = cerbosMapping {
            "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Tags.docId) {
                "name" to Tags.name
            }
            "request.resource.attr.tagNames" to
                many(Tags, from = Docs.id, to = Tags.docId, element = Tags.name)
        }

        val tags = mapping.resolve("request.resource.attr.tags") as AttributeMapping.Relation
        assertEquals(AttributeMapping.Relation.Cardinality.MANY, tags.cardinality)
        assertSame(Tags, tags.table)
        assertSame(Docs.id, tags.from)
        assertSame(Tags.docId, tags.to)
        assertNull(tags.element)
        assertEquals(setOf("name"), tags.fields.keys)

        val names = mapping.resolve("request.resource.attr.tagNames") as AttributeMapping.Relation
        assertSame(Tags.name, names.element!!.column)
    }

    @Test
    fun `a to-one chain nests`() {
        val mapping = cerbosMapping {
            "request.resource.attr.parent" to one(Parents, from = Docs.id, to = Parents.docId) {
                "label" to Parents.label
                "inner" to one(Inners, from = Parents.id, to = Inners.parentId) {
                    "note" to Inners.note
                }
            }
        }
        val parent = mapping.resolve("request.resource.attr.parent") as AttributeMapping.Relation
        assertEquals(AttributeMapping.Relation.Cardinality.ONE, parent.cardinality)
        val inner = parent.fields.getValue("inner") as AttributeMapping.Relation
        assertEquals(AttributeMapping.Relation.Cardinality.ONE, inner.cardinality)
        assertSame(Parents.id, inner.from)
        assertSame(Inners.parentId, inner.to)
        assertSame(Inners.note, (inner.fields.getValue("note") as AttributeMapping.Field).column)
    }

    @Test
    fun `a target column on another table is refused`() {
        // Nothing downstream could catch this: `Tags.docId = Docs.id` and `Elsewhere.id = Docs.id`
        // are both well-formed SQL, and the second one silently reads a table the caller never
        // named.
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping { "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Elsewhere.id) }
        }
        assertEquals(
            "Relation target column mapping_elsewhere.id is not a column of mapping_tags",
            error.message,
        )
    }

    @Test
    fun `an element column on another table is refused`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping {
                "request.resource.attr.tagNames" to
                    many(Tags, from = Docs.id, to = Tags.docId, element = Elsewhere.stray)
            }
        }
        assertEquals(
            "Relation element column mapping_elsewhere.stray is not a column of mapping_tags",
            error.message,
        )
    }

    @Test
    fun `a field column on another table is refused`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping {
                "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Tags.docId) {
                    "stray" to Elsewhere.stray
                }
            }
        }
        assertEquals(
            "Relation field 'stray' reads mapping_elsewhere, not the relation's table mapping_tags",
            error.message,
        )
    }

    @Test
    fun `a nested relation anchored to another table is refused`() {
        // The nested hop's `from` is a column of the ENCLOSING relation's table; anchoring it to
        // the root's would correlate the inner subquery to a table that is not in scope there.
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping {
                "request.resource.attr.parent" to one(Parents, from = Docs.id, to = Parents.docId) {
                    "inner" to one(Inners, from = Docs.id, to = Inners.parentId)
                }
            }
        }
        assertEquals(
            "Relation field 'inner' reads mapping_docs, not the relation's table mapping_parents",
            error.message,
        )
    }

    @Test
    fun `a field mapped twice inside a relation is refused`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping {
                "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Tags.docId) {
                    "name" to Tags.name
                    "name" to Tags.deleted
                }
            }
        }
        assertEquals("'name' is mapped twice", error.message)
    }

    // -- visibleWhen --------------------------------------------------------------------------------

    @Test
    fun `visibleWhen is declared at most once`() {
        // Two predicates have no obvious composition — AND is a guess, and the wrong guess widens
        // what the subquery examines — so the second declaration is an error rather than a merge.
        val error = assertThrows(IllegalArgumentException::class.java) {
            cerbosMapping {
                "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Tags.docId) {
                    visibleWhen { alias -> EqOp(alias[Tags.deleted], booleanParam(false)) }
                    visibleWhen { alias -> EqOp(alias[Tags.name], stringParam("x")) }
                }
            }
        }
        assertEquals("visibleWhen is declared twice", error.message)
    }

    @Test
    fun `whether a visibility predicate was declared is visible on the relation`() {
        val guarded = cerbosMapping {
            "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Tags.docId) {
                visibleWhen { alias -> EqOp(alias[Tags.deleted], booleanParam(false)) }
            }
        }.resolve("request.resource.attr.tags") as AttributeMapping.Relation
        assertTrue(guarded.hasVisibilityPredicate)

        val bare = cerbosMapping {
            "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Tags.docId)
        }.resolve("request.resource.attr.tags") as AttributeMapping.Relation
        assertFalse(bare.hasVisibilityPredicate)
    }

    @Test
    fun `visibleWhen is built against the alias the subquery reads through`() {
        // It takes a resolver rather than a ready predicate because every subquery instance gets its
        // own alias: a predicate built once would name whichever alias existed when the mapping was
        // declared, and correlate the guard to the wrong copy of the table.
        val relation = cerbosMapping {
            "request.resource.attr.tags" to many(Tags, from = Docs.id, to = Tags.docId) {
                visibleWhen { alias -> EqOp(alias[Tags.deleted], booleanParam(false)) }
            }
        }.resolve("request.resource.attr.tags") as AttributeMapping.Relation

        val first = relation.visibleWhenFor("cerbos_1")
        val second = relation.visibleWhenFor("cerbos_2")
        assertTrue(OfflineRenderer.renderOn(OfflineRenderer.POSTGRESQL, first).sql.startsWith("cerbos_1."))
        assertTrue(OfflineRenderer.renderOn(OfflineRenderer.POSTGRESQL, second).sql.startsWith("cerbos_2."))
    }

    private fun AttributeMapping.Relation.visibleWhenFor(alias: String): Op<Boolean> =
        checkNotNull(visibleWhen)(table.alias(alias))

    private val NULL_VALUE: Value = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
    private val STRING_VALUE: Value = Value.newBuilder().setStringValue("x").build()

    /** `request.resource.attr.aOptionalString <op> <constant>`, rendered under one dialect. */
    private fun render(options: Options, operator: String, constant: Operand): String {
        val condition = PlanResourcesFilter.newBuilder()
            .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
            .setCondition(
                Operand.newBuilder().setExpression(
                    PlanResourcesFilter.Expression.newBuilder()
                        .setOperator(operator)
                        .addOperands(Operand.newBuilder().setVariable("request.resource.attr.aOptionalString"))
                        .addOperands(constant),
                ),
            )
            .build()
        val op = OfflineRenderer.translate { ExposedQueryPlanAdapter.toFilter(condition, options).toOp() }
        return OfflineRenderer.renderOn(OfflineRenderer.H2, op).sql
    }

    /** The one plan every case here shares: `request.resource.attr.aString == "one"`. */
    private fun translate(resolver: AttributeResolver): Op<Boolean> =
        ExposedQueryPlanAdapter.toFilter(wireFixture("cs-eq"), Options.of(resolver)).toOp()

    /**
     * A golden `PlanResources` response from the shared corpus, decoded the way the PDP wrote it
     * (ADR 0006). Duplicated per suite on purpose: adapters share data, not code, and a loader that
     * every suite reaches through is one more thing to keep in step.
     */
    private fun wireFixture(action: String): PlanResourcesFilter {
        val file = Path.of(System.getProperty("user.dir"), "..", "conformance", "wire-fixtures", "$action.json")
        val filter = ObjectMapper().readTree(Files.readString(file)).get("filter")
        return PlanResourcesFilter.newBuilder()
            .also { JsonFormat.parser().merge(filter.toString(), it) }
            .build()
    }
}
