package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * Review of `in(attribute, collection-attribute)` against the null convention ADR 0004 declares.
 *
 * The corpus reaches [MembershipTranslator] through exactly one mapping — `in-var-var` and
 * `in-var-var-neg` compare `request.resource.attr.owner`, which DECLARES
 * `NullAttributeRepresentation.EXPLICIT`. Which convention an UNDECLARED member attribute gets is
 * therefore a caller-supplied argument the corpus structurally cannot vary, and it is the argument
 * that decides whether the emitted subquery matches a NULL member against a NULL element.
 */
class ReviewNullConventionTest {

    @Test
    @Disabled("REVIEW FINDING 3: in(undeclared-attr, collection) falls back to the call-level null option and matches NULL against NULL, which check() denies")
    fun `membership of an undeclared attribute must not match a NULL element against a NULL member`() {
        // CEL: `R.attr.owner in R.attr.tagNames` (the `in-var-var` corpus action), translated
        // against a mapping where `owner` declares NO convention.
        //
        // ADR 0004 and `LeafTranslator.isExplicitNull` agree that declaring nothing means "treat
        // this column as NOT NULL": a NULL column then sends no attribute at all, CEL raises a
        // missing-attribute error, and `check()` DENIES. `d1` is exactly that row — a NULL member
        // and a NULL-named element — and it must not come back.
        //
        // MembershipTranslator.kt:129-130 instead reads
        //     member.field.nullAttributeRepresentation ?: translation.options.nullAttributeRepresentation
        // whose default is EXPLICIT, so the subquery body gains
        //     (cerbos_1.NAME IS NULL AND REVIEW_DOCS.OWNER IS NULL)
        // and `d1` is returned. Recommended fix: read the declaration the way LeafTranslator does
        // — `member.field.nullAttributeRepresentation == EXPLICIT` — with no fallback.
        assertEquals(listOf("d2"), ids("in-var-var", UNDECLARED))
    }

    @Test
    @Disabled("REVIEW FINDING 2: !in(attr, collection) is a two-valued NOT EXISTS, so a NULL member readmits every row")
    fun `a negated membership must stay UNKNOWN when the member column is NULL`() {
        // CEL: `!(R.attr.owner in R.attr.tagNames)` (`in-var-var-neg`), under the OMITTED
        // call-level option — a fully supported configuration, and one that makes this finding
        // independent of finding 3: `explicitNulls` is false there, so the subquery body is the
        // plain equality it should be and the row set is still wrong.
        //
        // With no leading hop, Subqueries.chainContains takes the two-valued `existsOver` branch
        // (Subqueries.kt:102-104). A NULL member makes every element body UNKNOWN, `EXISTS`
        // answers FALSE rather than UNKNOWN, and `NOT FALSE` is TRUE — so d1 and d4, whose
        // `check()` is a missing-attribute deny, both come back.
        //
        // d3's owner is "a" and its only element is "b": a determined FALSE, so the negation
        // genuinely allows it. Recommended fix: when the member is not explicit-null, guard the
        // existence test with `TriLogic.baseUnlessUnknown(exists, IsNullOp(member.expression))`,
        // the way projectionIntersects guards its own NULL witness.
        val omitted = Options.of(UNDECLARED)
            .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
        assertEquals(
            listOf("d3"),
            idsOf(ExposedQueryPlanAdapter.toFilter(wireFixture("in-var-var-neg"), omitted).toOp()),
        )
    }

    @Test
    fun `the same undeclared attribute is NOT explicit-null anywhere else in the walk`() {
        // The control for findings 2 and 3: one attribute cannot be explicit-null for `in` and
        // omitted for `eq`. The membership subquery is the only place the two disagree.
        val undeclared = UNDECLARED.resolve("request.resource.attr.owner") as AttributeMapping.Field
        assertEquals(null, undeclared.nullAttributeRepresentation)

        val declaredSql = render(translate("in-var-var", DECLARED))
        val undeclaredSql = render(translate("in-var-var", UNDECLARED))
        assertTrue(declaredSql.contains("IS NULL"), declaredSql)
        assertEquals(
            declaredSql,
            undeclaredSql,
            "the undeclared mapping emits the DECLARED reading: same SQL, different convention",
        )
    }

    @Test
    fun `under the OMITTED option the undeclared member gets the plain equality it should`() {
        // Proof that the fallback is the whole mechanism: flipping the call-level option changes
        // the SQL of a mapping that declares nothing, which per ADR 0004 it must not.
        val omitted = Options.of(UNDECLARED)
            .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
        val op = ExposedQueryPlanAdapter.toFilter(wireFixture("in-var-var"), omitted).toOp()
        assertFalse(render(op).contains("IS NULL"), render(op))
        assertEquals(listOf("d2"), idsOf(op))
    }

    @Test
    fun `the declared explicit-null member keeps the definite reading the corpus pins`() {
        // Not a defect: with the convention DECLARED, `null in [null]` is TRUE in CEL and d1 is
        // exactly right. Pinned so a fix to findings 2 and 3 cannot take this with it.
        assertEquals(listOf("d1", "d2"), ids("in-var-var", DECLARED))
    }

    companion object {
        object ReviewDocs : Table("review_docs") {
            val id = varchar("id", 32)

            /** Mapped twice below: once declaring EXPLICIT, once declaring nothing. */
            val owner = varchar("owner", 64).nullable()
            override val primaryKey = PrimaryKey(id)
        }

        object ReviewTags : Table("review_tags") {
            val id = varchar("id", 32)
            val resourceId = varchar("resource_id", 32)

            /** NULLable: a NULL element is what the two conventions disagree about. */
            val name = varchar("name", 64).nullable()
            override val primaryKey = PrimaryKey(id)
        }

        /** `owner` declares nothing, so ADR 0004 says "treat this column as NOT NULL". */
        val UNDECLARED: AttributeMappings = cerbosMapping {
            "request.resource.attr.owner" to ReviewDocs.owner
            "request.resource.attr.tagNames" to
                many(ReviewTags, from = ReviewDocs.id, to = ReviewTags.resourceId, element = ReviewTags.name)
        }

        /** The corpus's own mapping: `owner` declares the explicit-null convention. */
        val DECLARED: AttributeMappings = cerbosMapping {
            "request.resource.attr.owner" to
                field(ReviewDocs.owner, nulls = NullAttributeRepresentation.EXPLICIT)
            "request.resource.attr.tagNames" to
                many(ReviewTags, from = ReviewDocs.id, to = ReviewTags.resourceId, element = ReviewTags.name)
        }

        private val database by lazy {
            val db = Database.connect("jdbc:h2:mem:review_nulls;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
            transaction(db) { seed() }
            db
        }

        fun translate(action: String, mapping: AttributeResolver): Op<Boolean> =
            ExposedQueryPlanAdapter.toFilter(wireFixture(action), Options.of(mapping)).toOp()

        fun render(op: Op<Boolean>): String = transaction(database) { op.toString() }

        fun ids(action: String, mapping: AttributeResolver): List<String> = idsOf(translate(action, mapping))

        fun idsOf(op: Op<Boolean>): List<String> = transaction(database) {
            ReviewDocs.selectAll().where(op).map { it[ReviewDocs.id] }.sorted()
        }

        fun wireFixture(action: String): PlanResourcesFilter {
            val file = Path.of(System.getProperty("user.dir"), "..", "conformance", "wire-fixtures", "$action.json")
            val filter = ObjectMapper().readTree(Files.readString(file)).get("filter")
            return PlanResourcesFilter.newBuilder()
                .also { JsonFormat.parser().merge(filter.toString(), it) }
                .build()
        }

        /**
         * | id | owner | tag names | check() under the OMITTED reading |
         * |----|-------|-----------|-----------------------------------|
         * | d1 | NULL  | [NULL]    | missing attribute -> DENY         |
         * | d2 | "a"   | ["a"]     | allow                             |
         * | d3 | "a"   | ["b"]     | deny (definite false)             |
         * | d4 | NULL  | ["b"]     | missing attribute -> DENY         |
         */
        private fun seed() {
            SchemaUtils.create(ReviewDocs, ReviewTags)
            listOf("d1" to null, "d2" to "a", "d3" to "a", "d4" to null).forEach { (docId, ownerValue) ->
                ReviewDocs.insert {
                    it[id] = docId
                    it[owner] = ownerValue
                }
            }
            listOf(
                Triple("t1", "d1", null),
                Triple("t2", "d2", "a"),
                Triple("t3", "d3", "b"),
                Triple("t4", "d4", "b"),
            ).forEach { (tagId, doc, tagName) ->
                ReviewTags.insert {
                    it[id] = tagId
                    it[resourceId] = doc
                    it[name] = tagName
                }
            }
        }
    }
}
