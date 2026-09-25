package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse
import dev.cerbos.queryplan.exposed.Corpus.LedgerEntry
import org.jetbrains.exposed.v1.core.JoinType
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.TreeMap
import java.util.stream.Stream

/**
 * The conformance harness. It implements `conformance/README.md`, "The harness contract":
 *
 * 1. Store the dataset (`seeds.json` + `derived-fields.json`) and map it ([MAPPING]).
 * 2. For each PDP and each recorded golden file, translate the plan, run the query, and compare the
 *    ids with the ones `check()` allowed. `conformance-ledger.json` lists the exceptions:
 *    `unsupported` must throw one of the adapter's refusal types, and `divergent` must still give a
 *    wrong answer.
 * 3. Fail if the ledger names a case that has no golden file.
 *
 * Needs no PDP: the plans and decisions are recorded. Runs on in-memory H2 by default; set
 * `adapter.test.db` (forwarded from `ADAPTER_TEST_DB`) to `sqlite`, `postgres` or `mysql` for
 * another store ([TestStore]). Every run writes each case's outcome to
 * `build/reports/conformance-<store>.json`, which is what a triage pass reads: the JUnit report
 * says an assertion failed, the summary says which ids each side returned.
 */
class AdversarialConformanceTest {

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class Tag(val id: String, val name: String?)

    /** One `seeds.json` row. Scalars and list elements are nullable: the corpus carries nulls. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class Seed(
        val id: String,
        val aBool: Boolean?,
        val aString: String?,
        val aNumber: Int?,
        val aOptionalString: String?,
        val aNumberList: List<Double?>,
        val aBoolList: List<Boolean?>,
        val tags: List<Tag>,
        val subCategoryNames: List<String>,
        val parentSeedId: String?,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class SeedsFile(val seeds: List<Seed>)

    /** One seed's `derived-fields.json` entry. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class Derived(
        val createdBy: String,
        val aDouble: Double?,
        val createdAt: String?,
        val updatedAt: String?,
        val scope: String?,
        val labels: List<String?>,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class DerivedFile(val derived: Map<String, Derived>)

    companion object {

        private val LEDGER: Map<String, LedgerEntry> = Corpus.ledger()

        private lateinit var seeds: List<Seed>
        private lateinit var derived: Map<String, Derived>
        private var testStore: TestStore? = null

        /** (tag, tier, outcome) to count, printed after the run. */
        private val TALLY = TreeMap<String, Int>()

        /** `<tag> <case id>` to what the adapter did, written to the run summary. */
        private val RESULTS = TreeMap<String, Map<String, Any?>>()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val conformance = Corpus.conformanceDir()
            seeds = Corpus.JSON.readValue(
                conformance.resolve("seeds.json").toFile(), SeedsFile::class.java,
            ).seeds
            derived = Corpus.JSON.readValue(
                conformance.resolve("derived-fields.json").toFile(), DerivedFile::class.java,
            ).derived
            testStore = TestStore.open()
            transaction(testStore!!.database) {
                SchemaUtils.create(tables = ADVERSARIAL_TABLES.toTypedArray())
            }
            seed()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            TALLY.forEach { (key, count) -> println("conformance $key: $count") }
            runCatching { writeRunSummary() }
                .onFailure { println("==> could not write the conformance run summary: $it") }
            testStore?.stop()
        }

        private fun writeRunSummary() {
            val store = testStore?.name ?: return
            val file = Path.of(System.getProperty("user.dir"), "build", "reports", "conformance-$store.json")
            Files.createDirectories(file.parent)
            synchronized(RESULTS) {
                Corpus.JSON.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), RESULTS)
            }
        }

        private fun tally(tag: String, tier: String, outcome: String) {
            synchronized(TALLY) { TALLY.merge("$tag $tier $outcome", 1, Int::plus) }
        }

        private fun record(tag: String, id: String, result: Map<String, Any?>) {
            synchronized(RESULTS) { RESULTS["$tag $id"] = result }
        }

        // -- the store ----------------------------------------------------------------------------

        private fun seed() = transaction(testStore!!.database) {
            val byId = seeds.associateBy { it.id }
            fun parentOf(s: Seed?): Seed? = s?.parentSeedId?.let {
                byId[it] ?: error("seeds.json: \"${s.id}\" names parent \"$it\", not a seed id")
            }

            // Distinct category graphs per seed, so no two rows share a relation by accident.
            var catSeq = 0
            var subSeq = 0
            for (s in seeds) {
                val d = derived[s.id] ?: error("derived-fields.json has no entry for \"${s.id}\"")
                Resources.insert {
                    it[id] = s.id
                    it[aBool] = s.aBool
                    it[aString] = s.aString
                    it[aNumber] = s.aNumber
                    it[aDouble] = d.aDouble
                    it[aOptionalString] = s.aOptionalString
                    it[createdBy] = d.createdBy
                    it[scope] = d.scope
                    it[createdAt] = d.createdAt?.let(Instant::parse)
                    it[updatedAt] = d.updatedAt?.let(Instant::parse)
                }
                s.tags.forEachIndexed { index, tag ->
                    Tags.insert {
                        it[tagId] = tag.id
                        it[name] = tag.name
                        it[resourceId] = s.id
                        it[position] = index
                    }
                }
                // One related row per element; a null element becomes a NULL column.
                s.aNumberList.forEachIndexed { index, element ->
                    NumberListElements.insert {
                        it[id] = "${s.id}-n$index"
                        it[this.element] = element
                        it[resourceId] = s.id
                        it[position] = index
                    }
                }
                s.aBoolList.forEachIndexed { index, element ->
                    BoolListElements.insert {
                        it[id] = "${s.id}-b$index"
                        it[this.element] = element
                        it[resourceId] = s.id
                        it[position] = index
                    }
                }
                // ONE category holding every subcategory name (conformance/README.md, "The
                // dataset"), not one category per name: a seed with two names has one category of
                // size two, which `size(c.subCategories) == 1` must not match.
                val cat = if (s.subCategoryNames.isEmpty()) null else "adv-cat-${++catSeq}"
                if (cat != null) {
                    Categories.insert {
                        it[id] = cat
                        it[name] = "business"
                        it[resourceId] = s.id
                    }
                }
                for (subName in s.subCategoryNames) {
                    val sub = "adv-sub-${++subSeq}"
                    SubCategories.insert {
                        it[id] = sub
                        it[name] = subName
                        it[categoryId] = cat!!
                    }
                    d.labels.forEachIndexed { index, labelName ->
                        Labels.insert {
                            it[id] = "adv-lab-$subSeq-${index + 1}"
                            it[name] = labelName
                            it[subCategoryId] = sub
                        }
                    }
                }

                // The to-one chain, one owned row per level: a seed with no parent gets no row, so
                // an absent parent is a missing attribute rather than a NULL value. Every resource
                // owns a FRESH parent and inner row rather than pointing at the named seed's own,
                // so a filter that returned the parent instead of the child cannot agree by accident.
                val parentSeed = parentOf(s) ?: continue
                val parent = "${s.id}-parent"
                Parents.insert {
                    it[id] = parent
                    it[aBool] = parentSeed.aBool
                    it[aString] = parentSeed.aString
                    it[aNumber] = parentSeed.aNumber
                    it[aOptionalString] = parentSeed.aOptionalString
                    it[resourceId] = s.id
                }
                val innerSeed = parentOf(parentSeed) ?: continue
                Inners.insert {
                    it[id] = "$parent-inner"
                    it[aBool] = innerSeed.aBool
                    it[aString] = innerSeed.aString
                    it[aNumber] = innerSeed.aNumber
                    it[aOptionalString] = innerSeed.aOptionalString
                    it[parentId] = parent
                }
            }
        }

        // -- the query ----------------------------------------------------------------------------

        private fun ids(golden: JsonNode): List<String> =
            idsSelectedBy(ExposedQueryPlanAdapter.toFilter(Corpus.plan(golden), Options.of(MAPPING)))

        /**
         * The ids a caller's query returns under [filter], each kind handled the way a caller is
         * told to handle it: an always-denied plan skips the query, an always-allowed one runs with
         * no authorization clause, and a conditional one composes the predicate into `where`.
         */
        private fun idsSelectedBy(filter: QueryPlanFilter): List<String> =
            transaction(testStore!!.database) {
                when (filter) {
                    QueryPlanFilter.AlwaysDenied -> emptyList()
                    QueryPlanFilter.AlwaysAllowed -> selectIds(null)
                    is QueryPlanFilter.Conditional -> selectIds(filter.op)
                }
            }

        /** Sorted in Kotlin, as `allowed` is: the database's collation must not decide the order. */
        private fun selectIds(predicate: Op<Boolean>?): List<String> {
            var query = Resources.select(Resources.id).withDistinct()
            if (predicate != null) query = query.where(predicate)
            return query.map { it[Resources.id] }.sorted()
        }
    }

    // -- the contract -----------------------------------------------------------------------------

    @TestFactory
    fun goldens(): Stream<DynamicTest> = Corpus.pdpTags().stream().flatMap { tag ->
        Corpus.goldens(tag).stream().map { golden ->
            DynamicTest.dynamicTest("$tag ${golden["tier"].asText()}: ${golden["id"].asText()}") {
                replay(tag, golden)
            }
        }
    }

    private fun replay(tag: String, golden: JsonNode) {
        val id = golden["id"].asText()
        val tier = golden["tier"].asText()
        assertEquals(tag, golden.path("pdp").asText(), "$id: golden filed under the wrong PDP tag")
        // A golden file carries `plannerDivergence` only for the PDP tags it applies to. The key
        // must be present: a missing one would otherwise read as non-null and skip the case.
        val divergence = golden["plannerDivergence"]
        assertTrue(divergence != null, "$id: golden has no plannerDivergence key")
        if (!divergence.isNull) {
            tally(tag, tier, "planner divergence")
            Assumptions.abort<Unit>("planner divergence: ${divergence["reason"]}")
        }
        val allowed = golden["allowed"].map { it.asText() }.sorted()

        val entry = LEDGER[id]
        val status = if (entry != null && entry.appliesTo(tag)) entry.status else "pass"
        val outcome = runCatching { ids(golden) }
        record(
            tag, id,
            outcome.fold(
                { mapOf("status" to status, "allowed" to allowed, "returned" to it) },
                { mapOf("status" to status, "allowed" to allowed, "raised" to "${it.javaClass.simpleName}: ${it.message}") },
            ),
        )
        when (status) {
            "pass" -> assertEquals(allowed, outcome.getOrThrow(), id)
            "unsupported" -> {
                // The adapter's two refusal types. MalformedPlanException is not one: the planner
                // never emits a malformed plan.
                val refusal = outcome.exceptionOrNull()
                    ?: fail("$id returned a filter; the ledger says it must be refused")
                assertTrue(refusal is UnsupportedPlanShapeException || refusal is UnmappedAttributeException) {
                    "$id was refused as ${refusal.javaClass.simpleName}: ${refusal.message}"
                }
            }
            "divergent" -> assertNotEquals(
                allowed, outcome.getOrThrow(), "$id now matches the PDP: remove its divergent ledger entry",
            )
            else -> fail("$id: unknown ledger status $status")
        }
        tally(tag, tier, status)
    }

    @Test
    fun everyLedgerEntryNamesAGoldenCase() {
        val tags = Corpus.pdpTags()
        val recorded = tags.flatMap { tag -> Corpus.goldens(tag).map { it["id"].asText() } }.toSet()
        assertEquals(emptySet<String>(), LEDGER.keys - recorded)
        LEDGER.forEach { (id, entry) ->
            assertTrue(entry.status in setOf("unsupported", "divergent"), id)
            // A `pdp` scope naming a tag no longer tested is stale, and an empty one is a typo.
            assertTrue(entry.pdp == null || (entry.pdp.isNotEmpty() && tags.containsAll(entry.pdp))) {
                "$id: pdp scope ${entry.pdp} is not within $tags"
            }
            assertFalse(entry.reason.isBlank(), id)
            assertTrue(entry.status != "divergent" || entry.issue != null, id)
        }
    }

    // -- the store's own configuration ------------------------------------------------------------

    /**
     * The store the leg claims to be running against is the store it is talking to. Without this a
     * leg whose `adapter.test.db` never reached the JVM replays the corpus on the default H2 and
     * reports the PostgreSQL leg green.
     */
    @Test
    fun theStoreUnderTestIsTheStoreSelected() {
        val store = testStore!!
        val banner = store.serverBanner()
        assertEquals(store.expectedEngine, banner.substringBefore(' '), "the ${store.name} leg: $banner")
        // MySQL's own default collation makes `=` case-insensitive while CEL's is byte-exact, so a
        // leg that lost the setting reports a translator bug that is not one.
        if (store.name == "mysql") {
            assertEquals(
                store.collation,
                transaction(store.database) {
                    exec("select @@collation_server") { rs -> if (rs.next()) rs.getString(1) else null }
                },
            )
        }
    }

    /**
     * Read the to-one chain back through a real join. No case would notice a seeder attaching every
     * parent to the wrong resource if the wrong resource happened to carry the same values.
     */
    @Test
    fun seededToOneChainMatchesTheCorpusRelation() {
        val byId = seeds.associateBy { it.id }
        fun parentOf(s: Seed?): Seed? = s?.parentSeedId?.let(byId::getValue)
        val want = seeds.associate { s ->
            s.id to listOf(parentOf(s)?.aString, parentOf(parentOf(s))?.aString)
        }
        val got = transaction(testStore!!.database) {
            Resources
                .join(Parents, JoinType.LEFT, onColumn = Resources.id, otherColumn = Parents.resourceId)
                .join(Inners, JoinType.LEFT, onColumn = Parents.id, otherColumn = Inners.parentId)
                .select(Resources.id, Parents.aString, Inners.aString)
                .associate {
                    it[Resources.id] to listOf(it.getOrNull(Parents.aString), it.getOrNull(Inners.aString))
                }
        }
        assertEquals(want, got)
    }

    /** An empty relation table leaves every collection macro trivially satisfied on both sides. */
    @Test
    fun everyMappedTableWasCreatedAndSeeded() {
        val rows = transaction(testStore!!.database) {
            ADVERSARIAL_TABLES.associate { it.tableName to it.selectAll().count() }
        }
        assertEquals(emptySet<String>(), rows.filterValues { it == 0L }.keys)
        assertEquals(seeds.size.toLong(), rows.getValue(Resources.tableName))
    }

    // -- KIND 3: a policy can reach these, and the corpus does not carry them yet -----------------

    /**
     * **Corpus gap.** #509: the corpus counts the `mainCategory` chain with two spellings. These are
     * the other thresholds and polarities, and rows without a `mainCategory` must stay out of every
     * one (#316). Seeds with one have at least one subCategory (i9 has two), and the rest are CEL
     * missing-path errors, so each of these is empty.
     */
    @Test
    fun everyCountThresholdOverTheChainInheritsTheAbsentParentGuard() {
        val size = expression(
            "size",
            Operand.newBuilder().setVariable("request.resource.attr.mainCategory.subCategories").build(),
        )
        val emptyByConstruction = linkedMapOf(
            "size(chain) == 0" to compare("eq", size, 0.0),
            "size(chain) <= 0" to compare("le", size, 0.0),
            "size(chain) < 1" to compare("lt", size, 1.0),
            "!(size(chain) > 0)" to expression("not", compare("gt", size, 0.0)),
            "!(size(chain) >= 1)" to expression("not", compare("ge", size, 1.0)),
        )
        emptyByConstruction.forEach { (shape, condition) ->
            assertEquals(emptyList<String>(), filteredIdsFor(condition), "absent-parent guard leaked for $shape")
        }

        // The mirror image, so the loop above cannot pass by denying everything.
        val withParent = filteredIdsFor(compare("ge", size, 0.0))
        assertFalse(withParent.isEmpty(), "some seed must carry a mainCategory")
        assertTrue(withParent.size < seeds.size, "not every seed carries one")
        assertEquals(withParent, filteredIdsFor(compare("lt", size, 3.0)))
    }

    private fun expression(operator: String, vararg operands: Operand): Operand {
        val e = Expression.newBuilder().setOperator(operator)
        operands.forEach { e.addOperands(it) }
        return Operand.newBuilder().setExpression(e).build()
    }

    private fun compare(operator: String, left: Operand, threshold: Double): Operand = expression(
        operator,
        left,
        Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(threshold)).build(),
    )

    private fun filteredIdsFor(condition: Operand): List<String> = idsSelectedBy(
        ExposedQueryPlanAdapter.toFilter(
            PlanResourcesResponse.newBuilder().setFilter(
                PlanResourcesFilter.newBuilder()
                    .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                    .setCondition(condition),
            ).build(),
            Options.of(MAPPING),
        ),
    )

    @Test
    fun anUnknownLedgerKeyFailsTheRead() {
        assertThrows(Exception::class.java) {
            Corpus.JSON.readValue("""{"status":"unsupported","reason":"x","bogus":1}""", LedgerEntry::class.java)
        }
    }
}
