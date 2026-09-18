package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.protobuf.NullValue
import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse
import dev.cerbos.queryplan.exposed.Corpus.ActionsFile
import dev.cerbos.queryplan.exposed.Corpus.AdapterUnsupported
import dev.cerbos.queryplan.exposed.Corpus.NullRepresentationOmitted
import dev.cerbos.sdk.CerbosBlockingClient
import dev.cerbos.sdk.CerbosClientBuilder
import dev.cerbos.sdk.PlanResourcesResult
import dev.cerbos.sdk.builders.AttributeValue
import dev.cerbos.sdk.builders.Principal
import dev.cerbos.sdk.builders.Resource
import org.jetbrains.exposed.v1.core.JoinType
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.TreeSet
import java.util.stream.Stream

/**
 * Adversarial differential suite: every action from the repo-level `../conformance/` corpus is
 * planned against a real Cerbos PDP, translated by the adapter, and executed against seeded rows.
 * The filtered id set is compared with an oracle computed by calling the check API for each row
 * with attributes that mirror that row exactly.
 *
 * No hand-computed expectations: if the adapter's SQL semantics diverge from Cerbos's own
 * evaluation for any row, the mismatch surfaces mechanically. See `conformance/README.md` for the
 * shared seed, NULL and degeneracy conventions.
 *
 * **Database selection.** By default the suite runs on in-memory H2. Set the `adapter.test.db`
 * system property (forwarded from `ADAPTER_TEST_DB`) to `sqlite`, `postgres` or `mysql` to replay
 * the same oracle against another store; see [TestStore]. Every run writes a compact per-action
 * summary to `build/reports/conformance-<store>.txt` beside the JUnit report, because triaging a
 * differential means reading which ids each side returned and a stack trace does not carry them.
 */
class AdversarialConformanceTest {

    // -- shared corpus (../conformance/): policy, seed data and action list are read from disk
    // rather than duplicated here. See conformance/README.md for the recipe these implement.

    private data class Tag(val id: String, val name: String?)

    /**
     * One seeded row; the single source of truth for BOTH the database row and the oracle
     * attributes. [note] is corpus documentation this harness never reads; it is named so that
     * strict decoding accepts it, and it is the one seed key [SEED_KEYS] omits.
     */
    private data class Seed(
        val id: String,
        val aBool: Boolean,
        val aString: String,
        val aNumber: Int,
        val aOptionalString: String?,
        val tags: List<Tag>,
        val subCategoryNames: List<String>,
        val parentSeedId: String?,
        val note: String? = null,
    )

    /**
     * [attr] is typed as raw JSON rather than `Map<String, List<String>>`: the corpus carries scalar
     * principal attributes as well as lists, and a narrower type would reject the file rather than
     * silently drop one — but it would still be this harness deciding what the corpus may contain.
     * [principal] converts each value by its actual JSON type.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class PrincipalSpec(
        val id: String,
        val roles: List<String>,
        val attr: Map<String, Any>,
    )

    /**
     * conformance/seeds.json. Every key the file carries is named, including the prose ones,
     * because unknown properties are rejected rather than ignored: a seed field this harness does
     * not consume would be dropped from the row AND the check() oracle at once, and the differential
     * would agree for the wrong reason.
     */
    private data class SeedsFile(
        @param:JsonProperty("\$schema") val schema: String,
        val description: String,
        val principal: PrincipalSpec,
        val resourceKind: String,
        val principalNote: String,
        val relationNote: String,
        val seeds: List<Seed>,
    )

    /** One seed's derived fields, exactly as conformance/derived-fields.json carries them. */
    private data class DerivedEntry(
        val createdBy: String,
        val aDouble: Double?,
        val createdAt: String?,
        val scope: String?,
        val labels: List<String?>,
    )

    private data class DerivedFile(
        @param:JsonProperty("\$schema") val schema: String,
        val description: String,
        val fields: List<String>,
        val derived: Map<String, DerivedEntry>,
    )

    // The actions.json records, the classification helpers and the wire-fixture reader live in
    // Corpus: a later translator unit test asserts the same classification offline, and one parse
    // of one file is what keeps the two suites from disagreeing about which shapes must throw.

    companion object {

        /** The corpus key for this adapter — its directory name, as every other harness uses. */
        private const val ADAPTER = Corpus.ADAPTER

        // -- corpus coverage guards -------------------------------------------------------------
        //
        // The same parsed seed feeds the persisted row AND the check() oracle, so a corpus field
        // this harness does not consume is dropped from both sides at once and the differential
        // agrees for the wrong reason — the projection trap conformance/README.md describes for
        // actions.json, applied to the seeds. Asserting set equality catches both directions: a
        // corpus key nothing here reads, and a key this harness reads that the corpus no longer
        // carries.

        private val SEED_KEYS = listOf(
            "id", "aBool", "aString", "aNumber", "aOptionalString", "tags", "subCategoryNames",
            "parentSeedId",
        )

        /** Corpus prose, never read by a harness: the one documented exclusion from SEED_KEYS. */
        private const val SEED_NOTE_KEY = "note"

        /**
         * The one nested object array a seed carries. A key added inside an element is dropped from
         * both sides of the differential just as silently as a top-level one, so it is guarded the
         * same way.
         */
        private val TAG_KEYS = listOf("id", "name")

        private val DERIVED_KEYS = listOf("createdBy", "aDouble", "createdAt", "scope", "labels")

        // The corpus principal is guarded the same way and for the same reason. It feeds the PLAN
        // under test AND the check() oracle, so an attribute dropped on the way in vanishes from
        // both sides at once: the plan folds to ALWAYS_DENIED and the oracle, built from the same
        // principal, agrees. `id` and `roles` are deliberately IN scope, guarded one level above
        // the attributes — a role dropped on the way in changes every policy decision at once.

        private val PRINCIPAL_KEYS = listOf("id", "roles", "attr")

        private val PRINCIPAL_ATTR_KEYS = listOf("allowedTags", "context", "fewTeams", "manyTeams")

        private lateinit var seedsFile: SeedsFile
        private lateinit var actionsFile: ActionsFile
        private lateinit var derivedFile: DerivedFile
        private lateinit var seeds: List<Seed>

        private var cerbos: GenericContainer<*>? = null
        private var client: CerbosBlockingClient? = null
        private var testStore: TestStore? = null

        /**
         * One line per action for the run summary, keyed by action so a re-run of one case
         * overwrites rather than appends. Written by [tearDown] to
         * `build/reports/conformance-<store>.txt`, which is what a triage pass reads: the JUnit
         * report says an assertion failed, this says which ids each side returned.
         */
        private val results = java.util.Collections.synchronizedMap(LinkedHashMap<String, String>())

        /**
         * The oracle is a pure function of (PDP build, principal, seed row, action), all fixed for
         * a run — so the same action asked twice is the same answer. The degeneracy guard re-asks
         * every compared action after the comparison cases already did, and the corpus is large
         * enough that paying for that twice is minutes of round trips.
         */
        private val oracleCache = HashMap<String, List<String>>()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val mapper = ObjectMapper().registerKotlinModule()
            val conformance = Corpus.conformanceDir()
            seedsFile = mapper.readValue(
                conformance.resolve("seeds.json").toFile(), SeedsFile::class.java,
            )
            actionsFile = Corpus.actionsFile()
            derivedFile = mapper.readValue(
                conformance.resolve("derived-fields.json").toFile(), DerivedFile::class.java,
            )
            seeds = seedsFile.seeds
            assertCorpusCoverage(mapper, conformance)

            // Pinned PDP image — see CerbosTestImage for the pin rationale and bump policy.
            val container = GenericContainer(CerbosTestImage.IMAGE)
                .withExposedPorts(3593)
                .withCommand("server", "--set=storage.disk.directory=/policies")
                .withEnv("CERBOS_NO_TELEMETRY", "1")
                .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("cerbos-adversarial-pdp")))
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server.*", 1))
            // The WHOLE policy directory, not the one file the corpus carries today. A second
            // policy file — a derived-roles or exported-variables file a future action depends on —
            // would otherwise be silently absent from the PDP, and every action reaching it would
            // plan against a policy that never loaded.
            val policyDir = conformance.resolve("policies")
            val policies = policyFiles(policyDir)
            assertFalse(policies.isEmpty(), "conformance/policies/ holds no policy file")
            for (policy in policies) {
                container.withCopyToContainer(
                    Transferable.of(Files.readAllBytes(policy)),
                    "/policies/" + policyDir.relativize(policy).toString(),
                )
            }
            container.start()
            cerbos = container
            CerbosTestImage.assertPinned(container)
            client = CerbosClientBuilder("${container.host}:${container.getMappedPort(3593)}")
                .withPlaintext().buildBlockingClient()

            testStore = TestStore.open()
            createSchema()
            seed()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            runCatching { writeRunSummary() }
                .onFailure { println("==> could not write the conformance run summary: $it") }
            testStore?.stop()
            cerbos?.stop()
        }

        /** Every regular file under the policy directory, in a stable order. */
        private fun policyFiles(directory: Path): List<Path> =
            Files.walk(directory).use { files ->
                files.filter { Files.isRegularFile(it) }.sorted().toList()
            }

        // -- the run summary --------------------------------------------------------------------

        private fun record(action: String, status: String, detail: String) {
            results[action] = "$status\u0000$detail"
        }

        private fun writeRunSummary() {
            val selected = TestStore.selected()
            val report = Path.of(System.getProperty("user.dir"), "build", "reports")
                .resolve("conformance-$selected.txt")
            val counts = LinkedHashMap<String, Int>()
            val body = StringBuilder()
            for ((action, value) in results.toSortedMap()) {
                val status = value.substringBefore('\u0000')
                counts[status] = (counts[status] ?: 0) + 1
                body.append(action.padEnd(30))
                    .append(status.padEnd(16))
                    .append(value.substringAfter('\u0000'))
                    .append('\n')
            }
            val banner = testStore?.let { store ->
                runCatching { store.serverBanner() }.getOrElse { "banner unavailable: $it" }
            }
            val header = buildString {
                append("# Cerbos Exposed adapter — adversarial conformance run\n")
                append("# store:    $selected")
                banner?.let { append(" — ").append(it) }
                testStore?.collation?.let { append(" collation=").append(it) }
                append('\n')
                append("# PDP:      ${CerbosTestImage.IMAGE}\n")
                append("# exposed:  ${System.getProperty("adapter.test.exposed.version", "?")}")
                append(" (ADAPTER_TEST_ORM=${System.getProperty("adapter.test.orm", "?")})\n")
                append("# outcomes: ")
                append(counts.entries.joinToString(", ") { "${it.value} ${it.key}" })
                append("\n#\n")
                append("#   PASS            oracle-compared, and the two id sets are equal\n")
                append("#   FAIL            oracle-compared, and they are not — or translation raised\n")
                append("#   THROWS          refused with the message actions.json pins\n")
                append("#   THROW-MISMATCH  refused for a reason actions.json does not pin, or not refused\n")
                append("#   SKIP            a known upstream divergence, exercised by a tripwire instead\n")
                append("#\n")
                append("ACTION".padEnd(30)).append("STATUS".padEnd(16)).append("DETAIL\n")
            }
            Files.createDirectories(report.parent)
            Files.writeString(report, header + body)
            println("==> conformance run summary: $report")
        }

        // -- classification, derived from actions.json at run time ------------------------------

        private fun adapterUnsupported(): List<AdapterUnsupported> =
            actionsFile.adapterUnsupportedFor(ADAPTER)

        private fun adapterSupportedExpectedActions(): Set<String> =
            actionsFile.adapterSupportedExpectedFor(ADAPTER).map { it.action }.toSet()

        @JvmStatic
        fun conformanceActions(): Stream<String> =
            Corpus.oracleActions(actionsFile, ADAPTER).stream()

        @JvmStatic
        fun adapterUnsupportedActions(): Stream<Arguments> =
            adapterUnsupported().stream().map {
                Arguments.of(
                    it.action,
                    it.reason,
                    Corpus.requireMessage("adapterUnsupported.$ADAPTER.${it.action}", it.message),
                )
            }

        @JvmStatic
        fun unsupportedShapes(): Stream<Arguments> {
            val promoted = adapterSupportedExpectedActions()
            return actionsFile.expectedUnsupported
                .filter { it.action !in promoted }
                .map {
                    Arguments.of(
                        it.action,
                        Corpus.requireMessage(
                            "expectedUnsupported.${it.action}.messages.$ADAPTER",
                            it.messages?.get(ADAPTER),
                        ),
                    )
                }
                .stream()
        }

        /**
         * Actions whose `== null` probe targets an attribute the oracle OMITS for NULL columns.
         * They carry no oracle comparison: under the omitted representation check() denies every
         * row, so the adapter must reject the shape rather than emit a filter (#302).
         */
        @JvmStatic
        fun nullRepresentationOmitted(): Stream<Arguments> =
            Corpus.nullRepresentationThrows(actionsFile).stream().map {
                Arguments.of(it.action, it.reason, nullOmittedMessage(it))
            }

        /** The substring this adapter's rejection under the omitted representation must contain. */
        private fun nullOmittedMessage(entry: NullRepresentationOmitted): String =
            Corpus.nullOmittedMessage(entry, ADAPTER)

        // -- deterministic derived fields -------------------------------------------------------

        /**
         * The deterministic derived fields for one seed, read from conformance/derived-fields.json
         * rather than restated here. The same value feeds the persisted row and the check() oracle,
         * so a transcription error would be self-consistent and invisible to the differential; one
         * machine-readable definition is what makes that impossible. conformance/README.md,
         * "Deterministic derived fields", states the rules the file materialises and what each
         * value witnesses, and validate-corpus.sh re-derives them.
         */
        private fun derivedFor(seed: Seed): DerivedEntry = derivedFile.derived[seed.id]
            ?: throw AssertionError("derived-fields.json has no entry for seed \"${seed.id}\"")

        /** Deterministic label names per seed for the `macro-depth3-*` actions. */
        private fun labelsFor(seed: Seed): List<String?> = derivedFor(seed).labels

        /** Deterministic ISO instant per seed for the timestamp probe: split around 2025-01-01. */
        private fun isoFor(seed: Seed): String = derivedFor(seed).createdBy

        /** Deterministic [Instant] per seed for the `ts-*` timestamp() comparison actions. */
        private fun tsFor(seed: Seed): Instant? = derivedFor(seed).createdAt?.let(Instant::parse)

        /** Deterministic fractional double per seed for the IEEE add-solve probes. */
        private fun doubleFor(seed: Seed): Double? = derivedFor(seed).aDouble

        /** Deterministic hierarchy path per seed for the `hier-*` actions. */
        private fun scopeFor(seed: Seed): String? = derivedFor(seed).scope

        // -- coverage assertions ----------------------------------------------------------------

        /**
         * Proves this harness consumes every seed key, every principal key and every derived field
         * the corpus defines, and nothing it does not. Rejecting unknown properties on decode
         * cannot do this alone: it catches an added key but says nothing about one that disappears,
         * and a disappeared key decodes to its default on both sides of the differential.
         */
        private fun assertCorpusCoverage(mapper: ObjectMapper, conformance: Path) {
            val rawSeedsFile = mapper.readTree(conformance.resolve("seeds.json").toFile())
            val rawSeeds = rawSeedsFile.get("seeds")
            assertEquals(seeds.size, rawSeeds.size(), "seeds.json rows lost in decoding")
            for (i in 0 until rawSeeds.size()) {
                val label = "seeds.json seeds[$i]"
                assertKeys(label, keysOf(rawSeeds.get(i)), SEED_KEYS, listOf(SEED_NOTE_KEY))
                val rawTags = rawSeeds.get(i).get("tags")
                for (j in 0 until rawTags.size()) {
                    assertKeys("$label.tags[$j]", keysOf(rawTags.get(j)), TAG_KEYS, listOf())
                }
            }

            assertPrincipalCoverage(rawSeedsFile.get("principal"))

            assertKeys("derived-fields.json fields", derivedFile.fields, DERIVED_KEYS, listOf())
            assertEquals(
                seeds.map { it.id }.toCollection(TreeSet()),
                TreeSet(derivedFile.derived.keys),
                "derived-fields.json must carry exactly one entry per seeds.json id",
            )
            val rawDerived = mapper
                .readTree(conformance.resolve("derived-fields.json").toFile()).get("derived")
            for (entry in rawDerived.properties()) {
                assertKeys(
                    "derived-fields.json derived[\"${entry.key}\"]",
                    keysOf(entry.value),
                    DERIVED_KEYS,
                    listOf(),
                )
            }
        }

        /**
         * Guards the corpus principal the way [assertCorpusCoverage] guards a seed row: the
         * top-level keys, then the keys one level in.
         *
         * Asserted against the RAW JSON because [principal] rebuilds the principal from
         * [PrincipalSpec] — a rebuilt object could only ever report the keys this harness already
         * names. The attribute VALUES are asserted too: a key-set guard says nothing about a change
         * inside one, and most of these attributes are lists. [asPrincipalAttribute] accepts
         * exactly a string and a list of strings, so a third shape fails here, next to the
         * declaration, rather than deep in the conversion.
         */
        private fun assertPrincipalCoverage(principal: JsonNode) {
            assertKeys("seeds.json principal", keysOf(principal), PRINCIPAL_KEYS, listOf())
            val attr = principal.get("attr")
            assertKeys("seeds.json principal.attr", keysOf(attr), PRINCIPAL_ATTR_KEYS, listOf())
            for (entry in attr.properties()) {
                val label = "seeds.json principal.attr.${entry.key}"
                val value = entry.value
                val listOfStrings = value.isArray && value.all { it.isTextual }
                assertTrue(value.isTextual || listOfStrings) {
                    "$label is neither a string nor a list of strings, the only two shapes this" +
                        " harness consumes: a reshaped principal attribute feeds the plan and the" +
                        " check() oracle at once"
                }
            }
        }

        private fun assertKeys(
            label: String,
            got: Collection<String>,
            want: Collection<String>,
            optional: Collection<String>,
        ) {
            val allowed = LinkedHashSet(want).also { it.addAll(optional) }
            for (key in got) {
                assertTrue(allowed.contains(key)) {
                    "$label carries \"$key\", which this harness does not consume: an unconsumed" +
                        " corpus field is dropped from the persisted row and the check() oracle at" +
                        " once"
                }
            }
            val missing = LinkedHashSet(want).also { it.removeAll(got.toSet()) }
            assertTrue(missing.isEmpty()) { "$label is missing $missing, which this harness consumes" }
        }

        private fun keysOf(node: JsonNode): List<String> = node.properties().map { it.key }

        // -- seeding ----------------------------------------------------------------------------

        private fun createSchema() = transaction(testStore!!.database) {
            SchemaUtils.create(tables = ADVERSARIAL_TABLES.toTypedArray())
        }

        private fun seed() = transaction(testStore!!.database) {
            // Distinct sub-category/category graphs per seed so no rows share relations by accident.
            var catSeq = 0
            for (s in seeds) {
                Resources.insert {
                    it[id] = s.id
                    it[aBool] = s.aBool
                    it[aString] = s.aString
                    it[aNumber] = s.aNumber
                    it[aDouble] = doubleFor(s)
                    it[aOptionalString] = s.aOptionalString
                    it[createdBy] = isoFor(s)
                    it[scope] = scopeFor(s)
                    it[createdAt] = tsFor(s)
                }
                for (tag in s.tags) {
                    Tags.insert {
                        it[tagId] = tag.id
                        it[name] = tag.name
                        it[resourceId] = s.id
                    }
                }
                for (subName in s.subCategoryNames) {
                    catSeq++
                    val cat = "adv-cat-$catSeq"
                    val sub = "adv-sub-$catSeq"
                    Categories.insert {
                        it[id] = cat
                        it[name] = "business"
                        it[resourceId] = s.id
                    }
                    SubCategories.insert {
                        it[id] = sub
                        it[name] = subName
                        it[categoryId] = cat
                    }
                    labelsFor(s).forEachIndexed { index, labelName ->
                        Labels.insert {
                            it[id] = "adv-lab-$catSeq-${index + 1}"
                            it[name] = labelName
                            it[subCategoryId] = sub
                        }
                    }
                }

                // The to-one chain, one owned row per level. A seed with no parent gets no row at
                // all, which is what makes the absent-parent hazard reachable through a SCALAR
                // rather than only through mainCategory's collection. Every resource owns a FRESH
                // parent and inner row rather than pointing at the named seed's own, so a filter
                // that returned the parent instead of the child cannot agree by accident.
                val parentSeed = parentSeedOf(s) ?: continue
                val parent = "${s.id}-parent"
                Parents.insert {
                    it[id] = parent
                    it[aBool] = parentSeed.aBool
                    it[aString] = parentSeed.aString
                    it[aNumber] = parentSeed.aNumber
                    it[aOptionalString] = parentSeed.aOptionalString
                    it[resourceId] = s.id
                }
                val innerSeed = parentSeedOf(parentSeed) ?: continue
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

        // -- the real to-one relation (conformance/README.md) ------------------------------------
        //
        // `parentSeedId` names the seed whose four scalars a row's `parent` carries, and that seed's
        // own `parentSeedId` names the ones `parent.inner` carries. The chain is cut at two levels.

        /** The seed one hop out, or null when this level has no parent. A null argument gives null. */
        private fun parentSeedOf(seed: Seed?): Seed? {
            val parentSeedId = seed?.parentSeedId ?: return null
            return seeds.firstOrNull { it.id == parentSeedId }
                ?: error(
                    "seeds.json: \"${seed.id}\" names parent \"$parentSeedId\", not a seed id",
                )
        }

        // -- oracle: ask the PDP itself, row by row ----------------------------------------------

        private fun principal(): Principal {
            val spec = seedsFile.principal
            var p = Principal.newInstance(spec.id, *spec.roles.toTypedArray())
            for ((key, value) in spec.attr) {
                p = p.withAttribute(key, asPrincipalAttribute(key, value))
            }
            return p
        }

        /**
         * One principal attribute, converted by the JSON type the corpus actually carries. Strings
         * and lists of strings are the two shapes today; anything else fails loudly rather than
         * being coerced, because a silently reshaped principal attribute feeds the plan and the
         * oracle at once and they would agree for the wrong reason.
         */
        private fun asPrincipalAttribute(key: String, value: Any): AttributeValue = when (value) {
            is String -> AttributeValue.stringValue(value)
            is List<*> -> AttributeValue.listValue(
                value.map {
                    (it as? String)?.let(AttributeValue::stringValue)
                        ?: error("seeds.json principal.attr.$key holds a non-string element")
                },
            )
            else -> error("seeds.json principal.attr.$key is neither a string nor a list of strings")
        }

        /**
         * One level of the chain as check() attributes. A NULL column is a MISSING attribute one hop
         * out, exactly as it is on the resource row itself.
         */
        private fun relationAttr(seed: Seed): MutableMap<String, AttributeValue> {
            val attrs = LinkedHashMap<String, AttributeValue>()
            attrs["aBool"] = AttributeValue.boolValue(seed.aBool)
            attrs["aString"] = AttributeValue.stringValue(seed.aString)
            attrs["aNumber"] = AttributeValue.doubleValue(seed.aNumber.toDouble())
            seed.aOptionalString?.let { attrs["aOptionalString"] = AttributeValue.stringValue(it) }
            return attrs
        }

        /** Cerbos attributes mirroring exactly what the seeded rows hold. */
        private fun asCheckResource(s: Seed): Resource {
            var r = Resource.newInstance(seedsFile.resourceKind, s.id)
                .withAttribute("aBool", AttributeValue.boolValue(s.aBool))
                .withAttribute("aString", AttributeValue.stringValue(s.aString))
                .withAttribute("aNumber", AttributeValue.doubleValue(s.aNumber.toDouble()))
                .withAttribute("createdBy", AttributeValue.stringValue(isoFor(s)))
                .withAttribute(
                    "obj",
                    AttributeValue.mapValue(mapOf("inner" to AttributeValue.stringValue(s.aString))),
                )
                .withAttribute("tags", AttributeValue.listValue(s.tags.map(::asTagAttribute)))
                .withAttribute(
                    "categories",
                    AttributeValue.listValue(
                        s.subCategoryNames.map { subName ->
                            AttributeValue.mapValue(
                                mapOf(
                                    "name" to AttributeValue.stringValue("business"),
                                    "subCategories" to AttributeValue.listValue(
                                        listOf(
                                            AttributeValue.mapValue(
                                                mapOf(
                                                    "name" to AttributeValue.stringValue(subName),
                                                    "labels" to AttributeValue.listValue(
                                                        labelsFor(s).map(::asLabelAttribute),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            )
                        },
                    ),
                )
            // A DB NULL is a missing attribute on the check side — conditions touching it must deny
            // (a CEL error), matching SQL three-valued logic excluding the row.
            s.aOptionalString?.let {
                r = r.withAttribute("aOptionalString", AttributeValue.stringValue(it))
            }
            // `owner` reads the SAME column under the OTHER null convention: a DB NULL is the
            // EXPLICITLY-null attribute. The two check() verdicts genuinely differ —
            // `null in ["x", null]` is TRUE (allow) while a MISSING owner is a CEL error (deny) —
            // and SQL cannot distinguish the two, so the adapter has to be told which one the
            // caller uses.
            r = r.withAttribute(
                "owner",
                s.aOptionalString?.let(AttributeValue::stringValue) ?: nullAttributeValue(),
            )
            // `coOwner` is the explicit-null alias of the `scope` column, the second half of
            // `null-value-f2f`: `scope` itself is omitted when NULL (below), so the corpus carries
            // the same column under both conventions.
            r = r.withAttribute(
                "coOwner",
                scopeFor(s)?.let(AttributeValue::stringValue) ?: nullAttributeValue(),
            )
            // tagNames: the scalar name projection of tags, with NULL name columns as explicit null
            // elements — the representation under which `null in R.attr.tagNames` is TRUE exactly
            // when a related row's member column IS NULL.
            r = r.withAttribute(
                "tagNames",
                AttributeValue.listValue(
                    s.tags.map { it.name?.let(AttributeValue::stringValue) ?: nullAttributeValue() },
                ),
            )
            doubleFor(s)?.let { r = r.withAttribute("aDouble", AttributeValue.doubleValue(it)) }
            scopeFor(s)?.let { r = r.withAttribute("scope", AttributeValue.stringValue(it)) }
            // A NULL created_at column is a missing attribute on the check side: timestamp() over
            // it is a CEL evaluation error (deny), matching SQL NULL exclusion.
            tsFor(s)?.let {
                r = r.withAttribute("createdAt", AttributeValue.stringValue(it.toString()))
            }
            // mainCategory mirrors the row's single category as ONE nested object (the seeder
            // creates at most one category per seed), so direct dotted-chain CEL expressions
            // evaluate cleanly; rows without a category get NO attribute — a CEL missing-attribute
            // error (deny), matching the adapter's empty chain excluding the row.
            if (s.subCategoryNames.isNotEmpty()) {
                r = r.withAttribute(
                    "mainCategory",
                    AttributeValue.mapValue(
                        mapOf(
                            "name" to AttributeValue.stringValue("business"),
                            "subCategories" to AttributeValue.listValue(
                                s.subCategoryNames.map {
                                    AttributeValue.mapValue(
                                        mapOf("name" to AttributeValue.stringValue(it)),
                                    )
                                },
                            ),
                            "subNames" to AttributeValue.listValue(
                                s.subCategoryNames.map(AttributeValue::stringValue),
                            ),
                        ),
                    ),
                )
            }
            // The real to-one chain, mirroring the seeded rows exactly. A row with no parent sends
            // NO `parent` attribute — a CEL missing-path error (deny) — matching a correlated read
            // that finds nothing; the same holds one level down for `parent.inner`.
            val parentSeed = parentSeedOf(s)
            if (parentSeed != null) {
                val parent = relationAttr(parentSeed)
                parentSeedOf(parentSeed)?.let {
                    parent["inner"] = AttributeValue.mapValue(relationAttr(it))
                }
                r = r.withAttribute("parent", AttributeValue.mapValue(parent))
            }
            return r
        }

        /**
         * An explicit protobuf NULL attribute value. The SDK's [AttributeValue] exposes no null
         * factory (string/double/bool/list/map only), so the private constructor is reached
         * reflectively — the null attribute is exactly what the `in-null-elem-*` actions exist to
         * exercise, and check() verdicts differ between an explicit null and a missing attribute.
         */
        private fun nullAttributeValue(): AttributeValue {
            val ctor = AttributeValue::class.java.getDeclaredConstructor(Value::class.java)
            ctor.isAccessible = true
            return ctor.newInstance(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
        }

        /** A NULL label name in the database is a missing element attribute on the check side. */
        private fun asLabelAttribute(name: String?): AttributeValue = AttributeValue.mapValue(
            buildMap { name?.let { put("name", AttributeValue.stringValue(it)) } },
        )

        /** A NULL tag name in the database is a missing element attribute on the check side. */
        private fun asTagAttribute(tag: Tag): AttributeValue = AttributeValue.mapValue(
            buildMap {
                put("id", AttributeValue.stringValue(tag.id))
                tag.name?.let { put("name", AttributeValue.stringValue(it)) }
            },
        )

        private fun oracleAllowedIds(action: String): List<String> = oracleCache.getOrPut(action) {
            seeds.filter { client!!.check(principal(), asCheckResource(it), action).isAllowed(action) }
                .map { it.id }
                .sorted()
        }

        /**
         * The plan the PDP produces for one action. The SDK's single-action `plan` overload is
         * deprecated in favour of the multi-action one; one action in the list is the same request,
         * and the result's filter is that action's.
         */
        private fun plan(action: String): PlanResourcesResult = client!!.plan(
            principal(), Resource.newInstance(seedsFile.resourceKind), listOf(action),
        )

        // -- adapter execution through the public surface ---------------------------------------

        private fun adapterFilteredIds(
            action: String,
            representation: NullAttributeRepresentation = NullAttributeRepresentation.EXPLICIT,
            mapping: AttributeMappings = MAPPING,
        ): List<String> = idsSelectedBy(
            ExposedQueryPlanAdapter.toFilter(
                plan(action),
                Options.of(mapping).withNullAttributeRepresentation(representation),
            ),
        )

        /**
         * The ids a caller's query returns under [filter], each kind handled the way a caller is
         * told to handle it: an always-denied plan skips the query, an always-allowed one runs with
         * no authorization clause, and a conditional one composes the predicate into `where`.
         * Collapsing all three onto `toOp()` would make the always-denied case a claim about
         * `Op.FALSE` rather than about the branch a caller writes.
         */
        private fun idsSelectedBy(filter: QueryPlanFilter): List<String> =
            transaction(testStore!!.database) {
                when (filter) {
                    QueryPlanFilter.AlwaysDenied -> emptyList()
                    QueryPlanFilter.AlwaysAllowed -> selectIds(null)
                    is QueryPlanFilter.Conditional -> selectIds(filter.op)
                }
            }

        private fun selectIds(predicate: Op<Boolean>?): List<String> {
            var query = Resources.select(Resources.id).withDistinct()
            if (predicate != null) query = query.where(predicate)
            return query.orderBy(Resources.id, SortOrder.ASC).map { it[Resources.id] }
        }

        /** Whether any operand anywhere in the plan is a literal null, or a list containing one. */
        private fun planCarriesNullLiteral(operand: Operand): Boolean = when (operand.nodeCase) {
            Operand.NodeCase.VALUE -> {
                val value = operand.value
                value.kindCase == Value.KindCase.NULL_VALUE ||
                    (
                        value.kindCase == Value.KindCase.LIST_VALUE &&
                            value.listValue.valuesList.any { it.kindCase == Value.KindCase.NULL_VALUE }
                        )
            }
            Operand.NodeCase.EXPRESSION ->
                operand.expression.operandsList.any(::planCarriesNullLiteral)
            else -> false
        }

        // -- synthesised plans, for properties no corpus action spells ---------------------------

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

        /** Translate a synthesised CONDITIONAL plan and execute it against the seeded store. */
        private fun filteredIdsFor(condition: Operand): List<String> {
            val response = PlanResourcesResponse.newBuilder()
                .setFilter(
                    PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition),
                )
                .build()
            return idsSelectedBy(ExposedQueryPlanAdapter.toFilter(response, Options.of(MAPPING)))
        }

        // -- the degeneracy guard's two lists ----------------------------------------------------

        /**
         * Oracle-compared actions whose oracle is degenerate BY CONSTRUCTION, each with the reason.
         *
         * The guard below sweeps EVERY oracle-compared action, so this is the only way out of it,
         * and each entry is asserted to be degenerate as claimed: an entry whose oracle stopped
         * being trivial fails, so the list cannot rot into a blanket exemption. This adapter
         * compares the whole `conformance` group, so it carries more of these than a harness that
         * refuses the shapes outright. `filter-as-conjunct` and `null-eq-missing` are also empty by
         * construction, but this adapter throws or rejects them, so they never reach the comparison
         * and carry their own anti-vacuity assertions instead.
         */
        private val DEGENERATE_BY_CONSTRUCTION = mapOf(
            // `R.attr.aString in []`: nothing is a member of the empty list, so the planner folds
            // the plan to ALWAYS_DENIED and check() denies every seed. The comparison is still made
            // — an adapter that emitted an empty IN list and let the database match nothing would
            // agree by accident — which is why it stays oracle-compared rather than excluded.
            "in-empty" to
                "statically false membership: the plan is ALWAYS_DENIED and the oracle is empty",
            // `R.attr.aDouble < -1e19`: no seed lies below the literal (g1, at -9.5e18, is the
            // closest), so check() denies every seed. The shape exists to catch a translator that
            // narrows the literal to Long.MIN, which would return g1; the mirrored `double-huge-gt`
            // carries the non-empty oracle.
            "double-huge-lt" to
                "no seed lies below -1e19: the oracle is empty, and the mirrored double-huge-gt is" +
                " the compared half",
            // `size(R.attr.aString) > 4294967296` and its `<` mirror. No string's length leaves int
            // range and every seed carries a non-null aString, so the `>` half denies every seed
            // and the `<` half allows every seed. The pair exists to catch an unguarded narrowing
            // cast that wrapped the threshold, and neither half can be non-degenerate while the
            // corpus holds no NULL aString.
            "size-huge-gt" to
                "no string's length exceeds 2^32: the oracle is empty, and the int-narrowing wrap" +
                " it catches would return every non-empty aString",
            "size-huge-lt" to
                "every seed carries a non-null aString shorter than 2^32: the oracle is total, and" +
                " the int-narrowing wrap it catches would return nothing",
            // The three chain-count spellings over `R.attr.mainCategory.subCategories`
            // (#316/#333): every seed that HOLDS a mainCategory holds exactly one subCategory, and
            // every seed without one is a CEL missing-path error (deny) — so a count of zero, a
            // negated count above zero, and a count of 1.5 or more each hold for no seed. The
            // corpus keeps them because the absent-parent guard fails in the OTHER direction.
            "w1-size-zero-chain" to
                "no seed holds a mainCategory with zero subCategories, and a seed with none is a" +
                " missing-path error: the oracle is empty",
            "w1-not-size-chain" to
                "the negation of a count every present chain satisfies, over rows whose absent" +
                " chain is a missing-path error: the oracle is empty",
            "w1-size-frac-chain" to
                "no seed holds a mainCategory with two or more subCategories, so a count of 1.5 or" +
                " more holds for none: the oracle is empty",
            // Three IEEE traps whose EMPTY oracle is the whole point: each denies every seed in
            // double space, and each has a sibling or a store that returns rows for it.
            "p-double-frac" to
                "3 * 0.1 is not 0.3 in IEEE double: the oracle is empty, and exact-decimal" +
                " arithmetic on the store would return the aNumber=3 seeds",
            "arith-add-eq-frac" to
                "no double satisfies aDouble + 0.7 == 0.1 in IEEE arithmetic: the oracle is empty," +
                " and an algebraic pre-solve would return a1",
            "nan-ord-le" to
                "1.0 <= 0.5 is false and every ordering against NaN is false: the oracle is empty," +
                " and a total-order comparison would return the aBool=false seeds",
        )

        /**
         * Shapes this adapter refuses to translate: they have no oracle comparison to guard, and
         * stay here as PDP/policy liveness probes for a group the sweep above cannot cover.
         *
         * The first six are this adapter's own `adapterUnsupported` entries, one per refusal
         * mechanism and every one with an oracle that discriminates, so each refused family still
         * proves its policy is live. The last two are `expectedUnsupported` shapes: `int()` over a
         * numeric column, where CEL truncates toward zero and PostgreSQL and MySQL round, and a
         * regex with a top-level alternation, which is a `matches()` no SQL engine implements in
         * CEL's dialect.
         */
        private val DEGENERACY_LIVENESS_PROBES = listOf(
            "arith-mod",
            "cr-div-then-add",
            "cr-div-then-add-ne",
            "hier-empty-delim",
            "index-scalar-list",
            "map-eq-list",
            "cast-int-double",
            "matches-alt",
        )
    }

    // -- the differential ----------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceActions")
    fun adapterMatchesCheckOracle(action: String) {
        val oracle = oracleAllowedIds(action)
        val filtered = try {
            adapterFilteredIds(action)
        } catch (e: Throwable) {
            record(action, "FAIL", "oracle=$oracle, raised ${e.javaClass.simpleName}: ${e.message}")
            throw e
        }
        val equal = oracle == filtered
        record(
            action,
            if (equal) "PASS" else "FAIL",
            if (equal) "${oracle.size} ids $oracle" else "oracle=$oracle adapter=$filtered",
        )
        assertEquals(oracle, filtered, "adapter result diverges from check-API oracle for '$action'")
    }

    /**
     * Probe shapes the adapter does not support: the translation must fail loudly (never a
     * silently-wrong filter). Messages pinned so a regression to silent acceptance is caught.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedShapes")
    fun unsupportedShapesThrow(action: String, expectedMessage: String) {
        assertRefused(action, expectedMessage, "expectedUnsupported")
    }

    /**
     * Conformance actions this adapter cannot express. They are excluded from the oracle comparison
     * and must fail loudly instead — the invariant is absolute either way: an inexpressible shape
     * throws before its filter can be used.
     *
     * Deliberately NOT `allowZeroInvocations`: the list is not empty, and a parser change that
     * dropped this adapter's group would otherwise turn every case here into silence.
     * [manifestAssignsEveryActionExactlyOneOutcome] pins the throwing count beside it, so a shape
     * that joins or leaves this group has to be counted there too.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapterUnsupportedActions")
    fun adapterUnsupportedActionsThrow(action: String, reason: String, expectedMessage: String) {
        assertRefused(action, expectedMessage, reason)
    }

    /** Translated once, so the summary line and the assertions describe the same attempt. */
    private fun assertRefused(action: String, expectedMessage: String, reason: String) {
        val raised = runCatching { adapterFilteredIds(action) }.exceptionOrNull()
        val pinned = raised is IllegalArgumentException &&
            expectedMessage in raised.message.orEmpty()
        record(
            action,
            if (pinned) "THROWS" else "THROW-MISMATCH",
            if (pinned) raised!!.message.orEmpty()
            else "wanted \"$expectedMessage\", got " +
                (raised?.let { "${it.javaClass.simpleName}: ${it.message}" } ?: "a filter"),
        )
        val error: Throwable = raised
            ?: fail("action '$action' returned a filter; actions.json says it must throw ($reason)")
        assertInstanceOf(IllegalArgumentException::class.java, error) {
            "action '$action' failed with an unclassified error ($reason): $error"
        }
        assertTrue(expectedMessage in error.message.orEmpty()) {
            "action '$action' was rejected for a reason actions.json does not declare: ${error.message}"
        }
    }

    /**
     * #302. `null-eq-missing` probes `aOptionalString == null`, and `aOptionalString` follows the
     * corpus default: a NULL column sends NO attribute. Both halves are asserted because the
     * rejection alone would pass vacuously if the adapter threw for an unrelated reason — the
     * over-grant under the default representation is what makes the rejection necessary.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nullRepresentationOmitted")
    fun nullRepresentationOmittedIsRejected(action: String, reason: String, message: String) {
        assertEquals(emptyList<String>(), oracleAllowedIds(action), reason)

        val underExplicit = runCatching { adapterFilteredIds(action) }
        val underOmitted = runCatching {
            adapterFilteredIds(action, NullAttributeRepresentation.OMITTED)
        }.exceptionOrNull()
        val pinned = underOmitted is IllegalArgumentException &&
            message in underOmitted.message.orEmpty()
        record(
            action,
            if (pinned) "THROWS" else "THROW-MISMATCH",
            "under EXPLICIT: " +
                underExplicit.fold({ "$it" }, { "raised ${it.javaClass.simpleName}: ${it.message}" }) +
                "; under OMITTED: ${underOmitted?.message ?: "returned a filter"}",
        )

        // The default translation emits IS NULL and returns exactly the rows the PDP denies.
        assertFalse(underExplicit.getOrThrow().isEmpty(), reason)
        val error: Throwable = underOmitted
            ?: fail("'$action' returned a filter under the omitted representation; it must throw")
        assertInstanceOf(IllegalArgumentException::class.java, error) {
            "'$action' must be refused under the omitted representation, got $error"
        }
        assertTrue(message in error.message.orEmpty(), error.message)
    }

    /**
     * #308. The per-attribute declaration overrides the call-level option, which is the property
     * that makes a suite mixing both conventions expressible at all. Asserted in both directions
     * against the SAME action and the SAME call-level option, varying only whether the mapping
     * declares the convention — so a declaration that did nothing would show up here as the two
     * runs agreeing. It also proves the completeness guard below is not quietly running against the
     * same mapping.
     */
    @Test
    fun perAttributeDeclarationOverridesTheCallLevelRepresentation() {
        // `owner` declares EXPLICIT, so the call-level OMITTED does not reach it.
        assertEquals(
            oracleAllowedIds("null-eq"),
            adapterFilteredIds("null-eq", NullAttributeRepresentation.OMITTED),
        )

        // Strip the declaration and the same action under the same option is rejected — so the
        // stripped mapping the completeness guard uses is not quietly equivalent to MAPPING.
        val ex = assertThrows(IllegalArgumentException::class.java) {
            adapterFilteredIds(
                "null-eq", NullAttributeRepresentation.OMITTED, MAPPING_WITHOUT_NULL_CONVENTIONS,
            )
        }
        assertTrue("null operand" in ex.message.orEmpty(), ex.message)
    }

    /**
     * #302 completeness guard. The rejection must key off the null OPERAND, not off a list of
     * operators: `hasIntersection(tagNames, ["public", null])` carries one in its value list, and
     * an allowlist of eq/ne/in silently misses it. Enumerating the corpus rather than naming shapes
     * means a newly added action carrying a null constant is covered automatically.
     */
    @Test
    fun everyNullCarryingActionIsRejectedUnderOmitted() {
        val manifest = LinkedHashSet(actionsFile.conformance)
        actionsFile.expectedUnsupported.forEach { manifest.add(it.action) }
        actionsFile.nullRepresentationOmitted.forEach { manifest.add(it.action) }

        val nullCarrying = manifest.sorted().filter { action ->
            plan(action).condition.orElse(null)?.let(::planCarriesNullLiteral) ?: false
        }

        // Guard the guard: if the walk stopped finding null operands the loop below is vacuous.
        assertTrue(nullCarrying.contains("null-eq-missing"), nullCarrying.toString())
        assertTrue(nullCarrying.contains("in-null-elem-hasint"), nullCarrying.toString())

        val pinned = nullOmittedMessage(actionsFile.nullRepresentationOmitted.first())
        val notRejected = mutableListOf<String>()
        for (action in nullCarrying) {
            val raised = runCatching {
                adapterFilteredIds(
                    action, NullAttributeRepresentation.OMITTED, MAPPING_WITHOUT_NULL_CONVENTIONS,
                )
            }.exceptionOrNull()
            when {
                raised == null -> notRejected.add(action)
                // The rejection must be the null-operand check talking, not an incidental failure:
                // a mapping typo counting as the required rejection is the silent pass the corpus
                // README warns about.
                raised !is IllegalArgumentException || pinned !in raised.message.orEmpty() ->
                    notRejected.add("$action (rejected for the wrong reason: ${raised.message})")
            }
        }
        assertEquals(emptyList<String>(), notRejected)
    }

    /**
     * Tripwire pinning the known UPSTREAM planner over-grant on the `has(...)` macro.
     *
     * The Cerbos query planner constant-folds `has(R.attr.aOptionalString)` (action `p-has`) to
     * `KIND_ALWAYS_ALLOWED` — "return every row" — even though the `check()` API denies resources
     * that lack the attribute. The fold happens at the PLANNER, so every query-plan adapter is
     * affected equally; this adapter translates the always-allowed plan faithfully. That is why
     * `p-has` is a known divergence rather than a conformance action.
     *
     * Both halves of the divergence are asserted — the plan kind AND the check() denials — so that
     * the moment an upstream image stops folding, this fails with explicit re-inclusion
     * instructions instead of the coverage hole silently becoming permanent. The suite runs against
     * the pinned image in [CerbosTestImage], so the "fires when upstream fixes the fold" property
     * is dormant between image bumps; re-evaluate it on every deliberate bump of that pin.
     */
    @Test
    fun upstreamHasFoldOverGrantTripwire() {
        val plan = plan("p-has")
        val allIds = seeds.map { it.id }.sorted()
        val oracle = oracleAllowedIds("p-has")
        record("p-has", "SKIP", "known divergence: plan is ${plan.raw.filter.kind}, oracle=$oracle")

        val upstreamChanged = """

            UPSTREAM CHANGE DETECTED: the Cerbos planner's has() -> KIND_ALWAYS_ALLOWED over-grant
            no longer reproduces on the image under test.

            Until now, has(R.attr.aOptionalString) (action 'p-has') planned as KIND_ALWAYS_ALLOWED
            while check() denied rows without the attribute — a known upstream planner fold this
            adapter translated faithfully into "return all rows". 'p-has' is therefore a
            knownDivergences entry naming this adapter, and excluded from the oracle comparison.
            This tripwire exists to keep that exclusion honest.

            The exclusion is no longer justified. Do ALL of the following:
              1. Classify "p-has" as a shared conformance action and drop this adapter from its
                 known-divergence entry — the differential oracle then owns has() semantics and
                 will catch any mistranslation of the new residual plan shape mechanically.
              2. Run the oracle. If the adapter cannot translate the residual shape the planner now
                 emits for has(), implement or fail-closed route that shape before re-including.
              3. Update the README's gotcha on has() to reflect the fixed planner behaviour.
              4. Delete this tripwire test.

            Observed on this run:
              plan kind for 'p-has': ${plan.raw.filter.kind} (pinned while broken: KIND_ALWAYS_ALLOWED)
              check() allowed ${oracle.size} of ${allIds.size} seeded rows: $oracle
        """.trimIndent()

        // Pinned fact 1: the planner still folds has(...) to an unconditional allow-all plan.
        assertTrue(plan.isAlwaysAllowed, upstreamChanged)
        // Pinned fact 2: the check() oracle diverges from that plan — at least one seeded row
        // (a2/a4/a8/c2 hold NULL aOptionalString) is denied while the plan admits everything.
        assertTrue(oracle.size < allIds.size, upstreamChanged)
        assertTrue(oracle.contains("a1")) {
            "sanity: check() must still allow rows whose aOptionalString is set; oracle=$oracle"
        }

        // The over-grant itself, measured. `allIds == adapterFilteredIds("p-has")` on its own is a
        // tautology — an always-allowed plan adds no WHERE clause, so the query returns every row
        // by construction whatever the PDP or the table held. What makes it an over-grant is the
        // set the PDP DENIES being non-empty and every one of those ids coming back from the
        // unfiltered query a caller runs for this plan.
        val denied = TreeSet(allIds).also { it.removeAll(oracle.toSet()) }
        assertFalse(denied.isEmpty()) {
            "p-has: check() must deny at least one seed, or there is no over-grant to see"
        }
        val unfiltered = idsSelectedBy(QueryPlanFilter.AlwaysAllowed)
        assertTrue(unfiltered.containsAll(denied)) {
            "the unfiltered query must return every row the PDP denies for p-has; denied $denied," +
                " got $unfiltered"
        }
        assertEquals(allIds, unfiltered)

        // And the adapter translates the always-allowed plan faithfully into that same unfiltered
        // query rather than second-guessing the plan kind.
        assertEquals(unfiltered, adapterFilteredIds("p-has")) {
            "the adapter is expected to translate KIND_ALWAYS_ALLOWED faithfully into all rows —" +
                " if this fails the adapter started second-guessing plan kinds"
        }
    }

    /**
     * The corpus pins two count spellings over the chain — `size(...) == 0` and `!(size(...) > 0)` —
     * but the guard has to be a property of the COUNT rather than of the two spellings that happen
     * to be pinned. These synthesise the remaining threshold/polarity combinations onto the same
     * seeded store and assert the parentless rows stay out of every one, including an arbitrary-N
     * threshold that neither corpus action reaches (cerbos/query-plan-adapters#316).
     */
    @Test
    fun everyCountThresholdOverTheChainInheritsTheAbsentParentGuard() {
        val chain = Operand.newBuilder()
            .setVariable("request.resource.attr.mainCategory.subCategories").build()
        val size = expression("size", chain)

        // Every seed that HAS a mainCategory holds exactly one subCategory, and every seed without
        // it is a CEL missing-path error — so each of these is empty unless the guard leaks.
        val emptyByConstruction = linkedMapOf(
            "size(chain) == 0" to compare("eq", size, 0.0),
            "size(chain) <= 0" to compare("le", size, 0.0),
            "size(chain) < 1" to compare("lt", size, 1.0),
            "size(chain) >= 2" to compare("ge", size, 2.0),
            "!(size(chain) > 0)" to expression("not", compare("gt", size, 0.0)),
            "!(size(chain) >= 1)" to expression("not", compare("ge", size, 1.0)),
            "!(size(chain) < 2)" to expression("not", compare("lt", size, 2.0)),
        )
        emptyByConstruction.forEach { (shape, condition) ->
            assertEquals(
                emptyList<String>(),
                filteredIdsFor(condition),
                "absent-parent guard leaked for $shape",
            )
        }

        // The mirror image, so the loop above cannot pass by denying everything: `>= 0` and `< 2`
        // are TRUE for exactly the rows that HAVE the parent.
        val withParent = oracleAllowedIds("w1-size-nonneg-chain")
        assertFalse(withParent.isEmpty(), "sanity: some seed must carry a mainCategory")
        assertTrue(withParent.size < seeds.size, "sanity: not every seed carries one")
        assertEquals(withParent, filteredIdsFor(compare("ge", size, 0.0)))
        assertEquals(withParent, filteredIdsFor(compare("lt", size, 2.0)))
    }

    /**
     * #387. `filter-as-conjunct` puts a filter() one level below the root, where the guard that
     * refuses `filter-as-condition` does not look. Its oracle is empty BY CONSTRUCTION — check()
     * cannot evaluate a non-boolean conjunction — so it belongs to neither degeneracy-guard list,
     * and the throw suite on its own would say nothing about whether refusing it is REQUIRED.
     *
     * This is that argument. The other conjunct is `R.attr.aBool`, which this adapter certainly can
     * express and which `root-bare-bool` spells on its own; an adapter that dropped the conjunct it
     * could not translate would emit exactly that predicate and return every row it selects, all of
     * which the PDP denies for this action.
     */
    @Test
    fun filterAsConjunctMustBeRefusedBecauseDroppingItsUntranslatableHalfOverGrants() {
        assertEquals(emptyList<String>(), oracleAllowedIds("filter-as-conjunct")) {
            "check() must deny every seed: a filter() in boolean position is not evaluable"
        }

        val survivingHalf = adapterFilteredIds("root-bare-bool")
        assertFalse(survivingHalf.isEmpty()) {
            "root-bare-bool must return rows, else dropping the other conjunct would cost nothing"
        }
        assertTrue(survivingHalf.size < seeds.size, "root-bare-bool must not return every seed")

        val message = unsupportedShapes()
            .filter { "filter-as-conjunct" == it.get()[0] }
            .map { it.get()[1] as String }
            .findFirst()
            .orElseThrow { AssertionError("filter-as-conjunct pins no throw message") }
        val ex = assertThrows(IllegalArgumentException::class.java) {
            adapterFilteredIds("filter-as-conjunct")
        }
        assertTrue(message in ex.message.orEmpty(), ex.message)
    }

    /**
     * Adding a throwing action without pinning its message must fail this harness rather than
     * silently degrade the throw suite to a bare "it threw" (cerbos/query-plan-adapters#326).
     */
    @Test
    fun throwingActionWithNoPinnedMessageFailsClassification() {
        for (absent in listOf(null, "")) {
            val ex = assertThrows(IllegalStateException::class.java) {
                Corpus.requireMessage("synthetic-entry", absent)
            }
            assertTrue("pins no throw message" in ex.message.orEmpty(), ex.message)
        }
    }

    /**
     * Corpus-size tripwire and exactly-once partition. A corpus edit must bump the pinned counts in
     * the same change — without this, a new hostile action silently joins the oracle run, and a
     * group dropped by [ActionsFile] would make its actions vanish from every parameterised case
     * with nothing failing.
     */
    @Test
    fun manifestAssignsEveryActionExactlyOneOutcome() {
        val supportedExpected = adapterSupportedExpectedActions()
        val oracle = conformanceActions().toList().toSet()
        val throwing = adapterUnsupported().map { it.action }.toMutableSet()
        actionsFile.expectedUnsupported
            .map { it.action }
            .filter { it !in supportedExpected }
            .forEach { throwing.add(it) }
        val nullOmitted = actionsFile.nullRepresentationOmitted.map { it.action }.toSet()
        val skipped = actionsFile.skippedDivergences(ADAPTER)

        // Every group, from the one place that knows them all — so a group added to actions.json
        // and not to `ActionsFile` fails here rather than vanishing from the count.
        val manifest = actionsFile.manifestActions()

        val misclassified = manifest.filter { action ->
            listOf(
                action in oracle,
                action in throwing,
                action in nullOmitted,
                action in skipped,
            ).count { it } != 1
        }

        assertEquals(205, manifest.size) {
            "corpus size changed; triage the new action(s) before bumping this pin"
        }
        assertEquals(22, seeds.size, "seed count changed")
        // Throwing-count tripwire: each of these carries a pinned message, so a shape gained or
        // lost has to be re-triaged here rather than joining the throw suite unnoticed.
        assertEquals(17, throwing.size, "throwing action count changed")
        assertEquals(
            throwing.size.toLong(),
            adapterUnsupportedActions().count() + unsupportedShapes().count(),
            "every throwing action must reach a parameterised throw case",
        )
        assertEquals(emptyList<String>(), misclassified) {
            "every manifest action must have exactly one $ADAPTER outcome"
        }
        // The throwing set derived here and the one [Corpus.throwingActions] derives — the form a
        // translator unit test consumes offline — must be the same set. Two derivations of one
        // classification that disagree is how a shape ends up asserted by one suite and by neither.
        assertEquals(
            throwing.toCollection(TreeSet()),
            TreeSet(Corpus.throwingActions(actionsFile, ADAPTER).keys),
        )
        assertTrue(
            actionsFile.expectedUnsupported.map { it.action }.toSet().containsAll(supportedExpected),
        ) { "every promoted action must exist in expectedUnsupported" }
    }

    /**
     * Guard the guard, over the WHOLE oracle set. The comparison in [adapterMatchesCheckOracle]
     * passes vacuously when the oracle is trivial — the PDP denying every seed, or allowing every
     * seed, whatever the adapter emitted — so every oracle-compared action must produce a non-empty,
     * non-total oracle, minus the entries [DEGENERATE_BY_CONSTRUCTION] accounts for. A
     * representative sample used to stand here (cerbos/query-plan-adapters#324); a sample leaves
     * the actions it does not name free to go degenerate unnoticed.
     */
    @Test
    fun everyOracleComparedActionHasANonDegenerateOracle() {
        val oracleActions = conformanceActions().toList()
        val compared = oracleActions.toSet()
        val degenerate = oracleActions
            .filter { it !in DEGENERATE_BY_CONSTRUCTION }
            .mapNotNull { action ->
                val ids = oracleAllowedIds(action)
                if (ids.isEmpty() || ids.size >= seeds.size) "$action: $ids" else null
            }
        assertEquals(emptyList<String>(), degenerate) {
            "these oracle-compared actions have a degenerate oracle: the differential cannot fail" +
                " for them, so either the corpus lost its discriminating seed or the action belongs" +
                " in DEGENERATE_BY_CONSTRUCTION with a reason"
        }

        // The allowlist is asserted in both directions: each entry is oracle-compared (an entry
        // this adapter refuses exempts nothing), and each is degenerate as it claims (an entry
        // whose oracle became discriminating is a guard entry wearing an exemption).
        for ((action, reason) in DEGENERATE_BY_CONSTRUCTION) {
            assertTrue(action in compared) {
                "'$action' exempts nothing: this adapter does not oracle-compare it"
            }
            val ids = oracleAllowedIds(action)
            assertTrue(ids.isEmpty() || ids.size >= seeds.size) {
                "'$action' is allowlisted as degenerate ($reason) but its oracle discriminates: $ids"
            }
        }
        // Asserting the complement keeps the split honest — an action this adapter gains support
        // for must move out of the liveness probes and into the total sweep above.
        for (action in DEGENERACY_LIVENESS_PROBES) {
            assertFalse(action in compared) {
                "'$action' is now oracle-compared: remove it from the liveness probes"
            }
            val ids = oracleAllowedIds(action)
            assertTrue(ids.isNotEmpty() && ids.size < seeds.size) {
                "oracle for '$action' is degenerate: $ids"
            }
        }
    }

    /**
     * The store the leg claims to be running against is the store it is talking to.
     *
     * Without this a leg whose container failed to start, or whose `adapter.test.db` never reached
     * the JVM, replays the corpus on the default H2 and reports the PostgreSQL leg green. Each
     * banner query is a syntax error or a different answer on the other engines, so this fails in
     * both directions rather than only when a container is missing.
     */
    @Test
    fun theStoreUnderTestIsTheStoreSelected() {
        val store = testStore!!
        val banner = store.serverBanner()
        assertEquals(
            mapOf("store" to store.name, "engine" to store.expectedEngine),
            mapOf("store" to store.name, "engine" to banner.substringBefore(' ')),
            "the ${store.name} leg is talking to a different engine: $banner",
        )
        // The MySQL leg's other precondition, and the one no banner reports: the collation the
        // server was started with. MySQL's own default makes `=` case-insensitive while CEL's is
        // byte-exact, so a leg that lost the setting reports a translator bug that is not one.
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
     * The to-one relation carries no corpus action that would notice a seeder storing no chain at
     * all, or attaching every parent to the wrong resource. Read the two hops back through a real
     * join rather than counting rows: a count cannot tell an inner row carrying the corpus's values
     * from one carrying the root's own columns, which is exactly the flat-column-alias failure this
     * relation exists to make visible.
     */
    @Test
    fun seededToOneChainMatchesTheCorpusRelation() {
        val withParent = seeds.count { parentSeedOf(it) != null }
        val withInner = seeds.count { parentSeedOf(parentSeedOf(it)) != null }
        assertTrue(withParent > 0, "no seed has a parent")
        assertTrue(withInner > 0, "no seed reaches parent.inner")
        assertTrue(withParent < seeds.size, "every seed has a parent")

        val want = seeds.associate { s ->
            val parent = parentSeedOf(s)
            s.id to listOf(parent?.aString, parentSeedOf(parent)?.aString)
        }

        val got = transaction(testStore!!.database) {
            Resources
                .join(Parents, JoinType.LEFT, onColumn = Resources.id, otherColumn = Parents.resourceId)
                .join(Inners, JoinType.LEFT, onColumn = Parents.id, otherColumn = Inners.parentId)
                .select(Resources.id, Parents.aString, Inners.aString)
                .associate {
                    it[Resources.id] to
                        listOf(it.getOrNull(Parents.aString), it.getOrNull(Inners.aString))
                }
        }
        assertEquals(want, got)
    }

    /**
     * Every table the mapping names was created and holds rows. An empty relation table leaves
     * every collection macro trivially satisfied on BOTH sides of the differential, which reads as
     * a pass rather than a failure.
     */
    @Test
    fun everyMappedTableWasCreatedAndSeeded() {
        val rows = transaction(testStore!!.database) {
            ADVERSARIAL_TABLES.associate { it.tableName to it.selectAll().count() }
        }
        assertEquals(
            emptySet<String>(),
            rows.filterValues { it == 0L }.keys,
            "seeds.json produced no rows for these tables",
        )
        assertEquals(seeds.size.toLong(), rows.getValue(Resources.tableName))
    }

    /**
     * The golden wire fixtures cover the manifest, and a fixture decodes to the plan the live PDP
     * produces — which is what lets a translator unit test stand in for this suite offline
     * (docs/adr/0006).
     */
    @Test
    fun everyManifestActionHasAWireFixture() {
        assertEquals(
            actionsFile.manifestActions().toList(),
            Corpus.wireFixtureActions(),
            "conformance/wire-fixtures/ must carry exactly one fixture per manifest action",
        )
        val action = "cs-eq"
        assertEquals(plan(action).raw.filter, Corpus.planFromWireFixture(action).filter)

        // The one operand a fixture cannot pin: `ts-window` compares against `now() - 24h`, which
        // the planner folds to a different literal on every capture, so the capture script rewrites
        // it to a placeholder and reading a fixture back means CHOOSING a value. That choice lands
        // in a translator unit test's golden expectation, so it is asserted here rather than left
        // to whichever suite happens to notice a placeholder reaching the translator.
        val substituted = Corpus.planFromWireFixture("ts-window").filter.toString()
        assertFalse("__NOW_MINUS_24H__" in substituted, "the placeholder reached the translator")
        assertTrue(Corpus.PLANNED_AT in substituted, substituted)
    }
}
