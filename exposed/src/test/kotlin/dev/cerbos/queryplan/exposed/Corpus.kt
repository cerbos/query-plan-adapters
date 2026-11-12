package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Reads the shared `../conformance/` corpus (the recorded goldens, the dataset and the PDP tags)
 * and this adapter's `conformance-ledger.json`. The mapping every case is translated with lives in
 * [TestSchema] beside the tables it names.
 *
 * Every adapter has its own copy of this loader on purpose; do not extract a shared one. See
 * `docs/adr/0007-adapters-share-data-not-code.md`.
 */
internal object Corpus {

    /** The corpus key for this adapter: its directory name. */
    const val ADAPTER: String = "exposed"

    /**
     * Kotlin data classes decode through the Kotlin module. A duplicated ledger key would otherwise
     * keep only its last entry.
     */
    val JSON: ObjectMapper = ObjectMapper().registerKotlinModule()
        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)

    fun conformanceDir(): Path =
        Path.of(System.getProperty("user.dir"), "..", "conformance").normalize()

    fun readJson(file: Path): JsonNode = JSON.readTree(file.toFile())

    // -- the PDPs and their goldens ---------------------------------------------------------------

    /** The tags of `pdp-versions.json`, current first. */
    fun pdpTags(): List<String> {
        val versions = readJson(conformanceDir().resolve("pdp-versions.json"))
        return listOf(versions["current"]["tag"].asText(), versions["previous"]["tag"].asText())
    }

    /** The current PDP's tag. */
    fun currentTag(): String = pdpTags()[0]

    /** Every golden file recorded for [tag] (in fuzz mode, the run's), sorted by case id. */
    fun goldens(tag: String): List<JsonNode> =
        Files.walk(if (fuzzing()) Path.of(FUZZ_DIR, tag) else conformanceDir().resolve("golden").resolve(tag)).use { files ->
            files.filter { it.toString().endsWith(".json") }.sorted().map(::readJson).toList()
        }

    /** One golden file by case id, for example `string/equals/case-sensitive`. */
    fun golden(tag: String, caseId: String): JsonNode =
        readJson(conformanceDir().resolve("golden").resolve(tag).resolve("$caseId.json"))

    /** The recorded plan of [caseId] under the current PDP. */
    fun plan(caseId: String): PlanResourcesResponse = plan(golden(currentTag(), caseId))

    /**
     * Every case the current PDP recorded a plan for, as (case id, filter), for a unit test that
     * states a property over everything the adapter emits.
     */
    fun currentPlans(): List<Pair<String, PlanResourcesFilter>> =
        goldens(currentTag()).filter { it["plan"]?.isNull == false }
            .map { it["id"].asText() to plan(it).filter }

    private const val NOW_MINUS_24H = "__NOW_MINUS_24H__"

    /** Nanosecond precision, as the PDP writes the literal it folds `now()` into. */
    private val RFC3339_NANOS: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'").withZone(ZoneOffset.UTC)

    /**
     * A golden's `plan` (the `PlanResources` filter, decoded with protobuf's JSON mapping), with
     * `__NOW_MINUS_24H__` filled in.
     */
    fun plan(golden: JsonNode): PlanResourcesResponse {
        val plan = golden["plan"]
        check(plan != null && !plan.isNull) { "${golden["id"]} records no plan" }
        val nowMinus24h = RFC3339_NANOS.format(Instant.now().minus(Duration.ofHours(24)))
        val filter = PlanResourcesFilter.newBuilder()
        JsonFormat.parser().merge(plan.toString().replace(NOW_MINUS_24H, nowMinus24h), filter)
        return PlanResourcesResponse.newBuilder().setFilter(filter).build()
    }

    // -- exposed/conformance-ledger.json ----------------------------------------------------------

    /**
     * One ledger entry. [pdp], when present, limits it to those PDP tags. Unknown keys fail the
     * read, so a misspelt `pdp` cannot widen an entry to every tag.
     */
    data class LedgerEntry(
        val status: String,
        val reason: String,
        val issue: String? = null,
        val pdp: List<String>? = null,
    ) {
        fun appliesTo(tag: String): Boolean = pdp == null || tag in pdp
    }

    /** A store capability the ledger lacks: on every store, or on [stores] alone. */
    private data class Lack(val reason: String, val stores: List<String>? = null)

    /** [lacks] maps each store capability some store lacks to the reason. */
    private data class Ledger(
        val adapter: String,
        val lacks: Map<String, Lack> = emptyMap(),
        val cases: Map<String, LedgerEntry>,
    )

    private fun readLedger(): Ledger {
        val file = Path.of(System.getProperty("user.dir"), "conformance-ledger.json")
        val ledger = JSON.readValue(file.toFile(), Ledger::class.java)
        check(ledger.adapter == ADAPTER) { "$file is not the $ADAPTER ledger" }
        return ledger
    }

    fun ledger(): Map<String, LedgerEntry> = readLedger().cases

    /**
     * The store capabilities [store] lacks. A seed requiring one is a resource the store cannot
     * hold: it is not loaded, and its id is struck from every golden's `allowed`.
     */
    fun lacks(store: String): Set<String> =
        readLedger().lacks.filterValues { it.stores == null || store in it.stores }.keys

    // -- fuzz mode --------------------------------------------------------------------------------

    /**
     * The directory a fuzz run was recorded into (conformance/README.md, "Fuzzing"), or empty.
     * When it is set, [goldens] reads the generated cases from it instead of the corpus, and the
     * harness judges each case with [fuzzVerdict] instead of the ledger.
     */
    val FUZZ_DIR: String = System.getenv("QPA_FUZZ_DIR").orEmpty()

    fun fuzzing(): Boolean = FUZZ_DIR.isNotEmpty()

    /** One case's line in `<FUZZ_DIR>/results/<adapter>-<store>.json`, for fuzz-report. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class FuzzVerdict(
        val pdp: String,
        val id: String,
        val verdict: String,
        val expected: List<String>? = null,
        val returned: List<String>? = null,
        val detail: String? = null,
    ) {
        /** A match or a refusal, the two answers the invariant allows. */
        @JsonIgnore
        fun isOk(): Boolean = verdict == "match" || verdict == "refused"
    }

    /** Judges one fuzz case: [refusal] says which throws are the adapter refusing. */
    fun fuzzVerdict(pdp: String, id: String, expected: List<String>, outcome: Result<List<String>>, refusal: (Throwable) -> Boolean): FuzzVerdict {
        val returned = outcome.getOrElse { e ->
            return if (refusal(e)) {
                FuzzVerdict(pdp, id, "refused")
            } else {
                FuzzVerdict(pdp, id, "error", detail = "${e.javaClass.simpleName}: ${e.message}")
            }
        }
        if (returned == expected) return FuzzVerdict(pdp, id, "match")
        val verdict = when {
            !expected.containsAll(returned) -> "over-grant"
            !returned.containsAll(expected) -> "under-grant"
            else -> "error"
        }
        return FuzzVerdict(
            pdp, id, verdict, expected, returned,
            if (verdict == "error") "the right rows, but some of them repeated" else null,
        )
    }

    /** Writes one store's verdicts; outside fuzz mode it does nothing. */
    fun writeFuzzResults(store: String, results: List<FuzzVerdict>) {
        if (!fuzzing()) return
        val dir = Path.of(FUZZ_DIR, "results")
        Files.createDirectories(dir)
        val sorted = results.sortedWith(compareBy({ it.id }, { it.pdp }))
        Files.writeString(
            dir.resolve("$ADAPTER-$store.json"),
            ObjectMapper().registerKotlinModule().writerWithDefaultPrettyPrinter()
                .writeValueAsString(mapOf("adapter" to ADAPTER, "store" to store, "results" to sorted)) + "\n",
        )
    }
}
