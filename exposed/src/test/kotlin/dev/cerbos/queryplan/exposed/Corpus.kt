package dev.cerbos.queryplan.exposed

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

    /** Every golden file recorded for [tag], sorted by case id. */
    fun goldens(tag: String): List<JsonNode> =
        Files.walk(conformanceDir().resolve("golden").resolve(tag)).use { files ->
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

    private data class Ledger(val adapter: String, val cases: Map<String, LedgerEntry>)

    fun ledger(): Map<String, LedgerEntry> {
        val file = Path.of(System.getProperty("user.dir"), "conformance-ledger.json")
        val ledger = JSON.readValue(file.toFile(), Ledger::class.java)
        check(ledger.adapter == ADAPTER) { "$file is not the $ADAPTER ledger" }
        return ledger.cases
    }
}
