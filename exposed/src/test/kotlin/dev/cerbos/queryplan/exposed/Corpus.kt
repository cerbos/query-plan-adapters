package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.protobuf.util.JsonFormat
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse
import java.nio.file.Files
import java.nio.file.Path
import java.util.TreeMap
import java.util.TreeSet
import kotlin.io.path.name

/**
 * This adapter's reader for the shared `../conformance/` corpus.
 *
 * **Deliberately duplicated.** Every adapter carries its own loader — `prisma/src/corpus.ts`,
 * `sqlalchemy/tests/corpus.py`, `pgx/corpus_test.go`, this one — so that each stays standalone
 * and none of them can break another by changing. Do not extract a shared one, and do not add a
 * drift check between the copies: they are allowed to differ
 * (`docs/adr/0007-adapters-share-data-not-code.md`).
 *
 * What lives here is what every corpus suite in this adapter must agree on: the classification in
 * `actions.json`, and the wire-fixture decoding. The mapping the corpus is read through lives in
 * [TestSchema] beside the tables it names, because the tables and the mapping are one statement.
 *
 * The seeds, the derived fields, the `check()` oracle and the coverage guards over all three stay
 * in the harness, which is the only thing that consumes them.
 */
internal object Corpus {

    /** The corpus key for this adapter — its directory name, as every other harness uses. */
    const val ADAPTER: String = "exposed"

    /** Kotlin data classes decode through the Kotlin module; unknown keys still fail by default. */
    private val JSON: ObjectMapper = ObjectMapper().registerKotlinModule()

    fun conformanceDir(): Path =
        Path.of(System.getProperty("user.dir"), "..", "conformance").normalize()

    // -- conformance/actions.json ---------------------------------------------------------------

    /**
     * An `expectedUnsupported` entry. [messages] carries one entry per adapter that must reject
     * the shape, keyed by adapter name; `validate-corpus.sh` asserts that key set is exactly the
     * roster minus the adapters that promoted the shape.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class UnsupportedShape(
        val action: String,
        val shape: String? = null,
        val messages: Map<String, String>? = null,
    )

    /**
     * A `nullRepresentationOmitted` entry. Every adapter must reject these — the two NULL
     * conventions are indistinguishable on the wire — so [messages] names the whole roster with no
     * promotions to subtract.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class NullRepresentationOmitted(
        val action: String,
        val reason: String,
        val messages: Map<String, String>? = null,
    )

    /**
     * An `adapterUnsupported` / `adapterSupportedExpected` entry. [message] is the substring this
     * adapter's error must contain — present on the first, absent on the second, which does not
     * throw.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class AdapterUnsupported(
        val action: String,
        val reason: String,
        val message: String? = null,
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class KnownDivergence(val action: String, val reason: String, val adapters: List<String>)

    /**
     * Every group in actions.json must be named here: Jackson silently drops a field this class
     * does not declare, and a dropped group makes its actions vanish from every count and every
     * parameterised case at once — the projection trap `conformance/README.md` warns about. The
     * manifest tripwire in the harness is what makes an undropped group load-bearing.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class ActionsFile(
        val conformance: List<String>,
        val adapterUnsupported: Map<String, List<AdapterUnsupported>>? = null,
        val adapterSupportedExpected: Map<String, List<AdapterUnsupported>>? = null,
        val expectedUnsupported: List<UnsupportedShape>,
        val nullRepresentationOmitted: List<NullRepresentationOmitted>,
        val knownDivergences: List<KnownDivergence>,
    ) {
        fun adapterUnsupportedFor(adapter: String): List<AdapterUnsupported> =
            adapterUnsupported?.get(adapter).orEmpty()

        fun adapterSupportedExpectedFor(adapter: String): List<AdapterUnsupported> =
            adapterSupportedExpected?.get(adapter).orEmpty()

        /** Every action the corpus declares, in any group. */
        fun manifestActions(): Set<String> {
            val manifest = TreeSet(conformance)
            expectedUnsupported.forEach { manifest.add(it.action) }
            nullRepresentationOmitted.forEach { manifest.add(it.action) }
            knownDivergences.forEach { manifest.add(it.action) }
            return manifest
        }

        fun skippedDivergences(adapter: String): Set<String> =
            knownDivergences.filter { adapter in it.adapters }.map { it.action }.toCollection(TreeSet())
    }

    fun actionsFile(): ActionsFile =
        JSON.readValue(conformanceDir().resolve("actions.json").toFile(), ActionsFile::class.java)

    /**
     * The substring this adapter's error must contain, or a loud failure. The message is what turns
     * "it threw" into "it threw for the declared reason": without it a mapping typo or an unrelated
     * validation satisfies the throw suite just as well as the documented limitation
     * (cerbos/query-plan-adapters#326).
     */
    fun requireMessage(label: String, message: String?): String {
        check(!message.isNullOrEmpty()) {
            "actions.json pins no throw message for $label: the throw suite would accept a failure" +
                " for any reason"
        }
        return message
    }

    /** Actions this adapter oracle-compares: conformance minus its own unsupported, plus promotions. */
    fun oracleActions(actions: ActionsFile, adapter: String): List<String> {
        val unsupported = actions.adapterUnsupportedFor(adapter).map { it.action }.toSet()
        return actions.conformance.filter { it !in unsupported } +
            actions.adapterSupportedExpectedFor(adapter).map { it.action }.sorted()
    }

    /**
     * Every action this adapter must refuse, each with the message it must refuse it with:
     * `adapterUnsupported[me]` plus `expectedUnsupported` minus its own promotions.
     *
     * `nullRepresentationOmitted` is deliberately absent — under the DEFAULT representation those
     * actions translate, so their refusal is a property of the flipped option (see
     * [nullRepresentationThrows]).
     */
    fun throwingActions(actions: ActionsFile, adapter: String): Map<String, String> {
        val promoted = actions.adapterSupportedExpectedFor(adapter).map { it.action }.toSet()
        val throwing = TreeMap<String, String>()
        for (entry in actions.adapterUnsupportedFor(adapter)) {
            throwing[entry.action] =
                requireMessage("adapterUnsupported.$adapter.${entry.action}", entry.message)
        }
        for (entry in actions.expectedUnsupported) {
            if (entry.action in promoted) continue
            throwing[entry.action] = requireMessage(
                "expectedUnsupported.${entry.action}.messages.$adapter",
                entry.messages?.get(adapter),
            )
        }
        return throwing
    }

    /** The `nullRepresentationOmitted` probes, each with the message its rejection must carry. */
    fun nullRepresentationThrows(actions: ActionsFile): List<NullRepresentationOmitted> =
        actions.nullRepresentationOmitted

    fun nullOmittedMessage(entry: NullRepresentationOmitted, adapter: String): String =
        requireMessage(
            "nullRepresentationOmitted.${entry.action}.messages.$adapter",
            entry.messages?.get(adapter),
        )

    // -- conformance/wire-fixtures/ -------------------------------------------------------------

    /**
     * The instant `regenerate-wire-fixtures.sh` substitutes for the one operand it cannot pin.
     *
     * `ts-window` and `ts-vf` compare against `now() - duration("24h")`, which the planner folds to
     * a literal timestamp: a different value on every capture, so the script rewrites it to
     * `__NOW_MINUS_24H__` to keep the drift check deterministic. Reading a fixture back therefore
     * means CHOOSING a value, and the choice is load-bearing — it lands in the translator unit
     * test's golden expectation as the instant those two actions compare against.
     *
     * Nanosecond precision, deliberately, and the same instant `sqlalchemy/tests/corpus.py` and
     * `spring-data`'s loader chose: the PDP emits nanoseconds, and this adapter maps `createdAt` to
     * a [java.time.Instant] column that carries them. A tidy millisecond substitution would pin a
     * rendering the PDP never produces.
     */
    const val PLANNED_AT: String = "2026-08-11T09:13:39.123456789Z"

    private const val NOW_MINUS_24H = "__NOW_MINUS_24H__"

    /** Every action the corpus has a golden wire fixture for, sorted. */
    fun wireFixtureActions(): List<String> =
        Files.list(conformanceDir().resolve("wire-fixtures")).use { files ->
            files.map { it.name }
                .filter { it.endsWith(".json") }
                .map { it.removeSuffix(".json") }
                .sorted()
                .toList()
        }

    /**
     * The plan the pinned PDP produced for [action], decoded into the protobuf response the SDK
     * hands a caller.
     *
     * The fixture IS the PDP's HTTP response body, so the decoding here is protobuf's own canonical
     * JSON mapping ([JsonFormat]) — the same mapping the PDP's HTTP API writes. It is deliberately
     * not a hand-built plan: a plan somebody typed is a BELIEF about what the planner emits, and
     * this repository keeps fixtures precisely because that belief has been wrong before. See
     * `docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md`.
     */
    fun planFromWireFixture(action: String, plannedAt: String = PLANNED_AT): PlanResourcesResponse {
        val fixture = conformanceDir().resolve("wire-fixtures").resolve("$action.json")
        val filter: JsonNode = checkNotNull(JSON.readTree(fixture.toFile()).get("filter")) {
            "$fixture carries no filter"
        }
        val builder = PlanResourcesFilter.newBuilder()
        JsonFormat.parser().merge(filter.toString().replace(NOW_MINUS_24H, plannedAt), builder)
        return PlanResourcesResponse.newBuilder().setFilter(builder).build()
    }
}
