package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.protobuf.Value
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import org.jetbrains.exposed.v1.core.LikeEscapeOp
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.stringParam
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Files
import java.nio.file.Path
import java.util.TreeMap
import java.util.TreeSet
import java.util.stream.Stream
import kotlin.reflect.KClass

/**
 * Translator unit test: for every action in the shared `../conformance/` corpus, the SQL this
 * adapter emits. Offline — no Cerbos sidecar, no container and no database server: the plans come
 * from `conformance/wire-fixtures/` and [OfflineRenderer] renders the emitted `Op<Boolean>` for
 * every dialect this adapter claims, through two in-process drivers and two connection stubs.
 *
 * Who owns which assertion:
 *
 * | assertion | owner |
 * |---|---|
 * | the plan the PDP produces for a policy | `conformance/wire-fixtures/`, replanned and diffed by the `Conformance Corpus` workflow |
 * | which shapes this adapter must refuse, and with what message | `conformance/actions.json` — read below, never restated |
 * | the rows a filter returns | [AdversarialConformanceTest], against real stores with `check()` as the oracle |
 * | **the SQL this adapter emits for a plan** | **here** |
 *
 * **The plans are read, not written.** A hand-built plan is a BELIEF about what the planner emits,
 * and this repository keeps golden fixtures because that belief has been wrong before
 * (`docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md`). The only
 * hand-built plans in this file are in [PlansThePlannerCannotProduce], which is input validation on
 * a public function rather than a policy shape.
 *
 * **The expectations are data, not literals.** They live in `exposed/golden/expectations.json`,
 * rewritten by `gradle goldenUpdate` and reviewed as a diff (`conformance/README.md`, "Golden
 * expectations"). CI never regenerates, so a translator change that moves the emitted SQL fails
 * there whatever anyone ran locally.
 *
 * **What a pinned statement buys over the harness.** The harness proves the query returns the right
 * rows AGAINST THE ROWS IT SEEDS. Two different queries can agree on all 22 of them and disagree on
 * the row a consumer has, so a rewrite that quietly changes the emitted SQL passes there and shows
 * up here as a diff a reviewer reads. What regeneration cannot protect is a property nobody wrote
 * down, which is why [WhatTheEmittedSqlContains] states rules over everything emitted rather than
 * leaving them to the pinned bytes.
 */
class ExposedTranslatorTest {

    companion object {

        // ==========================================================================================
        //  PINNED FROM A RUN — the whole block, and nothing outside it.
        //
        //  Everything here is a MEASUREMENT. Re-derive it by running `gradle goldenUpdate` and then
        //  `gradle test --tests '*ExposedTranslatorTest'` and reading the failures; never reason a
        //  value out from the source, because a constant derived from the code it guards guards
        //  nothing. Keeping them together is what makes a deliberate shift — a shape moving from the
        //  throwing bucket into the asset, say — a one-pass edit rather than a hunt.
        // ==========================================================================================

        /**
         * How many recorded actions carry SQL, and how many the planner folded to a constant.
         *
         * A count that moves without anyone noticing is how a shape gets dropped from an asset
         * nobody reads end to end.
         */
        private const val CONDITIONAL_ACTIONS = 188
        private const val UNCONDITIONAL_ACTIONS = 2

        /**
         * How many corpus actions this adapter must refuse, from `actions.json`: the 6
         * `adapterUnsupported.exposed` entries plus the 11 `expectedUnsupported` shapes.
         */
        private const val THROWING_ACTIONS = 15

        /**
         * Where in the walk each rejection happens, and how many corpus shapes reach each site.
         *
         * One entry per mechanism, not per action — [WhereTheRefusalsHappen] declares the site that
         * each name stands for and the exception type it raises.
         */
        private val REFUSAL_SITE_COUNTS: Map<String, Int> = sortedMapOf(
            "ambiguous temporal column" to 1,
            "CEL numeric cast" to 3,
            "empty hierarchy delimiter" to 1,
            "list-valued macro in boolean position" to 3,
            "map projection compared directly" to 1,
            "modulo" to 1,
            "mixed null conventions across two columns" to 1,
            "operator the adapter never translates" to 2,
            "positional list access" to 1,
            "struct member access" to 1,
        )

        /**
         * Corpus actions Exposed 1.0.0 — the `floor` ORM set, and the release the published jar is
         * compiled against — renders differently from the 1.5 the asset was generated under, from
         * the SAME expression tree.
         *
         * Empty, and asserted to STAY empty: the two releases spell every shape this adapter emits
         * identically. The list is asserted in BOTH directions on the floor leg, so an action that
         * starts diverging lands here rather than silently widening an exemption, and one that stops
         * diverging fails just as loudly.
         */
        private val RENDERING_DIFFERS_ON_THE_FLOOR: List<String> = emptyList()

        /**
         * Every bound argument whose value JSON cannot hold, as `action/dialect/index`.
         *
         * `-0.0`, `NaN` and the infinities are outside JSON's number grammar, so the asset records
         * each one's own text instead ([RenderedParam.normalisedFrom]). The distinction is real —
         * CEL's `x / -0.0` is `-Infinity` where `x / 0.0` is `+Infinity` — so pinning WHERE the
         * asset is a stand-in is what stops the normalisation from hiding a change.
         */
        private val NON_JSON_DOUBLE_PARAMS: List<String> = emptyList()

        /**
         * Corpus actions whose SQL carries a `LIKE`, on any dialect.
         *
         * The anti-vacuity half of the rule that every `LIKE` declares an `ESCAPE`: pinned in both
         * directions, so a translation that stopped emitting one fails here rather than leaving the
         * rule passing over nothing.
         */
        private val ACTIONS_EMITTING_LIKE: List<String> = listOf(
            "cr-contains", "cr-endswith", "cr-startswith", "cr-startswith-concat",
            "cs-contains", "cs-endswith", "cs-startswith",
            "f2f-contains", "f2f-endswith", "f2f-startswith",
            "hier-ancestor-cf", "hier-bracket", "hier-descendent-ff", "hier-meta-like",
            "hier-overlaps-cf", "hier-overlaps-ff", "hier-overlaps-meta",
            "like-backslash", "like-bracket", "like-percent", "like-underscore",
            "not-contains", "not-startswith",
            "p-deep-nest", "p-lambda-f2f-like", "p-startswith-concat",
            "rel-contains-hop", "rel-hop-and-root", "rel-startswith-hop2",
            "ternary-expr-cond",
        )

        /**
         * Corpus actions whose SQL carries a correlated subquery alias.
         *
         * The anti-vacuity half of the alias rule, pinned in both directions for the same reason.
         */
        private val ACTIONS_EMITTING_A_SUBQUERY_ALIAS: List<String> = listOf(
            "all-on-empty", "cr-size-frac-ge", "exists-on-empty", "exists-one-multi",
            "in-null-elem-hasint", "in-null-elem-rel", "in-null-elem-rel-neg", "in-var-var",
            "in-var-var-neg", "lambda-field-to-field", "lambda-in-principal", "macro-depth3-all",
            "macro-depth3-exists", "macro-depth3-not-exists", "n-all-mixed-null", "n-not-all-absorb",
            "n-not-all-null", "n-not-exists-one-null", "not-empty", "not-exists", "or-eq-exists",
            "or-eq-in", "outer-attr-depth2", "p-arith-in-lambda", "p-deep-nest",
            "p-hasintersection-map", "p-lambda-f2f-like", "p-lambda-inner-f2f", "p-not-exists-empty",
            "p-size-nested", "p-ternary-in-exists", "p-ternary-under-all", "rel-bool-hop",
            "rel-bool-hop2", "rel-contains-hop", "rel-eq-hop", "rel-eq-num-hop", "rel-ge-hop",
            "rel-gt-hop", "rel-hop-and-root", "rel-hop2-or-exists", "rel-le-hop", "rel-lt-hop",
            "rel-ne-null-hop", "rel-not-bool-hop", "rel-range-hop", "rel-startswith-hop2",
            "size-filter-count", "size-threshold", "vf-hasint", "vf-size", "w1-all-chain",
            "w1-exists-chain", "w1-in-chain", "w1-not-exists-chain", "w1-not-hasint-chain",
            "w1-not-in-chain", "w1-not-size-chain", "w1-size-chain", "w1-size-frac-chain",
            "w1-size-frac-le-chain", "w1-size-nonneg-chain", "w1-size-zero-chain",
            "w1-ternary-chain-cond", "w2-outer-relation",
        )

        /**
         * The named spellings [DIALECT_SPELLINGS] the corpus actually exercises.
         *
         * A spelling that fires nowhere is a rewrite that could be hiding a real divergence instead
         * of a declared one, so the set is pinned rather than left to grow silently.
         */
        private val DIALECT_SPELLINGS_EXERCISED: Set<String> = sortedSetOf(
            "boolean literal spelling",
            "character count function",
            "concatenation spelling",
            "double cast target",
            "identifier case folding",
            "identifier quoting",
            "text cast target",
        )

        // ==========================================================================================
        //  end of the pinned block
        // ==========================================================================================

        /** The corpus key for this adapter — its directory name, as every other suite uses. */
        private const val ADAPTER = Corpus.ADAPTER

        private val ACTIONS: Corpus.ActionsFile = Corpus.actionsFile()

        /**
         * The shapes `actions.json` says this adapter must refuse, each with the message it must
         * refuse them with. Identical to the classification the harness asserts against a live PDP;
         * asserting it here as well is what lets the completeness guard below be total, and it
         * costs a millisecond rather than a container.
         *
         * A throwing action needs no golden expectation of its own: the message is already corpus
         * data, pinned once in `actions.json` and read by every adapter. Writing it into this
         * adapter's asset too would create two places to change one string with nothing to say
         * which is authoritative.
         */
        private val THROWING: Map<String, String> = Corpus.throwingActions(ACTIONS, ADAPTER)

        private val GOLDEN = Golden()

        /** The one mapping the corpus is classified against, with the adapter's defaults. */
        private val OPTIONS: Options = Options.of(MAPPING)

        /**
         * Every emitted predicate, rendered once per action per dialect and read by every rule
         * below.
         *
         * One pass, deliberately: the rules are about what the translator emits RIGHT NOW rather
         * than about the pinned bytes, and a second pass would let those two answers drift apart
         * within a single run. A folded plan carries no rendering, so its entry here is empty.
         */
        private lateinit var rendered: Map<String, Map<String, Rendered>>

        /** The plan kind each non-throwing action folds to, in [Golden]'s spelling. */
        private lateinit var kinds: Map<String, String>

        /**
         * Every corpus action this adapter refuses although `actions.json` classifies it as an
         * oracle comparison, with what it raised.
         *
         * This is the gate the whole suite stands on, and it must stay empty. A refusal is
         * indistinguishable from a declared limitation to any assertion that only asks whether
         * something threw, so an oracle-compared action that refuses has to be named rather than
         * counted.
         */
        private lateinit var refusedAnyway: Map<String, Throwable>

        private lateinit var recorded: Map<String, ObjectNode>
        private lateinit var recordedActions: List<String>

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val renderings = LinkedHashMap<String, Map<String, Rendered>>()
            val planKinds = LinkedHashMap<String, String>()
            val failures = LinkedHashMap<String, Throwable>()

            for (action in Corpus.wireFixtureActions()) {
                // A throwing action is never rendered: its message is corpus data, and asking the
                // translator for SQL it must refuse would fail here rather than in the throw suite
                // that owns the question.
                if (action in THROWING) continue
                try {
                    when (val filter = filterFor(action)) {
                        QueryPlanFilter.AlwaysAllowed -> {
                            planKinds[action] = Golden.KIND_ALWAYS_ALLOWED
                            renderings[action] = emptyMap()
                        }

                        QueryPlanFilter.AlwaysDenied -> {
                            planKinds[action] = Golden.KIND_ALWAYS_DENIED
                            renderings[action] = emptyMap()
                        }

                        is QueryPlanFilter.Conditional -> {
                            planKinds[action] = Golden.KIND_CONDITIONAL
                            renderings[action] = OfflineRenderer.render(filter.op)
                        }
                    }
                } catch (error: RuntimeException) {
                    failures[action] = error
                }
            }

            rendered = renderings
            kinds = planKinds
            refusedAnyway = failures

            // `gradle goldenUpdate` rewrites the file from what the translator emits today and
            // preserves every note. Skipping the throwing actions above is also what keeps
            // regeneration from papering over a misclassification — an action moved into
            // `adapterUnsupported` that this adapter still translates fails the throw suite, and
            // one moved out of it that this adapter still refuses fails regeneration itself. An
            // action that raised is likewise skipped rather than recorded as something it is not.
            if (System.getProperty("golden.update").toBoolean()) {
                val expectations = TreeMap<String, ObjectNode>()
                renderings.forEach { (action, one) ->
                    expectations[action] = when (val kind = planKinds.getValue(action)) {
                        Golden.KIND_CONDITIONAL -> Golden.entry(one)
                        else -> Golden.entry(kind)
                    }
                }
                GOLDEN.write(expectations)
                println("==> rewrote ${GOLDEN.file} (${expectations.size} expectations)")
            }

            recorded = GOLDEN.read()
            recordedActions = recorded.keys.toList()
        }

        // -- translating one corpus action ------------------------------------------------------

        /**
         * The filter [action]'s wire fixture translates to, outside any transaction.
         *
         * [OfflineRenderer.translate] is the assertion, not the plumbing: translation must not read
         * the dialect, because the predicate renders later inside the caller's transaction.
         */
        private fun filterFor(
            action: String,
            options: Options = OPTIONS,
            plannedAt: String = Corpus.PLANNED_AT,
        ): QueryPlanFilter = OfflineRenderer.translate {
            ExposedQueryPlanAdapter.toFilter(Corpus.planFromWireFixture(action, plannedAt), options)
        }

        /**
         * How many aliases [action]'s translation allocated.
         *
         * The one place in this file that reaches past `ExposedQueryPlanAdapter`, and deliberately:
         * the count is an internal property of ONE translation, and putting it on
         * [QueryPlanFilter] so a test could read it would be a test's needs in the published
         * surface. The steps below are `ExposedQueryPlanAdapter.conditional`'s, in its order, and
         * the caller asserts that the predicate this produces renders exactly as the entry point's
         * does — so a translation step added there and missed here fails rather than drifting.
         */
        private fun aliasesAllocatedBy(action: String): Pair<Op<Boolean>, Int> = OfflineRenderer.translate {
            val condition = Corpus.planFromWireFixture(action).filter.condition
            NullOperandScan.assertTranslatable(condition, OPTIONS)
            val translation = Translation(OPTIONS)
            translation.walker.traverse(condition, translation.rootScope()) to translation.aliases.calls
        }

        /** The golden entry for [action]: the whole handover, plan to recorded document. */
        private fun entryFor(action: String): ObjectNode = Golden.entryFor {
            ExposedQueryPlanAdapter.toFilter(Corpus.planFromWireFixture(action), OPTIONS)
        }

        /** [action]'s predicate rendered under [dialect], under a mapping other than the corpus's. */
        private fun sqlFor(
            action: String,
            dialect: String = OfflineRenderer.POSTGRESQL,
            options: Options = OPTIONS,
            plannedAt: String = Corpus.PLANNED_AT,
        ): String = OfflineRenderer.renderOn(dialect, filterFor(action, options, plannedAt).toOp()).sql

        /**
         * [action]'s rendering under [dialect] from the single pass, or a failure that says why
         * there is none — a shape whose translation has not been written yet is the gap, and a
         * rule that quietly skipped it would report the incomplete translator as compliant.
         */
        private fun renderingOf(action: String, dialect: String): Rendered {
            refusedAnyway[action]?.let { throw AssertionError("'$action' did not translate: $it", it) }
            val one = rendered[action] ?: throw AssertionError("'$action' carries no rendering")
            return one[dialect] ?: throw AssertionError("'$action' folded to ${kinds[action]}, so it renders no SQL")
        }

        /** Every conditional action's SQL, dialect by dialect, for the rules that sweep the corpus. */
        private fun conditionalRenderings(): List<Triple<String, String, Rendered>> =
            conditionalActions().flatMap { action ->
                OfflineRenderer.DIALECTS.map { dialect -> Triple(action, dialect, renderingOf(action, dialect)) }
            }

        private fun conditionalActions(): List<String> =
            recordedActions.filter { recorded.getValue(it).path(Golden.KIND_KEY).asText() == Golden.KIND_CONDITIONAL }

        private fun unconditionalActions(): List<String> =
            recordedActions.filter { recorded.getValue(it).path(Golden.KIND_KEY).asText() != Golden.KIND_CONDITIONAL }

        // -- @MethodSource feeds ----------------------------------------------------------------

        @JvmStatic
        fun recordedActions(): Stream<String> = recordedActions.stream()

        @JvmStatic
        fun throwingActions(): Stream<Arguments> =
            THROWING.entries.stream().map { Arguments.of(it.key, it.value) }
    }

    // -- the corpus, action by action -----------------------------------------------------------

    /**
     * The whole handover: a wire fixture in, [Golden.entryFor] out, compared against the bytes the
     * asset pins. Per dialect rather than per entry, so a failure names the dialect whose SQL moved
     * rather than printing two documents and leaving the reader to diff them.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("recordedActions")
    fun `emits the golden expectation`(action: String) {
        refusedAnyway[action]?.let {
            throw AssertionError(
                "${GOLDEN.file} records '$action', which this translator now refuses: $it." +
                    " A refusal on an oracle-compared action is never an expected throw — fix the" +
                    " translation, or reclassify the action in actions.json deliberately.",
                it,
            )
        }

        val entry = entryFor(action)

        if (!GOLDEN.rendersUnderRunningExposed && action in RENDERING_DIFFERS_ON_THE_FLOOR) {
            // On the floor leg a listed shape is asserted to DIFFER from the asset; a byte match
            // here would mean the list is stale in the other direction.
            assertNotEquals(recorded.getValue(action), entry) {
                "'$action' is listed as diverging on Exposed ${Golden.runningExposedVersion()} but" +
                    " renders byte-identically; shrink the list deliberately"
            }
            return
        }

        assertSameEntry(action, recorded.getValue(action), entry)
    }

    private fun assertSameEntry(action: String, expected: ObjectNode, actual: ObjectNode) {
        assertEquals(expected.path(Golden.KIND_KEY), actual.path(Golden.KIND_KEY)) {
            "'$action' folds to a different plan kind than ${GOLDEN.file} pins"
        }
        OfflineRenderer.DIALECTS.forEach { dialect ->
            val pinned = expected.path(Golden.RENDERED_KEY).path(dialect)
            val emitted = actual.path(Golden.RENDERED_KEY).path(dialect)
            assertEquals(pinned.path(Golden.SQL_KEY), emitted.path(Golden.SQL_KEY)) {
                "the SQL emitted for '$action' on $dialect is not the SQL ${GOLDEN.file} pins;" +
                    " run `${Golden.REGENERATE_COMMAND}` and review the diff"
            }
            assertEquals(pinned.path(Golden.PARAMS_KEY), emitted.path(Golden.PARAMS_KEY)) {
                "the arguments bound for '$action' on $dialect are not the ones ${GOLDEN.file}" +
                    " pins; run `${Golden.REGENERATE_COMMAND}` and review the diff"
            }
        }
        // …and nothing the per-dialect comparison does not reach moved either.
        assertEquals(expected, actual) { "'$action' differs from ${GOLDEN.file} outside its rendered SQL" }
    }

    /**
     * The gate the whole suite stands on: a shape the corpus says this adapter translates must
     * produce a filter.
     *
     * A refusal fails closed, which is right, and that is exactly what makes it invisible: to any
     * assertion that only asks whether something threw, a shape nobody has finished and a
     * documented limitation look the same. Naming the actions is what keeps an adapter that
     * quietly stopped translating something from reporting itself as an adapter that refuses a lot.
     */
    @Test
    fun `no oracle-compared action is refused`() {
        assertEquals(emptyList<String>(), refusedAnyway.keys.toList()) {
            "these corpus actions raised, and `actions.json` classifies every one of them as an" +
                " oracle comparison, so each refusal is this adapter coming up short rather than a" +
                " declared limitation: ${refusedAnyway.entries.joinToString("\n  ", "\n  ")}"
        }
    }

    // -- refusals ---------------------------------------------------------------------------------

    /**
     * The message, not just the throw: a mapping typo or an unrelated validation satisfies a bare
     * `assertThrows` just as well as the limitation the corpus documents
     * (cerbos/query-plan-adapters#326). The harness makes the same assertion against a live PDP;
     * here it costs a millisecond, which is what lets the completeness guard below be total.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("throwingActions")
    fun `is refused with the message actions_json pins`(action: String, message: String) {
        val error = assertThrows(IllegalArgumentException::class.java) { filterFor(action) }
        assertTrue(error.message.orEmpty().contains(message)) {
            "action '$action' was rejected for a reason actions.json does not declare: ${error.message}"
        }
    }

    /**
     * Adding a throwing action without pinning its message must fail this suite rather than
     * silently degrade the throw assertions to a bare "it threw" (#326).
     */
    @Test
    fun `a throwing action with no pinned message fails classification`() {
        listOf(null, "").forEach { absent ->
            val error = assertThrows(IllegalStateException::class.java) {
                Corpus.requireMessage("synthetic-entry", absent)
            }
            assertTrue(error.message!!.contains("pins no throw message"), error.message)
        }
    }

    @Test
    fun `every corpus action is accounted for here exactly once`() {
        val classified = (recordedActions + THROWING.keys).sorted()

        // Total: a corpus action with no golden expectation and no pinned throw lands as a failure
        // rather than as silence. This is the assertion that makes the asset self-maintaining —
        // adding a hostile shape to the corpus forces someone to look at the SQL this adapter emits
        // for it, and `gradle goldenUpdate` refuses to invent one for a shape that throws.
        assertEquals(Corpus.wireFixtureActions(), classified) {
            "every wire fixture must be accounted for exactly once${unaccountedFor()}"
        }
        // Disjoint: an action carrying a golden expectation AND declared unsupported would satisfy
        // the union above while asserting two contradictory things.
        assertEquals(classified.size, classified.toSet().size, "an action is either recorded or thrown, never both")
        // The asset is written sorted, so a translator change reads as the list of shapes it moved.
        assertEquals(recordedActions.sorted(), recordedActions, "golden/expectations.json must stay sorted by action")

        // Tripwires, from the pinned block at the top of this file.
        assertEquals(
            mapOf(
                "conditional" to CONDITIONAL_ACTIONS,
                "unconditional" to UNCONDITIONAL_ACTIONS,
                "throwing" to THROWING_ACTIONS,
            ),
            mapOf(
                "conditional" to conditionalActions().size,
                "unconditional" to unconditionalActions().size,
                "throwing" to THROWING.size,
            ),
        )
    }

    /** What the completeness guard's failure is really saying, when it has more to say. */
    private fun unaccountedFor(): String {
        val missing = Corpus.wireFixtureActions().toSet() - recordedActions.toSet() - THROWING.keys
        if (missing.isEmpty()) return ""
        return "; ${missing.size} of them have neither a golden expectation nor a pinned throw," +
            " ${refusedAnyway.keys.count { it in missing }} because this adapter refused them —" +
            " see `no oracle-compared action is refused`"
    }

    @Test
    fun `the unconditional actions are the planner folds the corpus declares`() {
        // `p-has` is the corpus's one knownDivergences entry: the planner folds has() on a missing
        // attribute to ALWAYS_ALLOWED while check() denies those rows. The adapter must translate
        // that faithfully — no filter at all — and this is the assertion that says an unfiltered
        // plan belongs to that shape rather than to a translation that quietly stopped emitting one.
        //
        // `in-empty` is the other: `x in []` is constant-false in CEL, so the planner folds the
        // whole plan to ALWAYS_DENIED and the caller can skip the query. Both are properties of the
        // PLANNER rather than of this translator, so the pair is permanent rather than provisional.
        assertEquals(listOf("in-empty", "p-has"), unconditionalActions())
        assertEquals(Golden.KIND_ALWAYS_ALLOWED, recorded.getValue("p-has").path(Golden.KIND_KEY).asText())
        assertEquals(Golden.KIND_ALWAYS_DENIED, recorded.getValue("in-empty").path(Golden.KIND_KEY).asText())
        assertTrue(ACTIONS.skippedDivergences(ADAPTER).contains("p-has"))
    }

    /**
     * Where in the walk each rejection happens, and how many corpus shapes reach each site.
     *
     * `actions.json` pins a substring of the message per action, so the throw test above proves
     * every refusal is the declared one. It cannot say anything about the SHAPE of the refusals
     * taken together: whether the shapes this adapter refuses land on several distinct mechanisms
     * or on one catch-all, and whether a translator change moved a shape from one to another. That
     * is a property no corpus action can state, because a corpus action asks which rows come back.
     */
    @Nested
    inner class WhereTheRefusalsHappen {

        /**
         * One entry per `throw` site the corpus reaches, named for the MECHANISM rather than for
         * the message or the action, and carrying the exception type that site raises.
         *
         * Each substring is narrowed to the part that identifies the SITE: `actions.json` pins a
         * substring per action, and several actions share one mechanism (three casts, two
         * `matches()`, two nested divisions), so the pins would count actions rather than sites.
         */
        private val sites: List<RefusalSite> = listOf(
            // PlanWalker, by name and before any predicate is built: filter() and map() return a
            // LIST, and `filter(...)` in condition position is not `size(filter(...)) > 0`.
            RefusalSite(
                "list-valued macro in boolean position",
                "returns a list, not a boolean",
                UnsupportedPlanShapeException::class,
            ),
            // The leaf's operator dispatch default. The corpus reaches it through matches() alone:
            // CEL matches with RE2, which no SQL engine implements.
            RefusalSite(
                "operator the adapter never translates",
                "Unsupported operator: ",
                UnsupportedPlanShapeException::class,
            ),
            // ScalarRefusals.numericCastUnsupported, from the leaf operand and from inside an
            // arithmetic operand alike: SQL CAST is not a CEL conversion in either direction.
            RefusalSite(
                "CEL numeric cast",
                "SQL CAST reads the numeric prefix of a string where CEL requires the whole string",
                UnsupportedPlanShapeException::class,
            ),
            // HierarchyTranslator: an empty delimiter splits the path per character, and the prefix
            // LIKE this adapter emits would then also match the path itself.
            RefusalSite(
                "empty hierarchy delimiter",
                "hierarchy delimiter must be a non-empty string",
                UnsupportedPlanShapeException::class,
            ),
            // CEL % has no double overload and attribute values are always doubles at check time,
            // so the satisfiable spelling needs an int() cast that is itself unlowerable.
            RefusalSite("modulo", "mod is not supported in comparisons", UnsupportedPlanShapeException::class),
            // A map() projection compared straight to a value: it is held as a deferred projection
            // until hasIntersection() or size() gives it a scalar meaning.
            RefusalSite(
                "map projection compared directly",
                "Direct comparison of map(...) to a value is not supported",
                UnsupportedPlanShapeException::class,
            ),
            // index(): a list element addressed by position, and the rows a relation subquery
            // returns carry no CEL list order to index into.
            RefusalSite("positional list access", "Cannot translate index()", UnsupportedPlanShapeException::class),
            // get-field(): it projects a member out of a single element, and a relation subquery
            // has no single element to project from.
            RefusalSite("struct member access", "Cannot translate get-field()", UnsupportedPlanShapeException::class),
            // Two columns under different null conventions: the declared side needs a definite
            // answer for its NULL and the undeclared side needs UNKNOWN, and no predicate is both
            // (#308). A mapping decision, so an UnmappedAttributeException.
            RefusalSite(
                "mixed null conventions across two columns",
                "between two columns under mixed null conventions",
                UnmappedAttributeException::class,
            ),
            // timestamp() over a column whose type does not pin an absolute instant. Also a mapping
            // decision: the fix is to remap the column, not to rewrite the policy.
            RefusalSite(
                "ambiguous temporal column",
                "requires a column that stores an absolute instant",
                UnmappedAttributeException::class,
            ),
        )

        private fun siteOf(action: String): String {
            val error = try {
                filterFor(action)
                return "<did not throw>"
            } catch (raised: RuntimeException) {
                raised
            }
            val message = error.message.orEmpty()
            val matched = sites.filter { message.contains(it.substring) }
            assertEquals(1, matched.size) {
                "$action is refused with \"$message\", which matches ${matched.size} of this" +
                    " adapter's known rejection sites"
            }
            val site = matched.single()
            assertEquals(site.type.java, error.javaClass) {
                "$action is refused at the '${site.name}' site, which raises ${site.type.simpleName}"
            }
            return site.name
        }

        @Test
        fun `every refused shape lands on exactly one of them in these numbers`() {
            val counts = TreeMap<String, Int>()
            THROWING.keys.forEach { counts.merge(siteOf(it), 1, Int::plus) }
            assertEquals(REFUSAL_SITE_COUNTS, counts)
            assertEquals(THROWING.size, counts.values.sum(), "every refusal has to be counted exactly once")
        }

        /**
         * The substrings raised when the MAPPING, not the plan shape, is what fell short: a
         * reference the mapping does not name, and a reference resolved to the wrong kind of entry.
         * Neither is a limitation of Exposed, so neither may be the reason a corpus shape is refused
         * — it is the exact accident #326 was filed for, where an unmapped field had six actions
         * throwing "Unknown attribute" while never reaching the mechanism their `reason` claimed.
         *
         * Stated over EVERY corpus action that refuses, not only the declared ones: a shape the
         * corpus says this adapter translates, refused because the mapping came up short, is the
         * same accident wearing a different classification.
         */
        @Test
        fun `no refusal is the mapping coming up short`() {
            val shortfalls = listOf(
                "Unknown attribute",
                "is mapped as a relation, but this position needs a scalar column",
                "is mapped as a column, but this position needs a relation",
            )
            val unmapped = (THROWING.keys + refusedAnyway.keys).mapNotNull { action ->
                val message = runCatching { filterFor(action) }.exceptionOrNull()?.message.orEmpty()
                if (shortfalls.any { message.contains(it) }) "$action: $message" else null
            }
            assertEquals(emptyList<String>(), unmapped)

            // Anti-vacuity: the detector must recognise the messages it is looking for, built here
            // rather than hoped for. An empty mapping names nothing…
            assertTrue(refusalOf("cs-eq", Options.of(cerbosMapping { })).contains(shortfalls[0]))
            // …and a bare boolean redirected at a relation resolves to the wrong kind of entry.
            val asRelation = Options.of(
                cerbosMapping {
                    "request.resource.attr.aBool" to many(Tags, from = Resources.id, to = Tags.resourceId) {
                        "name" to Tags.name
                    }
                },
            )
            assertTrue(refusalOf("root-bare-bool", asRelation).contains(shortfalls[1]))
        }

        private fun refusalOf(action: String, options: Options): String =
            assertThrows(IllegalArgumentException::class.java) { filterFor(action, options) }.message.orEmpty()
    }

    /** One `throw` site, named for the mechanism, with the exception type it raises. */
    private class RefusalSite(
        val name: String,
        val substring: String,
        val type: KClass<out IllegalArgumentException>,
    )

    // -- rules over everything emitted -------------------------------------------------------------

    /**
     * The properties a regenerated asset must not silently accept.
     *
     * Pinned bytes do not survive `gradle goldenUpdate` being run and committed unread; rules do.
     * So each of these is stated over every translated corpus action rather than over a chosen
     * shape, each carries an anti-vacuity assertion, and each holds for a corpus action nobody has
     * added yet — which is the case the pinned bytes structurally cannot cover.
     */
    @Nested
    inner class WhatTheEmittedSqlContains {

        @Test
        fun `every LIKE carries an ESCAPE clause`() {
            // LIKE metacharacters in a needle are this corpus's founding bug class (#258/#259): an
            // unescaped `%` in a value turns an equality into a wildcard match and returns rows the
            // PDP denies. The adapter escapes them itself and declares the escape character, and a
            // LIKE that reached the database without one would read those backslashes as literal
            // text.
            val unescaped = mutableListOf<String>()
            val withLike = TreeSet<String>()
            conditionalRenderings().forEach { (action, dialect, one) ->
                val likes = one.sql.occurrencesOf(" LIKE ")
                if (likes == 0) return@forEach
                withLike.add(action)
                if (likes != one.sql.occurrencesOf(" ESCAPE ")) {
                    unescaped.add("$action ($dialect): ${one.sql}")
                }
            }
            assertEquals(emptyList<String>(), unescaped)

            // Anti-vacuity, in two parts. The detector must reject something, which Exposed will
            // happily render: `LikeEscapeOp` with no escape character emits a bare LIKE.
            val bare = OfflineRenderer.renderOn(
                OfflineRenderer.POSTGRESQL,
                LikeEscapeOp(Resources.aString, stringParam("100\\%%"), true, null),
            ).sql
            assertEquals(1, bare.occurrencesOf(" LIKE "))
            assertEquals(0, bare.occurrencesOf(" ESCAPE "))
            // …and the corpus has to reach the rule at all, which is a fact about the corpus and is
            // therefore pinned in both directions rather than asserted to be non-empty.
            assertEquals(ACTIONS_EMITTING_LIKE, withLike.toList())
        }

        @Test
        fun `no NULL is ever a bound parameter`() {
            // PostgreSQL cannot infer a type for a bare placeholder, so `? IS NULL` is rejected
            // outright; MySQL and SQLite accept it and compare against a value the plan never
            // carried. The adapter renders NULL as a literal for both reasons (`sql/UnknownOp`).
            val bound = conditionalRenderings().flatMap { (action, dialect, one) ->
                one.params.indices.filter { one.params[it].value == null }
                    .map { "$action ($dialect) parameter $it: ${one.params[it]}" }
            }
            assertEquals(emptyList<String>(), bound)

            // Anti-vacuity, in two parts: the detector recognises a null bind, built here…
            assertTrue(RenderedParam.of("VarCharColumnType", null).value == null)
            // …and the corpus binds arguments at all, so the rule is not passing over an empty list.
            assertTrue(conditionalRenderings().sumOf { it.third.params.size } > 0)
        }

        /**
         * `AliasAllocator` gives every subquery instance its own `cerbos_N`, so one relation entered
         * twice in one plan cannot have its inner table capture the outer correlation, and the
         * prefix keeps it from colliding with an alias the caller's own query declares.
         *
         * Two things are asserted. **Dense** — the numbers used are exactly `1..n`, so an allocation
         * that went missing shows up as a gap. **One table per number** — every place a statement
         * declares `cerbos_1` names the same table, which is the shape of the capture bug: a second
         * `FROM <other table> cerbos_1` inside an enclosing `cerbos_1` re-binds every correlation
         * under it, and the subquery then answers about the wrong rows.
         *
         * **The assertion is a count against a count**: the DISTINCT aliases a statement carries
         * must equal the number of `allocate` CALLS that translation made. An allocator that
         * regressed to one alias per TABLE would still number densely from 1 and still bind each
         * name to one table — every weaker check passes, and the inner subquery captures the outer
         * correlation — but it would ask for three aliases and render two, and only these two
         * counts disagreeing says so. An alias allocated and then dropped fails it from the other
         * side. [AliasAllocator.calls] counts invocations rather than the numbering precisely so it
         * cannot collapse along with it.
         *
         * Density is asserted beside it and is a different claim: the numbers being exactly `1..n`
         * is what makes the recorded SQL a function of how many subqueries a plan has rather than
         * of how many times something happened to call the allocator, which is what keeps the asset
         * from churning on an unrelated translator change.
         *
         * Two weaker-looking facts are deliberately NOT asserted, because neither is a property of
         * the allocator:
         *
         * - **The numbers need not appear in ascending order.** Allocation is in WALK order and the
         *   renderer nests, so a chained macro emits its inner subquery inside the outer's select
         *   list and the text reads `cerbos_3` before `cerbos_2` before `cerbos_1`.
         * - **One number may be introduced several times.** Exposed expression trees are immutable,
         *   so `TriLogic` shares one node between a positive and a negated occurrence and a ternary
         *   rewrite renders the same subquery in each arm. Each rendering is a self-contained
         *   correlated subquery whose alias is scoped to it, so the same `FROM … cerbos_1` appearing
         *   twice is one allocation, not two.
         */
        @Test
        fun `every subquery alias rendered is one the allocator handed out`() {
            val offenders = mutableListOf<String>()
            val withAliases = TreeSet<String>()
            var nodesRenderedTwice = 0

            conditionalActions().forEach { action ->
                val (op, allocated) = aliasesAllocatedBy(action)
                OfflineRenderer.DIALECTS.forEach { dialect ->
                    val emitted = renderingOf(action, dialect)
                    // The walk above is a copy of the entry point's steps, so it has to keep
                    // producing what the entry point produces or the count belongs to some other
                    // translation than the one the asset records.
                    assertEquals(emitted.sql, OfflineRenderer.renderOn(dialect, op).sql) {
                        "$action ($dialect) translates differently through the adapter than through" +
                            " the alias-counting copy of its steps"
                    }

                    // H2 folds an unquoted identifier to upper case, so the statement is read in one
                    // case rather than every pattern below being written twice.
                    val sql = emitted.sql.lowercase()
                    val occurrences = ALIAS.findAll(sql).toList()
                    if (occurrences.isEmpty()) {
                        if (allocated != 0) offenders.add("$action ($dialect): $allocated allocated, none rendered")
                        return@forEach
                    }
                    withAliases.add(action)

                    val stray = occurrences.map { it.value }.filterNot { NUMBERED_ALIAS.matches(it) }.distinct()
                    if (stray.isNotEmpty()) offenders.add("$action ($dialect): $stray")

                    // THE assertion. A collapse asks for more aliases than it renders; an alias
                    // allocated and then dropped renders fewer than it asked for, from the other
                    // direction.
                    val distinct = occurrences.map { it.value }.distinct()
                    if (distinct.size != allocated) {
                        offenders.add(
                            "$action ($dialect): $allocated aliases asked for but ${distinct.size}" +
                                " rendered (${distinct.sorted()})",
                        )
                    }

                    // …and they are exactly 1..n, so the recorded SQL is a function of the plan
                    // rather than of the call count.
                    val numbers = distinct.filter { NUMBERED_ALIAS.matches(it) }
                        .map { it.removePrefix(AliasAllocator.PREFIX).toInt() }
                        .sorted()
                    if (numbers != (1..numbers.size).toList()) {
                        offenders.add("$action ($dialect): aliases are numbered $numbers")
                    }

                    // …and each of them introduces exactly one table, which names the hazard in the
                    // failure rather than leaving a reader to work it out from two numbers. A use
                    // reads THROUGH the alias (`cerbos_1.a_bool`); anything else introduces it, and
                    // what it introduces is the identifier just before it.
                    occurrences.groupBy { it.value }.forEach { (name, uses) ->
                        val introductions = uses.filter { sql.getOrNull(it.range.last + 1) != '.' }
                        val tables = introductions
                            .map { sql.take(it.range.first).trimEnd().substringAfterLast(' ') }
                            .distinct()
                        if (tables.size != 1) offenders.add("$action ($dialect): $name is bound to $tables")
                        if (introductions.size > 1) nodesRenderedTwice++
                    }
                }
            }
            assertEquals(emptyList<String>(), offenders)

            // Anti-vacuity, in four parts: the alias detector rejects the shapes it looks for…
            assertFalse(NUMBERED_ALIAS.matches("cerbos_outer"))
            assertTrue(NUMBERED_ALIAS.matches("cerbos_12"))
            // …the corpus emits subqueries at all, pinned in both directions…
            assertEquals(ACTIONS_EMITTING_A_SUBQUERY_ALIAS, withAliases.toList())
            // …a node really is rendered in more than one place, so "one number may be introduced
            // several times" is a fact about this corpus and not a hedge…
            assertTrue(nodesRenderedTwice > 0, "no subquery node is rendered twice, so that claim is untested")
            // …and, the one that makes the count comparison bite, the corpus really does enter one
            // table twice in a single plan. `p-hasintersection-map` reads adversarial_tags through
            // two independent subqueries, which is exactly the shape a per-table allocator would
            // collapse into one and the shape AliasAllocator exists for.
            val (_, allocatedForTwoEntries) = aliasesAllocatedBy("p-hasintersection-map")
            assertEquals(2, allocatedForTwoEntries)
            val sql = renderingOf("p-hasintersection-map", OfflineRenderer.POSTGRESQL).sql.lowercase()
            assertEquals(
                listOf("adversarial_tags cerbos_1", "adversarial_tags cerbos_2"),
                Regex("adversarial_tags cerbos_\\d+").findAll(sql).map { it.value }.distinct().sorted().toList(),
                "the one corpus shape that enters a table twice no longer does",
            )
        }

        /**
         * The four dialects render ONE translation, so they may differ only in how a store spells
         * something — never in what is compared.
         *
         * Stating that as a rule rather than as a list of actions means a NEW divergence fails here
         * instead of arriving as an unexplained diff in the asset, and it holds for a shape nobody
         * has added yet. PostgreSQL is the reference each dialect is compared against, so adding a
         * dialect adds one comparison rather than rewriting the rule.
         */
        @Test
        fun `the dialects differ only in the spellings named here`() {
            val unexplained = mutableListOf<String>()
            val exercised = TreeSet<String>()
            conditionalActions().forEach { action ->
                val reference = renderingOf(action, OfflineRenderer.POSTGRESQL)
                OfflineRenderer.DIALECTS.filterNot { it == OfflineRenderer.POSTGRESQL }.forEach { dialect ->
                    val one = renderingOf(action, dialect)
                    // The bind arguments are not a dialect's business at all: a constant is bound
                    // by the VALUE's type, so a dialect that changed one changed the comparison.
                    assertEquals(reference.params, one.params) { "$action ($dialect) binds different arguments" }
                    if (canonicalise(one.sql, exercised) != canonicalise(reference.sql, exercised)) {
                        unexplained.add(
                            "$action ($dialect)\n  $dialect:     ${canonicalise(one.sql)}" +
                                "\n  postgresql: ${canonicalise(reference.sql)}",
                        )
                    }
                }
            }
            assertEquals(emptyList<String>(), unexplained)

            // Anti-vacuity: a rewrite that fires nowhere could be hiding a real divergence instead
            // of a declared one, so which of them the corpus exercises is pinned in both directions.
            assertEquals(DIALECT_SPELLINGS_EXERCISED, exercised.toSortedSet())
        }

        @Test
        fun `the folded now() literal keeps the nanosecond precision the PDP emits`() {
            // The one operand a wire fixture cannot pin: `now() - duration("24h")` differs on every
            // capture, so the fixture carries a placeholder and this adapter's reader chooses a
            // value (Corpus.PLANNED_AT). The choice is load-bearing — the PDP emits NANOSECONDS,
            // and a tidy millisecond substitution would pin a comparison the PDP never produces
            // against a column that carries them.
            assertTrue(Corpus.PLANNED_AT.endsWith(".123456789Z"), Corpus.PLANNED_AT)
            listOf("ts-window", "ts-vf").forEach { action ->
                OfflineRenderer.DIALECTS.forEach { dialect ->
                    val one = renderingOf(action, dialect)
                    assertTrue(one.params.any { it.value.toString().contains("123456789") }) {
                        "$action ($dialect) binds ${one.params}, which has lost the PDP's nanoseconds"
                    }
                }
            }
        }

        @Test
        fun `the arguments JSON cannot hold are pinned rather than trusted to the encoder`() {
            val normalised = conditionalRenderings().flatMap { (action, dialect, one) ->
                one.normalisedParams.map { "$action/$dialect/$it" }
            }
            assertEquals(NON_JSON_DOUBLE_PARAMS, normalised)

            // Anti-vacuity: the reporter recognises the values it is looking for, built here rather
            // than hoped for — JSON has no negative zero, no NaN and no infinity.
            assertEquals(
                listOf("-0.0", "NaN", "Infinity", "-Infinity"),
                listOf(-0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
                    .map { RenderedParam.of("DoubleColumnType", it).normalisedFrom },
            )
        }
    }

    // -- the renderer that wrote the asset, and the other one --------------------------------------

    /**
     * The asset records Exposed's rendering of this adapter's expression trees, so it records WHICH
     * Exposed — `conformance/README.md`, "When the generator is an input". CI runs this suite under
     * two releases (`build.gradle.kts`, `ADAPTER_TEST_ORM`): the one the asset was rendered under,
     * where every recorded byte is asserted, and the floor the published jar is compiled against,
     * where [RENDERING_DIFFERS_ON_THE_FLOOR] is asserted instead.
     */
    @Nested
    inner class TheGeneratorIsAnInput {

        @Test
        fun `the asset declares the renderer that wrote it, and the build says which leg this is`() {
            // Which leg this is comes from the BUILD, not the classpath: a resolution that quietly
            // drifted to a third release would otherwise read as whichever leg it happened to
            // match, and a dependency bump that makes the recorded bytes somebody else's fails here
            // before it fails on the shapes.
            val selected = System.getProperty("adapter.test.orm", "baseline")
            val running = Golden.runningExposedVersion()
            when (selected) {
                "baseline" -> assertTrue(running.startsWith("${Golden.EXPOSED_MINOR}.")) {
                    "${GOLDEN.file} was rendered by Exposed ${Golden.EXPOSED_MINOR} and the baseline" +
                        " build runs $running: re-record the asset deliberately rather than editing" +
                        " the header"
                }

                "floor" -> assertFalse(running.startsWith("${Golden.EXPOSED_MINOR}.")) {
                    "the `floor` ORM set must resolve an Exposed older than the" +
                        " ${Golden.EXPOSED_MINOR} the asset declares; this build runs $running"
                }

                else -> throw AssertionError("adapter.test.orm must be `baseline` or `floor`, got `$selected`")
            }
            assertEquals(selected == "baseline", GOLDEN.rendersUnderRunningExposed)
        }

        @Test
        fun `regeneration is refused on the leg the asset was not rendered under`(@TempDir directory: Path) {
            // `gradle goldenUpdate` calls `Golden.write`, and rule 2 of "When the generator is an
            // input" is that it refuses BEFORE the write: regenerating on the floor leg would
            // rewrite every entry the two renderers spell differently and label it 1.5.
            val floor = Golden(directory.resolve("expectations.json"), runningExposedVersion = "1.0.0")
            val error = assertThrows(IllegalStateException::class.java) { floor.write(emptyMap()) }
            assertTrue(error.message!!.contains("1.0.0 is on the classpath"), error.message)
            assertFalse(Files.exists(floor.file), "the asset must not be created by a refused write")
        }

        @Test
        fun `diverges from the asset on exactly the shapes the list names`() {
            // A name in the list that is not a recorded action can never fire, on either leg.
            // Checked before the leg split, because the baseline is the leg that runs on every push.
            assertEquals(
                emptyList<String>(),
                RENDERING_DIFFERS_ON_THE_FLOOR.filterNot { it in recordedActions },
            ) { "the divergence list names shapes the asset does not record" }
            assertEquals(
                RENDERING_DIFFERS_ON_THE_FLOOR.sorted().distinct(),
                RENDERING_DIFFERS_ON_THE_FLOOR,
                "the divergence list must stay sorted and free of duplicates",
            )
            // The reason the list is allowed to be a list rather than a second pinned asset: an
            // entry on it is a shape whose ROWS the harness proves against check() on both legs,
            // so what the bytes do not cover, the oracle does.
            val oracle = Corpus.oracleActions(ACTIONS, ADAPTER).toSet()
            assertEquals(
                emptyList<String>(),
                RENDERING_DIFFERS_ON_THE_FLOOR.filterNot { it in oracle },
            ) { "every shape the renderers disagree on must still be an oracle comparison" }

            assumeFalse(
                GOLDEN.rendersUnderRunningExposed,
                "the divergence set is empty on the renderer the asset was generated under",
            )

            val diverging = recordedActions.filterNot { recorded.getValue(it) == entryFor(it) }
            assertEquals(RENDERING_DIFFERS_ON_THE_FLOOR, diverging) {
                "Exposed ${Golden.runningExposedVersion()} diverges from the asset on a different" +
                    " set of shapes than the list pins"
            }
        }

        @Test
        fun `the asset names a command this build defines`() {
            // The asset carries the command that rewrites it, so a reader who opens the file after
            // a failing assertion is told how to look at the difference. That is only useful while
            // the command exists.
            val (runner, task) = Golden.REGENERATE_COMMAND.split(" ")
            assertEquals("gradle", runner)
            val build = Files.readString(Path.of(System.getProperty("user.dir"), "build.gradle.kts"))
            assertTrue(build.contains("tasks.register<Test>(\"$task\")")) {
                "build.gradle.kts defines no task named $task"
            }
            // …and that task runs THIS class, which is what makes regeneration and comparison one
            // pair rather than two commands that happen to read the same file.
            assertTrue(build.contains("includeTestsMatching(\"${ExposedTranslatorTest::class.java.name}\")")) {
                "the $task task does not filter to ${ExposedTranslatorTest::class.java.name}"
            }
        }
    }

    // -- the caller's contracts, which no corpus action can vary -----------------------------------

    /**
     * Kind 2 material (`CLAUDE.md`, "What a translator unit test may pin"): `actions.json`
     * classifies each action against exactly ONE mapping and one [Options] per adapter, so a second
     * mapper form, a call-level NULL convention and a macro-depth bound have no corpus spelling at
     * all. The corpus asks what a policy produces; these ask what a caller passes.
     */
    @Nested
    inner class ContractsTheCorpusCannotVary {

        /** The corpus's `nullRepresentationOmitted` probe, which has no store in it at all. */
        private val probe = Corpus.nullRepresentationThrows(ACTIONS).single()

        @Test
        fun `EXPLICIT emits an IS NULL filter and OMITTED refuses the same plan`() {
            // The planner emits the same `eq(attr, null)` node whichever convention the caller
            // uses, so the adapter has to be TOLD, and what it does when it is told is a pure
            // translator property (#302).
            assertEquals("null-eq-missing", probe.action)
            assertTrue(sqlFor(probe.action).contains("IS NULL")) { sqlFor(probe.action) }

            // Under OMITTED a NULL column sends no attribute, so check() denies on a
            // missing-attribute error while the filter above returns exactly those rows.
            val omitted = OPTIONS.withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
            val error = assertThrows(IllegalArgumentException::class.java) { filterFor(probe.action, omitted) }
            assertTrue(error.message.orEmpty().contains(Corpus.nullOmittedMessage(probe, ADAPTER))) {
                error.message.orEmpty()
            }
        }

        @Test
        fun `a per-attribute declaration overrides the call-level option`() {
            // #308. `owner` declares EXPLICIT in the corpus mapping, so `null-eq` — which probes it
            // — must still translate under a call-level OMITTED…
            val omitted = OPTIONS.withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
            assertEquals(sqlFor("null-eq"), sqlFor("null-eq", options = omitted))

            // …and stripping the declaration must reject the same action under the same option, so
            // the override above is doing work rather than being quietly equivalent to the default.
            val stripped = omitted.withMapping(MAPPING_WITHOUT_NULL_CONVENTIONS)
            val error = assertThrows(IllegalArgumentException::class.java) { filterFor("null-eq", stripped) }
            assertTrue(error.message.orEmpty().contains(Corpus.nullOmittedMessage(probe, ADAPTER))) {
                error.message.orEmpty()
            }
        }

        /**
         * A shallow reference and a deep one, so the equivalence covers references resolved through
         * several relation hops rather than one lookup off the root.
         */
        private val mapperFormActions = listOf("cs-eq", "macro-depth3-exists")

        @Test
        fun `a resolver function resolves the same references as a declared table`() {
            // The two mapper forms are one contract with two spellings, and a function resolver is
            // asked for progressively shorter PREFIXES of a dotted reference — which a declared
            // table answers from its own keys. Nothing in the corpus can vary the form.
            val byRule = OPTIONS.withMapping(AttributeResolver { MAPPING.resolve(it) })
            mapperFormActions.forEach { action ->
                OfflineRenderer.DIALECTS.forEach { dialect ->
                    assertEquals(
                        OfflineRenderer.renderOn(dialect, filterFor(action).toOp()).sql,
                        OfflineRenderer.renderOn(dialect, filterFor(action, byRule).toOp()).sql,
                    ) { "$action ($dialect) resolves differently through a function mapper" }
                }
            }
        }

        @Test
        fun `maxMacroDepth refuses a nesting the default admits`() {
            // Each macro level multiplies the correlated subqueries the filter carries, so the
            // bound is a cost guard — and a plan nested past it is refused rather than emitted at
            // whatever size it happens to be.
            val action = "macro-depth3-exists"
            val shallow = OPTIONS.withMaxMacroDepth(1)
            val error = assertThrows(IllegalArgumentException::class.java) { filterFor(action, shallow) }
            assertTrue(error.message.orEmpty().contains("past maxMacroDepth=1")) { error.message.orEmpty() }

            // …and the default admits it, so the bound is doing work rather than the shape being
            // untranslatable anyway.
            assertTrue(filterFor(action) is QueryPlanFilter.Conditional)
        }
    }

    // -- plans the planner cannot produce ----------------------------------------------------------

    /**
     * Input validation on a public function, not policy shapes. Every other assertion in this file
     * reads its plan from a fixture precisely because a typed plan is a belief about the planner —
     * but these are malformed by construction, so there is no fixture to read and nothing to
     * believe. They exist so a caller who hands the adapter a hand-rolled or half-decoded plan gets
     * an error rather than a filter.
     *
     * A shape CEL *can* express does not belong here, whatever its plan looks like: it belongs in
     * the corpus, where every adapter is asked about it.
     */
    @Nested
    inner class PlansThePlannerCannotProduce {

        @Test
        fun `an unrecognised plan kind`() {
            val plan = PlanResourcesFilter.newBuilder().setKindValue(4242).build()
            val error = assertThrows(MalformedPlanException::class.java) {
                ExposedQueryPlanAdapter.toFilter(plan, OPTIONS)
            }
            assertTrue(error.message!!.contains("Unknown filter kind"), error.message)
        }

        @Test
        fun `a conditional plan with no condition`() {
            val plan = PlanResourcesFilter.newBuilder()
                .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                .build()
            val error = assertThrows(MalformedPlanException::class.java) {
                ExposedQueryPlanAdapter.toFilter(plan, OPTIONS)
            }
            assertTrue(error.message!!.contains("Conditional plan has no condition"), error.message)
        }

        @Test
        fun `an operand with no node`() {
            // Nested under a junction, because the top-level case is the one above: an operand slot
            // the decoder left empty reaches the walk rather than the entry point.
            val error = assertThrows(MalformedPlanException::class.java) {
                translate(expression("and", Operand.getDefaultInstance()))
            }
            assertTrue(error.message!!.contains("Plan operand has no node set"), error.message)
        }

        @Test
        fun `not with two operands`() {
            val bool = variable("request.resource.attr.aBool")
            val error = assertThrows(MalformedPlanException::class.java) {
                translate(expression("not", bool, bool))
            }
            assertTrue(error.message!!.contains("not requires exactly 1 operand, got 2"), error.message)
        }

        @Test
        fun `a protobuf value with no kind`() {
            val error = assertThrows(MalformedPlanException::class.java) {
                translate(
                    expression(
                        "eq",
                        variable("request.resource.attr.aString"),
                        Operand.newBuilder().setValue(Value.getDefaultInstance()).build(),
                    ),
                )
            }
            assertTrue(error.message!!.contains("Protobuf Value has no kind set"), error.message)
        }

        private fun translate(condition: Operand): QueryPlanFilter =
            ExposedQueryPlanAdapter.toFilter(
                PlanResourcesFilter.newBuilder()
                    .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                    .setCondition(condition)
                    .build(),
                OPTIONS,
            )

        private fun variable(name: String): Operand = Operand.newBuilder().setVariable(name).build()

        private fun expression(operator: String, vararg operands: Operand): Operand =
            Operand.newBuilder()
                .setExpression(Expression.newBuilder().setOperator(operator).addAllOperands(operands.toList()))
                .build()
    }
}

// -- the rules' vocabulary -------------------------------------------------------------------------

/** Anything spelled like one of this adapter's subquery aliases, whatever the dialect's case. */
private val ALIAS = Regex("${AliasAllocator.PREFIX}[A-Za-z0-9_]*", RegexOption.IGNORE_CASE)

/** …and the one shape it is allowed to take. */
private val NUMBERED_ALIAS = Regex("${AliasAllocator.PREFIX}[1-9][0-9]*")

/** Every identifier the corpus schema declares, which is every identifier the SQL may name. */
private val DECLARED_IDENTIFIERS: Set<String> =
    ADVERSARIAL_TABLES.flatMap { table -> listOf(table.tableName) + table.columns.map { it.name } }.toSet()

private val IDENTIFIER_TOKEN = Regex("[A-Za-z_][A-Za-z0-9_]*")

private val BOOLEAN_LITERAL = Regex("\\b(TRUE|FALSE)\\b", RegexOption.IGNORE_CASE)

/** The target of a to-double cast, which every store spells differently and no two agree on. */
private val DOUBLE_CAST_TARGET = Regex("\\bAS (DOUBLE PRECISION|DOUBLE|REAL|FLOAT\\(53\\))\\b", RegexOption.IGNORE_CASE)

/** The target of a to-text cast: `CHAR` on MySQL, `VARCHAR` everywhere else. */
private val TEXT_CAST_TARGET = Regex("\\bAS (VARCHAR|CHAR)\\b", RegexOption.IGNORE_CASE)

/** SQLite has no `CHAR_LENGTH`; its `LENGTH` already counts characters for a text value. */
private val CHARACTER_COUNT = Regex("\\bLENGTH\\(", RegexOption.IGNORE_CASE)

/**
 * `CONCAT(a, b, …)` rewritten to `(a || b || …)`, the spelling every dialect but MySQL uses.
 *
 * A textual rewrite rather than a regex, because the arguments nest: a concatenation of a cast of a
 * concatenation is one statement, and only depth tracking finds the right closing parenthesis and
 * the right commas. Prepared rendering means there are no string literals to confuse the scan.
 */
private fun rewriteConcat(sql: String): String {
    val open = sql.indexOf(CONCAT_CALL)
    if (open < 0) return sql
    var depth = 0
    val args = mutableListOf(StringBuilder())
    var index = open + CONCAT_CALL.length
    while (index < sql.length) {
        when (val ch = sql[index]) {
            '(' -> { depth++; args.last().append(ch) }
            ')' -> if (depth == 0) {
                val rewritten = args.joinToString(" || ") { it.toString().trim() }
                return rewriteConcat(sql.substring(0, open) + "(" + rewritten + ")" + sql.substring(index + 1))
            } else {
                depth--
                args.last().append(ch)
            }
            ',' -> if (depth == 0) args.add(StringBuilder()) else args.last().append(ch)
            else -> args.last().append(ch)
        }
        index++
    }
    // An unbalanced CONCAT( is a rendering this rewrite has no reading of; leave it for the
    // comparison to report as an unexplained difference rather than silently half-rewriting it.
    return sql
}

private const val CONCAT_CALL = "CONCAT("

/**
 * The ways two dialects are allowed to spell ONE translation differently, each named.
 *
 * Applied in order; the name of every rewrite that changed the text is collected, so a rule can pin
 * which ones the corpus exercises rather than letting a rewrite quietly absorb a real divergence.
 */
private val DIALECT_SPELLINGS: List<Pair<String, (String) -> String>> = listOf(
    // MySQL quotes with a backtick, the others with a double quote, and each quotes a different set
    // of identifiers: PostgreSQL's keyword list is the SERVER's, not the driver's.
    "identifier quoting" to { sql: String -> sql.replace("\"", "").replace("`", "") },
    // H2 folds an unquoted identifier to UPPER case, PostgreSQL to lower, MySQL and SQLite leave it
    // as written. Only tokens the schema declares are folded, so a keyword is never touched.
    "identifier case folding" to { sql: String ->
        IDENTIFIER_TOKEN.replace(sql) { match ->
            val lower = match.value.lowercase()
            if (lower in DECLARED_IDENTIFIERS || NUMBERED_ALIAS.matches(lower)) lower else match.value
        }
    },
    // SQLite has no boolean literal and spells one 1/0.
    "boolean literal spelling" to { sql: String ->
        BOOLEAN_LITERAL.replace(sql) { if (it.value.equals("TRUE", ignoreCase = true)) "1" else "0" }
    },
    // `DOUBLE PRECISION` on PostgreSQL, `DOUBLE` on MySQL, `REAL` on SQLite, `FLOAT(53)` on H2 —
    // all 53-bit binary floating point, which is what CEL arithmetic needs (`sql/IeeeDoubleCast`).
    "double cast target" to { sql: String -> DOUBLE_CAST_TARGET.replace(sql, "AS <double>") },
    // MySQL's CAST accepts CHAR and rejects an unqualified VARCHAR (`sql/TextCastExpression`).
    "text cast target" to { sql: String -> TEXT_CAST_TARGET.replace(sql, "AS <text>") },
    // SQLite has no CHAR_LENGTH, and its LENGTH already counts characters rather than bytes
    // (`sql/CountChars`). `\b` does not match inside CHAR_LENGTH, so this never doubles a prefix.
    "character count function" to { sql: String -> CHARACTER_COUNT.replace(sql, "CHAR_LENGTH(") },
    // `||` is logical OR on MySQL outside PIPES_AS_CONCAT, and PostgreSQL's CONCAT() skips NULL
    // where CEL's `+` propagates it, so the two spellings are not interchangeable in either
    // direction (`sql/ConcatExpression`).
    "concatenation spelling" to ::rewriteConcat,
)

/** [sql] with every declared spelling difference rewritten away, recording which ones fired. */
private fun canonicalise(sql: String, exercised: MutableSet<String> = mutableSetOf()): String =
    DIALECT_SPELLINGS.fold(sql) { text, (name, rewrite) ->
        rewrite(text).also { if (it != text) exercised.add(name) }
    }

private fun String.occurrencesOf(needle: String): Int {
    var count = 0
    var at = indexOf(needle)
    while (at >= 0) {
        count++
        at = indexOf(needle, at + 1)
    }
    return count
}
