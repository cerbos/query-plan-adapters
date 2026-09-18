package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.core.util.DefaultIndenter
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.core.util.Separators
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/**
 * `exposed/golden/expectations.json`: the SQL this adapter is pinned to emit for each corpus action,
 * read, validated and rewritten.
 *
 * The format is the repository's shared one (`conformance/README.md`, "Golden expectations"), and
 * what is per adapter is the value. Here the value is the emitted `Op<Boolean>` **rendered** —
 * Exposed emits an expression tree, not text, and a tree says nothing about the `LIKE` escaping or
 * the cast target a store actually sees. So each conditional entry records the SQL and the typed
 * bind arguments under every dialect this adapter claims ([OfflineRenderer.DIALECTS]).
 *
 * That makes **Exposed's own renderer an input to the bytes**, which is the case
 * `conformance/README.md` calls "When the generator is an input", and all three of its rules apply:
 *
 * 1. The loader checks the [EXPOSED_MINOR] header, exactly as it checks [ADAPTER].
 * 2. [write] refuses under any other Exposed minor, BEFORE the write, so regeneration can never
 *    present a toolchain swap as a translation change.
 * 3. The `floor` leg reads the asset and asserts a pinned divergence list in both directions rather
 *    than the bytes — which is why [read] validates the header against [EXPOSED_MINOR] rather than
 *    against the Exposed that is running: refusing to read on the floor leg would leave that leg
 *    with nothing to compare. [rendersUnderRunningExposed] is what tells the two legs apart.
 *
 * The header is load-bearing rather than decorative because `exposed-core` is a `compileOnly`
 * dependency: a consumer brings their own renderer, so which one wrote these bytes has to be
 * answerable from the file.
 */
internal class Golden(
    val file: Path = defaultFile(),
    /** The Exposed the tests are running against, as `build.gradle.kts` forwarded it. */
    private val runningExposedVersion: String = runningExposedVersion(),
) {

    /** Whether the running Exposed is the one this asset's bytes were rendered by. */
    val rendersUnderRunningExposed: Boolean
        get() = runningExposedVersion == EXPOSED_MINOR || runningExposedVersion.startsWith("$EXPOSED_MINOR.")

    /**
     * The expectations, keyed by corpus action, in file order, each with its [NOTE_KEY] removed:
     * commentary is carried across regeneration and never compared.
     *
     * Everything the format constrains is checked here rather than trusted. [ADAPTER] because the
     * file is a flat map of action names, so a copy taken from another adapter parses cleanly and
     * would be compared against this translator with only the diff to say something went wrong;
     * [EXPOSED_MINOR] for the sharper reason above; the sort order and each entry's shape because a
     * hand-edit is exactly how an entry stops meaning what the suite reads it as.
     */
    fun read(): Map<String, ObjectNode> {
        val contents = JSON.readTree(Files.readString(file))
        expect(contents.path("adapter").asText(null), ADAPTER, "adapter")
        expect(contents.path(EXPOSED_KEY).asText(null), EXPOSED_MINOR, "$EXPOSED_KEY")
        expect(contents.path("regenerate").asText(null), REGENERATE_COMMAND, "regenerate")

        val expectations = contents.get(EXPECTATIONS_KEY) as? ObjectNode
            ?: throw IllegalStateException("$file has no '$EXPECTATIONS_KEY' object")
        val actions = expectations.fieldNames().asSequence().toList()
        check(actions == actions.sorted()) {
            "$file lists its expectations out of order; `$REGENERATE_COMMAND` writes them sorted so a" +
                " translator change reads as the list of shapes it moved"
        }

        val recorded = LinkedHashMap<String, ObjectNode>()
        actions.forEach { action ->
            val entry = expectations.get(action) as? ObjectNode
                ?: throw IllegalStateException("$file: '$action' is not an object")
            validateEntry(action, entry)
            recorded[action] = entry.deepCopy().also { it.remove(NOTE_KEY) }
        }
        return recorded
    }

    /**
     * Rewrites the asset from what the translator emits today, carrying every [NOTE_KEY] across.
     *
     * Only ever called under `-Dgolden.update=true` (`gradle goldenUpdate`). Regeneration is the
     * same deliberate act as `conformance/scripts/regenerate-wire-fixtures.sh`, with the same
     * safety: CI never sets the property, so a translator change that moves the emitted SQL fails
     * there whatever anyone ran locally, and the diff is what a reviewer reads.
     *
     * A missing file is not an error here, and only here — that is how a new adapter bootstraps one.
     * Reading a missing file for an assertion stays an error, because a suite that quietly asserts
     * nothing is the failure mode the completeness guard exists to prevent.
     */
    fun write(expectations: Map<String, ObjectNode>) {
        check(rendersUnderRunningExposed) {
            "$file records Exposed $EXPOSED_MINOR's rendering, and $runningExposedVersion is on the" +
                " classpath. Regenerating here would rewrite every entry the two renderers spell" +
                " differently and label it $EXPOSED_MINOR."
        }
        expectations.forEach { (action, entry) -> validateEntry(action, entry) }

        // Notes are read WITHOUT the header validation `read` applies. The file about to be
        // overwritten may legitimately carry an older header — that is what a header change looks
        // like — and refusing to carry the commentary across because of one would silently drop it.
        val notes = LinkedHashMap<String, String>()
        if (Files.exists(file)) {
            JSON.readTree(Files.readString(file)).path(EXPECTATIONS_KEY).properties().forEach { (action, entry) ->
                entry.get(NOTE_KEY)?.let { notes[action] = it.asText() }
            }
        }

        val root = JSON.createObjectNode()
        root.put("adapter", ADAPTER)
        root.put(EXPOSED_KEY, EXPOSED_MINOR)
        root.put("regenerate", REGENERATE_COMMAND)
        val body = root.putObject(EXPECTATIONS_KEY)
        expectations.keys.sorted().forEach { action ->
            val entry = JSON.createObjectNode()
            notes[action]?.let { entry.put(NOTE_KEY, it) }
            entry.setAll<ObjectNode>(expectations.getValue(action))
            body.set<JsonNode>(action, entry)
        }

        Files.createDirectories(file.parent)
        Files.writeString(file, JSON.writer(prettyPrinter()).writeValueAsString(root) + "\n", StandardCharsets.UTF_8)
    }

    private fun validateEntry(action: String, entry: ObjectNode) {
        val keys = entry.fieldNames().asSequence().toSet()
        val unknown = keys - ENTRY_KEYS
        check(unknown.isEmpty()) { "$file: '$action' carries unknown keys $unknown; the entry holds $ENTRY_KEYS" }

        val kind = entry.path(KIND_KEY).asText(null)
        check(kind in KINDS) { "$file: '$action' declares kind '$kind', expected one of $KINDS" }

        val rendered = entry.get(RENDERED_KEY)
        if (kind != KIND_CONDITIONAL) {
            // A plan the planner folded to a constant has no SQL to record, and recording an empty
            // one would make the two kinds read alike in a diff.
            check(rendered == null) { "$file: '$action' is $kind and must carry no '$RENDERED_KEY'" }
            return
        }
        check(rendered is ObjectNode) { "$file: '$action' is $KIND_CONDITIONAL and must carry '$RENDERED_KEY'" }
        val dialects = (rendered as ObjectNode).fieldNames().asSequence().toList()
        check(dialects == OfflineRenderer.DIALECTS) {
            "$file: '$action' records dialects $dialects, expected ${OfflineRenderer.DIALECTS}"
        }
        dialects.forEach { dialect ->
            val one = rendered.get(dialect)
            check(one is ObjectNode && one.get(SQL_KEY)?.isTextual == true) {
                "$file: '$action'.$dialect has no '$SQL_KEY' string"
            }
            val params = (one as ObjectNode).get(PARAMS_KEY)
            check(params is ArrayNode) { "$file: '$action'.$dialect has no '$PARAMS_KEY' array" }
            (params as ArrayNode).forEachIndexed { index, param ->
                check(param.path(PARAM_TYPE_KEY).isTextual && param.has(PARAM_VALUE_KEY)) {
                    "$file: '$action'.$dialect param $index needs '$PARAM_TYPE_KEY' and '$PARAM_VALUE_KEY'"
                }
                val type = param.path(PARAM_TYPE_KEY).asText()
                check(type in BIND_TYPES) {
                    "$file: '$action'.$dialect param $index is bound through $type, which is not one" +
                        " of $BIND_TYPES. A new bind type is a new way a constant reaches the" +
                        " database, so it is a decision to review rather than to absorb: add it to" +
                        " BIND_TYPES once someone has read what it does to the comparison."
                }
                val value = param.get(PARAM_VALUE_KEY)
                check(value.isTextual || value.isNumber || value.isBoolean || value.isNull) {
                    "$file: '$action'.$dialect param $index records a ${value.nodeType} value; the" +
                        " asset holds JSON scalars, so a structured one means something reached it" +
                        " that `RenderedParam.of` did not normalise"
                }
            }
        }
    }

    private fun expect(actual: String?, expected: String, key: String) {
        check(actual == expected) { "$file declares $key \"$actual\", not \"$expected\"" }
    }

    companion object {
        /** Checked by [read]: the roster key of this adapter, which is its directory name. */
        const val ADAPTER: String = "exposed"

        /**
         * Every Exposed column type a corpus constant is bound through, by simple name.
         *
         * An ALLOWLIST, checked on read and on write, and the reason is the half of a filter a
         * statement does not show: `=` renders the same `?` whether the value behind it is a double
         * or a decimal, and only one of those compares the way CEL does. A new name here means a
         * constant started reaching the database some other way, and that is a decision to review
         * in a diff rather than one to discover from a store leg.
         *
         * `JavaInstantColumnType` is the one whose VALUE is not a JSON scalar to begin with:
         * `RenderedParam.of` records the instant's own text, and `ExposedTranslatorTest` pins that
         * text for `ts-window` so the normalisation is asserted rather than assumed.
         */
        val BIND_TYPES: Set<String> = linkedSetOf(
            "BooleanColumnType",
            "DoubleColumnType",
            "JavaInstantColumnType",
            "LongColumnType",
            "TextColumnType",
        )

        /**
         * The Exposed MINOR this asset's SQL was rendered by, and the `baseline` ORM set in
         * `build.gradle.kts`. Stated once, here, and checked against the running version rather
         * than restated beside it.
         */
        const val EXPOSED_MINOR: String = "1.5"

        /** The command that rewrites the asset. Documentation that travels with the data. */
        const val REGENERATE_COMMAND: String = "gradle goldenUpdate"

        const val EXPOSED_KEY: String = "exposed"
        const val EXPECTATIONS_KEY: String = "expectations"

        /** The one reserved key inside an entry: commentary, never compared, carried across. */
        const val NOTE_KEY: String = "note"
        const val KIND_KEY: String = "kind"
        const val RENDERED_KEY: String = "rendered"
        const val SQL_KEY: String = "sql"
        const val PARAMS_KEY: String = "params"
        const val PARAM_TYPE_KEY: String = "type"
        const val PARAM_VALUE_KEY: String = "value"

        const val KIND_CONDITIONAL: String = "KIND_CONDITIONAL"
        const val KIND_ALWAYS_ALLOWED: String = "KIND_ALWAYS_ALLOWED"
        const val KIND_ALWAYS_DENIED: String = "KIND_ALWAYS_DENIED"

        val KINDS: Set<String> = linkedSetOf(KIND_CONDITIONAL, KIND_ALWAYS_ALLOWED, KIND_ALWAYS_DENIED)
        val ENTRY_KEYS: Set<String> = linkedSetOf(NOTE_KEY, KIND_KEY, RENDERED_KEY)

        /** The system property `build.gradle.kts` forwards, and nothing else. */
        const val EXPOSED_VERSION_PROPERTY: String = "adapter.test.exposed.version"

        private val JSON = ObjectMapper()

        fun defaultFile(): Path =
            Path.of(System.getProperty("user.dir"), "golden", "expectations.json").normalize()

        /**
         * Which Exposed the test classpath resolved.
         *
         * Absent rather than defaulted: the property is how `build.gradle.kts` says which ORM set
         * it selected, and a default would let a suite run outside Gradle and report the baseline's
         * result for whatever happened to be on the classpath.
         */
        fun runningExposedVersion(): String = System.getProperty(EXPOSED_VERSION_PROPERTY)
            ?: throw IllegalStateException(
                "-D$EXPOSED_VERSION_PROPERTY is not set; build.gradle.kts forwards it, so this suite" +
                    " is being run outside Gradle",
            )

        /**
         * The entry for one corpus action, from the filter the translator returned for it.
         *
         * This is what a translator unit test calls per action. [build] runs outside any
         * transaction ([OfflineRenderer.translate]), so a translator that read the dialect fails
         * here rather than pinning one dialect's rendering four times.
         */
        fun entryFor(build: () -> QueryPlanFilter): ObjectNode =
            when (val filter = OfflineRenderer.translate(build)) {
                QueryPlanFilter.AlwaysAllowed -> entry(KIND_ALWAYS_ALLOWED)
                QueryPlanFilter.AlwaysDenied -> entry(KIND_ALWAYS_DENIED)
                is QueryPlanFilter.Conditional -> entry(OfflineRenderer.render(filter.op))
            }

        /** The entry for an action the planner folded to a constant: a kind and nothing else. */
        fun entry(kind: String): ObjectNode = JSON.createObjectNode().put(KIND_KEY, kind)

        /**
         * [node] as the asset stores it: encoded and parsed back.
         *
         * A built entry and the one [read] returns have to be `equals`, because that comparison IS
         * the translator unit test. Jackson's numeric nodes are TYPED, and its parser picks the type
         * from the value rather than from how the node was built — a bound `1L` written with
         * `putLong` is an `IntNode` when it comes back, and `LongNode(1) != IntNode(1)`. So every
         * entry is put through the encoding once here rather than each caller comparing two
         * in-memory spellings of the same bytes. It costs nothing in discrimination: `1` and `1.0`
         * still parse to different nodes, which is the integer-versus-double bind the recorded
         * parameters exist to catch.
         */
        private fun storedForm(node: ObjectNode): ObjectNode =
            JSON.readTree(JSON.writeValueAsString(node)) as ObjectNode

        /** The entry for a conditional action: the kind, and the rendering under every dialect. */
        fun entry(rendered: Map<String, Rendered>): ObjectNode {
            require(rendered.keys.toList() == OfflineRenderer.DIALECTS) {
                "a conditional entry records ${OfflineRenderer.DIALECTS}, got ${rendered.keys}"
            }
            val node = entry(KIND_CONDITIONAL)
            val body = node.putObject(RENDERED_KEY)
            rendered.forEach { (dialect, one) ->
                val recorded = body.putObject(dialect)
                recorded.put(SQL_KEY, one.sql)
                val params = recorded.putArray(PARAMS_KEY)
                one.params.forEach { param ->
                    val recordedParam = params.addObject()
                    recordedParam.put(PARAM_TYPE_KEY, param.type)
                    when (val value = param.value) {
                        null -> recordedParam.putNull(PARAM_VALUE_KEY)
                        is String -> recordedParam.put(PARAM_VALUE_KEY, value)
                        is Boolean -> recordedParam.put(PARAM_VALUE_KEY, value)
                        is Long -> recordedParam.put(PARAM_VALUE_KEY, value)
                        is Double -> recordedParam.put(PARAM_VALUE_KEY, value)
                        // Unreachable while `RenderedParam.of` normalises everything it does not
                        // recognise to text, which it does today — so this is a second line, not
                        // the guard. What actually keeps a library type out of the asset is
                        // [BIND_TYPES], checked on every read and every write: a temporal, a
                        // BigDecimal or an EntityID arrives here already stringified, and only its
                        // recorded TYPE says which.
                        else -> throw IllegalStateException(
                            "RenderedParam.of left a ${value::class.simpleName} in the asset; the" +
                                " recorded value has to be something JSON round-trips",
                        )
                    }
                }
            }
            return storedForm(node)
        }

        /** Two-space indent, no space before a colon, LF line endings — the other assets' shape. */
        private fun prettyPrinter(): DefaultPrettyPrinter {
            val indenter = DefaultIndenter("  ", "\n")
            return DefaultPrettyPrinter()
                .withObjectIndenter(indenter)
                .withArrayIndenter(indenter)
                .withSeparators(Separators().withObjectFieldValueSpacing(Separators.Spacing.AFTER))
        }
    }
}
