package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.protobuf.util.JsonFormat;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * This adapter's reader for the shared {@code ../conformance/} corpus, and for the golden
 * expectations that belong to it alone.
 *
 * <p><strong>Deliberately duplicated.</strong> Every adapter carries its own loader —
 * {@code prisma/src/corpus.ts}, {@code sqlalchemy/tests/corpus.py}, {@code pgx/corpus_test.go},
 * this one — so that each stays standalone and none of them can break another by changing.
 * Do not extract a shared one, and do not add a drift check between the copies: they are
 * allowed to differ ({@code docs/adr/0007-adapters-share-data-not-code.md}).
 *
 * <p>What lives here is what BOTH of this adapter's corpus suites must agree on: the
 * classification in {@code actions.json}, the wire-fixture decoding, and the
 * {@link AttributeMapping} the corpus is mapped through. That last one is the load-bearing
 * part — {@link SpringDataTranslatorTest} pins the SQL the adapter emits for a corpus action
 * and {@link AdversarialConformanceTest} proves the rows that same SQL returns, and the two
 * statements are only about the same query while both are built from this mapping.
 *
 * <p>The seeds, the derived fields, the {@code check()} oracle and the coverage guards over
 * all three stay in the harness, which is the only thing that consumes them.
 */
final class Corpus {

    private Corpus() {}

    /** The corpus key for this adapter — its directory name, as every other harness uses. */
    static final String ADAPTER = "spring-data";

    private static final ObjectMapper JSON = new ObjectMapper();

    static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    // -- conformance/actions.json ---------------------------------------------------------------

    /**
     * An {@code expectedUnsupported} entry. {@code messages} carries one entry per adapter that
     * must reject the shape, keyed by adapter name; {@code validate-corpus.sh} asserts that key
     * set is exactly the roster minus the adapters that promoted the shape.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record UnsupportedShape(String action, String shape, Map<String, String> messages) {}

    /**
     * A {@code nullRepresentationOmitted} entry. Every adapter must reject these — the two NULL
     * conventions are indistinguishable on the wire — so {@code messages} names the whole roster
     * with no promotions to subtract.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record NullRepresentationOmitted(String action, String reason, Map<String, String> messages) {}

    /**
     * An {@code adapterUnsupported} / {@code adapterSupportedExpected} entry. {@code message} is
     * the substring this adapter's error must contain — present on the first, absent on the
     * second, which does not throw.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record AdapterUnsupported(String action, String reason, String message) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record KnownDivergence(String action, String reason, List<String> adapters) {}

    /**
     * Every group in actions.json must be named here: Jackson silently drops a field this
     * record does not declare, and a dropped group makes its actions vanish from every count
     * and every parameterised case at once — the projection trap conformance/README.md warns
     * about. The manifest tripwire in each suite is what makes an undropped group load-bearing.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record ActionsFile(
            List<String> conformance,
            Map<String, List<AdapterUnsupported>> adapterUnsupported,
            Map<String, List<AdapterUnsupported>> adapterSupportedExpected,
            List<UnsupportedShape> expectedUnsupported,
            List<NullRepresentationOmitted> nullRepresentationOmitted,
            List<KnownDivergence> knownDivergences) {

        List<AdapterUnsupported> adapterUnsupportedFor(String adapter) {
            return adapterUnsupported == null
                    ? List.of()
                    : adapterUnsupported.getOrDefault(adapter, List.of());
        }

        List<AdapterUnsupported> adapterSupportedExpectedFor(String adapter) {
            return adapterSupportedExpected == null
                    ? List.of()
                    : adapterSupportedExpected.getOrDefault(adapter, List.of());
        }

        /** Every action the corpus declares, in any group. */
        Set<String> manifestActions() {
            Set<String> manifest = new TreeSet<>(conformance);
            expectedUnsupported.forEach(u -> manifest.add(u.action()));
            nullRepresentationOmitted.forEach(n -> manifest.add(n.action()));
            knownDivergences.forEach(d -> manifest.add(d.action()));
            return manifest;
        }

        Set<String> skippedDivergences(String adapter) {
            return knownDivergences.stream()
                    .filter(d -> d.adapters().contains(adapter))
                    .map(KnownDivergence::action)
                    .collect(java.util.stream.Collectors.toCollection(TreeSet::new));
        }
    }

    static ActionsFile actionsFile() {
        try {
            return JSON.readValue(
                    conformanceDir().resolve("actions.json").toFile(), ActionsFile.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The substring this adapter's error must contain, or a loud failure. The message is what
     * turns "it threw" into "it threw for the declared reason": without it a mapper typo or an
     * unrelated validation satisfies the throw suite just as well as the documented limitation
     * (cerbos/query-plan-adapters#326).
     */
    static String requireMessage(String label, String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalStateException("actions.json pins no throw message for " + label
                    + ": the throw suite would accept a failure for any reason");
        }
        return message;
    }

    /** Actions this adapter oracle-compares: conformance minus its own unsupported, plus promotions. */
    static Stream<String> oracleActions(ActionsFile actions, String adapter) {
        Set<String> unsupported = actions.adapterUnsupportedFor(adapter).stream()
                .map(AdapterUnsupported::action)
                .collect(java.util.stream.Collectors.toSet());
        return Stream.concat(
                actions.conformance().stream().filter(a -> !unsupported.contains(a)),
                actions.adapterSupportedExpectedFor(adapter).stream()
                        .map(AdapterUnsupported::action).sorted());
    }

    /**
     * Every action this adapter must refuse, each with the message it must refuse it with:
     * {@code adapterUnsupported[me]} plus {@code expectedUnsupported} minus its own promotions.
     *
     * <p>{@code nullRepresentationOmitted} is deliberately absent — under the DEFAULT
     * representation those actions translate, so they carry a golden expectation like any other
     * and their refusal is a property of the flipped option (see
     * {@link #nullRepresentationThrows}).
     */
    static Map<String, String> throwingActions(ActionsFile actions, String adapter) {
        Set<String> promoted = actions.adapterSupportedExpectedFor(adapter).stream()
                .map(AdapterUnsupported::action)
                .collect(java.util.stream.Collectors.toSet());
        Map<String, String> throwing = new TreeMap<>();
        for (AdapterUnsupported entry : actions.adapterUnsupportedFor(adapter)) {
            throwing.put(entry.action(), requireMessage(
                    "adapterUnsupported." + adapter + "." + entry.action(), entry.message()));
        }
        for (UnsupportedShape entry : actions.expectedUnsupported()) {
            if (promoted.contains(entry.action())) {
                continue;
            }
            throwing.put(entry.action(), requireMessage(
                    "expectedUnsupported." + entry.action() + ".messages." + adapter,
                    entry.messages() == null ? null : entry.messages().get(adapter)));
        }
        return throwing;
    }

    /** The {@code nullRepresentationOmitted} probes, each with the message its rejection must carry. */
    static List<NullRepresentationOmitted> nullRepresentationThrows(ActionsFile actions) {
        return actions.nullRepresentationOmitted();
    }

    static String nullOmittedMessage(NullRepresentationOmitted entry, String adapter) {
        return requireMessage(
                "nullRepresentationOmitted." + entry.action() + ".messages." + adapter,
                entry.messages() == null ? null : entry.messages().get(adapter));
    }

    // -- conformance/wire-fixtures/ -------------------------------------------------------------

    /**
     * The instant {@code regenerate-wire-fixtures.sh} substitutes for the one operand it cannot
     * pin.
     *
     * <p>{@code ts-window} and {@code ts-vf} compare against {@code now() - duration("24h")},
     * which the planner folds to a literal timestamp: a different value on every capture, so the
     * script rewrites it to {@code __NOW_MINUS_24H__} to keep the drift check deterministic.
     * Reading a fixture back therefore means CHOOSING a value, and the choice is load-bearing —
     * it lands in the golden expectation as the instant those two actions compare against.
     *
     * <p>Nanosecond precision, deliberately, and the same instant {@code sqlalchemy/tests/corpus.py}
     * chose: the PDP emits nanoseconds, and this adapter maps {@code createdAt} to a
     * {@link java.time.Instant} column that carries them. A tidy millisecond substitution would
     * pin a rendering the PDP never produces. {@code SpringDataTranslatorTest} asserts the
     * precision survives into the emitted SQL, so this stays a decision rather than an accident.
     */
    static final String PLANNED_AT = "2026-08-11T09:13:39.123456789Z";

    private static final String NOW_MINUS_24H = "__NOW_MINUS_24H__";

    /** Every action the corpus has a golden wire fixture for, sorted. */
    static List<String> wireFixtureActions() {
        try (Stream<Path> files = Files.list(conformanceDir().resolve("wire-fixtures"))) {
            return files.map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(".json"))
                    .map(name -> name.substring(0, name.length() - ".json".length()))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The plan the pinned PDP produced for {@code action}, decoded into the protobuf response
     * the SDK hands a caller.
     *
     * <p>The fixture IS the PDP's HTTP response body, so the decoding here is protobuf's own
     * canonical JSON mapping ({@link JsonFormat}) — the same mapping the PDP's HTTP API writes.
     * It is deliberately not a hand-built plan: a plan somebody typed is a BELIEF about what the
     * planner emits, and this repository keeps fixtures precisely because that belief has been
     * wrong before. See {@code docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md}.
     */
    static PlanResourcesResponse planFromWireFixture(String action) {
        return planFromWireFixture(action, PLANNED_AT);
    }

    static PlanResourcesResponse planFromWireFixture(String action, String plannedAt) {
        Path fixture = conformanceDir().resolve("wire-fixtures").resolve(action + ".json");
        try {
            JsonNode filter = JSON.readTree(fixture.toFile()).get("filter");
            if (filter == null) {
                throw new IllegalStateException(fixture + " carries no filter");
            }
            PlanResourcesFilter.Builder builder = PlanResourcesFilter.newBuilder();
            JsonFormat.parser().merge(
                    filter.toString().replace(NOW_MINUS_24H, plannedAt), builder);
            return PlanResourcesResponse.newBuilder().setFilter(builder).build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // -- the corpus mapped onto the JPA model ---------------------------------------------------

    /**
     * The corpus's attribute mapping. Read by BOTH corpus suites: the harness executes the
     * Specifications it produces against seeded rows, and the translator suite pins the SQL.
     */
    static final Map<String, AttributeMapping> MAPPING = Map.ofEntries(
            // The primary key, reached as `request.resource.id` rather than through `attr` (the
            // `id-*` actions). An adapter that resolves references by stripping a
            // `request.resource.attr.` prefix never sees this name.
            Map.entry("request.resource.id", AttributeMapping.field("id")),
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aDouble", AttributeMapping.field("aDouble")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            // ISO-date string column + flattened struct member for the p-* probes
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
            // Delimited hierarchy path column for the hier-* actions
            Map.entry("request.resource.attr.scope", AttributeMapping.field("scope")),
            // Instant column for the ts-* timestamp() comparison actions
            Map.entry("request.resource.attr.createdAt", AttributeMapping.field("createdAt")),
            Map.entry("request.resource.attr.obj.inner", AttributeMapping.field("aString")),
            // The corpus's one REAL to-one chain (the `rel-*` actions). obj.inner above is a flat
            // column wearing a dotted name; these are a genuine association, and a dotted jpaPath
            // through a to-ONE association is an implicit INNER JOIN in the Criteria API. That is
            // what makes the absent parent deny under BOTH polarities without a separate guard:
            // a row with no parent produces no join row at all, so it is excluded from the query
            // rather than being readmitted by a negation (cerbos/query-plan-adapters#375).
            Map.entry("request.resource.attr.parent.aBool", AttributeMapping.field("parent.aBool")),
            Map.entry("request.resource.attr.parent.aString", AttributeMapping.field("parent.aString")),
            Map.entry("request.resource.attr.parent.aNumber", AttributeMapping.field("parent.aNumber")),
            Map.entry("request.resource.attr.parent.aOptionalString",
                    AttributeMapping.field("parent.aOptionalString")),
            Map.entry("request.resource.attr.parent.inner.aBool",
                    AttributeMapping.field("parent.inner.aBool")),
            Map.entry("request.resource.attr.parent.inner.aString",
                    AttributeMapping.field("parent.inner.aString")),
            Map.entry("request.resource.attr.parent.inner.aNumber",
                    AttributeMapping.field("parent.inner.aNumber")),
            Map.entry("request.resource.attr.parent.inner.aOptionalString",
                    AttributeMapping.field("parent.inner.aOptionalString")),
            // in-null-elem-*: same column as aOptionalString, but the oracle sends an
            // EXPLICIT null attribute for NULL columns (aOptionalString is OMITTED instead)
            // — pinning the adapter's convention that a DB NULL is the explicitly-null
            // attribute (eq-null → IS NULL, and `x in [..., null]` → OR IS NULL).
            // `owner` and `coOwner` alias columns that `aOptionalString` and `scope` also map,
            // under the OTHER null convention: the oracle sends a real null attribute for them
            // rather than omitting it. Declaring that here is what makes the equality family
            // definite for these two attributes and leaves it untouched for every other
            // mapping (cerbos/query-plan-adapters#308).
            Map.entry("request.resource.attr.owner",
                    AttributeMapping.field("aOptionalString", NullAttributeRepresentation.EXPLICIT)),
            Map.entry("request.resource.attr.coOwner",
                    AttributeMapping.field("scope", NullAttributeRepresentation.EXPLICIT)),
            // Scalar projection of tags (defaultMemberField=name) for `null in R.attr.tagNames`;
            // NULL name columns become explicit null list elements on the check side.
            Map.entry("request.resource.attr.tagNames", AttributeMapping.relation("tags", "name")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")
            ))),
            Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name"),
                            // Third macro level for the macro-depth3-* actions.
                            "labels", AttributeMapping.relation("labels", Map.of(
                                    "name", AttributeMapping.field("name")
                            ))
                    ))
            ))),
            // Multi-hop chain probe (W1): mainCategory is a SINGLE nested object on the check
            // side (every seed holds at most one category), so CEL evaluates dotted chains
            // like R.attr.mainCategory.subCategories naturally — while the ADAPTER maps the
            // same path through TWO collection hops (categories JOIN subCategories), pinning
            // that chained variables join through every intermediate hop, never off the root.
            Map.entry("request.resource.attr.mainCategory", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name")
                    )),
                    // subNames: the same 2-hop chain but with a defaultMemberField, so plain
                    // `in` membership compares the flattened tail's name column.
                    "subNames", AttributeMapping.relation("subCategories", "name")
            )))
    );

    /**
     * The same mapping with every per-attribute null convention stripped, so the call-level
     * option is the only thing governing null operands.
     *
     * <p>The #302 completeness guard is a statement about that option: every corpus action
     * carrying a null literal must be rejected under OMITTED. Declaring {@code owner}/
     * {@code coOwner} as explicit-null (#308) deliberately overrides the option for those two
     * attributes — which would otherwise read as the guard going quiet, when in fact it is the
     * per-attribute declaration doing exactly its job. Stripping the declarations keeps the
     * guard testing what it was written to test.
     */
    static final Map<String, AttributeMapping> MAPPING_WITHOUT_NULL_CONVENTIONS =
            MAPPING.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    e -> e.getValue() instanceof AttributeMapping.Field f
                            ? AttributeMapping.field(f.jpaPath())
                            : e.getValue()));

    // -- spring-data/golden/expectations.json ---------------------------------------------------

    /** The reserved key an entry may carry alongside its expectation; never compared. */
    static final String NOTE_KEY = "note";

    /**
     * The Hibernate minor this asset's SQL was rendered by, and the one this build compiles and
     * tests against.
     *
     * <p>The adapter emits a Criteria tree; the SQL in the asset is HIBERNATE'S rendering of that
     * tree, so the renderer is an input to the recorded value the same way the SQLAlchemy major is
     * to that adapter's ({@code conformance/README.md}, "When the generator is an input"). Unlike
     * SQLAlchemy this build has exactly one Hibernate on the classpath, so there is no second leg
     * to assert a divergence list against — but a consumer brings their own (the dependency is
     * {@code compileOnly}), so which one wrote these bytes has to be answerable from the file.
     */
    static final String HIBERNATE_MINOR = "6.6";

    /** The command that rewrites the asset. Documentation that travels with the data. */
    static final String GOLDEN_REGENERATE_COMMAND = "gradle goldenUpdate";

    static Path goldenFile() {
        return Path.of(System.getProperty("user.dir"), "golden", "expectations.json").normalize();
    }

    /**
     * The golden expectations, keyed by action, in file order, each with its {@code note}
     * removed — commentary is carried across regeneration and never compared.
     *
     * <p>{@code adapter} is checked rather than ignored: the file is a flat map of action names,
     * so a copy taken from another adapter parses cleanly and would be compared against this
     * adapter's output with only the diff to say something went wrong. {@code hibernate} is
     * checked for the same reason and a sharper one — the bytes are one renderer's.
     */
    static Map<String, ObjectNode> readGoldenExpectations() {
        JsonNode contents;
        try {
            contents = JSON.readTree(goldenFile().toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        String adapter = contents.path("adapter").asText(null);
        if (!ADAPTER.equals(adapter)) {
            throw new IllegalStateException(goldenFile() + " declares adapter \"" + adapter
                    + "\", not \"" + ADAPTER + "\"");
        }
        String hibernate = contents.path("hibernate").asText(null);
        if (!HIBERNATE_MINOR.equals(hibernate)) {
            throw new IllegalStateException(goldenFile() + " declares Hibernate \"" + hibernate
                    + "\", not \"" + HIBERNATE_MINOR + "\"");
        }
        Map<String, ObjectNode> recorded = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : contents.get("expectations").properties()) {
            ObjectNode value = ((ObjectNode) entry.getValue()).deepCopy();
            value.remove(NOTE_KEY);
            recorded.put(entry.getKey(), value);
        }
        return recorded;
    }

    /**
     * Rewrite the golden expectations, carrying every existing {@code note} across.
     *
     * <p>Only ever called under {@code -Dgolden.update=true} ({@code gradle goldenUpdate}).
     * Regeneration is the same deliberate act as {@code conformance/scripts/regenerate-wire-fixtures.sh},
     * with the same safety: the diff is what a reviewer reads, which is why the entries are
     * written sorted and one action per key. CI never sets the property.
     *
     * <p>A missing file is not an error here, and only here — that is how a new adapter
     * bootstraps one. Reading a missing file for an assertion stays an error, because a suite
     * that quietly asserts nothing is the failure mode the completeness guard exists to prevent.
     *
     * <p><strong>Regenerating under another Hibernate IS an error</strong>, and it is refused
     * BEFORE the write rather than caught after it — {@code conformance/README.md}, "When the
     * generator is an input", rule 2. Two renderers do not agree on every tree, so writing here
     * would produce a file that declares {@value #HIBERNATE_MINOR} while holding some other
     * renderer's bytes: a diff a reviewer would read line by line to discover said nothing about
     * translation.
     */
    static void writeGoldenExpectations(Map<String, ObjectNode> expectations) {
        String running = org.hibernate.Version.getVersionString();
        if (!running.startsWith(HIBERNATE_MINOR + ".")) {
            throw new IllegalStateException(goldenFile() + " is generated under Hibernate "
                    + HIBERNATE_MINOR + ", and " + running + " is on the classpath. Regenerating"
                    + " here would rewrite every entry the two renderers spell differently and"
                    + " label it " + HIBERNATE_MINOR + ".");
        }
        // Notes are read WITHOUT the header validation `readGoldenExpectations` applies. The file
        // about to be overwritten may legitimately carry an older header — that is what a header
        // change looks like — and refusing to carry the commentary across because of one would
        // make every such change silently drop it.
        Map<String, String> notes = new LinkedHashMap<>();
        if (Files.exists(goldenFile())) {
            JsonNode existing;
            try {
                existing = JSON.readTree(goldenFile().toFile());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            for (Map.Entry<String, JsonNode> entry : existing.path("expectations").properties()) {
                if (entry.getValue().has(NOTE_KEY)) {
                    notes.put(entry.getKey(), entry.getValue().get(NOTE_KEY).asText());
                }
            }
        }

        ObjectNode root = JSON.createObjectNode();
        root.put("adapter", ADAPTER);
        root.put("hibernate", HIBERNATE_MINOR);
        root.put("regenerate", GOLDEN_REGENERATE_COMMAND);
        ObjectNode body = root.putObject("expectations");
        for (String action : new TreeSet<>(expectations.keySet())) {
            ObjectNode entry = JSON.createObjectNode();
            if (notes.containsKey(action)) {
                entry.put(NOTE_KEY, notes.get(action));
            }
            entry.setAll(expectations.get(action));
            body.set(action, entry);
        }

        try {
            Files.createDirectories(goldenFile().getParent());
            Files.writeString(goldenFile(), JSON.writer(prettyPrinter()).writeValueAsString(root)
                    + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Two-space indent, no space before a colon, LF line endings — the other assets' shape. */
    private static DefaultPrettyPrinter prettyPrinter() {
        DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
        return new DefaultPrettyPrinter()
                .withObjectIndenter(indenter)
                .withArrayIndenter(indenter)
                .withSeparators(new Separators()
                        .withObjectFieldValueSpacing(Separators.Spacing.AFTER));
    }
}
