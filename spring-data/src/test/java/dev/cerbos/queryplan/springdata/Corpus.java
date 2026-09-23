/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

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
 * Reads the shared {@code conformance/} corpus and this adapter's golden expectations. Holds what
 * {@link SpringDataTranslatorTest} and {@link AdversarialConformanceTest} must share, including
 * the {@link AttributeMapping}, so both suites test the same query.
 *
 * <p>Every adapter has its own copy of this loader on purpose; do not extract a shared one. See
 * {@code docs/adr/0007-adapters-share-data-not-code.md}.
 */
final class Corpus {

    private Corpus() {}

    /** This adapter's key in the corpus files. */
    static final String ADAPTER = "spring-data";

    private static final ObjectMapper JSON = new ObjectMapper();

    static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    // -- conformance/actions.json ---------------------------------------------------------------

    /** An {@code expectedUnsupported} entry. {@code messages} is keyed by adapter name. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record UnsupportedShape(String action, String shape, Map<String, String> messages) {}

    /** A {@code nullRepresentationOmitted} entry, rejected by every adapter under OMITTED. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record NullRepresentationOmitted(String action, String reason, Map<String, String> messages) {}

    /**
     * An {@code adapterUnsupported} or {@code adapterSupportedExpected} entry. {@code message} is
     * the substring the error must contain; it is absent on the second, which does not throw.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record AdapterUnsupported(String action, String reason, String message) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record KnownDivergence(String action, String reason, List<String> adapters) {}

    /** An action whose oracle is always {@code "empty"} or {@code "total"}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record DegenerateOracle(String action, String oracle, String reason) {}

    /**
     * Declare every group in actions.json here. Jackson silently drops an undeclared field, and
     * its actions would vanish from every test.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record ActionsFile(
            List<String> conformance,
            Map<String, List<AdapterUnsupported>> adapterUnsupported,
            Map<String, List<AdapterUnsupported>> adapterSupportedExpected,
            List<UnsupportedShape> expectedUnsupported,
            List<NullRepresentationOmitted> nullRepresentationOmitted,
            List<KnownDivergence> knownDivergences,
            List<DegenerateOracle> degenerateOracles) {

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

        /** Degenerate oracles, action to {@code "empty"} or {@code "total"}. */
        Map<String, String> degenerateOracleShapes() {
            if (degenerateOracles == null) {
                throw new IllegalStateException(
                        "actions.json declares no degenerateOracles: the degeneracy sweep would"
                                + " exempt nothing and fail every by-construction oracle");
            }
            Map<String, String> shapes = new TreeMap<>();
            for (DegenerateOracle entry : degenerateOracles) {
                if (!"empty".equals(entry.oracle()) && !"total".equals(entry.oracle())) {
                    throw new IllegalStateException("degenerateOracles." + entry.action()
                            + " declares oracle '" + entry.oracle()
                            + "': it must be \"empty\" or \"total\"");
                }
                if (shapes.put(entry.action(), entry.oracle()) != null) {
                    throw new IllegalStateException(
                            "degenerateOracles lists '" + entry.action() + "' twice");
                }
            }
            return shapes;
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
     * The substring the error must contain. Fails when none is pinned, so an unrelated error
     * cannot pass as the declared refusal.
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
     * Actions this adapter must refuse, with their messages: its {@code adapterUnsupported} plus
     * {@code expectedUnsupported} minus its promotions. {@code nullRepresentationOmitted} is not
     * included because those actions translate under the default null representation.
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
     * The value substituted for {@code __NOW_MINUS_24H__} in the wire fixtures. The planner folds
     * {@code now() - duration("24h")} to a literal, so the fixture script replaces it with a
     * placeholder. It has nanosecond precision because the PDP emits nanoseconds; it ends up in
     * the golden expectations.
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
     * The plan the pinned PDP produced for {@code action}. The fixture is the PDP's HTTP response,
     * so it is decoded with protobuf's JSON mapping.
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

    /** The corpus's attribute mapping, used by both corpus suites. */
    static final Map<String, AttributeMapping> MAPPING = Map.ofEntries(
            // The primary key, not under `attr` (the `id-*` actions).
            Map.entry("request.resource.id", AttributeMapping.field("id")),
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aDouble", AttributeMapping.field("aDouble")),
            Map.entry("request.resource.attr.aOptionalString", AttributeMapping.field("aOptionalString")),
            // ISO-date string column for the p-* actions.
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
            // Delimited hierarchy path for the hier-* actions.
            Map.entry("request.resource.attr.scope", AttributeMapping.field("scope")),
            // Temporal columns for the ts-* actions.
            Map.entry("request.resource.attr.createdAt", AttributeMapping.field("createdAt")),
            Map.entry("request.resource.attr.updatedAt", AttributeMapping.field("updatedAt")),
            Map.entry("request.resource.attr.obj.inner", AttributeMapping.field("aString")),
            // The to-one chain (the `rel-*` actions). A dotted path through a to-one association
            // is an implicit inner join, so a row with no parent is excluded even under negation.
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
            // `owner` and `coOwner` reuse the aOptionalString and scope columns, but the oracle
            // sends an explicit null for a NULL column instead of omitting the attribute.
            Map.entry("request.resource.attr.owner",
                    AttributeMapping.field("aOptionalString", NullAttributeRepresentation.EXPLICIT)),
            Map.entry("request.resource.attr.coOwner",
                    AttributeMapping.field("scope", NullAttributeRepresentation.EXPLICIT)),
            // Tag names as a scalar list. A NULL name is a null list element.
            Map.entry("request.resource.attr.tagNames", AttributeMapping.relation("tags", "name")),
            // Scalar lists, one related row per element; a NULL element column is a null element.
            Map.entry("request.resource.attr.aNumberList",
                    AttributeMapping.relation("aNumberList", "element")),
            Map.entry("request.resource.attr.aBoolList",
                    AttributeMapping.relation("aBoolList", "element")),
            Map.entry("request.resource.attr.tags", AttributeMapping.relation("tags", Map.of(
                    "id", AttributeMapping.field("id"),
                    "name", AttributeMapping.field("name")
            ))),
            Map.entry("request.resource.attr.categories", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name"),
                            // Third macro level, for the macro-depth3-* actions.
                            "labels", AttributeMapping.relation("labels", Map.of(
                                    "name", AttributeMapping.field("name")
                            ))
                    ))
            ))),
            // A single object on the check side, but two collection hops here (categories, then
            // subCategories). Checks that a chained path joins through every hop.
            Map.entry("request.resource.attr.mainCategory", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name")
                    )),
                    // The same chain as a scalar list of names.
                    "subNames", AttributeMapping.relation("subCategories", "name")
            )))
    );

    /**
     * {@link #MAPPING} without per-attribute null conventions, so only the call-level option
     * applies. Used to check that every action with a null literal is rejected under OMITTED.
     */
    static final Map<String, AttributeMapping> MAPPING_WITHOUT_NULL_CONVENTIONS =
            MAPPING.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    e -> e.getValue() instanceof AttributeMapping.Field f
                            ? AttributeMapping.field(f.jpaPath())
                            : e.getValue()));

    // -- spring-data/golden/expectations.json ---------------------------------------------------

    /** Free-text commentary on an entry. Never compared. */
    static final String NOTE_KEY = "note";

    /**
     * The Hibernate version that rendered the golden SQL. Hibernate's renderer changes the
     * output, so the file records it. The {@code next} CI leg runs the following major. See
     * {@code conformance/README.md}, "When the generator is an input".
     */
    static final String HIBERNATE_MINOR = "6.6";

    /** The command that rewrites the golden file, recorded in it. */
    static final String GOLDEN_REGENERATE_COMMAND = "gradle goldenUpdate";

    static Path goldenFile() {
        return Path.of(System.getProperty("user.dir"), "golden", "expectations.json").normalize();
    }

    /**
     * The golden expectations by action, with notes removed. Fails if the header names another
     * adapter or Hibernate version.
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
     * Rewrites the golden file, sorted by action and keeping existing notes. Called only by
     * {@code gradle goldenUpdate}. A missing file is created. Refuses to run under a Hibernate
     * version other than {@value #HIBERNATE_MINOR}, since another renderer writes different SQL.
     */
    static void writeGoldenExpectations(Map<String, ObjectNode> expectations) {
        String running = org.hibernate.Version.getVersionString();
        if (!running.startsWith(HIBERNATE_MINOR + ".")) {
            throw new IllegalStateException(goldenFile() + " is generated under Hibernate "
                    + HIBERNATE_MINOR + ", and " + running + " is on the classpath. Regenerating"
                    + " here would rewrite every entry the two renderers spell differently and"
                    + " label it " + HIBERNATE_MINOR + ".");
        }
        // Skip header validation: the old file may carry an outdated header, and its notes
        // should still be kept.
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

    /** Two-space indent, no space before a colon, LF line endings. */
    private static DefaultPrettyPrinter prettyPrinter() {
        DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
        return new DefaultPrettyPrinter()
                .withObjectIndenter(indenter)
                .withArrayIndenter(indenter)
                .withSeparators(new Separators()
                        .withObjectFieldValueSpacing(Separators.Spacing.AFTER));
    }
}
