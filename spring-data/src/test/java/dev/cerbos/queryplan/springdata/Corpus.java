/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.springdata;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.protobuf.util.JsonFormat;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reads the shared {@code conformance/} corpus (the recorded goldens, the dataset and the PDP
 * tags) and this adapter's {@code conformance-ledger.json}, and holds the one
 * {@link AttributeMapping} every conformance case is translated with.
 *
 * <p>Every adapter has its own copy of this loader on purpose; do not extract a shared one. See
 * {@code docs/adr/0007-adapters-share-data-not-code.md}.
 */
final class Corpus {

    private Corpus() {}

    /**
     * Reads floats as doubles, never as {@code BigDecimal}, so a recorded {@code -0.0} keeps its
     * sign on the way to protobuf.
     */
    private static final ObjectMapper JSON = new ObjectMapper()
            // A duplicated ledger key would otherwise keep only its last entry.
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    static Path conformanceDir() {
        return Path.of(System.getProperty("user.dir"), "..", "conformance").normalize();
    }

    static JsonNode readJson(Path file) {
        try {
            return JSON.readTree(file.toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static <T> T readJson(Path file, Class<T> type) {
        try {
            return JSON.readValue(file.toFile(), type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // -- the PDPs and their goldens ---------------------------------------------------------------

    /** The tags of {@code pdp-versions.json}, current first. */
    static List<String> pdpTags() {
        JsonNode versions = readJson(conformanceDir().resolve("pdp-versions.json"));
        return List.of(versions.get("current").get("tag").asText(),
                versions.get("previous").get("tag").asText());
    }

    /** The current PDP's tag. */
    static String currentTag() {
        return pdpTags().get(0);
    }

    /** Every golden file recorded for {@code tag}, sorted by case id. */
    static List<JsonNode> goldens(String tag) {
        Path dir = conformanceDir().resolve("golden").resolve(tag);
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .sorted()
                    .map(Corpus::readJson)
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One golden file by case id, for example {@code string/equals/case-sensitive}. */
    static JsonNode golden(String tag, String caseId) {
        return readJson(conformanceDir().resolve("golden").resolve(tag).resolve(caseId + ".json"));
    }

    /** The recorded plan of {@code caseId} under the current PDP. */
    static PlanResourcesResponse plan(String caseId) {
        return plan(golden(currentTag(), caseId));
    }

    private static final String NOW_MINUS_24H = "__NOW_MINUS_24H__";

    /** Nanosecond precision, as the PDP writes the literal it folds {@code now()} into. */
    private static final DateTimeFormatter RFC3339_NANOS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'")
                    .withZone(ZoneOffset.UTC);

    /**
     * A golden's {@code plan} (the {@code PlanResources} filter, decoded with protobuf's JSON
     * mapping), with {@code __NOW_MINUS_24H__} filled in.
     */
    static PlanResourcesResponse plan(JsonNode golden) {
        JsonNode plan = golden.get("plan");
        if (plan == null || plan.isNull()) {
            throw new IllegalStateException(golden.get("id") + " records no plan");
        }
        String nowMinus24h = RFC3339_NANOS.format(Instant.now().minus(Duration.ofHours(24)));
        PlanResourcesFilter.Builder filter = PlanResourcesFilter.newBuilder();
        try {
            JsonFormat.parser().merge(plan.toString().replace(NOW_MINUS_24H, nowMinus24h), filter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return PlanResourcesResponse.newBuilder().setFilter(filter).build();
    }

    // -- spring-data/conformance-ledger.json ------------------------------------------------------

    /**
     * One ledger entry. {@code pdp}, when present, limits it to those PDP tags. Unknown keys fail
     * the read, so a misspelt {@code pdp} cannot widen an entry to every tag.
     */
    record LedgerEntry(String status, String reason, String issue, List<String> pdp) {
        boolean appliesTo(String tag) {
            return pdp == null || pdp.contains(tag);
        }
    }

    private record Ledger(String adapter, Map<String, LedgerEntry> cases) {}

    static Map<String, LedgerEntry> ledger() {
        Path file = Path.of(System.getProperty("user.dir"), "conformance-ledger.json");
        Ledger ledger = readJson(file, Ledger.class);
        if (!"spring-data".equals(ledger.adapter()) || ledger.cases() == null) {
            throw new IllegalStateException(file + " is not the spring-data ledger");
        }
        return ledger.cases();
    }

    // -- the corpus mapped onto the JPA model ---------------------------------------------------

    /**
     * The one mapping every case is translated with. Each scalar {@code resources.json} omits when
     * its column is NULL declares {@link NullAttributeRepresentation#OMITTED}; {@code owner} and
     * {@code coOwner}, which send an explicit null, declare {@link NullAttributeRepresentation#EXPLICIT}.
     */
    static final Map<String, AttributeMapping> MAPPING = Map.ofEntries(
            Map.entry("request.resource.id", AttributeMapping.field("id")),
            Map.entry("request.resource.attr.aBool", AttributeMapping.field("aBool")),
            Map.entry("request.resource.attr.aString", AttributeMapping.field("aString")),
            Map.entry("request.resource.attr.aNumber", AttributeMapping.field("aNumber")),
            Map.entry("request.resource.attr.aDouble", omitted("aDouble")),
            Map.entry("request.resource.attr.aOptionalString", omitted("aOptionalString")),
            // An ISO-date string column.
            Map.entry("request.resource.attr.createdBy", AttributeMapping.field("createdBy")),
            // A delimited hierarchy path.
            Map.entry("request.resource.attr.scope", omitted("scope")),
            Map.entry("request.resource.attr.createdAt", omitted("createdAt")),
            Map.entry("request.resource.attr.updatedAt", omitted("updatedAt")),
            Map.entry("request.resource.attr.obj.inner", AttributeMapping.field("aString")),
            // The to-one chain. Associations are LEFT-joined, so an absent parent leaves only its
            // own comparison UNKNOWN.
            Map.entry("request.resource.attr.parent.aBool", AttributeMapping.field("parent.aBool")),
            Map.entry("request.resource.attr.parent.aString", AttributeMapping.field("parent.aString")),
            Map.entry("request.resource.attr.parent.aNumber", AttributeMapping.field("parent.aNumber")),
            Map.entry("request.resource.attr.parent.aOptionalString",
                    omitted("parent.aOptionalString")),
            Map.entry("request.resource.attr.parent.inner.aBool",
                    AttributeMapping.field("parent.inner.aBool")),
            Map.entry("request.resource.attr.parent.inner.aString",
                    AttributeMapping.field("parent.inner.aString")),
            Map.entry("request.resource.attr.parent.inner.aNumber",
                    AttributeMapping.field("parent.inner.aNumber")),
            Map.entry("request.resource.attr.parent.inner.aOptionalString",
                    omitted("parent.inner.aOptionalString")),
            // `owner` and `coOwner` reuse the aOptionalString and scope columns, but send a NULL
            // column as an explicit null instead of omitting the attribute.
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
                            "labels", AttributeMapping.relation("labels", Map.of(
                                    "name", AttributeMapping.field("name")
                            ))
                    ))
            ))),
            // A single object on the check side, but two collection hops here (categories, then
            // subCategories), so a chained path joins through every hop.
            Map.entry("request.resource.attr.mainCategory", AttributeMapping.relation("categories", Map.of(
                    "name", AttributeMapping.field("name"),
                    "subCategories", AttributeMapping.relation("subCategories", Map.of(
                            "name", AttributeMapping.field("name")
                    )),
                    // The same chain as a scalar list of names.
                    "subNames", AttributeMapping.relation("subCategories", "name")
            )))
    );

    private static AttributeMapping omitted(String jpaPath) {
        return AttributeMapping.field(jpaPath, NullAttributeRepresentation.OMITTED);
    }

    /** {@link #MAPPING} without per-attribute null conventions, so only the call-level option applies. */
    static final Map<String, AttributeMapping> MAPPING_WITHOUT_NULL_CONVENTIONS =
            MAPPING.entrySet().stream().collect(Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    e -> e.getValue() instanceof AttributeMapping.Field f
                            ? AttributeMapping.field(f.jpaPath())
                            : e.getValue()));
}
