/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.protobuf.NullValue;
import com.google.protobuf.Value;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs emitted clauses against a real Elasticsearch to check how they compose with a caller's
 * query, and measures the store facts behind this adapter's refusals in
 * {@code conformance-ledger.json}: an empty array or JSON null is not indexed, an analyzed field
 * is matched per token, and a {@code date} field drops sub-millisecond precision. Needs Docker
 * (Elasticsearch only, no PDP).
 *
 * <p>Clauses always come from the adapter, from a recorded golden plan where one exists or a
 * hand-built plan otherwise. A corpus refusal is checked against its {@code unsupported} ledger
 * entry and the exception type.
 */
class ElasticsearchSurfaceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Composition and scoring. */
    private static final String SURFACE_INDEX = "surface";

    /** Empty arrays, JSON nulls, regex, wildcards and date precision. */
    private static final String SEMANTIC_SAFETY_INDEX = "semantic-safety";

    /** The one index whose string field is analyzed {@code text} rather than {@code keyword}. */
    private static final String ANALYZED_MAPPING_INDEX = "analyzed-mapping";

    /** The same timestamps under a {@code date_nanos} mapping, the remedy the refusal names. */
    private static final String DATE_NANOS_INDEX = "date-nanos";

    private static ElasticsearchContainer elasticsearch;
    private static TestElasticsearch es;

    @BeforeAll
    static void setUp() throws Exception {
        elasticsearch = new ElasticsearchContainer(ElasticsearchTestImage.IMAGE)
                .withEnv("xpack.security.enabled", "false");
        elasticsearch.start();
        es = new TestElasticsearch(elasticsearch.getHttpHostAddress());
        createIndices();
        seed();
        for (String index : List.of(SURFACE_INDEX, SEMANTIC_SAFETY_INDEX, ANALYZED_MAPPING_INDEX,
                DATE_NANOS_INDEX)) {
            es.refresh(index);
        }
    }

    @AfterAll
    static void tearDown() {
        if (elasticsearch != null) {
            elasticsearch.stop();
        }
    }

    private static void createIndices() throws Exception {
        es.createIndex(SURFACE_INDEX, Map.of("properties", Map.of(
                "aString", Map.of("type", "keyword"),
                "aBool", Map.of("type", "boolean"))));

        Map<String, Object> tagObjectProperties = Map.of(
                "id", Map.of("type", "keyword"),
                "name", Map.of("type", "keyword"));
        es.createIndex(SEMANTIC_SAFETY_INDEX, Map.of("properties", Map.ofEntries(
                Map.entry("scenario", Map.of("type", "keyword")),
                Map.entry("tags", Map.of("type", "nested", "properties", tagObjectProperties)),
                // A flat array of scalars, like the corpus's `tagNames`.
                Map.entry("tagNames", Map.of("type", "keyword")),
                Map.entry("owner", Map.of("type", "keyword")),
                Map.entry("aString", Map.of("type", "keyword")),
                Map.entry("createdAt", Map.of(
                        "type", "date", "format", "strict_date_optional_time_nanos")))));

        // `aString` is analyzed `text` with an exact `keyword` sub-field, so the same documents
        // can be queried both ways.
        es.createIndex(ANALYZED_MAPPING_INDEX, Map.of("properties", Map.of(
                "aString", Map.of(
                        "type", "text",
                        "fields", Map.of("keyword", Map.of("type", "keyword"))))));

        es.createIndex(DATE_NANOS_INDEX, Map.of("properties", Map.of(
                "createdAt", Map.of("type", "date_nanos"))));
    }

    private static void seed() throws Exception {
        // Each filter admits half of these, and their intersection is one document.
        index(SURFACE_INDEX, "s1", Map.of("aString", "one", "aBool", true));
        index(SURFACE_INDEX, "s2", Map.of("aString", "one", "aBool", false));
        index(SURFACE_INDEX, "s3", Map.of("aString", "two", "aBool", true));
        index(SURFACE_INDEX, "s4", Map.of("aString", "two", "aBool", false));

        // An empty array, an absent field, and a populated one. CEL tells the first two apart;
        // Elasticsearch cannot.
        index(SEMANTIC_SAFETY_INDEX, "empty", Map.of("scenario", "collection", "tags", List.of()));
        index(SEMANTIC_SAFETY_INDEX, "missing", Map.of("scenario", "collection"));
        index(SEMANTIC_SAFETY_INDEX, "present", Map.of(
                "scenario", "collection", "tags", List.of(Map.of("id", "t1", "name", "public"))));

        // The same for a flat array of scalars.
        index(SEMANTIC_SAFETY_INDEX, "flat-empty",
                Map.of("scenario", "flat", "tagNames", List.of()));
        index(SEMANTIC_SAFETY_INDEX, "flat-missing", Map.of("scenario", "flat"));
        index(SEMANTIC_SAFETY_INDEX, "flat-present",
                Map.of("scenario", "flat", "tagNames", List.of("a")));

        // A null element in a flat array, the same array without it, and an array of only null.
        // `List.of` rejects null, hence `Arrays.asList`.
        index(SEMANTIC_SAFETY_INDEX, "null-element",
                Map.of("scenario", "flat-null", "tagNames", Arrays.asList("a", null)));
        index(SEMANTIC_SAFETY_INDEX, "null-only",
                Map.of("scenario", "flat-null", "tagNames", Arrays.asList((Object) null)));
        index(SEMANTIC_SAFETY_INDEX, "no-null",
                Map.of("scenario", "flat-null", "tagNames", List.of("a")));

        // An explicit null, an absent field, and a value.
        index(SEMANTIC_SAFETY_INDEX, "explicit-null-owner",
                nullable("scenario", "null", "owner", null));
        index(SEMANTIC_SAFETY_INDEX, "missing-owner", Map.of("scenario", "null"));
        index(SEMANTIC_SAFETY_INDEX, "other-owner", Map.of("scenario", "null", "owner", "other"));

        // An indexed empty string, a missing field, and a value.
        index(SEMANTIC_SAFETY_INDEX, "string-empty", Map.of("scenario", "string", "aString", ""));
        index(SEMANTIC_SAFETY_INDEX, "string-missing", Map.of("scenario", "string"));
        index(SEMANTIC_SAFETY_INDEX, "string-value",
                Map.of("scenario", "string", "aString", "value"));

        index(SEMANTIC_SAFETY_INDEX, "regex-at", Map.of("scenario", "regex", "aString", "@"));
        index(SEMANTIC_SAFETY_INDEX, "regex-other", Map.of("scenario", "regex", "aString", "anything"));
        index(SEMANTIC_SAFETY_INDEX, "regex-containing",
                Map.of("scenario", "regex", "aString", "prefix@suffix"));
        index(SEMANTIC_SAFETY_INDEX, "regex-newline", Map.of("scenario", "regex", "aString", "a\nb"));

        // `a` and `b`, plus two values RE2's `^a|b$` admits but a whole-field `a|b` does not.
        index(SEMANTIC_SAFETY_INDEX, "alt-a", Map.of("scenario", "alternation", "aString", "a"));
        index(SEMANTIC_SAFETY_INDEX, "alt-b", Map.of("scenario", "alternation", "aString", "b"));
        index(SEMANTIC_SAFETY_INDEX, "alt-ab", Map.of("scenario", "alternation", "aString", "ab"));
        index(SEMANTIC_SAFETY_INDEX, "alt-xb", Map.of("scenario", "alternation", "aString", "xb"));

        // A literal `*` or `?` beside a value the unescaped character would also match.
        index(SEMANTIC_SAFETY_INDEX, "wild-star", Map.of("scenario", "wildcard", "aString", "a*b"));
        index(SEMANTIC_SAFETY_INDEX, "wild-x", Map.of("scenario", "wildcard", "aString", "axb"));
        index(SEMANTIC_SAFETY_INDEX, "wild-question",
                Map.of("scenario", "wildcard", "aString", "a?b"));
        index(SEMANTIC_SAFETY_INDEX, "wild-c", Map.of("scenario", "wildcard", "aString", "acb"));

        // Under both date mappings: one instant at millisecond precision, one 456 microseconds
        // later.
        for (String indexName : List.of(SEMANTIC_SAFETY_INDEX, DATE_NANOS_INDEX)) {
            index(indexName, "timestamp-millis",
                    Map.of("scenario", "timestamp", "createdAt", "2024-06-01T00:00:00.123Z"));
            index(indexName, "timestamp-nanos",
                    Map.of("scenario", "timestamp", "createdAt", "2024-06-01T00:00:00.123456Z"));
        }

        // Only "exact" equals "one"; an analyzed mapping also returns the other three.
        index(ANALYZED_MAPPING_INDEX, "exact", Map.of("aString", "one"));
        index(ANALYZED_MAPPING_INDEX, "phrase", Map.of("aString", "several words including one"));
        index(ANALYZED_MAPPING_INDEX, "casing", Map.of("aString", "ONE"));
        index(ANALYZED_MAPPING_INDEX, "unrelated", Map.of("aString", "oneiric"));
    }

    /** Builds a map that may hold a null value, which {@link Map#of} rejects. */
    private static Map<String, Object> nullable(Object... keyValues) {
        Map<String, Object> document = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            document.put((String) keyValues[i], keyValues[i + 1]);
        }
        return document;
    }

    private static void index(String index, String id, Map<String, Object> document)
            throws Exception {
        es.index(index, id, document);
    }

    private static List<Map<String, Object>> hits(String index, Map<String, Object> body)
            throws Exception {
        return es.hits("/" + index + "/_search", body);
    }

    /** The ids a clause selects when placed in {@code bool.filter}, as the README advises. */
    private static List<String> search(String index, Map<String, Object> clause) throws Exception {
        return es.ids("/" + index + "/_search",
                Map.of("query", Map.of("bool", Map.of("filter", List.of(clause)))));
    }

    /** Restricts a semantic-safety search to one scenario's documents. */
    private static Map<String, Object> inScenario(String scenario, Map<String, Object> clause) {
        return Map.of("bool", Map.of("must", List.of(
                Map.of("term", Map.of("scenario", Map.of("value", scenario))),
                clause)));
    }

    /** The documents in one scenario that a clause does not select. */
    private static Map<String, Object> notInScenario(String scenario, Map<String, Object> clause) {
        return Map.of("bool", Map.of(
                "must", List.of(Map.of("term", Map.of("scenario", Map.of("value", scenario)))),
                "must_not", List.of(clause)));
    }

    /**
     * The clause the adapter emits for a corpus case's recorded plan. Only the field map differs
     * from {@link Corpus#OPTIONS}, so refusals happen for the same reason as in the harness.
     */
    private static Map<String, Object> clauseFor(String caseId, Map<String, String> fieldMap) {
        return clauseOf(caseId, ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                Corpus.plan(caseId),
                Corpus.OPTIONS.withFieldMap(fieldMap).withScalarTypes(SCALAR_TYPES)));
    }

    /** The corpus scalar types plus {@code aString.keyword}, the analyzed index's exact sub-field. */
    private static final Map<String, ElasticsearchQueryPlanAdapter.ScalarType> SCALAR_TYPES =
            Stream.concat(Corpus.SCALAR_TYPES.entrySet().stream(),
                            Stream.of(Map.entry("aString.keyword",
                                    ElasticsearchQueryPlanAdapter.ScalarType.STRING)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    /** The clause a hand-built plan translates to under {@code options}. */
    private static Map<String, Object> clauseFor(Operand condition, Options options) {
        return clauseOf("the hand-built plan",
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan(condition), options));
    }

    private static Map<String, Object> clauseOf(String label, Result result) {
        return assertInstanceOf(Result.Conditional.class, result,
                label + " must translate, or there is no query to execute").query();
    }

    /**
     * Asserts the adapter refuses a corpus case the ledger marks {@code unsupported}, with the
     * given exception type, so the store fact measured here is the one behind that entry.
     */
    private static IllegalArgumentException assertRefusedAsLedgered(
            String caseId, Class<? extends IllegalArgumentException> type) {
        Corpus.LedgerEntry entry = Corpus.LEDGER.get(caseId);
        assertNotNull(entry, () -> caseId + " has no entry in conformance-ledger.json");
        assertEquals("unsupported", entry.status(), caseId);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Corpus.translate(caseId), caseId);
        assertInstanceOf(type, ex, caseId);
        return ex;
    }

    /** Asserts a hand-built plan is refused with the given type and message. */
    private static IllegalArgumentException assertRefused(Operand condition, Options options,
            Class<? extends IllegalArgumentException> type, String messageSubstring) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan(condition), options));
        assertTrue(ex.getMessage().contains(messageSubstring), ex.getMessage());
        assertInstanceOf(type, ex);
        return ex;
    }

    // -- hand-built plans -----------------------------------------------------------------------
    //
    // Used only where no golden plan has the shape needed. The executed clause is still the one
    // the adapter emits.

    private static PlanResourcesResponse plan(Operand condition) {
        return PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition))
                .build();
    }

    private static Operand expression(String operator, Operand... operands) {
        Expression.Builder expression = Expression.newBuilder().setOperator(operator);
        for (Operand operand : operands) {
            expression.addOperands(operand);
        }
        return Operand.newBuilder().setExpression(expression).build();
    }

    private static Operand variable(String name) {
        return Operand.newBuilder().setVariable(name).build();
    }

    private static Operand string(String value) {
        return Operand.newBuilder().setValue(Value.newBuilder().setStringValue(value)).build();
    }

    private static Operand number(double value) {
        return Operand.newBuilder().setValue(Value.newBuilder().setNumberValue(value)).build();
    }

    private static Operand nullValue() {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)).build();
    }

    private static final String A_STRING = "request.resource.attr.aString";
    private static final String TAG_NAMES = "request.resource.attr.tagNames";
    private static final String CREATED_AT = "request.resource.attr.createdAt";

    private static final Options STRING_OPTIONS =
            Corpus.OPTIONS.withFieldMap(Map.of(A_STRING, "aString"));

    /** {@code tagNames} declared as a flat collection. */
    private static final Options FLAT_ARRAY_OPTIONS =
            Corpus.OPTIONS.withFieldMap(Map.of(TAG_NAMES, "tagNames"))
                    .withCollectionFields(Set.of("tagNames"));

    private static final Options TIMESTAMP_OPTIONS =
            Corpus.OPTIONS.withFieldMap(Map.of(CREATED_AT, "createdAt"));

    /** {@code size(x) > 0}, which the adapter lowers to a presence check. */
    private static Operand nonEmpty(String variable) {
        return expression("gt", expression("size", variable(variable)), number(0));
    }

    private static Operand matches(String pattern) {
        return expression("matches", variable(A_STRING), string(pattern));
    }

    // -- usage --------------------------------------------------------------------------------

    /**
     * Filter context does not score, so an authorization clause in {@code bool.filter} cannot
     * change the ranking of the caller's query.
     */
    @Test
    void aClauseInFilterContextScoresNothing() throws Exception {
        List<Map<String, Object>> hits = hits(SURFACE_INDEX, Map.of("query", Map.of(
                "bool", Map.of("filter",
                        List.of(clauseFor("string/equals/case-sensitive", Map.of(A_STRING, "aString")))))));

        assertFalse(hits.isEmpty(), "the filter matched nothing, so there is no score to check");
        for (Map<String, Object> hit : hits) {
            assertEquals(0.0, ((Number) hit.get("_score")).doubleValue(),
                    "filter context must not contribute to the score");
        }
    }

    /** The clause composes with a caller's query as a set intersection. */
    @Test
    void aClauseComposesWithACallerQueryAsAnIntersection() throws Exception {
        Map<String, Object> cerbos = clauseFor("string/equals/case-sensitive", Map.of(A_STRING, "aString"));
        Map<String, Object> caller = Map.of("term", Map.of("aBool", Map.of("value", true)));

        Set<String> fromCerbos = Set.copyOf(search(SURFACE_INDEX, cerbos));
        Set<String> fromCaller = Set.copyOf(search(SURFACE_INDEX, caller));
        Set<String> intersection = new LinkedHashSet<>(fromCerbos);
        intersection.retainAll(fromCaller);

        // Each side must exclude something the other admits, or dropping either one would go
        // unnoticed.
        assertFalse(intersection.isEmpty(), "the two filters must overlap");
        assertTrue(intersection.size() < fromCerbos.size(), "the caller query must narrow");
        assertTrue(intersection.size() < fromCaller.size(), "the adapter clause must narrow");

        // Caller's query scored in `must`, authorization clause in `filter`.
        assertEquals(intersection, Set.copyOf(hits(SURFACE_INDEX, Map.of("query", Map.of(
                        "bool", Map.of("must", List.of(caller), "filter", List.of(cerbos)))))
                .stream().map(hit -> (String) hit.get("_id")).toList()));
        // Both in `filter`, for a caller with no scoring query.
        assertEquals(intersection, Set.copyOf(hits(SURFACE_INDEX, Map.of("query", Map.of(
                        "bool", Map.of("filter", List.of(cerbos, caller)))))
                .stream().map(hit -> (String) hit.get("_id")).toList()));
    }

    // -- the mechanisms the corpus reasons name -------------------------------------------------

    /**
     * Elasticsearch does not index an empty array, so {@code []} and a missing collection look the
     * same to every query. CEL tells them apart, so polarities that would read a missing collection
     * as an allow are refused.
     */
    @Test
    void anEmptyArrayAndAMissingArrayAreTheSameDocument() throws Exception {
        Map<String, Object> hasAnElement = Map.of("nested", Map.of(
                "path", "tags", "query", Map.of("match_all", Map.of())));

        assertEquals(List.of("present"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("collection", hasAnElement)));
        // The complement includes both the empty and the missing document.
        assertEquals(List.of("empty", "missing"),
                search(SEMANTIC_SAFETY_INDEX, notInScenario("collection", hasAnElement)));
        // `exists` on a nested path matches nothing, because nested elements are separate Lucene
        // documents, so it cannot separate them either.
        assertEquals(List.of(), search(SEMANTIC_SAFETY_INDEX,
                inScenario("collection", Map.of("exists", Map.of("field", "tags")))));
        assertEquals(List.of("empty", "missing", "present"), search(SEMANTIC_SAFETY_INDEX,
                notInScenario("collection", Map.of("exists", Map.of("field", "tags")))));

        // So these corpus cases are refused.
        assertRefusedAsLedgered("collection/all/empty-collection", UnsupportedPlanShapeException.class);
        assertRefusedAsLedgered("collection/exists/negated", UnsupportedPlanShapeException.class);
    }

    /**
     * The same for a flat array, where the presence check is {@code exists}: the positive
     * direction translates and its negation ("the collection is empty") is refused.
     */
    @Test
    void anEmptyFlatArrayAndAMissingFieldAreTheSameDocumentUnderExists() throws Exception {
        Map<String, Object> present = clauseFor(nonEmpty(TAG_NAMES), FLAT_ARRAY_OPTIONS);
        assertEquals(Map.of("exists", Map.of("field", "tagNames")), present);

        assertEquals(List.of("flat-present"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("flat", present)));
        assertEquals(List.of("flat-empty", "flat-missing"),
                search(SEMANTIC_SAFETY_INDEX, notInScenario("flat", present)));

        assertRefused(expression("not", nonEmpty(TAG_NAMES)), FLAT_ARRAY_OPTIONS,
                UnsupportedPlanShapeException.class,
                "emptiness cannot distinguish a missing collection from an empty collection");
    }

    /**
     * A null element in a flat array is not indexed: {@code ["a", null]} behaves like
     * {@code ["a"]}, and {@code exists} does not see {@code [null]}. CEL does see the element, so
     * null membership is refused.
     */
    @Test
    void aNullElementInAFlatArrayIsSelectableByNoEmittedQuery() throws Exception {
        Map<String, Object> present = clauseFor(nonEmpty(TAG_NAMES), FLAT_ARRAY_OPTIONS);
        Map<String, Object> holdsA = clauseFor(
                expression("eq", variable(TAG_NAMES), string("a")), FLAT_ARRAY_OPTIONS);
        assertEquals(Map.of("term", Map.of("tagNames", Map.of("value", "a"))), holdsA);

        assertEquals(List.of("no-null", "null-element"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("flat-null", present)));
        assertEquals(List.of("no-null", "null-element"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("flat-null", holdsA)));
        assertEquals(List.of("null-only"),
                search(SEMANTIC_SAFETY_INDEX, notInScenario("flat-null", present)));

        assertRefusedAsLedgered("null/in/null-literal-in-resource-list", UnsupportedPlanShapeException.class);
        assertRefused(expression("in", nullValue(), variable(TAG_NAMES)), FLAT_ARRAY_OPTIONS,
                UnsupportedPlanShapeException.class,
                "null membership in a document array requires an explicit null-value mapping");
    }

    /**
     * Elasticsearch does not index a JSON null, so an explicit null and an absent field look the
     * same. Under the explicit-null convention CEL can tell them apart.
     */
    @Test
    void anExplicitNullAndAMissingFieldAreTheSameDocument() throws Exception {
        assertEquals(List.of("explicit-null-owner", "missing-owner"), search(SEMANTIC_SAFETY_INDEX,
                notInScenario("null", Map.of("exists", Map.of("field", "owner")))));

        // The null-selecting direction is refused...
        assertRefusedAsLedgered("null/equals/null-literal", UnsupportedPlanShapeException.class);
        // ...and the presence-selecting one translates to `exists`.
        assertEquals(List.of("other-owner"), search(SEMANTIC_SAFETY_INDEX, inScenario("null",
                clauseFor("null/not-equals/null-literal", Map.of("request.resource.attr.owner", "owner")))));
    }

    /**
     * Elasticsearch coerces a term to the field's mapped type ({@code "true"} matches a boolean
     * {@code true}), but CEL's cross-type equality is false. So a comparison needs a declared
     * scalar type, and with one the adapter answers as CEL does.
     */
    @Test
    void aTermQueryCoercesItsValueOntoTheMappedTypeWhichIsWhyScalarTypesAreRequired()
            throws Exception {
        assertEquals(List.of("s1", "s3"), search(SURFACE_INDEX,
                Map.of("term", Map.of("aBool", Map.of("value", "true")))));

        String aBool = "request.resource.attr.aBool";
        Operand stringEquality = expression("eq", variable(aBool), string("true"));
        Options typed = Corpus.OPTIONS.withFieldMap(Map.of(aBool, "aBool"));
        assertEquals(List.of(), search(SURFACE_INDEX, clauseFor(stringEquality, typed)));
        assertEquals(List.of("s1", "s2", "s3", "s4"), search(SURFACE_INDEX,
                clauseFor(expression("ne", variable(aBool), string("true")), typed)));

        assertRefused(stringEquality, typed.withScalarTypes(Map.of()),
                UnmappedAttributeException.class, "Field 'aBool' has no declared scalar type");
    }

    /**
     * {@code exists} matches an indexed empty string, so {@code size(aString) > 0} lowered to
     * {@code exists} would return a row CEL rejects. This shows the result when a caller wrongly
     * declares {@code aString} a collection; undeclared, the adapter refuses.
     */
    @Test
    void existsMatchesAnIndexedEmptyStringWhichIsWhySizeOverAnUndeclaredFieldIsRefused()
            throws Exception {
        Map<String, Object> presence = clauseFor(nonEmpty(A_STRING),
                STRING_OPTIONS.withCollectionFields(Set.of("aString")));
        assertEquals(Map.of("exists", Map.of("field", "aString")), presence);

        // The empty string is returned with the value; only the missing field is excluded.
        assertEquals(List.of("string-empty", "string-value"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("string", presence)));

        // Undeclared, the same size() is refused, as are the corpus's size() cases over
        // scalar fields.
        assertRefusedAsLedgered("size/greater-than/string-length", UnsupportedPlanShapeException.class);
        assertRefused(nonEmpty(A_STRING), STRING_OPTIONS, UnsupportedPlanShapeException.class,
                "size() over a field not declared as a collection");
    }

    /**
     * The adapter cannot tell an analyzed field from a keyword one, so a {@code fieldMap} entry
     * pointing at {@code text} silently widens string comparisons. Both extra documents here are
     * ones {@code check()} denies.
     */
    @Test
    void anAnalyzedMappingWidensEqualityAndTheKeywordSubFieldRestoresIt() throws Exception {
        // The standard analyzer lowercases "ONE" and splits "several words including one" into
        // tokens, so both match.
        assertEquals(List.of("casing", "exact", "phrase"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("string/equals/case-sensitive", Map.of(A_STRING, "aString"))));

        // The remedy: map the field to the exact `keyword` sub-field.
        assertEquals(List.of("exact"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("string/equals/case-sensitive", Map.of(A_STRING, "aString.keyword"))));
    }

    /** The same widening through {@code prefix}, which is per-token on a {@code text} field. */
    @Test
    void anAnalyzedMappingWidensStartsWith() throws Exception {
        assertEquals(List.of("casing", "exact", "phrase", "unrelated"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("string/starts-with/case-sensitive", Map.of(A_STRING, "aString"))));
        assertEquals(List.of("exact", "unrelated"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("string/starts-with/case-sensitive", Map.of(A_STRING, "aString.keyword"))));
    }

    /**
     * A {@code date} field stores milliseconds, so a nanosecond value collapses onto the
     * millisecond and a {@code term} query cannot separate the two. That is why
     * {@code timestamp/less-than/relative-window} and
     * {@code timestamp/greater-than/relative-window-value-first}, whose folded {@code now()} has
     * nanoseconds, are refused.
     */
    @Test
    void anOrdinaryDateMappingCollapsesSubMillisecondPrecision() throws Exception {
        assertEquals(List.of("timestamp-millis", "timestamp-nanos"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("timestamp", Map.of(
                        "term", Map.of("createdAt", Map.of("value", "2024-06-01T00:00:00.123Z"))))));

        IllegalArgumentException ex =
                assertRefusedAsLedgered("timestamp/less-than/relative-window", UnsupportedPlanShapeException.class);
        assertTrue(ex.getMessage().contains("Sub-millisecond timestamp literals"), ex.getMessage());
    }

    /**
     * Under {@code date_nanos} the two instants stay distinct, so the adapter's own
     * millisecond-literal clause gives CEL's answer there and not on a {@code date} field.
     *
     * <p>This uses {@code >} rather than the corpus's {@code <}: truncation preserves
     * {@code t < X} for a millisecond {@code X}, so {@code <} would agree under both mappings.
     */
    @Test
    void aDateNanosMappingSeparatesTheInstantsADateMappingCollapses() throws Exception {
        Map<String, Object> after = clauseFor(expression("gt",
                        expression("timestamp", variable(CREATED_AT)),
                        expression("timestamp", string("2024-06-01T00:00:00.123Z"))),
                TIMESTAMP_OPTIONS);
        assertEquals(Map.of("range", Map.of("createdAt", Map.of("gt", "2024-06-01T00:00:00.123Z"))),
                after);

        assertEquals(List.of(), search(SEMANTIC_SAFETY_INDEX, inScenario("timestamp", after)),
                "a `date` field rounds .123456Z down to .123Z, so nothing is after .123Z");
        assertEquals(List.of("timestamp-nanos"), search(DATE_NANOS_INDEX, after),
                "a `date_nanos` field keeps .123456Z after .123Z, which is CEL's answer");

        // The collapse does not happen under `date_nanos`.
        assertEquals(List.of("timestamp-millis"), search(DATE_NANOS_INDEX, Map.of(
                "term", Map.of("createdAt", Map.of("value", "2024-06-01T00:00:00.123Z")))));

        // The adapter still refuses a nanosecond literal, since it cannot know which mapping the
        // caller uses.
        assertRefused(expression("gt",
                        expression("timestamp", variable(CREATED_AT)),
                        expression("timestamp", string("2024-06-01T00:00:00.123456Z"))),
                TIMESTAMP_OPTIONS, UnsupportedPlanShapeException.class,
                "Sub-millisecond timestamp literals");
    }

    /**
     * The adapter escapes {@code *} and {@code ?} in {@code contains}/{@code endsWith} needles.
     * This checks that Lucene then matches them literally, and that the unescaped needles would
     * match more.
     */
    @Test
    void wildcardMetacharactersInANeedleAreLiteralByTheTimeTheyReachLucene() throws Exception {
        Map<String, Object> containsStar = clauseFor(
                expression("contains", variable(A_STRING), string("a*b")), STRING_OPTIONS);
        Map<String, Object> containsQuestion = clauseFor(
                expression("contains", variable(A_STRING), string("a?b")), STRING_OPTIONS);
        Map<String, Object> endsWithQuestion = clauseFor(
                expression("endsWith", variable(A_STRING), string("?b")), STRING_OPTIONS);
        assertEquals(Map.of("wildcard", Map.of("aString", Map.of("value", "*a\\*b*"))), containsStar);
        assertEquals(Map.of("wildcard", Map.of("aString", Map.of("value", "*a\\?b*"))),
                containsQuestion);
        assertEquals(Map.of("wildcard", Map.of("aString", Map.of("value", "*\\?b"))), endsWithQuestion);

        assertEquals(List.of("wild-star"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("wildcard", containsStar)));
        assertEquals(List.of("wild-question"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("wildcard", containsQuestion)));
        assertEquals(List.of("wild-question"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("wildcard", endsWithQuestion)));

        // Anti-vacuity: unescaped, `?` matches any one character and `*` any run, so each needle
        // matches all four documents.
        assertEquals(List.of("wild-c", "wild-question", "wild-star", "wild-x"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("wildcard",
                        Map.of("wildcard", Map.of("aString", Map.of("value", "a?b"))))));
        assertEquals(List.of("wild-c", "wild-question", "wild-star", "wild-x"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("wildcard",
                        Map.of("wildcard", Map.of("aString", Map.of("value", "a*b"))))));
    }

    // -- regex, against real Lucene -------------------------------------------------------------
    //
    // Hand-built plans that execute the Lucene behaviour behind the adapter's regex handling. The
    // corpus carries the same shapes (`regex/matches/lucene-reserved-characters-in-class`,
    // `regex/matches/dot-excludes-newline`, `regex/matches/top-level-alternation-single-characters`,
    // `regex/matches/grouped-alternation`).

    private static Result translateMatches(String pattern) {
        return ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan(matches(pattern)), STRING_OPTIONS);
    }

    /**
     * The adapter sets {@code flags: NONE} so Lucene's optional operators, such as {@code @}, are
     * matched literally as in RE2.
     */
    @Test
    void luceneOptionalOperatorsAreLiteralsBecauseTheAdapterDisablesThem() throws Exception {
        Result result = translateMatches("^@$");
        Map<String, Object> clause =
                assertInstanceOf(Result.Conditional.class, result).query();
        assertEquals(Map.of("regexp", Map.of("aString", Map.of("value", "@", "flags", "NONE"))),
                clause);

        assertEquals(List.of("regex-at"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("regex", clause)));
    }

    /**
     * Lucene's {@code .} matches a newline and RE2's does not, so the adapter refuses {@code .}.
     */
    @Test
    void luceneDotMatchesANewlineWhichIsWhyTheAdapterRefusesIt() throws Exception {
        assertEquals(List.of("regex-newline"), search(SEMANTIC_SAFETY_INDEX, inScenario("regex",
                Map.of("regexp", Map.of("aString", Map.of("value", "a.b", "flags", "NONE"))))));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> translateMatches("^a.b$"));
        assertTrue(ex.getMessage().contains("supported RE2/Lucene subset"), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }

    /**
     * A Lucene {@code regexp} matches the whole field, so {@code a|b} means "exactly a or exactly
     * b". RE2 reads {@code ^a|b$} as two one-side-anchored alternatives, which also admit
     * {@code ab} and {@code xb}. The adapter refuses a top-level {@code |} and translates
     * {@code ^(a|b)$}, where both agree.
     */
    @Test
    void luceneAlternationIsWholeFieldWhichIsWhyATopLevelBarIsRefused() throws Exception {
        // RE2's reading of the pattern. java.util.regex gives `|` the same lowest precedence.
        Pattern re2 = Pattern.compile("^a|b$");
        assertEquals(List.of("a", "ab", "b", "xb"), List.of("a", "b", "ab", "xb").stream()
                .filter(value -> re2.matcher(value).find()).sorted().toList());

        // Lucene's reading: whole-field, so only the two literals.
        assertEquals(List.of("alt-a", "alt-b"), search(SEMANTIC_SAFETY_INDEX, inScenario("alternation",
                Map.of("regexp", Map.of("aString", Map.of("value", "a|b", "flags", "NONE"))))));

        // The grouped form means the same in both.
        Map<String, Object> grouped =
                assertInstanceOf(Result.Conditional.class, translateMatches("^(a|b)$")).query();
        assertEquals(Map.of("regexp", Map.of("aString", Map.of("value", "(a|b)", "flags", "NONE"))),
                grouped);
        assertEquals(List.of("alt-a", "alt-b"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("alternation", grouped)));

        // The ungrouped form is refused.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> translateMatches("^a|b$"));
        assertTrue(ex.getMessage().contains("top-level alternation"), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }
}
