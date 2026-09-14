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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Elasticsearch glue, executed: what a real server does with the query this adapter returns,
 * and what it does with the documents the adapter's refusals are ABOUT.
 *
 * <p>Nothing here is a claim about translation — {@link ElasticsearchTranslatorTest} pins the Query
 * DSL and {@link ElasticsearchAdversarialConformanceTest} proves the documents against
 * {@code check()}. What this covers is two things neither of those can say:
 *
 * <ul>
 *   <li><strong>Usage.</strong> The emitted clause is a fragment: a caller drops it into
 *       {@code bool.filter} beside its own query. That composition, and the scoring behaviour that
 *       makes {@code filter} the right place for it, is a fact about Elasticsearch.
 *   <li><strong>The mechanisms the corpus reasons name.</strong> Most of
 *       {@code conformance/actions.json}'s {@code elasticsearch-java} entries cite one of three
 *       store facts — an empty array is not indexed, a JSON null is not indexed, and an analyzed
 *       field is compared per token. A harness cannot demonstrate any of them: it only ever sees
 *       the refusal. These execute them against a real server, so the reasons are measured rather
 *       than asserted.
 * </ul>
 *
 * <p><strong>Why this is not a third classification bucket.</strong> #372's binary triage — every
 * surviving shape becomes a corpus action, there is no "unit-test-only" shape — is about SHAPES:
 * what the planner can emit and what the adapter translates. Almost nothing here is a shape. "An
 * explicitly-null field and a missing field are the same document" is a fact about Elasticsearch,
 * and the corpus has no way to ask it — every conformance harness seeds one index and compares id
 * sets. The regex probes ARE shapes, and they are labelled as the corpus gaps they are.
 *
 * <p><strong>Plans from fixtures, expectations from invariants.</strong> The plans come from
 * {@code conformance/wire-fixtures/} wherever a fixture carries the shape, and are hand-built
 * through the adapter where none does — never a hand-written query, because a query somebody
 * typed proves nothing about what the adapter emits. No PDP and no policy file is involved, so
 * this class starts one container rather than two. The documents are seeded locally because they
 * are INPUTS — this suite needs a document whose array is EMPTY and one where it is absent, which
 * no corpus seed is obliged to carry — and the assertions relate what the server returns to the
 * shape of the seeded data rather than to a written-down row set wherever they can.
 *
 * <p><strong>A refusal is asserted the way the corpus pins it.</strong> Where a test ends on "and
 * that is why this corpus action is refused", the refusal carries the substring
 * {@code conformance/actions.json} pins for this adapter and the typed exception the translator
 * raises for that mechanism. A bare "it threw" accepts an unmapped field or an unrelated
 * validation just as readily as the limitation the test is about (cerbos/query-plan-adapters#326).
 */
class ElasticsearchSurfaceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Composition and scoring: four documents, exactly mapped. */
    private static final String SURFACE_INDEX = "surface";

    /** The store facts the corpus reasons cite: empty arrays, JSON nulls, regex, date precision. */
    private static final String SEMANTIC_SAFETY_INDEX = "semantic-safety";

    /** The one index whose string field is NOT `keyword` (cerbos/query-plan-adapters#322). */
    private static final String ANALYZED_MAPPING_INDEX = "analyzed-mapping";

    /** The remedy the timestamp refusal names: the same documents under a `date_nanos` mapping. */
    private static final String DATE_NANOS_INDEX = "date-nanos";

    /**
     * Every action this adapter must refuse, with the message it must refuse it with — the same
     * classification the translator suite and the harness assert, read once.
     */
    private static final Map<String, String> PINNED_REFUSALS =
            Corpus.throwingActions(Corpus.actionsFile(), Corpus.ADAPTER);

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
                // A FLAT array of scalars, the shape the corpus's `tagNames` takes.
                Map.entry("tagNames", Map.of("type", "keyword")),
                Map.entry("owner", Map.of("type", "keyword")),
                Map.entry("aString", Map.of("type", "keyword")),
                Map.entry("createdAt", Map.of(
                        "type", "date", "format", "strict_date_optional_time_nanos")))));

        // `aString` uses the Elasticsearch default multi-field shape: an analyzed `text` parent
        // with an exact `keyword` sub-field, so the same documents can be queried both ways and
        // the two result sets compared directly. Without this index the README's "use keyword"
        // advice is unenforced, and a caller who ignores it gets an over-grant nothing in the
        // repository would notice (cerbos/query-plan-adapters#322).
        es.createIndex(ANALYZED_MAPPING_INDEX, Map.of("properties", Map.of(
                "aString", Map.of(
                        "type", "text",
                        "fields", Map.of("keyword", Map.of("type", "keyword"))))));

        // The other half of the timestamp measurement: `date_nanos` keeps what `date` drops.
        es.createIndex(DATE_NANOS_INDEX, Map.of("properties", Map.of(
                "createdAt", Map.of("type", "date_nanos"))));
    }

    private static void seed() throws Exception {
        // Four documents across the two fields `cs-eq` and the caller query below read, so each
        // filter admits exactly half and their intersection is exactly one.
        index(SURFACE_INDEX, "s1", Map.of("aString", "one", "aBool", true));
        index(SURFACE_INDEX, "s2", Map.of("aString", "one", "aBool", false));
        index(SURFACE_INDEX, "s3", Map.of("aString", "two", "aBool", true));
        index(SURFACE_INDEX, "s4", Map.of("aString", "two", "aBool", false));

        // An EMPTY array, an absent field, and a populated one. CEL tells the first two apart —
        // an empty collection versus a missing-attribute error — and Elasticsearch cannot.
        index(SEMANTIC_SAFETY_INDEX, "empty", Map.of("scenario", "collection", "tags", List.of()));
        index(SEMANTIC_SAFETY_INDEX, "missing", Map.of("scenario", "collection"));
        index(SEMANTIC_SAFETY_INDEX, "present", Map.of(
                "scenario", "collection", "tags", List.of(Map.of("id", "t1", "name", "public"))));

        // The same three shapes for a FLAT array of scalars rather than a nested one.
        index(SEMANTIC_SAFETY_INDEX, "flat-empty",
                Map.of("scenario", "flat", "tagNames", List.of()));
        index(SEMANTIC_SAFETY_INDEX, "flat-missing", Map.of("scenario", "flat"));
        index(SEMANTIC_SAFETY_INDEX, "flat-present",
                Map.of("scenario", "flat", "tagNames", List.of("a")));

        // A null ELEMENT inside a flat array, beside the array without it and the array that is
        // nothing but the null. `List.of` rejects null, hence the explicit lists.
        index(SEMANTIC_SAFETY_INDEX, "null-element",
                Map.of("scenario", "flat-null", "tagNames", Arrays.asList("a", null)));
        index(SEMANTIC_SAFETY_INDEX, "null-only",
                Map.of("scenario", "flat-null", "tagNames", Arrays.asList((Object) null)));
        index(SEMANTIC_SAFETY_INDEX, "no-null",
                Map.of("scenario", "flat-null", "tagNames", List.of("a")));

        // An EXPLICIT null, an absent field, and a value. Same shape, one level down.
        index(SEMANTIC_SAFETY_INDEX, "explicit-null-owner",
                nullable("scenario", "null", "owner", null));
        index(SEMANTIC_SAFETY_INDEX, "missing-owner", Map.of("scenario", "null"));
        index(SEMANTIC_SAFETY_INDEX, "other-owner", Map.of("scenario", "null", "owner", "other"));

        // An indexed EMPTY string beside a missing field and a value: `exists` sees the first.
        index(SEMANTIC_SAFETY_INDEX, "string-empty", Map.of("scenario", "string", "aString", ""));
        index(SEMANTIC_SAFETY_INDEX, "string-missing", Map.of("scenario", "string"));
        index(SEMANTIC_SAFETY_INDEX, "string-value",
                Map.of("scenario", "string", "aString", "value"));

        index(SEMANTIC_SAFETY_INDEX, "regex-at", Map.of("scenario", "regex", "aString", "@"));
        index(SEMANTIC_SAFETY_INDEX, "regex-other", Map.of("scenario", "regex", "aString", "anything"));
        index(SEMANTIC_SAFETY_INDEX, "regex-containing",
                Map.of("scenario", "regex", "aString", "prefix@suffix"));
        index(SEMANTIC_SAFETY_INDEX, "regex-newline", Map.of("scenario", "regex", "aString", "a\nb"));

        // The two alternatives of `a|b`, and two values RE2's `^a|b$` admits that a whole-field
        // alternation does not: one starting with `a`, one ending with `b`.
        index(SEMANTIC_SAFETY_INDEX, "alt-a", Map.of("scenario", "alternation", "aString", "a"));
        index(SEMANTIC_SAFETY_INDEX, "alt-b", Map.of("scenario", "alternation", "aString", "b"));
        index(SEMANTIC_SAFETY_INDEX, "alt-ab", Map.of("scenario", "alternation", "aString", "ab"));
        index(SEMANTIC_SAFETY_INDEX, "alt-xb", Map.of("scenario", "alternation", "aString", "xb"));

        // A literal `*` beside the value an unescaped `*` would also match, and the same for `?`.
        index(SEMANTIC_SAFETY_INDEX, "wild-star", Map.of("scenario", "wildcard", "aString", "a*b"));
        index(SEMANTIC_SAFETY_INDEX, "wild-x", Map.of("scenario", "wildcard", "aString", "axb"));
        index(SEMANTIC_SAFETY_INDEX, "wild-question",
                Map.of("scenario", "wildcard", "aString", "a?b"));
        index(SEMANTIC_SAFETY_INDEX, "wild-c", Map.of("scenario", "wildcard", "aString", "acb"));

        // The same two instants under both date mappings, one millisecond and one 456
        // microseconds later.
        for (String indexName : List.of(SEMANTIC_SAFETY_INDEX, DATE_NANOS_INDEX)) {
            index(indexName, "timestamp-millis",
                    Map.of("scenario", "timestamp", "createdAt", "2024-06-01T00:00:00.123Z"));
            index(indexName, "timestamp-nanos",
                    Map.of("scenario", "timestamp", "createdAt", "2024-06-01T00:00:00.123456Z"));
        }

        // Four values an analyzed mapping and an exact one disagree about. "exact" is the only one
        // a caller means by aString == "one"; the other three are what tokenising hands back too.
        index(ANALYZED_MAPPING_INDEX, "exact", Map.of("aString", "one"));
        index(ANALYZED_MAPPING_INDEX, "phrase", Map.of("aString", "several words including one"));
        index(ANALYZED_MAPPING_INDEX, "casing", Map.of("aString", "ONE"));
        index(ANALYZED_MAPPING_INDEX, "unrelated", Map.of("aString", "oneiric"));
    }

    /** {@link Map#of} rejects a null value, and an explicit JSON null is the point of one seed. */
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

    /** The ids a clause selects, run the way the README tells a caller to run it. */
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

    /** The complement of a clause within one scenario: the documents it does NOT select. */
    private static Map<String, Object> notInScenario(String scenario, Map<String, Object> clause) {
        return Map.of("bool", Map.of(
                "must", List.of(Map.of("term", Map.of("scenario", Map.of("value", scenario)))),
                "must_not", List.of(clause)));
    }

    /**
     * The clause this adapter emits for one corpus action under {@code fieldMap}.
     *
     * <p>The field map is the ONE argument this suite varies — its indices are its own, and the
     * analyzed-mapping tests exist precisely to run one plan through two of them. Every other
     * declaration comes from {@link Corpus#OPTIONS}, so a shape refused there is refused here for
     * the same reason rather than for a locally weaker declaration.
     */
    private static Map<String, Object> clauseFor(String action, Map<String, String> fieldMap) {
        return clauseOf(action, ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                Corpus.planFromWireFixture(action), Corpus.OPTIONS.withFieldMap(fieldMap)));
    }

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
     * The refusal {@code actions.json} pins for a corpus action, asserted as the corpus pins it
     * and as the translator types it. Returned so a test can say more about it.
     */
    private static IllegalArgumentException assertRefusedAsPinned(
            String action, Class<? extends IllegalArgumentException> type) {
        String pinned = PINNED_REFUSALS.get(action);
        assertNotNull(pinned, () -> action + " is not an action actions.json says this adapter"
                + " refuses, so there is no pinned message to assert");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Corpus.translate(action), action);
        assertTrue(ex.getMessage().contains(pinned), () -> action
                + " was refused for a reason actions.json does not declare: " + ex.getMessage());
        assertInstanceOf(type, ex, action);
        return ex;
    }

    /** A hand-built refusal, typed and with its message checked. */
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
    // Used only where no wire fixture carries the shape a store fact needs — a needle holding a
    // `*`, a `size()` over a flat array, a `>` against a timestamp. Each is still run THROUGH the
    // adapter: the clause executed is the one it emits, never one typed here.

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

    /** `tagNames` declared as the flat collection it is. */
    private static final Options FLAT_ARRAY_OPTIONS =
            Corpus.OPTIONS.withFieldMap(Map.of(TAG_NAMES, "tagNames"))
                    .withCollectionFields(Set.of("tagNames"));

    private static final Options TIMESTAMP_OPTIONS =
            Corpus.OPTIONS.withFieldMap(Map.of(CREATED_AT, "createdAt"));

    /** `size(x) > 0`, the one count comparison the adapter lowers to a presence check. */
    private static Operand nonEmpty(String variable) {
        return expression("gt", expression("size", variable(variable)), number(0));
    }

    private static Operand matches(String pattern) {
        return expression("matches", variable(A_STRING), string(pattern));
    }

    // -- usage --------------------------------------------------------------------------------

    /**
     * The README tells a caller to put the emitted clause in {@code bool.filter}, and this is why:
     * filter context does not score, so an authorization filter cannot perturb the ranking of the
     * caller's own query. Nothing in the translator can assert that — it is Elasticsearch's rule.
     */
    @Test
    void aClauseInFilterContextScoresNothing() throws Exception {
        List<Map<String, Object>> hits = hits(SURFACE_INDEX, Map.of("query", Map.of(
                "bool", Map.of("filter",
                        List.of(clauseFor("cs-eq", Map.of(A_STRING, "aString")))))));

        assertFalse(hits.isEmpty(), "the filter matched nothing, so there is no score to check");
        for (Map<String, Object> hit : hits) {
            assertEquals(0.0, ((Number) hit.get("_score")).doubleValue(),
                    "filter context must not contribute to the score");
        }
    }

    /**
     * The clause composes with the caller's own query rather than replacing it. Asserted as set
     * intersection rather than against a written-down id list, with each side required to exclude
     * a document the other admits — a composition that dropped either operand would show up as the
     * intersection being wrong rather than as an id list needing maintenance.
     */
    @Test
    void aClauseComposesWithACallerQueryAsAnIntersection() throws Exception {
        Map<String, Object> cerbos = clauseFor("cs-eq", Map.of(A_STRING, "aString"));
        Map<String, Object> caller = Map.of("term", Map.of("aBool", Map.of("value", true)));

        Set<String> fromCerbos = Set.copyOf(search(SURFACE_INDEX, cerbos));
        Set<String> fromCaller = Set.copyOf(search(SURFACE_INDEX, caller));
        Set<String> intersection = new LinkedHashSet<>(fromCerbos);
        intersection.retainAll(fromCaller);

        // Non-degenerate: each side must exclude a document the other admits, or a composition
        // that dropped either operand would still produce the intersection.
        assertFalse(intersection.isEmpty(), "the two filters must overlap");
        assertTrue(intersection.size() < fromCerbos.size(), "the caller query must narrow");
        assertTrue(intersection.size() < fromCaller.size(), "the adapter clause must narrow");

        // The shape the README documents: the caller's query scored in `must`, the authorization
        // clause unscored in `filter`.
        assertEquals(intersection, Set.copyOf(hits(SURFACE_INDEX, Map.of("query", Map.of(
                        "bool", Map.of("must", List.of(caller), "filter", List.of(cerbos)))))
                .stream().map(hit -> (String) hit.get("_id")).toList()));
        // ...and both in `filter`, which is what a caller with no scoring query writes.
        assertEquals(intersection, Set.copyOf(hits(SURFACE_INDEX, Map.of("query", Map.of(
                        "bool", Map.of("filter", List.of(cerbos, caller)))))
                .stream().map(hit -> (String) hit.get("_id")).toList()));
    }

    // -- the mechanisms the corpus reasons name -------------------------------------------------

    /**
     * Elasticsearch does not index an empty array, so a document whose collection is {@code []} and
     * one with no collection at all are indistinguishable to every query the DSL can express. CEL
     * tells them apart — an empty collection is a value, a missing attribute is an evaluation error
     * — which is why the polarities that would read a missing collection as an allow are refused.
     *
     * <p>This is the store fact behind more of {@code actions.json}'s {@code elasticsearch-java}
     * reasons than any other, and the harness cannot show it: it only ever sees the refusal.
     */
    @Test
    void anEmptyArrayAndAMissingArrayAreTheSameDocument() throws Exception {
        Map<String, Object> hasAnElement = Map.of("nested", Map.of(
                "path", "tags", "query", Map.of("match_all", Map.of())));

        assertEquals(List.of("present"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("collection", hasAnElement)));
        // The complement lumps the empty document in with the missing one.
        assertEquals(List.of("empty", "missing"),
                search(SEMANTIC_SAFETY_INDEX, notInScenario("collection", hasAnElement)));
        // And there is no second query to reach for: `exists` over a `nested` path matches NO
        // document at all, because a nested element is indexed as a separate Lucene document and
        // the parent carries no field of that name. So its complement is every document, which
        // separates nothing in the other direction.
        assertEquals(List.of(), search(SEMANTIC_SAFETY_INDEX,
                inScenario("collection", Map.of("exists", Map.of("field", "tags")))));
        assertEquals(List.of("empty", "missing", "present"), search(SEMANTIC_SAFETY_INDEX,
                notInScenario("collection", Map.of("exists", Map.of("field", "tags")))));

        // ...which is why these two corpus actions are refused rather than answered — each with
        // the message the corpus pins and as the shape refusal it is, not a mapping gap.
        assertRefusedAsPinned("all-on-empty", UnsupportedPlanShapeException.class);
        assertRefusedAsPinned("not-exists", UnsupportedPlanShapeException.class);
    }

    /**
     * The same fact for a FLAT array of scalars, where the adapter's emitted presence check is
     * {@code exists} rather than a {@code nested} query: an empty {@code []} and an absent field
     * are the same document under it, so the positive direction translates and its negation —
     * "the collection is empty", which CEL answers true for {@code []} and with an error for a
     * missing attribute — is refused.
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
     * A null ELEMENT in a flat keyword array is not indexed, so no query this adapter emits can
     * select a document by it: {@code ["a", null]} answers every emitted query exactly as
     * {@code ["a"]} does, and {@code [null]} is a document {@code exists} does not see at all.
     * CEL sees the element — {@code null in R.attr.tagNames} is true of the first two — which is
     * the store fact behind the null-membership refusals.
     */
    @Test
    void aNullElementInAFlatArrayIsSelectableByNoEmittedQuery() throws Exception {
        Map<String, Object> present = clauseFor(nonEmpty(TAG_NAMES), FLAT_ARRAY_OPTIONS);
        Map<String, Object> holdsA = clauseFor(
                expression("eq", variable(TAG_NAMES), string("a")), FLAT_ARRAY_OPTIONS);
        assertEquals(Map.of("term", Map.of("tagNames", Map.of("value", "a"))), holdsA);

        // `[null]` is invisible to `exists`, and `["a", null]` is `["a"]` to both queries.
        assertEquals(List.of("no-null", "null-element"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("flat-null", present)));
        assertEquals(List.of("no-null", "null-element"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("flat-null", holdsA)));
        assertEquals(List.of("null-only"),
                search(SEMANTIC_SAFETY_INDEX, notInScenario("flat-null", present)));

        // ...so the corpus action that asks for the null element is refused rather than answered.
        assertRefusedAsPinned("in-null-elem-rel", UnsupportedPlanShapeException.class);
        assertRefused(expression("in", nullValue(), variable(TAG_NAMES)), FLAT_ARRAY_OPTIONS,
                UnsupportedPlanShapeException.class,
                "null membership in a document array requires an explicit null-value mapping");
    }

    /**
     * The same limitation one level down: Elasticsearch does not index a JSON null, so an
     * explicitly-null field and an absent field are the same document. Under the explicit-null
     * convention CEL holds a null VALUE and answers definitely, which is what makes the two
     * disagree (cerbos/query-plan-adapters#302, #308).
     */
    @Test
    void anExplicitNullAndAMissingFieldAreTheSameDocument() throws Exception {
        assertEquals(List.of("explicit-null-owner", "missing-owner"), search(SEMANTIC_SAFETY_INDEX,
                notInScenario("null", Map.of("exists", Map.of("field", "owner")))));

        // So the null-SELECTING direction is refused...
        assertRefusedAsPinned("null-eq", UnsupportedPlanShapeException.class);
        // ...and the presence-selecting one translates, to the only query that is definite here.
        assertEquals(List.of("other-owner"), search(SEMANTIC_SAFETY_INDEX, inScenario("null",
                clauseFor("null-ne", Map.of("request.resource.attr.owner", "owner")))));
    }

    /**
     * {@code exists} matches an indexed empty string — {@code ""} is a term on a {@code keyword}
     * field like any other — so lowering {@code size(aString) > 0} to {@code exists} would return
     * a document CEL evaluates to {@code size("") > 0}, which is false. The adapter is handed a
     * plan, never a mapping, and cannot tell that {@code size()} from one over an array; the
     * caller's {@code collectionFields} declaration is what tells it, and a field declared in
     * neither that nor {@code nestedPaths} is refused. This is what a wrong declaration costs,
     * measured: the emitted clause under a caller who declares {@code aString} a collection.
     */
    @Test
    void existsMatchesAnIndexedEmptyStringWhichIsWhySizeOverAnUndeclaredFieldIsRefused()
            throws Exception {
        Map<String, Object> presence = clauseFor(nonEmpty(A_STRING),
                STRING_OPTIONS.withCollectionFields(Set.of("aString")));
        assertEquals(Map.of("exists", Map.of("field", "aString")), presence);

        // The empty string comes back beside the value; only the missing field is excluded.
        assertEquals(List.of("string-empty", "string-value"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("string", presence)));

        // ...which is why, undeclared, the same size() is refused before its threshold is read —
        // the corpus's three string-length actions all land there.
        assertRefusedAsPinned("string-size", UnsupportedPlanShapeException.class);
        assertRefused(nonEmpty(A_STRING), STRING_OPTIONS, UnsupportedPlanShapeException.class,
                "size() over a field not declared as a collection");
    }

    /**
     * The mapping hazard the adapter cannot reject, measured. It is handed a plan, never an index,
     * so it has no way to tell an exactly-compared field from an analyzed one — the plan looks
     * identical either way. Pointing {@code fieldMap} at a {@code text} field silently widens every
     * string comparison, and both extra documents here are ones {@code check()} denies
     * (cerbos/query-plan-adapters#322).
     */
    @Test
    void anAnalyzedMappingWidensEqualityAndTheKeywordSubFieldRestoresIt() throws Exception {
        // "ONE" tokenises to [one] because the standard analyzer lowercases; "one of several
        // words" matches on one of its tokens. Neither has an aString the policy's == "one" is
        // true of.
        assertEquals(List.of("casing", "exact", "phrase"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("cs-eq", Map.of(A_STRING, "aString"))));

        // The documented remedy: point the field map at the exact sub-field, not at the parent.
        assertEquals(List.of("exact"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("cs-eq", Map.of(A_STRING, "aString.keyword"))));
    }

    /** The same widening through {@code prefix}, which is per-token on a {@code text} field. */
    @Test
    void anAnalyzedMappingWidensStartsWith() throws Exception {
        assertEquals(List.of("casing", "exact", "phrase", "unrelated"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("cs-startswith", Map.of(A_STRING, "aString"))));
        assertEquals(List.of("exact", "unrelated"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("cs-startswith", Map.of(A_STRING, "aString.keyword"))));
    }

    /**
     * An ordinary Elasticsearch {@code date} field stores milliseconds, so a nanosecond-precision
     * value collapses into the millisecond next to it and a {@code term} query cannot separate the
     * two documents. That is the store fact {@code ts-window} and {@code ts-vf} are refused for:
     * the planner folds {@code now() - duration("24h")} to a nanosecond literal.
     */
    @Test
    void anOrdinaryDateMappingCollapsesSubMillisecondPrecision() throws Exception {
        assertEquals(List.of("timestamp-millis", "timestamp-nanos"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("timestamp", Map.of(
                        "term", Map.of("createdAt", Map.of("value", "2024-06-01T00:00:00.123Z"))))));

        IllegalArgumentException ex =
                assertRefusedAsPinned("ts-window", UnsupportedPlanShapeException.class);
        assertTrue(ex.getMessage().contains("Sub-millisecond timestamp literals"), ex.getMessage());
    }

    /**
     * The remedy that refusal names, executed: under {@code date_nanos} the same two instants stay
     * two, and the adapter's own emitted clause — a millisecond literal, which it does translate —
     * gives CEL's answer on one mapping and not the other. {@code createdAt > .123Z} is true of
     * the document at {@code .123456Z}; a {@code date} field has already rounded that document
     * down to {@code .123Z} and returns nothing.
     *
     * <p>{@code >} rather than the corpus's {@code <} on purpose: truncating to the millisecond
     * grid preserves {@code t < X} for any {@code X} on the grid, so the fixture direction would
     * agree under both mappings and show nothing. {@code >} and {@code <=} are the two that
     * diverge, and the second of them over-grants.
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

        // The collapse itself, under the term query the first test runs on the `date` mapping.
        assertEquals(List.of("timestamp-millis"), search(DATE_NANOS_INDEX, Map.of(
                "term", Map.of("createdAt", Map.of("value", "2024-06-01T00:00:00.123Z")))));

        // The adapter still refuses the nanosecond literal itself: it cannot know which mapping
        // the caller chose, and only one of the two answers the way CEL does.
        assertRefused(expression("gt",
                        expression("timestamp", variable(CREATED_AT)),
                        expression("timestamp", string("2024-06-01T00:00:00.123456Z"))),
                TIMESTAMP_OPTIONS, UnsupportedPlanShapeException.class,
                "Sub-millisecond timestamp literals");
    }

    /**
     * A {@code wildcard} query reads {@code *} and {@code ?} as operators, so the adapter escapes
     * them in a {@code contains} or {@code endsWith} needle — this repository's founding bug class
     * (#258/#259) in its Elasticsearch spelling. {@link ElasticsearchTranslatorTest} pins that the
     * escape is EMITTED; this is the escape REACHING Lucene: the emitted clause selects the
     * document holding the literal metacharacter and not the one the unescaped pattern would also
     * match, which the same query unescaped is shown to do.
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

        // Anti-vacuity: unescaped, the same needles ARE patterns. `?` is any one character —
        // a `*`, an `x`, a `?` or a `c` alike — and `*` any run, so each admits all four.
        assertEquals(List.of("wild-c", "wild-question", "wild-star", "wild-x"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("wildcard",
                        Map.of("wildcard", Map.of("aString", Map.of("value", "a?b"))))));
        assertEquals(List.of("wild-c", "wild-question", "wild-star", "wild-x"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("wildcard",
                        Map.of("wildcard", Map.of("aString", Map.of("value", "a*b"))))));
    }

    // -- regex, against real Lucene -------------------------------------------------------------
    //
    // The plans below are HAND-BUILT because the corpus carries exactly one `matches()` action
    // today (`p-matches`, the `^h` literal prefix, which lowers to a `prefix` query and never
    // reaches Lucene's regex engine). Every one of them is a shape a policy can express, so by
    // #372's binary triage they belong in the corpus: they are corpus gaps wearing a unit test,
    // named as such rather than left to read like coverage, and filed as
    // cerbos/query-plan-adapters#414 — NOT covered by #387 or #388, whose actions are enumerated
    // and landed.

    private static Result translateMatches(String pattern) {
        return ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                plan(matches(pattern)), STRING_OPTIONS);
    }

    /**
     * Lucene's {@code regexp} query supports optional operators that RE2 does not have, and the
     * adapter disables them with {@code flags: NONE} so a character like {@code @} is matched
     * literally. Only a real server can confirm the flag is doing that.
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
     * Lucene's {@code .} matches a newline and RE2's does not, so a pattern the adapter accepted
     * would select a document CEL's {@code matches()} rejects. The adapter refuses the pattern
     * instead — and this is the document that makes the refusal necessary rather than cautious.
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
     * A Lucene {@code regexp} under {@code flags: NONE} matches the WHOLE field, so {@code a|b}
     * is "the field is {@code a} or the field is {@code b}". RE2 — and CEL's {@code matches()} —
     * parses {@code ^a|b$} as two alternatives each anchored on one side, admitting {@code ab} and
     * {@code xb} as well. The two agree only once the alternation is parenthesised under both
     * anchors, which is why the adapter refuses a top-level {@code |} and translates
     * {@code ^(a|b)$}: this is the emitted clause returning CEL's rows, and the hand-written
     * whole-field alternation beside it showing what the unparenthesised pattern would have
     * meant to Lucene.
     */
    @Test
    void luceneAlternationIsWholeFieldWhichIsWhyATopLevelBarIsRefused() throws Exception {
        // RE2's reading of the unparenthesised pattern, stated as the row set it would allow.
        // java.util.regex gives `|` the same lowest precedence RE2 does, so it stands in here.
        Pattern re2 = Pattern.compile("^a|b$");
        assertEquals(List.of("a", "ab", "b", "xb"), List.of("a", "b", "ab", "xb").stream()
                .filter(value -> re2.matcher(value).find()).sorted().toList());

        // Lucene's reading of the same pattern, executed: whole-field, so only the two literals.
        assertEquals(List.of("alt-a", "alt-b"), search(SEMANTIC_SAFETY_INDEX, inScenario("alternation",
                Map.of("regexp", Map.of("aString", Map.of("value", "a|b", "flags", "NONE"))))));

        // The form the adapter translates means the same thing in both languages.
        Map<String, Object> grouped =
                assertInstanceOf(Result.Conditional.class, translateMatches("^(a|b)$")).query();
        assertEquals(Map.of("regexp", Map.of("aString", Map.of("value", "(a|b)", "flags", "NONE"))),
                grouped);
        assertEquals(List.of("alt-a", "alt-b"),
                search(SEMANTIC_SAFETY_INDEX, inScenario("alternation", grouped)));

        // ...and the form that does not is refused, by name.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> translateMatches("^a|b$"));
        assertTrue(ex.getMessage().contains("top-level alternation"), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }
}
