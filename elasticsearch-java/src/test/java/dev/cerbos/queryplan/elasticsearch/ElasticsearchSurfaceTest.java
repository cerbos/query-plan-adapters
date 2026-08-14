package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.protobuf.Value;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
 * sets. The two regex probes ARE shapes, and they are labelled as the corpus gaps they are.
 *
 * <p><strong>Plans from fixtures, expectations from invariants.</strong> The plans come from
 * {@code conformance/wire-fixtures/} like every other suite here, so no PDP and no policy file is
 * involved and this class starts one container rather than two. The documents are seeded locally
 * because they are INPUTS — this suite needs a document whose array is EMPTY and one where it is
 * absent, which no corpus seed is obliged to carry — and the assertions relate what the server
 * returns to the shape of the seeded data rather than to a written-down row set wherever they can.
 */
class ElasticsearchSurfaceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Composition and scoring: four documents, exactly mapped. */
    private static final String SURFACE_INDEX = "surface";

    /** The store facts the corpus reasons cite: empty arrays, JSON nulls, regex, date precision. */
    private static final String SEMANTIC_SAFETY_INDEX = "semantic-safety";

    /** The one index whose string field is NOT `keyword` (cerbos/query-plan-adapters#322). */
    private static final String ANALYZED_MAPPING_INDEX = "analyzed-mapping";

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
        for (String index : List.of(SURFACE_INDEX, SEMANTIC_SAFETY_INDEX, ANALYZED_MAPPING_INDEX)) {
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

        // An EXPLICIT null, an absent field, and a value. Same shape, one level down.
        index(SEMANTIC_SAFETY_INDEX, "explicit-null-owner",
                nullable("scenario", "null", "owner", null));
        index(SEMANTIC_SAFETY_INDEX, "missing-owner", Map.of("scenario", "null"));
        index(SEMANTIC_SAFETY_INDEX, "other-owner", Map.of("scenario", "null", "owner", "other"));

        index(SEMANTIC_SAFETY_INDEX, "regex-at", Map.of("scenario", "regex", "aString", "@"));
        index(SEMANTIC_SAFETY_INDEX, "regex-other", Map.of("scenario", "regex", "aString", "anything"));
        index(SEMANTIC_SAFETY_INDEX, "regex-containing",
                Map.of("scenario", "regex", "aString", "prefix@suffix"));
        index(SEMANTIC_SAFETY_INDEX, "regex-newline", Map.of("scenario", "regex", "aString", "a\nb"));

        index(SEMANTIC_SAFETY_INDEX, "timestamp-millis",
                Map.of("scenario", "timestamp", "createdAt", "2024-06-01T00:00:00.123Z"));
        index(SEMANTIC_SAFETY_INDEX, "timestamp-nanos",
                Map.of("scenario", "timestamp", "createdAt", "2024-06-01T00:00:00.123456Z"));

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

    /**
     * The clause this adapter emits for one corpus action under {@code fieldMap}.
     *
     * <p>The field map is the ONE argument this suite varies — its indices are its own, and the
     * analyzed-mapping tests exist precisely to run one plan through two of them. The other two
     * arguments come from {@link Corpus}, so a shape refused there is refused here for the same
     * reason rather than for a locally weaker declaration.
     */
    private static Map<String, Object> clauseFor(String action, Map<String, String> fieldMap) {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                Corpus.planFromWireFixture(action), fieldMap, Map.of(),
                Corpus.NESTED_PATHS, Corpus.EXPLICIT_NULL_ATTRIBUTES);
        return assertInstanceOf(Result.Conditional.class, result,
                action + " must translate, or there is no query to execute").query();
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
                        List.of(clauseFor("cs-eq", Map.of("request.resource.attr.aString", "aString")))))));

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
        Map<String, Object> cerbos =
                clauseFor("cs-eq", Map.of("request.resource.attr.aString", "aString"));
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
        assertEquals(List.of("empty", "missing"), search(SEMANTIC_SAFETY_INDEX, Map.of(
                "bool", Map.of(
                        "must", List.of(Map.of("term", Map.of("scenario", Map.of("value", "collection")))),
                        "must_not", List.of(hasAnElement)))));
        // And there is no second query to reach for: `exists` over a `nested` path matches NO
        // document at all, because a nested element is indexed as a separate Lucene document and
        // the parent carries no field of that name. So its complement is every document, which
        // separates nothing in the other direction.
        assertEquals(List.of(), search(SEMANTIC_SAFETY_INDEX,
                inScenario("collection", Map.of("exists", Map.of("field", "tags")))));
        assertEquals(List.of("empty", "missing", "present"), search(SEMANTIC_SAFETY_INDEX, Map.of(
                "bool", Map.of(
                        "must", List.of(Map.of("term", Map.of("scenario", Map.of("value", "collection")))),
                        "must_not", List.of(Map.of("exists", Map.of("field", "tags")))))));

        // ...which is why these two corpus actions are refused rather than answered.
        for (String action : List.of("all-on-empty", "not-exists")) {
            assertThrows(IllegalArgumentException.class, () -> Corpus.translate(action), action);
        }
    }

    /**
     * The same limitation one level down: Elasticsearch does not index a JSON null, so an
     * explicitly-null field and an absent field are the same document. Under the explicit-null
     * convention CEL holds a null VALUE and answers definitely, which is what makes the two
     * disagree (cerbos/query-plan-adapters#302, #308).
     */
    @Test
    void anExplicitNullAndAMissingFieldAreTheSameDocument() throws Exception {
        assertEquals(List.of("explicit-null-owner", "missing-owner"),
                search(SEMANTIC_SAFETY_INDEX, Map.of("bool", Map.of(
                        "must", List.of(Map.of("term", Map.of("scenario", Map.of("value", "null")))),
                        "must_not", List.of(Map.of("exists", Map.of("field", "owner")))))));

        // So the null-SELECTING direction is refused...
        assertThrows(IllegalArgumentException.class, () -> Corpus.translate("null-eq"));
        // ...and the presence-selecting one translates, to the only query that is definite here.
        assertEquals(List.of("other-owner"), search(SEMANTIC_SAFETY_INDEX, inScenario("null",
                clauseFor("null-ne", Map.of("request.resource.attr.owner", "owner")))));
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
                clauseFor("cs-eq", Map.of("request.resource.attr.aString", "aString"))));

        // The documented remedy: point the field map at the exact sub-field, not at the parent.
        assertEquals(List.of("exact"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("cs-eq", Map.of("request.resource.attr.aString", "aString.keyword"))));
    }

    /** The same widening through {@code prefix}, which is per-token on a {@code text} field. */
    @Test
    void anAnalyzedMappingWidensStartsWith() throws Exception {
        assertEquals(List.of("casing", "exact", "phrase", "unrelated"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("cs-startswith", Map.of("request.resource.attr.aString", "aString"))));
        assertEquals(List.of("exact", "unrelated"), search(ANALYZED_MAPPING_INDEX,
                clauseFor("cs-startswith", Map.of("request.resource.attr.aString", "aString.keyword"))));
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

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Corpus.translate("ts-window"));
        assertTrue(ex.getMessage().contains("Sub-millisecond timestamp literals"), ex.getMessage());
    }

    // -- regex, against real Lucene -------------------------------------------------------------
    //
    // The two plans below are HAND-BUILT, and they are the only ones in this class that are. Both
    // are shapes a policy can express, so by #372's binary triage they belong in the corpus — but
    // the corpus carries exactly one `matches()` action today (`p-matches`, the `^h` literal
    // prefix, which lowers to a `prefix` query and never reaches Lucene's regex engine). They are
    // corpus gaps wearing a unit test, named as such rather than left to read like coverage, and
    // filed as cerbos/query-plan-adapters#414 — NOT covered by #387 or #388, whose actions are
    // enumerated and landed.

    private static PlanResourcesResponse matches(String pattern) {
        Operand condition = Operand.newBuilder().setExpression(Expression.newBuilder()
                .setOperator("matches")
                .addOperands(Operand.newBuilder().setVariable("request.resource.attr.aString"))
                .addOperands(Operand.newBuilder()
                        .setValue(Value.newBuilder().setStringValue(pattern)))).build();
        return PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition))
                .build();
    }

    private static Result translateMatches(String pattern) {
        return ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                matches(pattern), Map.of("request.resource.attr.aString", "aString"),
                Map.of(), Corpus.NESTED_PATHS, Corpus.EXPLICIT_NULL_ATTRIBUTES);
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
    }
}
