/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Translator unit test: what this adapter can be asked without a store. Which rows a translated
 * case returns is the conformance harness's job ({@link ElasticsearchAdversarialConformanceTest});
 * this suite pins the caller-supplied options the corpus cannot vary and the rules every emitted
 * query must follow. Plans come from {@code conformance/golden/<current PDP>/}. Needs no Docker,
 * PDP or Elasticsearch.
 */
class ElasticsearchTranslatorTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * The query emitted for every current-PDP case this adapter translates to a condition, keyed
     * by case id, as the JSON a caller sends. Refused cases are the harness's to assert.
     */
    private static final Map<String, Map<String, Object>> CONDITIONAL = new TreeMap<>();

    private static final Map<String, JsonNode> EMITTED = new TreeMap<>();

    static {
        for (Corpus.Golden golden : Corpus.goldens(Corpus.CURRENT_TAG)) {
            Result result;
            try {
                result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(golden.plan(), Corpus.OPTIONS);
            } catch (UnsupportedPlanShapeException refused) {
                continue;
            }
            if (result instanceof Result.Conditional conditional) {
                CONDITIONAL.put(golden.id(), conditional.query());
                EMITTED.put(golden.id(), JSON.valueToTree(conditional.query()));
            }
        }
    }

    private static final String NULL_ON_MISSING_ATTRIBUTE = "null/equals/null-literal-on-missing-attribute";

    /**
     * {@code R.attr.aOptionalString == null} plans to the same node whichever null convention the
     * attribute follows, so other adapters take an option choosing one. Elasticsearch does not
     * index an explicit null, so this adapter refuses the probe whether or not the attribute is
     * declared explicit-null: the corpus translates it one way only.
     */
    @Test
    void theNullOnMissingAttributeProbeIsRefusedUnderEitherNullConvention() {
        assertThrows(UnsupportedPlanShapeException.class,
                () -> Corpus.translate(NULL_ON_MISSING_ATTRIBUTE));
        assertThrows(UnsupportedPlanShapeException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        Corpus.plan(NULL_ON_MISSING_ATTRIBUTE),
                        Corpus.OPTIONS.withExplicitNullAttributes(
                                Set.of("request.resource.attr.aOptionalString"))));
    }

    /**
     * The relative-window cases compare against a folded {@code now()} that the goldens store as a
     * placeholder. At the PDP's nanosecond precision they are refused, since an Elasticsearch
     * {@code date} field holds milliseconds; at millisecond precision they translate. This pins
     * the refusal to the precision, not to the shape.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"timestamp/less-than/relative-window",
            "timestamp/greater-than/relative-window-value-first"})
    void theRelativeWindowCasesAreRefusedForTheirPrecision(String caseId) {
        UnsupportedPlanShapeException ex = assertThrows(UnsupportedPlanShapeException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        Corpus.plan(caseId, "2026-08-11T09:13:39.123456789Z"), Corpus.OPTIONS));
        assertTrue(ex.getMessage().contains("Sub-millisecond"), ex.getMessage());

        assertInstanceOf(Result.Conditional.class,
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        Corpus.plan(caseId, "2026-08-11T09:13:39.123Z"), Corpus.OPTIONS),
                caseId + " no longer translates at millisecond precision, so the nanoseconds are"
                        + " not what refuses it");
    }

    /**
     * An unmapped field is a caller mistake, not a shape the Query DSL cannot express, so it
     * raises {@link UnmappedAttributeException} and never passes for an {@code unsupported} case.
     */
    @Test
    void anUnmappedFieldIsNotARefusal() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        Corpus.plan("string/equals/case-sensitive"),
                        Corpus.OPTIONS.withFieldMap(Map.of())));
        assertInstanceOf(UnmappedAttributeException.class, ex);
        assertFalse(ex instanceof UnsupportedPlanShapeException);
    }

    /** This file and the test helpers it uses; none may reach a PDP or a container. */
    private static final List<String> OFFLINE_SOURCES =
            List.of("ElasticsearchTranslatorTest", "Corpus");

    /** The suite stays offline: neither it nor {@link Corpus} imports a PDP or container client. */
    @Test
    void thisSuiteReachesNoPdpAndNoContainer() {
        List<String> forbidden = List.of("org.testcontainers.", "dev.cerbos.sdk.CerbosBlockingClient", "java.net.http.");
        // Check import lines, not the whole file, since these names appear here as literals.
        for (String source : OFFLINE_SOURCES) {
            assertEquals(List.of(), importsOf(source).stream()
                            .filter(imported -> forbidden.stream().anyMatch(imported::startsWith))
                            .toList(),
                    source + " reaches a PDP or a container, so this suite is no longer offline");
        }
        // Every sibling this file uses in code must be in OFFLINE_SOURCES.
        assertEquals(Set.of("Corpus"), siblingsReferencedBy("ElasticsearchTranslatorTest"),
                "this suite reaches a test helper OFFLINE_SOURCES does not scan");
        assertEquals(Set.of(), siblingsReferencedBy("Corpus"),
                "Corpus reaches a test helper OFFLINE_SOURCES does not scan");
        // The container-backed siblings do import these, so the prefixes are real.
        List<String> siblings = Stream.of("ElasticsearchAdversarialConformanceTest",
                        "ElasticsearchSurfaceTest", "TestElasticsearch")
                .flatMap(name -> importsOf(name).stream())
                .toList();
        for (String prefix : List.of("org.testcontainers.", "java.net.http.")) {
            assertTrue(siblings.stream().anyMatch(imported -> imported.startsWith(prefix)), prefix);
        }
    }

    private static final Path TEST_SOURCES = Path.of(System.getProperty("user.dir"), "src",
            "test", "java", "dev", "cerbos", "queryplan", "elasticsearch");

    /** The fully-qualified names one test source imports. */
    private static List<String> importsOf(String simpleName) {
        try (Stream<String> lines = Files.lines(TEST_SOURCES.resolve(simpleName + ".java"))) {
            return lines.map(String::strip)
                    .filter(line -> line.startsWith("import ") && line.endsWith(";"))
                    .map(line -> line.substring("import ".length(), line.length() - 1)
                            .replace("static ", "").strip())
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Sibling test classes this source names in code. Comments and string literals are stripped
     * first, since this file names its siblings in both without using them.
     */
    private static Set<String> siblingsReferencedBy(String simpleName) {
        String code;
        try (Stream<Path> files = Files.list(TEST_SOURCES)) {
            code = Files.readString(TEST_SOURCES.resolve(simpleName + ".java"))
                    .replaceAll("(?s)/\\*.*?\\*/", "")
                    .replaceAll("//[^\\n]*", "")
                    .replaceAll("\"(?:\\\\.|[^\"\\\\])*\"", "\"\"");
            Set<String> referenced = new TreeSet<>();
            for (Path file : files.toList()) {
                String sibling = file.getFileName().toString().replace(".java", "");
                if (!sibling.equals(simpleName)
                        && Pattern.compile("\\b" + Pattern.quote(sibling) + "\\b").matcher(code).find()) {
                    referenced.add(sibling);
                }
            }
            return referenced;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Rules over the query emitted for every case this adapter translates, each with an
     * anti-vacuity check. The conformance harness compares rows, so it cannot see these: a
     * library type in the query, an unescaped wildcard that happens to match the same seeds, or
     * a misspelled field on a case whose allowed set is empty.
     */
    @Nested
    class WhatTheEmittedQueryContains {

        /** One leaf clause of an emitted query: {@code {"term": {"aBool": {"value": true}}}}. */
        record Comparison(String action, String clause, String field, JsonNode operand) {}

        /** One {@code nested} query: the action that emitted it, its path, and its inner query. */
        record NestedScope(String action, String path, JsonNode query) {}

        private List<Comparison> comparisons;
        private List<NestedScope> nested;

        private List<Comparison> comparisons() {
            if (comparisons == null) {
                comparisons = new ArrayList<>();
                nested = new ArrayList<>();
                EMITTED.forEach((action, query) -> walk(action, query, comparisons, nested));
            }
            return comparisons;
        }

        /**
         * Splits an emitted query into leaf clauses and nested scopes. An unknown clause fails, so
         * a new emission shape cannot slip past the rules.
         */
        private void walk(String action, JsonNode query, List<Comparison> leaves,
                          List<NestedScope> scopes) {
            for (Map.Entry<String, JsonNode> clause : query.properties()) {
                String name = clause.getKey();
                JsonNode body = clause.getValue();
                switch (name) {
                    case "bool" -> body.properties().stream()
                            .filter(occur -> !"minimum_should_match".equals(occur.getKey()))
                            .forEach(occur -> occur.getValue()
                                    .forEach(child -> walk(action, child, leaves, scopes)));
                    case "nested" -> {
                        scopes.add(new NestedScope(
                                action, body.get("path").asText(), body.get("query")));
                        walk(action, body.get("query"), leaves, scopes);
                    }
                    case "match_all", "match_none" -> { }
                    case "exists" -> leaves.add(new Comparison(
                            action, name, body.get("field").asText(), null));
                    case "term", "terms", "range", "prefix", "wildcard", "regexp" ->
                            body.properties().forEach(field -> leaves.add(new Comparison(
                                    action, name, field.getKey(), field.getValue())));
                    default -> throw new AssertionError(
                            action + " emits a clause this rule does not recognise: " + name);
                }
            }
        }

        /**
         * The adapter never sees the index, so a misspelled field would match nothing and silently
         * deny. The harness cannot catch that when the oracle also allows nothing.
         */
        @Test
        void everyFieldNamedIsOneTheCorpusMapsOrANestedScopeOfOne() {
            Set<String> declared = Set.copyOf(Corpus.FIELD_MAP.values());
            List<String> stray = comparisons().stream()
                    .map(Comparison::field)
                    .filter(field -> !declared.contains(field)
                            && Corpus.NESTED_PATHS.stream()
                                    .noneMatch(path -> field.startsWith(path + ".")))
                    .distinct()
                    .sorted()
                    .toList();

            assertEquals(List.of(), stray);
            // Anti-vacuity: comparisons exist, including some inside a nested scope.
            assertFalse(comparisons().isEmpty());
            assertTrue(comparisons().stream().anyMatch(leaf -> Corpus.NESTED_PATHS.stream()
                            .anyMatch(path -> leaf.field().startsWith(path + "."))),
                    "no comparison is made inside a nested scope");
        }

        /**
         * Inside a {@code nested} query only fields under that path are visible, so an inner clause
         * naming any other field silently matches nothing.
         */
        @Test
        void everyNestedQueryOnlyNamesFieldsInsideItsOwnPath() {
            comparisons();
            List<String> offenders = new ArrayList<>();
            for (NestedScope scope : nested) {
                List<Comparison> inner = new ArrayList<>();
                walk(scope.action(), scope.query(), inner, new ArrayList<>());
                inner.stream()
                        .filter(leaf -> !leaf.field().startsWith(scope.path() + "."))
                        .forEach(leaf -> offenders.add(
                                scope.action() + ": " + scope.path() + " -> " + leaf.field()));
            }

            assertEquals(List.of(), offenders);
            // Anti-vacuity: nested scopes exist, and each path is mapped as `nested` (Elasticsearch
            // would reject one that is not only at search time).
            assertFalse(nested.isEmpty());
            nested.forEach(scope -> assertTrue(
                    Corpus.NESTED_PATHS.contains(scope.path()), scope.path()));
        }

        /**
         * No emitted query contains a null literal. Elasticsearch does not index nulls, so a
         * {@code term} on null matches nothing and its {@code must_not} matches everything.
         */
        @Test
        void noEmittedQueryBindsANullLiteral() {
            List<String> offenders = EMITTED.entrySet().stream()
                    .filter(entry -> containsNull(entry.getValue()))
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .toList();

            assertEquals(List.of(), offenders);
            // Anti-vacuity: the null comparisons are still translated, so the rule inspects them.
            // Which filter they lower to is the harness's to judge, by the rows it returns.
            for (String action : List.of("null/not-equals/null-literal",
                    "null/equals/negated-null-literal", "null/not-equals/null-literal-value-first")) {
                assertTrue(EMITTED.containsKey(action), action);
            }
        }

        private boolean containsNull(JsonNode node) {
            if (node.isNull()) {
                return true;
            }
            for (JsonNode child : node) {
                if (containsNull(child)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * {@code *}, {@code ?} and a backslash in a {@code wildcard} value must be escaped, apart
         * from the anchors the adapter adds; otherwise a substring test becomes a pattern match.
         * {@code %}, {@code _} and {@code [} are literal in a wildcard query.
         */
        @Test
        void everyWildcardNeedleEscapesItsMetacharacters() {
            List<Comparison> wildcards = comparisons().stream()
                    .filter(leaf -> "wildcard".equals(leaf.clause()))
                    .toList();
            List<String> offenders = wildcards.stream()
                    .filter(leaf -> hasUnescapedMetacharacter(
                            stripAnchors(leaf.operand().get("value").asText())))
                    .map(leaf -> leaf.action() + ": " + leaf.operand())
                    .toList();

            assertEquals(List.of(), offenders);
            // Anti-vacuity: wildcards are emitted, some needle carries an escaped backslash (so
            // a dangling one would be seen), and the detector rejects an unescaped `*` and a
            // trailing lone backslash.
            assertFalse(wildcards.isEmpty());
            assertTrue(wildcards.stream().anyMatch(leaf ->
                            stripAnchors(leaf.operand().get("value").asText()).contains("\\\\")),
                    "no wildcard needle escapes a backslash");
            assertTrue(hasUnescapedMetacharacter("a*b"));
            assertTrue(hasUnescapedMetacharacter("a\\"));
            assertFalse(hasUnescapedMetacharacter("a\\*b"));
        }

        /** Drop the {@code *} anchors the adapter adds for {@code contains} / {@code endsWith}. */
        private String stripAnchors(String value) {
            String body = value.startsWith("*") ? value.substring(1) : value;
            return body.endsWith("*") && !endsEscaped(body)
                    ? body.substring(0, body.length() - 1) : body;
        }

        private boolean endsEscaped(String value) {
            int backslashes = 0;
            for (int i = value.length() - 2; i >= 0 && value.charAt(i) == '\\'; i--) {
                backslashes++;
            }
            return backslashes % 2 != 0;
        }

        private boolean hasUnescapedMetacharacter(String value) {
            boolean escaped = false;
            for (int i = 0; i < value.length(); i++) {
                char current = value.charAt(i);
                if (escaped) {
                    escaped = false;
                } else if (current == '\\') {
                    escaped = true;
                } else if (current == '*' || current == '?') {
                    return true;
                }
            }
            // A lone trailing backslash escapes nothing, so the needle is malformed.
            return escaped;
        }

        /**
         * Every emitted value is a plain JDK type, so a caller can serialise the query with any
         * JSON library and no client library is needed on the classpath.
         */
        @Test
        void everyEmittedValueIsAPlainJdkType() {
            List<String> exotic = new ArrayList<>();
            CONDITIONAL.forEach((action, query) -> assertPlain(action, query, exotic));
            assertEquals(List.of(), exotic);
            assertFalse(CONDITIONAL.isEmpty());
        }

        private void assertPlain(String action, Object value, List<String> exotic) {
            if (value instanceof Map<?, ?> map) {
                map.forEach((key, child) -> {
                    if (!(key instanceof String)) {
                        exotic.add(action + ": non-string key "
                                + (key == null ? "null" : key.getClass().getName()));
                    }
                    assertPlain(action, child, exotic);
                });
            } else if (value instanceof List<?> list) {
                list.forEach(child -> assertPlain(action, child, exotic));
            } else if (!(value instanceof String || value instanceof Boolean
                    || value instanceof Long || value instanceof Integer
                    || value instanceof Double)) {
                exotic.add(action + ": " + (value == null ? "null" : value.getClass().getName()));
            }
        }

    }
}
