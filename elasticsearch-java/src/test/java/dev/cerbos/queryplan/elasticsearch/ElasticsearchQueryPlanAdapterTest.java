/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.elasticsearch;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The plans this suite hands the adapter are HAND-BUILT, and that is the exception rather than
 * the rule. {@link ElasticsearchTranslatorTest} reads its plans from
 * {@code conformance/wire-fixtures/} and accounts for every corpus action exactly once; a
 * hand-built plan is a BELIEF about what the planner emits, and this repository keeps golden
 * fixtures because that belief has been wrong before
 * ({@code docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md}).
 *
 * <p>What remains is of the three kinds {@code CLAUDE.md} ("What a translator unit test may pin")
 * admits, and every test below sits under the banner of exactly one:
 *
 * <ol>
 *   <li><strong>A branch CEL itself cannot reach.</strong> No policy compiles to it, so there is
 *       no corpus action to substitute for and never can be. Each test quotes the type error CEL
 *       raises for the shape, rather than inferring unreachability from the adapter's own code.
 *       Permanent.
 *   <li><strong>A caller-supplied argument the corpus structurally cannot vary.</strong>
 *       {@code actions.json} classifies each action against ONE {@link Options} per adapter, so
 *       the field map, {@code nestedPaths}, {@code collectionFields}, an {@link OperatorFunction}
 *       override, and the caller-facing contracts around them — the {@link Options} record, the
 *       typed exceptions — have no corpus spelling. Permanent.
 *   <li><strong>A corpus gap wearing a unit test.</strong> Policy-reachable, and the corpus does
 *       not carry it yet. A bridge, not a home: each is pinned in this adapter alone and asked of
 *       none of the others, which is the condition every bug this repository exists to stop was
 *       living in. Every test under that banner opens with <em>Corpus gap.</em>, is tracked by
 *       cerbos/query-plan-adapters#414, and is deleted when the corpus action lands.
 * </ol>
 *
 * <p>Everything else this file used to hold — the operator table, the negation forms, the null
 * polarities, the collection macros, the value-list fold, the fail-closed families — is a corpus
 * action now, asserted against a real planner's output and against {@code check()}.
 */
class ElasticsearchQueryPlanAdapterTest {

    private static final Map<String, String> FIELD_MAP = Map.ofEntries(
            Map.entry("request.resource.attr.department", "department"),
            Map.entry("request.resource.attr.aBool", "aBool"),
            Map.entry("request.resource.attr.aString", "aString"),
            Map.entry("request.resource.attr.aNumber", "aNumber"),
            Map.entry("request.resource.attr.title", "title"),
            Map.entry("request.resource.attr.scope", "scope"),
            Map.entry("request.resource.attr.tags", "tags"),
            Map.entry("request.resource.attr.ownedBy", "ownedBy"),
            Map.entry("request.resource.attr.tagObjects", "tagObjects")
    );

    private static final Set<String> NESTED_PATHS = Set.of("tagObjects");

    /** {@code tags} and {@code ownedBy} are flat keyword arrays; {@code aString} is a string. */
    private static final Set<String> COLLECTION_FIELDS = Set.of("tags", "ownedBy");

    private static final Options OPTIONS = Options.of(FIELD_MAP)
            .withNestedPaths(NESTED_PATHS)
            .withCollectionFields(COLLECTION_FIELDS);

    private static PlanResourcesResponse conditionalPlan(Operand condition) {
        return PlanResourcesResponse.newBuilder()
                .setFilter(PlanResourcesFilter.newBuilder()
                        .setKind(PlanResourcesFilter.Kind.KIND_CONDITIONAL)
                        .setCondition(condition))
                .build();
    }

    private static Operand expressionOperand(String operator, Operand... operands) {
        Expression.Builder expr = Expression.newBuilder().setOperator(operator);
        for (Operand op : operands) {
            expr.addOperands(op);
        }
        return Operand.newBuilder().setExpression(expr).build();
    }

    private static Operand timestampOperand(Operand operand) {
        return expressionOperand("timestamp", operand);
    }

    private static Operand lambdaOperand(String lambdaVar, Operand body) {
        return expressionOperand("lambda", body, variableOperand(lambdaVar));
    }

    private static Operand variableOperand(String name) {
        return Operand.newBuilder().setVariable(name).build();
    }

    private static Operand valueOperand(Value value) {
        return Operand.newBuilder().setValue(value).build();
    }

    private static Value string(String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value number(double value) {
        return Value.newBuilder().setNumberValue(value).build();
    }

    private static final Value NULL = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

    private static Value list(Value... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (Value value : values) {
            list.addValues(value);
        }
        return Value.newBuilder().setListValue(list).build();
    }

    private static Value struct(String field, Value value) {
        return Value.newBuilder()
                .setStructValue(Struct.newBuilder().putFields(field, value))
                .build();
    }

    private static Operand stringValueOperand(String val) {
        return valueOperand(string(val));
    }

    private static Operand numberValueOperand(double val) {
        return valueOperand(number(val));
    }

    private static Operand boolValueOperand(boolean val) {
        return Operand.newBuilder().setValue(Value.newBuilder().setBoolValue(val)).build();
    }

    private static Operand listValueOperand(String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String v : values) {
            list.addValues(string(v));
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand emptyListValueOperand() {
        return Operand.newBuilder()
                .setValue(Value.newBuilder().setListValue(ListValue.newBuilder()))
                .build();
    }

    private static Operand structListValueOperand(String field, String... values) {
        ListValue.Builder list = ListValue.newBuilder();
        for (String value : values) {
            list.addValues(struct(field, string(value)));
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setListValue(list)).build();
    }

    private static Operand valueListMacro(String operator, Operand collection, Operand body) {
        return expressionOperand(operator, collection, lambdaOperand("t", body));
    }

    private static Operand hierarchy(Operand path, String delimiter) {
        return expressionOperand("hierarchy", path, stringValueOperand(delimiter));
    }

    private static Operand sizeOf(String variable) {
        return expressionOperand("size", variableOperand(variable));
    }

    private static Map<String, Object> translate(Operand condition) {
        return translate(condition, OPTIONS);
    }

    private static Map<String, Object> translate(Operand condition, Options options) {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition), options);
        return ((Result.Conditional) result).query();
    }

    private static IllegalArgumentException refusal(Operand condition) {
        return refusal(condition, OPTIONS);
    }

    private static IllegalArgumentException refusal(Operand condition, Options options) {
        return assertThrows(IllegalArgumentException.class, () -> translate(condition, options));
    }

    private static Map<String, Object> term(String field, Object value) {
        return Map.of("term", Map.of(field, Map.of("value", value)));
    }

    private static Map<String, Object> exists(String field) {
        return Map.of("exists", Map.of("field", field));
    }

    private static Map<String, Object> mustNot(Map<String, Object> query) {
        return Map.of("bool", Map.of("must_not", List.of(query)));
    }

    private static Map<String, Object> nested(String path, Map<String, Object> query) {
        return Map.of("nested", Map.of("path", path, "query", query));
    }

    private static Map<String, Object> definedAndNot(String field, Map<String, Object> query) {
        return Map.of("bool", Map.of("must", List.of(exists(field), mustNot(query))));
    }

    // ============================================================================================
    // KIND 1 — a branch CEL itself cannot reach
    // ============================================================================================

    /**
     * CEL has no such function, so no policy compiles to this operator: the checker raises
     * {@code undeclared reference to 'unsupported_op' (in container '')} and the planner never
     * runs.
     */
    @Test
    void anOperatorTheAdapterDoesNotKnowIsRefusedByName() {
        IllegalArgumentException ex = refusal(expressionOperand("unsupported_op",
                variableOperand("request.resource.attr.department"), stringValueOperand("value")));
        assertTrue(ex.getMessage().contains("Unknown operator"), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }

    /**
     * {@code isSet} is not a registered CEL function, so a policy naming it does not compile
     * ({@code undeclared reference to 'isSet'}) and the operator can never reach the wire
     * (cerbos/query-plan-adapters#261). It appears in zero of the corpus's wire fixtures;
     * existence is spelled {@code R.attr.x != null}, which the corpus carries as {@code null-ne}
     * and this adapter lowers to {@code exists}.
     */
    @Test
    void isSetIsRejectedRatherThanTranslated() {
        IllegalArgumentException ex = refusal(expressionOperand("isSet",
                variableOperand("request.resource.attr.department"), boolValueOperand(true)));
        assertTrue(ex.getMessage().contains("isSet"),
                "the unknown operator must be named in the error, got: " + ex.getMessage());
    }

    /**
     * A lambda body referencing a variable the lambda does not bind: CEL's checker refuses the
     * comprehension with {@code undeclared reference to 'x'}, so no plan carries one. Here it is
     * a malformed plan, not a policy shape.
     */
    @Test
    void aLambdaBodyReferencingAnUnboundVariableIsRefused() {
        IllegalArgumentException ex = refusal(expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("x.name"), stringValueOperand("public")))));
        assertTrue(ex.getMessage().contains("does not start with lambda variable"), ex.getMessage());
        assertInstanceOf(MalformedPlanException.class, ex);
    }

    /**
     * A macro whose collection operand is a scalar literal rather than a list. CEL refuses it at
     * check time — {@code expression of type 'string' cannot be range of a comprehension (must be
     * list, map, or dynamic)} — so the planner never folds one.
     */
    @Test
    void aValueListMacroOverANonListLiteralIsRefused() {
        IllegalArgumentException ex = refusal(valueListMacro("exists",
                stringValueOperand("not a list"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"), variableOperand("t"))));
        assertTrue(ex.getMessage()
                .contains("exists over a literal collection requires a list value"), ex.getMessage());
        assertInstanceOf(MalformedPlanException.class, ex);
    }

    /**
     * CEL's own {@code timestamp()} rejects each of these, so the planner cannot emit one — a
     * calendar-invalid date ({@code parsing time "2024-02-30T00:00:00Z": day out of range}), a
     * non-RFC-3339 spelling, and the two ends of CEL's representable range. The adapter validates
     * the literal anyway, because it is what decides whether a {@code term} or {@code range}
     * query is even well-formed, and reports it as a malformed plan.
     */
    @Test
    void aTimestampLiteralOutsideStrictRfc3339IsRefused() {
        for (String literal : List.of(
                "2024-W01-1T00:00:00Z",
                "2024-06-01 00:00:00Z",
                "0000-01-01T00:00:00Z",
                "2024-02-30T00:00:00Z",
                "0001-01-01T00:00:00+02:00",
                "9999-12-31T23:00:00-02:00")) {
            IllegalArgumentException ex = refusal(expressionOperand("eq",
                    timestampOperand(variableOperand("request.resource.attr.aString")),
                    timestampOperand(stringValueOperand(literal))));
            assertTrue(ex.getMessage().contains("valid RFC 3339"), literal);
            assertInstanceOf(MalformedPlanException.class, ex, literal);
        }
    }

    // ============================================================================================
    // KIND 2 — a caller-supplied argument the corpus structurally cannot vary
    // ============================================================================================

    /**
     * The field map is a CALLER argument, so a reference it does not name is not a policy shape.
     * The corpus maps every level of every path precisely so that no corpus action lands here
     * (cerbos/query-plan-adapters#326) — this is the other side of that rule.
     */
    @Test
    void anUnmappedReferenceIsRefusedRatherThanUsedVerbatim() {
        IllegalArgumentException ex = refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.nonexistent"),
                stringValueOperand("value")));
        assertTrue(ex.getMessage().contains("Unknown attribute"), ex.getMessage());
        assertInstanceOf(UnmappedAttributeException.class, ex);
    }

    /** {@link OperatorFunction} overrides are a caller argument; no plan selects one. */
    @Test
    void aCallerSuppliedOperatorOverrideReplacesTheDefaultTranslation() {
        Operand condition = expressionOperand("eq",
                variableOperand("request.resource.attr.department"),
                stringValueOperand("engineering"));
        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (field, value) -> Map.of("match", Map.of(field, value)));

        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(condition), FIELD_MAP, overrides);

        assertEquals(Map.of("match", Map.of("department", "engineering")),
                ((Result.Conditional) result).query());
    }

    /**
     * Which operators and polarities an override reaches, pinned as the README states them.
     *
     * <p>{@code in} reaches its override in every polarity — positive, negated inside the
     * {@code exists} guard, and the null-aware negated form — where it used to be built from the
     * default {@code terms} regardless. The negation of an ordering operator is its MIRROR, so a
     * negated {@code lt} applies the {@code ge} override. A positive {@code ne} with no {@code ne}
     * override is {@code exists AND NOT eq}, and the {@code eq} inside it is the caller's.
     */
    @Test
    void anOverrideReachesEveryPolarityOfItsOperator() {
        Map<String, OperatorFunction> overrides = Map.of(
                "in", (field, value) -> Map.of("custom_in", Map.of(field, value)),
                "ge", (field, value) -> Map.of("custom_ge", Map.of(field, value)),
                "eq", (field, value) -> Map.of("custom_eq", Map.of(field, value)));
        Options options = OPTIONS.withOperatorOverrides(overrides);
        Operand membership = expressionOperand("in",
                variableOperand("request.resource.attr.aString"), listValueOperand("a", "b"));
        Operand nullAwareMembership = expressionOperand("in",
                variableOperand("request.resource.attr.aString"),
                valueOperand(list(string("a"), NULL)));

        assertEquals(Map.of("custom_in", Map.of("aString", List.of("a", "b"))),
                translate(membership, options));
        assertEquals(definedAndNot("aString", Map.of("custom_in", Map.of("aString", List.of("a", "b")))),
                translate(expressionOperand("not", membership), options));
        assertEquals(definedAndNot("aString", Map.of("custom_in", Map.of("aString", List.of("a")))),
                translate(expressionOperand("not", nullAwareMembership), options));
        assertEquals(Map.of("custom_ge", Map.of("aNumber", 5L)),
                translate(expressionOperand("not", expressionOperand("lt",
                        variableOperand("request.resource.attr.aNumber"), numberValueOperand(5))),
                        options));
        assertEquals(definedAndNot("aString", Map.of("custom_eq", Map.of("aString", "x"))),
                translate(expressionOperand("ne",
                        variableOperand("request.resource.attr.aString"), stringValueOperand("x")),
                        options));
    }

    /**
     * The two lowerings that borrow a shape without being that operator: a hierarchy relation is
     * not {@code startsWith} or {@code in} even though it emits {@code prefix} and {@code terms},
     * and the {@code ^literal} form of {@code matches} is not {@code startsWith} even though it
     * emits {@code prefix}. Neither consults the borrowed operator's override.
     */
    @Test
    void hierarchyAndTheLiteralPrefixOfMatchesDoNotBorrowAnotherOperatorsOverride() {
        Options options = OPTIONS.withOperatorOverrides(Map.of(
                "startsWith", (field, value) -> Map.of("custom_prefix", Map.of(field, value)),
                "in", (field, value) -> Map.of("custom_in", Map.of(field, value))));

        assertEquals(Map.of("prefix", Map.of("scope", Map.of("value", "a.b."))),
                translate(expressionOperand("descendentOf",
                        hierarchy(variableOperand("request.resource.attr.scope"), "."),
                        hierarchy(stringValueOperand("a.b"), ".")), options));
        assertEquals(Map.of("terms", Map.of("scope", List.of("a", "a.b"))),
                translate(expressionOperand("ancestorOf",
                        hierarchy(variableOperand("request.resource.attr.scope"), "."),
                        hierarchy(stringValueOperand("a.b.c"), ".")), options));
        assertEquals(Map.of("prefix", Map.of("aString", Map.of("value", "adm"))),
                translate(expressionOperand("matches",
                        variableOperand("request.resource.attr.aString"),
                        stringValueOperand("^adm")), options));
    }

    /**
     * {@code nestedPaths} is a caller argument too, and the adapter cannot read an index mapping to
     * check it. A collection macro over a field the caller did not declare has no {@code nested}
     * query to emit, so it fails closed rather than emitting one that would match nothing.
     */
    @Test
    void aCollectionMacroOverAnUndeclaredNestedPathIsRefused() {
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("t.name"), stringValueOperand("public"))));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), FIELD_MAP));
        assertTrue(ex.getMessage().contains("not declared in nestedPaths"), ex.getMessage());
        assertInstanceOf(UnmappedAttributeException.class, ex);
    }

    /**
     * The complement of the rule above, and the reason it is scoped to DOCUMENT collections: a
     * macro over a literal value list has no document to reach, so it must translate with an empty
     * {@code nestedPaths}.
     */
    @Test
    void aValueListMacroNeedsNoNestedPathDeclaration() {
        Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                conditionalPlan(valueListMacro("exists", listValueOperand("string"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t")))),
                FIELD_MAP, Set.of());

        assertEquals(Map.of("bool", Map.of(
                        "should", List.of(term("aString", "string")),
                        "minimum_should_match", 1)),
                ((Result.Conditional) result).query());
    }

    /**
     * {@code collectionFields} is the declaration that lets a {@code size()} emptiness check over
     * a FLAT array lower to {@code exists}. The adapter is handed a plan, never a mapping, so it
     * cannot tell {@code size(tagNames)} from {@code size(aString)}; declaring the field is how the
     * caller says which it is. Undeclared, the same plan is refused by name — and a nested path
     * needs no second declaration.
     */
    @Test
    void sizeOverADeclaredFlatCollectionIsAnExistsCheckAndUndeclaredItIsRefused() {
        Operand nonEmpty = expressionOperand("gt", sizeOf("request.resource.attr.tags"),
                numberValueOperand(0));

        assertEquals(exists("tags"), translate(nonEmpty));
        assertEquals(nested("tagObjects", Map.of("match_all", Map.of())),
                translate(expressionOperand("gt", sizeOf("request.resource.attr.tagObjects"),
                        numberValueOperand(0))));

        IllegalArgumentException ex = refusal(nonEmpty, OPTIONS.withCollectionFields(Set.of()));
        assertTrue(ex.getMessage().contains("size() over a field not declared as a collection"),
                ex.getMessage());
        assertTrue(ex.getMessage().contains("'tags' is declared in nestedPaths or collectionFields"),
                ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }

    /**
     * The {@link Options} record is the caller's whole contract, so its immutability is pinned:
     * every collection is copied on the way in, each {@code with…} returns a new instance, and the
     * positional convenience overloads are exactly the {@link Options} form with the rest empty.
     */
    @Test
    void optionsAreImmutableAndTheConvenienceOverloadsDelegateToThem() {
        Map<String, String> mutable = new HashMap<>(FIELD_MAP);
        Options options = Options.of(mutable);
        mutable.clear();
        assertEquals(FIELD_MAP, options.fieldMap());

        Options widened = options.withNestedPaths(NESTED_PATHS);
        assertNotSame(options, widened);
        assertEquals(Set.of(), options.nestedPaths());
        assertEquals(NESTED_PATHS, widened.nestedPaths());
        assertEquals(Set.of(), widened.collectionFields());
        assertEquals(Set.of(), widened.explicitNullAttributes());
        assertEquals(Map.of(), widened.operatorOverrides());
        assertThrows(NullPointerException.class, () -> Options.of(null));
        assertThrows(NullPointerException.class, () -> options.withCollectionFields(null));

        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("t.name"), stringValueOperand("public"))));
        assertEquals(
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), FIELD_MAP, Map.of(), NESTED_PATHS, Set.of()),
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), widened));
    }

    /**
     * Every refusal is one of three types, so a caller can route without matching on the message:
     * a shape the Query DSL cannot express, a variable the caller did not declare, or a plan that
     * violates the wire contract. All three are {@link IllegalArgumentException}, which stays the
     * documented base type.
     */
    @Test
    void refusalsAreTypedByTheirMechanism() {
        assertInstanceOf(UnsupportedPlanShapeException.class, refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.aString"),
                variableOperand("request.resource.attr.title"))));
        assertInstanceOf(UnmappedAttributeException.class, refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.missing"), stringValueOperand("x"))));
        assertInstanceOf(MalformedPlanException.class, refusal(expressionOperand("eq",
                variableOperand("request.resource.attr.aString"))));
        assertInstanceOf(MalformedPlanException.class, refusal(expressionOperand("not")));
        assertInstanceOf(MalformedPlanException.class, refusal(expressionOperand("eq",
                stringValueOperand("a"), stringValueOperand("b"))));
    }

    // ============================================================================================
    // KIND 3 — a policy can reach these, and the corpus does not carry them yet
    //
    // Every test here is a corpus gap, tracked by cerbos/query-plan-adapters#414, and is deleted
    // when its corpus action lands. They are NOT covered by #387 or #388, whose actions are
    // enumerated and landed.
    // ============================================================================================

    /**
     * <strong>Corpus gap.</strong> Substituting a lambda variable into a struct element drills a
     * dotted path into it. A path the element does not carry fails closed — that element's CEL
     * evaluation would error — rather than silently dropping the comparison. A principal
     * attribute holding a list of structs is a policy any application can write; the corpus's
     * principal collections are lists of scalars.
     */
    @Test
    void aValueListElementMissingTheProjectedFieldIsRefused() {
        IllegalArgumentException ex = refusal(valueListMacro("exists",
                structListValueOperand("name", "string"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"),
                        variableOperand("t.missing"))));
        assertTrue(ex.getMessage().contains("Cannot resolve \"t.missing\""), ex.getMessage());
    }

    /**
     * <strong>Corpus gap.</strong> A {@code wildcard} query reads {@code *} and {@code ?} as
     * operators, so an unescaped one in a needle turns a substring test into a pattern match — this
     * repository's founding bug class (#258/#259) in its Elasticsearch spelling. The corpus seeds
     * {@code %}, {@code _}, {@code \} and {@code [}, which are the SQL and Lucene-regex
     * metacharacters; no seed value carries a {@code *} or a {@code ?}, so no corpus action
     * exercises the escaping this adapter actually needs.
     */
    @Test
    void containsEscapesTheWildcardMetacharactersInItsNeedle() {
        assertEquals(Map.of("wildcard", Map.of("title",
                        Map.of("value", "*foo\\*bar\\?baz\\\\qux*"))),
                translate(expressionOperand("contains",
                        variableOperand("request.resource.attr.title"),
                        stringValueOperand("foo*bar?baz\\qux"))));
    }

    /** <strong>Corpus gap.</strong> The same escaping through the other anchored operator. */
    @Test
    void endsWithEscapesTheWildcardMetacharactersInItsNeedle() {
        assertEquals(Map.of("wildcard", Map.of("title", Map.of("value", "*a\\*b"))),
                translate(expressionOperand("endsWith",
                        variableOperand("request.resource.attr.title"),
                        stringValueOperand("a*b"))));
    }

    /**
     * <strong>Corpus gap.</strong> The corpus carries exactly one {@code matches()} action —
     * {@code p-matches}, the {@code ^h} literal prefix, which lowers to a {@code prefix} query and
     * never reaches Lucene's regex engine. Everything about the RE2-to-Lucene subset is therefore
     * untested by it.
     *
     * <p>Each pattern below is one RE2 construct Lucene either lacks or reads differently: a
     * character class shorthand, an inline flag, an unanchored form, a POSIX class, and the two
     * spellings of {@code .} — which in Lucene matches a newline and in RE2 does not.
     * {@link ElasticsearchSurfaceTest} executes that last difference against a real server.
     */
    @Test
    void aRegexOutsideTheSharedRe2LuceneSubsetIsRefused() {
        for (String pattern : List.of(
                "^\\d+$", "^(?i)admin$", "^a^b$", "^[[:alpha:]]$", "^a.b$", "^a.*b$")) {
            IllegalArgumentException ex = refusal(expressionOperand("matches",
                    variableOperand("request.resource.attr.aString"),
                    stringValueOperand(pattern)));
            assertTrue(ex.getMessage().contains("supported RE2/Lucene subset"), pattern);
        }
    }

    /**
     * <strong>Corpus gap.</strong> RE2 parses {@code ^a|b$} as two alternatives — {@code ^a} and
     * {@code b$} — each anchored on one side only, while Lucene matches the whole field against
     * {@code a|b}. The two languages agree only once the alternation is parenthesised under both
     * anchors, so an unparenthesised top-level {@code |} is refused and the parenthesised form
     * translates. An alternation inside a group or a character class is not top-level.
     */
    @Test
    void aTopLevelAlternationInAnAnchoredRegexIsRefusedAndAParenthesisedOneTranslates() {
        IllegalArgumentException ex = refusal(expressionOperand("matches",
                variableOperand("request.resource.attr.aString"), stringValueOperand("^a|b$")));
        assertTrue(ex.getMessage().contains("top-level alternation"), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);

        assertEquals(Map.of("regexp", Map.of("aString", Map.of("value", "(a|b)", "flags", "NONE"))),
                translate(expressionOperand("matches",
                        variableOperand("request.resource.attr.aString"),
                        stringValueOperand("^(a|b)$"))));
        assertEquals(Map.of("regexp", Map.of("aString", Map.of("value", "[a|b]", "flags", "NONE"))),
                translate(expressionOperand("matches",
                        variableOperand("request.resource.attr.aString"),
                        stringValueOperand("^[a|b]$"))));
    }

    /**
     * <strong>Corpus gap.</strong> Lucene accepts a {@code {} only as the start of a {@code {n}},
     * {@code {n,}} or {@code {n,m}} interval and rejects any other brace at query time — after the
     * adapter has already returned a filter. The three interval forms translate; every other
     * brace is refused here, where the caller can see why.
     */
    @Test
    void aBraceThatDoesNotBeginARepetitionIntervalIsRefused() {
        for (String pattern : List.of("^a{x}$", "^a{$", "^a{1,2,3}$", "^a{,2}$", "^a{}$")) {
            IllegalArgumentException ex = refusal(expressionOperand("matches",
                    variableOperand("request.resource.attr.aString"), stringValueOperand(pattern)));
            assertTrue(ex.getMessage().contains("does not begin a {n}, {n,} or {n,m} repetition"),
                    pattern + ": " + ex.getMessage());
        }
        for (String body : List.of("a{2}", "a{2,}", "a{2,3}", "(ab){1,2}")) {
            assertEquals(Map.of("regexp", Map.of("aString", Map.of("value", body, "flags", "NONE"))),
                    translate(expressionOperand("matches",
                            variableOperand("request.resource.attr.aString"),
                            stringValueOperand("^" + body + "$"))), body);
        }
    }

    /**
     * <strong>Corpus gap.</strong> The corpus drives {@code exists} and {@code all} over a
     * principal value list ({@code pv-exists}, {@code pv-all}) but not the other three macros. Two
     * of them — {@code filter} and {@code map} — reach the LEAF traversal rather than the collection
     * handler, so without an explicit guard they would surface an unrelated operand-shape error
     * instead of naming the real limitation.
     */
    @ParameterizedTest
    @ValueSource(strings = {"exists_one", "filter", "map"})
    void anUnfoldableMacroOverAValueListIsRefusedByName(String operator) {
        IllegalArgumentException ex = refusal(valueListMacro(operator,
                listValueOperand("string"),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"), variableOperand("t"))));
        assertTrue(ex.getMessage()
                        .contains(operator + " over a literal collection value is not supported"),
                "unexpected message: " + ex.getMessage());
    }

    /**
     * <strong>Corpus gap.</strong> Cerbos {@code except(list, list)} is a two-list function
     * returning a list difference; no lambda form of it exists on the wire, and the nested
     * {@code must_not} this adapter once emitted for one was unreachable from any real plan. The
     * list difference has no Query DSL translation, so {@code except} is refused by name at every
     * position it can arrive in: as a condition, as the argument of {@code size()}, as a leaf
     * operand, and inside a nested lambda scope. The corpus carries no {@code except} action.
     */
    @Test
    void exceptIsRefusedByNameWhereverItAppears() {
        Operand except = expressionOperand("except",
                variableOperand("request.resource.attr.tags"), listValueOperand("archived"));
        List<Operand> positions = List.of(
                except,
                expressionOperand("not", except),
                expressionOperand("gt", expressionOperand("size", except), numberValueOperand(0)),
                expressionOperand("eq", except, listValueOperand("x")),
                expressionOperand("exists", variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", expressionOperand("except",
                                variableOperand("t.labels"), listValueOperand("archived")))));
        for (Operand condition : positions) {
            IllegalArgumentException ex = refusal(condition);
            assertTrue(ex.getMessage().contains(
                    "except is not supported: Cerbos except(list, list) computes a list difference"),
                    ex.getMessage());
            assertTrue(ex.getMessage().contains("R.attr.tags.exists(t, !(t in [\"x\"]))"),
                    ex.getMessage());
            assertInstanceOf(UnsupportedPlanShapeException.class, ex);
        }
    }

    /**
     * <strong>Corpus gap.</strong> A ternary used as the CONDITION of a nested lambda. The corpus
     * pins the bare ternary at the top of the filter ({@code ternary-bare}) and a ternary as a
     * leaf OPERAND inside a lambda ({@code p-ternary-in-exists}); the ternary that IS the lambda
     * body takes the scoped walk, which has its own {@code if} refusal. Same message, same type.
     */
    @Test
    void aTernaryAsALambdaBodyIsRefusedByName() {
        IllegalArgumentException ex = refusal(expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("if",
                        variableOperand("request.resource.attr.aBool"),
                        expressionOperand("eq", variableOperand("t.name"), stringValueOperand("a")),
                        expressionOperand("eq", variableOperand("t.name"), stringValueOperand("b"))))));
        assertTrue(ex.getMessage().contains("if (CEL ternary) cannot be expressed"), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }

    /**
     * <strong>Corpus gap.</strong> The corpus principal's collections are never empty, so no action
     * folds a macro over {@code []}. CEL's identity semantics say {@code exists} over an empty
     * collection is false and {@code all} over one is true, and each flips under negation — which a
     * fold that simply dropped the empty chain would get wrong in one direction.
     */
    @Test
    void anEmptyValueListKeepsCelIdentitySemantics() {
        Operand body = expressionOperand("eq",
                variableOperand("request.resource.attr.aString"), variableOperand("t"));

        assertEquals(Map.of("match_none", Map.of()),
                translate(valueListMacro("exists", emptyListValueOperand(), body)));
        assertEquals(Map.of("match_all", Map.of()),
                translate(valueListMacro("all", emptyListValueOperand(), body)));
        assertEquals(Map.of("match_all", Map.of()),
                translate(expressionOperand("not",
                        valueListMacro("exists", emptyListValueOperand(), body))));
        assertEquals(Map.of("match_none", Map.of()),
                translate(expressionOperand("not",
                        valueListMacro("all", emptyListValueOperand(), body))));
    }

    /**
     * <strong>Corpus gap.</strong> A negated macro over a principal value list. The corpus's one
     * negated {@code pv-*} action ({@code null-value-pv-not-exists}) is refused for an unrelated
     * reason — it probes the explicit-null attribute — so nothing there reaches this fold.
     *
     * <p>The invariant that matters is not the emitted shape but that the fold is
     * INDISTINGUISHABLE from the or-chain the planner emits below its 10-element unroll cliff
     * (cerbos/cerbos#2570, #2817), under negation as well as positively.
     */
    @Test
    void aNegatedValueListMacroFoldsExactlyAsTheUnrolledChainDoes() {
        assertEquals(translate(expressionOperand("not",
                        expressionOperand("or",
                                expressionOperand("eq",
                                        variableOperand("request.resource.attr.aString"),
                                        stringValueOperand("string")),
                                expressionOperand("eq",
                                        variableOperand("request.resource.attr.aString"),
                                        stringValueOperand("anotherString"))))),
                translate(expressionOperand("not",
                        valueListMacro("exists",
                                listValueOperand("string", "anotherString"),
                                expressionOperand("eq",
                                        variableOperand("request.resource.attr.aString"),
                                        variableOperand("t"))))));
    }

    /**
     * <strong>Corpus gap.</strong> The corpus's principal collections are lists of scalars, so no
     * action folds a macro whose elements are STRUCTS and whose body reads a member of one.
     */
    @Test
    void aValueListElementFieldIsDrilledIntoDuringTheFold() {
        assertEquals(Map.of("bool", Map.of(
                        "should", List.of(term("aString", "string"), term("aString", "anotherString")),
                        "minimum_should_match", 1)),
                translate(valueListMacro("exists",
                        structListValueOperand("name", "string", "anotherString"),
                        expressionOperand("eq",
                                variableOperand("request.resource.attr.aString"),
                                variableOperand("t.name")))));
    }

    /**
     * <strong>Corpus gap.</strong> A struct element whose member is a JSON null. Substituting it
     * yields {@code eq(aString, null)}, which is the explicit-null refusal — and the struct is read
     * into a map that ACCEPTS a null value, so the refusal is that one rather than a
     * {@link NullPointerException} from the value conversion.
     */
    @Test
    void aStructElementHoldingANullMemberIsRefusedRatherThanCrashing() {
        IllegalArgumentException ex = refusal(valueListMacro("exists",
                valueOperand(list(struct("name", NULL))),
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aString"),
                        variableOperand("t.name"))));
        assertTrue(ex.getMessage().contains("explicit null value from a missing field"),
                ex.getMessage());
        Map<String, Object> expected = new HashMap<>();
        expected.put("name", null);
        assertEquals(expected, PlanValues.protoValueToJava(struct("name", NULL)));
    }

    /**
     * <strong>Corpus gap.</strong> A nested macro that rebinds the outer macro's iteration variable
     * name. The corpus's nested macros all bind distinct names, so nothing there exercises the
     * shadowing rule — and getting it wrong would substitute a literal into the INNER lambda's
     * body, silently changing which documents match.
     */
    @Test
    void aNestedLambdaRebindingTheVariableShadowsTheSubstitution() {
        Map<String, Object> nested = nested("tagObjects", term("tagObjects.name", "public"));

        assertEquals(Map.of("bool", Map.of(
                        "should", List.of(nested, nested), "minimum_should_match", 1)),
                translate(valueListMacro("exists",
                        listValueOperand("ignored-a", "ignored-b"),
                        expressionOperand("exists",
                                variableOperand("request.resource.attr.tagObjects"),
                                lambdaOperand("t", expressionOperand("eq",
                                        variableOperand("t.name"),
                                        stringValueOperand("public")))))));
    }

    /**
     * <strong>Corpus gap.</strong> The corpus's emptiness checks are {@code 0 < size(c)}
     * ({@code vf-size}) and {@code !(size(c) == 0)} ({@code not-empty}). {@code size(c) >= 1} and
     * {@code size(c) != 0} are the third and fourth spellings of the same predicate, each taking a
     * different branch of the threshold recogniser — {@code >= 1} the one that has to know a count
     * is an integer, {@code != 0} the one that used to be refused as an unsupported threshold. Under
     * negation {@code != 0} is the emptiness check, refused for the missing-versus-empty ambiguity.
     */
    @Test
    void everySpellingOfNonEmptinessIsTheSameCheck() {
        assertEquals(exists("ownedBy"), translate(expressionOperand("ge",
                sizeOf("request.resource.attr.ownedBy"), numberValueOperand(1))));
        Operand notEqualZero = expressionOperand("ne",
                sizeOf("request.resource.attr.ownedBy"), numberValueOperand(0));
        assertEquals(exists("ownedBy"), translate(notEqualZero));
        IllegalArgumentException ex = refusal(expressionOperand("not", notEqualZero));
        assertTrue(ex.getMessage().contains("emptiness cannot distinguish a missing collection"),
                ex.getMessage());
    }

    /**
     * <strong>Corpus gap, and the over-grant this adapter used to carry.</strong>
     *
     * <p>The adapter is handed a plan, never a mapping, so it cannot tell a string field from an
     * array one: {@code size(x) > 0} used to become {@code exists x} either way. For a COLLECTION
     * that is right. For a STRING it is not, and the corpus seeds the witness: {@code a8} holds
     * {@code aString: ""}. CEL evaluates {@code size("") > 0} to false and {@code check()} denies
     * that seed, while an empty string is an indexed term on a {@code keyword} field, so
     * {@code exists} matched it and the document came back — a row the PDP denies. Over a NUMBER,
     * {@code exists} matched every document where CEL raises a no-overload error.
     *
     * <p>Both are now refused unless the caller declares the field a collection
     * ({@code collectionFields} or {@code nestedPaths}). The corpus still cannot catch the
     * original over-grant: its three string-length actions are thresholds
     * ({@code string-size}, {@code size-huge-gt}, {@code size-huge-lt}), and
     * {@code size(R.attr.aString) > 0} appears in no policy. Tracked in #414.
     */
    @Test
    void sizeOverAStringOrANumberIsRefusedRatherThanLoweredToExists() {
        for (String variable : List.of("request.resource.attr.aString", "request.resource.attr.aNumber")) {
            IllegalArgumentException ex = refusal(expressionOperand("gt",
                    sizeOf(variable), numberValueOperand(0)));
            assertTrue(ex.getMessage().contains("size() over a field not declared as a collection"),
                    ex.getMessage());
            assertTrue(ex.getMessage().contains("size(" + variable + ")"), ex.getMessage());
        }
    }

    /**
     * <strong>Corpus gap.</strong> Every {@code terms} query the corpus emits binds strings. A
     * numeric membership test goes through the same lowering but a different value conversion —
     * the planner sends every number as a double, and the adapter narrows an integral one to a
     * {@code long} so the query does not read {@code 1.0} against an {@code integer} field.
     */
    @Test
    void membershipOverNumbersBindsIntegralValuesAsIntegers() {
        assertEquals(Map.of("terms", Map.of("aNumber", List.of(1L, 2L, 3L))),
                translate(expressionOperand("in",
                        variableOperand("request.resource.attr.aNumber"),
                        valueOperand(list(number(1), number(2), number(3))))));
    }

    /**
     * <strong>Corpus gap.</strong> The narrowing above has a range. A double outside
     * {@code [-2^63, 2^63)} is integral too, but a {@code long} cannot hold it: the cast saturates
     * to {@code Long.MAX_VALUE} and silently changes the operand. Such a literal stays a double;
     * the two ends of the range are pinned, {@code -2^63} narrowing and {@code 2^63} not. The
     * corpus's largest literal is {@code 4294967296}.
     */
    @Test
    void anIntegralLiteralOutsideTheLongRangeStaysADouble() {
        assertEquals(term("aNumber", 1.0e19), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(1.0e19))));
        assertEquals(term("aNumber", 0x1p63), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(0x1p63))));
        assertEquals(term("aNumber", Long.MIN_VALUE), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(-0x1p63))));
        assertEquals(term("aNumber", 4294967296L), translate(expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), numberValueOperand(4294967296.0))));
    }

    /**
     * <strong>Corpus gap.</strong> CEL's {@code 1.0 / 0.0} is {@code +Inf}, and the planner folds
     * constant arithmetic, so a non-finite literal can reach a leaf. JSON has no representation for
     * it — Jackson would write the STRING {@code "Infinity"}, which parses back cleanly and has
     * silently stopped being a number — so it is refused at the leaf, as {@code size()} already
     * refuses a non-finite threshold. The corpus's non-finite actions ({@code nan-ord-*}) keep the
     * division unfolded, so none reaches this branch.
     */
    @Test
    void aNonFiniteNumericLiteralIsRefusedAtTheLeaf() {
        for (double value : new double[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN}) {
            IllegalArgumentException ex = refusal(expressionOperand("gt",
                    variableOperand("request.resource.attr.aNumber"), numberValueOperand(value)));
            assertTrue(ex.getMessage().contains("non-finite numeric literal"), ex.getMessage());
            assertInstanceOf(UnsupportedPlanShapeException.class, ex);
        }
        IllegalArgumentException inList = refusal(expressionOperand("in",
                variableOperand("request.resource.attr.aNumber"),
                valueOperand(list(number(1), number(Double.NaN)))));
        assertTrue(inList.getMessage().contains("non-finite numeric literal"), inList.getMessage());
    }

    /**
     * <strong>Corpus gap.</strong> {@code in-null-elem-hasint} pins a null element on the SECOND
     * operand of a flat {@code hasIntersection}. {@code hasIntersection} is symmetric, and the
     * {@code map()} projection and the lambda-scoped form carry the same null the same way, so the
     * same refusal has to fire from every position the literal can sit in — it used to fire from
     * one.
     */
    @Test
    void hasIntersectionWithANullElementIsRefusedWhicheverPositionCarriesIt() {
        Operand withNull = valueOperand(list(string("public"), NULL));
        Operand projection = expressionOperand("map",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", variableOperand("t.name")));
        List<Operand> positions = List.of(
                expressionOperand("hasIntersection", withNull, variableOperand("request.resource.attr.tags")),
                expressionOperand("hasIntersection", projection, withNull),
                expressionOperand("hasIntersection", withNull, projection),
                expressionOperand("exists", variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", expressionOperand("hasIntersection",
                                variableOperand("t.labels"), withNull))));
        for (Operand condition : positions) {
            IllegalArgumentException ex = refusal(condition);
            assertTrue(ex.getMessage().contains(
                    "hasIntersection with null requires an explicit null-value mapping"),
                    ex.getMessage());
        }
    }

    /**
     * <strong>Corpus gap.</strong> {@code hasIntersection} is symmetric, so the {@code map()}
     * projection may sit on either side; the corpus writes it first. Both spellings must emit the
     * same nested query, or the second one falls through to the flat lowering and is refused as a
     * computed leaf operand.
     */
    @Test
    void hasIntersectionWithTheProjectionOnEitherSideEmitsTheSameNestedQuery() {
        Operand projection = expressionOperand("map",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", variableOperand("t.name")));
        Operand values = listValueOperand("a", "b");

        Map<String, Object> projectionFirst = translate(
                expressionOperand("hasIntersection", projection, values));
        assertEquals(Map.of("bool", Map.of("must", List.of(
                        nested("tagObjects", Map.of("terms", Map.of("tagObjects.name", List.of("a", "b")))),
                        mustNot(nested("tagObjects", mustNot(exists("tagObjects.name"))))))),
                projectionFirst);
        assertEquals(projectionFirst, translate(
                expressionOperand("hasIntersection", values, projection)));
    }

    /**
     * <strong>Corpus gap.</strong> A term or range query compares a field against ONE scalar, and
     * CEL's list and map literals mean something else where they appear: whole-list equality
     * ({@code R.attr.tags == ["a"]}), key membership ({@code R.attr.aString in {"a": 1}}), a
     * non-scalar element of a {@code terms} list. Each is valid CEL over a dynamic attribute, and
     * each used to be serialised into a query Elasticsearch would reject or, worse, read as a
     * different predicate. The corpus's only non-scalar equality is {@code map-eq-list}, which is
     * refused earlier as a computed operand.
     */
    @Test
    void aNonScalarLiteralWhereAScalarIsExpectedIsRefused() {
        Operand aString = variableOperand("request.resource.attr.aString");
        Operand aList = listValueOperand("a");
        Operand aMap = valueOperand(struct("a", number(1)));

        for (String operator : List.of("eq", "ne", "lt", "contains", "startsWith", "matches")) {
            IllegalArgumentException ex = refusal(expressionOperand(operator, aString, aList));
            assertTrue(ex.getMessage().contains(operator + " against a list literal"),
                    operator + ": " + ex.getMessage());
            assertInstanceOf(UnsupportedPlanShapeException.class, ex);
        }
        assertTrue(refusal(expressionOperand("eq", aString, aMap)).getMessage()
                .contains("eq against a map literal"));
        // The struct-with-null case is the one that used to surface a NullPointerException.
        assertTrue(refusal(expressionOperand("eq", aString, valueOperand(struct("a", NULL))))
                .getMessage().contains("eq against a map literal"));

        assertTrue(refusal(expressionOperand("in", aString, aMap)).getMessage()
                .contains("CEL map membership tests the map's keys"));
        assertTrue(refusal(expressionOperand("in", aString, valueOperand(list(list(string("a"))))))
                .getMessage().contains("in over a list holding a list element"));
        assertTrue(refusal(expressionOperand("in", aList, variableOperand("request.resource.attr.tags")))
                .getMessage().contains("in with a list element"));
        assertTrue(refusal(expressionOperand("hasIntersection",
                variableOperand("request.resource.attr.tags"), valueOperand(list(struct("a", number(1))))))
                .getMessage().contains("hasIntersection over a list holding a map element"));
    }

    /**
     * <strong>Corpus gap.</strong> {@code hierarchy(R.attr.scope, "")} is a policy an application
     * can write. Splitting on an empty delimiter would produce one segment per character plus a
     * trailing empty one, and a relation over that segmentation is not the relation the policy
     * stated — so the constant is refused by name rather than split into characters. Every corpus
     * hierarchy action uses {@code .} or {@code :}.
     */
    @Test
    void anEmptyHierarchyDelimiterIsRefused() {
        IllegalArgumentException ex = refusal(expressionOperand("descendentOf",
                hierarchy(variableOperand("request.resource.attr.scope"), ""),
                hierarchy(stringValueOperand("a.b"), "")));
        assertTrue(ex.getMessage().contains("hierarchy delimiter is empty"), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
    }
}
