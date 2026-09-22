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
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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

    /**
     * The CEL type of every scalar field above, keyed by Elasticsearch field name. A flat array
     * declares its element type, and a nested document's sub-field is declared by its full path.
     */
    private static final Map<String, ScalarType> SCALAR_TYPES = Map.ofEntries(
            Map.entry("department", ScalarType.STRING),
            Map.entry("aBool", ScalarType.BOOLEAN),
            Map.entry("aString", ScalarType.STRING),
            Map.entry("aNumber", ScalarType.NUMBER),
            Map.entry("title", ScalarType.STRING),
            Map.entry("scope", ScalarType.STRING),
            Map.entry("tags", ScalarType.STRING),
            Map.entry("ownedBy", ScalarType.STRING),
            Map.entry("tagObjects.name", ScalarType.STRING));

    private static final Options OPTIONS = Options.of(FIELD_MAP)
            .withNestedPaths(NESTED_PATHS)
            .withCollectionFields(COLLECTION_FIELDS)
            .withScalarTypes(SCALAR_TYPES);

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
     * CEL has no such function, so no policy compiles to either operator: the checker raises
     * {@code undeclared reference to 'unsupported_op' (in container '')} and the planner never
     * runs. {@code isSet} is the one worth naming: it is not a registered CEL function either
     * ({@code undeclared reference to 'isSet'}), so it can never reach the wire
     * (cerbos/query-plan-adapters#261), and existence is spelled {@code R.attr.x != null}, which
     * the corpus carries as {@code null-ne} and this adapter lowers to {@code exists}.
     */
    @ParameterizedTest
    @ValueSource(strings = {"unsupported_op", "isSet"})
    void anOperatorTheAdapterDoesNotKnowIsRefusedByName(String operator) {
        IllegalArgumentException ex = refusal(expressionOperand(operator,
                variableOperand("request.resource.attr.department"), stringValueOperand("value")));
        assertTrue(ex.getMessage().contains("Unknown operator: " + operator), ex.getMessage());
        assertInstanceOf(UnsupportedPlanShapeException.class, ex);
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
                Options.of(FIELD_MAP).withScalarTypes(SCALAR_TYPES));

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
        assertEquals(Set.of(), options.nestedPaths());
        assertEquals(NESTED_PATHS, widened.nestedPaths());
        assertEquals(Set.of(), widened.collectionFields());
        assertEquals(Set.of(), widened.explicitNullAttributes());
        assertEquals(Map.of(), widened.operatorOverrides());
        Map<String, ElasticsearchQueryPlanAdapter.ScalarType> types = new HashMap<>();
        types.put("aNumber", ElasticsearchQueryPlanAdapter.ScalarType.NUMBER);
        Options typed = options.withScalarTypes(types);
        types.clear();
        assertEquals(ElasticsearchQueryPlanAdapter.ScalarType.NUMBER, typed.scalarTypes().get("aNumber"));
        assertEquals(typed.scalarTypes(), typed.withNestedPaths(NESTED_PATHS).scalarTypes());
        assertThrows(UnsupportedOperationException.class, () -> typed.scalarTypes().clear());
        assertThrows(NullPointerException.class, () -> options.withScalarTypes(null));
        assertThrows(NullPointerException.class, () -> Options.of(null));
        assertThrows(NullPointerException.class, () -> options.withCollectionFields(null));

        // The positional overloads carry no scalar types, so the comparison goes through an
        // `eq` override, which owns the operator and needs no declaration.
        Map<String, OperatorFunction> overrides = Map.of(
                "eq", (field, value) -> Map.of("custom_eq", Map.of(field, value)));
        Operand condition = expressionOperand("exists",
                variableOperand("request.resource.attr.tagObjects"),
                lambdaOperand("t", expressionOperand("eq",
                        variableOperand("t.name"), stringValueOperand("public"))));
        assertEquals(
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), FIELD_MAP, overrides, NESTED_PATHS, Set.of()),
                ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
                        conditionalPlan(condition), widened.withOperatorOverrides(overrides)));
    }

    /**
     * {@code scalarTypes} is a caller argument: the corpus translates through one declaration that
     * covers its whole mapping, so no corpus action can leave a field undeclared. Undeclared, a
     * comparison is refused rather than lowered untyped, because Elasticsearch coerces a query
     * term onto the field's mapped type — {@code "5"} matches the integer {@code 5} — where CEL's
     * cross-type equality is false and {@code check()} denies the row
     * (cerbos/query-plan-adapters#496). Every operator whose default lowering is a term, terms,
     * range, prefix, wildcard or regexp query needs the declaration, at the top level and inside
     * a lambda; an override owns its operator, so it needs none.
     */
    @Test
    void aComparisonAgainstAnUndeclaredFieldIsRefusedRatherThanCoerced() {
        Options untyped = OPTIONS.withScalarTypes(Map.of());
        List<Operand> comparisons = List.of(
                expressionOperand("eq",
                        variableOperand("request.resource.attr.aNumber"), stringValueOperand("5")),
                expressionOperand("not", expressionOperand("ne",
                        variableOperand("request.resource.attr.aNumber"), stringValueOperand("5"))),
                expressionOperand("lt",
                        numberValueOperand(5), variableOperand("request.resource.attr.aString")),
                expressionOperand("startsWith",
                        variableOperand("request.resource.attr.aNumber"), stringValueOperand("5")),
                expressionOperand("in",
                        variableOperand("request.resource.attr.aBool"), listValueOperand("true")),
                expressionOperand("exists",
                        variableOperand("request.resource.attr.tagObjects"),
                        lambdaOperand("t", expressionOperand("eq",
                                variableOperand("t.name"), numberValueOperand(5)))));
        for (Operand comparison : comparisons) {
            IllegalArgumentException ex = refusal(comparison, untyped);
            assertInstanceOf(UnmappedAttributeException.class, ex);
            assertTrue(ex.getMessage().contains("has no declared scalar type"), ex.getMessage());
            assertTrue(ex.getMessage().contains("Options.withScalarTypes"), ex.getMessage());
        }
        assertTrue(refusal(comparisons.get(5), untyped).getMessage()
                .startsWith("Field 'tagObjects.name' has no declared scalar type"));

        Options overridden = untyped.withOperatorOverrides(Map.of(
                "eq", (field, value) -> Map.of("custom_eq", Map.of(field, value))));
        assertEquals(Map.of("custom_eq", Map.of("aNumber", "5")),
                translate(comparisons.get(0), overridden));
    }

    /**
     * What a declaration buys: a literal the declared type cannot inhabit is answered by CEL's
     * semantics rather than by Elasticsearch's coercion. Cross-type equality is false, so
     * {@code ==} matches nothing and {@code !=} holds wherever the field is present; membership is
     * equality against each element, so an element of the wrong type is dropped from the
     * {@code terms} list, and a list with none of the right type is false.
     */
    @Test
    void aDeclaredTypeAnswersACrossTypeComparisonAsCelDoes() {
        Map<String, Object> matchNone = Map.of("match_none", Map.of());
        Operand eqString = expressionOperand("eq",
                variableOperand("request.resource.attr.aNumber"), stringValueOperand("5"));
        assertEquals(matchNone, translate(eqString));
        assertEquals(exists("aNumber"), translate(expressionOperand("not", eqString)));
        assertEquals(exists("aNumber"), translate(expressionOperand("ne",
                variableOperand("request.resource.attr.aNumber"), stringValueOperand("5"))));

        Operand mixedMembership = expressionOperand("in",
                variableOperand("request.resource.attr.aNumber"),
                valueOperand(list(string("5"), number(6))));
        assertEquals(Map.of("terms", Map.of("aNumber", List.of(6L))), translate(mixedMembership));
        assertEquals(definedAndNot("aNumber", Map.of("terms", Map.of("aNumber", List.of(6L)))),
                translate(expressionOperand("not", mixedMembership)));

        Operand wrongTypeMembership = expressionOperand("in",
                variableOperand("request.resource.attr.aBool"), listValueOperand("true"));
        assertEquals(matchNone, translate(wrongTypeMembership));
        assertEquals(exists("aBool"), translate(expressionOperand("not", wrongTypeMembership)));
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

    /**
     * The numeric wire-value decoder has a range. A double outside
     * {@code [-2^63, 2^63)} is integral too, but a {@code long} cannot hold it: the cast saturates
     * to {@code Long.MAX_VALUE} and silently changes the operand. Such a literal stays a double;
     * the two ends of the range are pinned, {@code -2^63} narrowing and {@code 2^63} not. The
     * corpus covers a negative out-of-range literal in {@code double-huge-lt}/{@code double-huge-gt},
     * but not positive equality or the exact narrowing boundaries.
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
     * A caller-supplied protobuf value can hold a non-finite number. The pinned
     * PDP cannot serialize such a literal (the div-by-nan planning probe returns HTTP 500),
     * so this is a wire-value validation contract rather than a policy corpus gap. JSON has no representation for
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

    // ============================================================================================
    // KIND 3 — a policy can reach these, and the corpus does not carry them yet
    //
    // Every test here is a corpus gap, tracked by cerbos/query-plan-adapters#414, and is deleted
    // when its corpus action lands. They are NOT covered by #387 or #388, whose actions are
    // enumerated and landed.
    // ============================================================================================

    /**
     * <strong>Corpus gap.</strong> The corpus covers literal exists_one and filter/map as
     * computed operands. Direct filter/map in boolean position over a principal value list
     * remains a separate arrival position, which must name the list-valued result.
     */
    @ParameterizedTest
    @ValueSource(strings = {"filter", "map"})
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
     * operand, and inside a nested lambda scope. The corpus carries the root, size and comparison forms; the remaining
     * bridge pins the negated root and the nested lambda arrival positions.
     */
    @Test
    void exceptIsRefusedByNameWhereverItAppears() {
        Operand except = expressionOperand("except",
                variableOperand("request.resource.attr.tags"), listValueOperand("archived"));
        List<Operand> positions = List.of(
                expressionOperand("not", except),
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
     * <strong>Corpus gap.</strong> size-ge-one covers {@code >= 1}; this bridge retains
     * the direct {@code size(c) != 0} spelling and its negation over a flat collection.
     * Negation requires distinguishing an empty indexed array from a missing field.
     */
    @Test
    void everySpellingOfNonEmptinessIsTheSameCheck() {
        Operand notEqualZero = expressionOperand("ne",
                sizeOf("request.resource.attr.ownedBy"), numberValueOperand(0));
        assertEquals(exists("ownedBy"), translate(notEqualZero));
        IllegalArgumentException ex = refusal(expressionOperand("not", notEqualZero));
        assertTrue(ex.getMessage().contains("emptiness cannot distinguish a missing collection"),
                ex.getMessage());
    }

    /**
     * <strong>Corpus gap.</strong> The corpus covers both flat and map-projection operand
     * orders. A null-bearing intersection inside a nested lambda still exercises a separate
     * scoped arrival position.
     */
    @Test
    void hasIntersectionWithANullElementIsRefusedWhicheverPositionCarriesIt() {
        Operand withNull = valueOperand(list(string("public"), NULL));
        List<Operand> positions = List.of(
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
     * <strong>Corpus gap.</strong> The corpus now covers eq/ne list comparisons and real
     * struct()/list() operands. Ordering and string operations against a list remain bridges.
     * The raw protobuf map/list value encodings below additionally pin decoder refusals: the
     * corresponding real planner fixtures instead carry struct()/list() expressions.
     */
    @Test
    void aNonScalarLiteralWhereAScalarIsExpectedIsRefused() {
        Operand aString = variableOperand("request.resource.attr.aString");
        Operand aList = listValueOperand("a");
        Operand aMap = valueOperand(struct("a", number(1)));

        for (String operator : List.of("lt", "contains", "startsWith", "matches")) {
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
}
