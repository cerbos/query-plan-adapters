package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import org.springframework.data.jpa.domain.Specification;

import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

import com.google.protobuf.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Translates a Cerbos {@code PlanResources} response into a Spring Data JPA
 * {@link Specification} that can be executed by any {@code JpaSpecificationExecutor}.
 *
 * <p>Every {@code toSpecification} overload returns a Specification covering all three plan
 * kinds, so the caller never switches on the kind: {@code KIND_ALWAYS_ALLOWED} yields
 * {@link Specification#unrestricted()} (Spring Data omits the {@code WHERE} clause),
 * {@code KIND_ALWAYS_DENIED} yields an always-false predicate ({@code 1=0}), and a
 * conditional plan yields the translated predicate tree. All three compose with the caller's
 * own Specifications via {@code .and(...)} / {@code .or(...)}. To skip the database entirely
 * on a denied plan, test {@code planResult.isAlwaysDenied()} on the SDK response (or
 * {@code response.getFilter().getKind()} on the raw protobuf) before translating.
 *
 * <p>Hand the Specification to a repository method and let Spring Data invoke it. Calling
 * {@link Specification#toPredicate} yourself is not a supported path.
 *
 * <p>Everything a caller tells the adapter lives in one immutable record, {@link Options}; the
 * positional overloads are that record with the rest left at its defaults. The adapter fails
 * closed: a shape the Criteria API cannot express faithfully throws
 * {@link UnsupportedPlanShapeException} rather than emitting a best-effort filter, an attribute
 * the mapping does not cover throws {@link UnmappedAttributeException}, and a plan that
 * violates the planner's wire contract throws {@link MalformedPlanException}. All three extend
 * {@link IllegalArgumentException}, which remains the documented base type. Translation of a
 * conditional plan is deferred to the Specification's first evaluation, so that is where the
 * three are raised — except the {@link NullAttributeRepresentation#OMITTED} scan, which runs
 * from {@code toSpecification} itself.
 *
 * <p><strong>The returned Specification is SELECT-only.</strong> Never pass it to
 * {@code JpaSpecificationExecutor.delete(Specification)} or any other criteria bulk
 * operation. When the attribute mapping contains {@link AttributeMapping.Relation} entries,
 * the translation builds correlated subqueries over collection/join tables, and Hibernate's
 * multi-table bulk delete first clears those {@code @ElementCollection}/join tables using
 * the same predicate — self-invalidating the correlated subquery so that 0 entity rows are
 * deleted while their collection rows are silently destroyed (which can in turn flip the
 * outcome of ownership/blocklist policies for the surviving rows). The adapter detects the
 * bulk-delete invocation context and throws {@link UnsupportedOperationException} before
 * anything is deleted. To delete policy-permitted rows, select first and delete by id:
 *
 * <pre>{@code
 * Specification<MyEntity> spec = SpringDataQueryPlanAdapter.toSpecification(plan, MAPPING);
 * List<Long> ids = repository.findAll(spec).stream().map(MyEntity::getId).toList();
 * repository.deleteAllById(ids);
 * }</pre>
 */
public final class SpringDataQueryPlanAdapter {

    /**
     * System property bounding collection-macro nesting depth
     * ({@code exists}/{@code exists_one}/{@code all}/{@code filter}/
     * {@code size(filter(...))}) accepted by the translator. Each nesting level multiplies the
     * number of correlated subqueries in the translated filter (×2 for the {@code exists} family,
     * ×3 for {@code exists_one}/{@code size(filter(...))} — see the collection-macro Javadoc in
     * the translator), so unbounded nesting can silently degrade query latency on large tables.
     * Plans nested deeper than the limit throw {@link UnsupportedPlanShapeException} at
     * translation time (fail closed).
     *
     * <p>Precedence, resolved once per translation: a limit declared on the call's
     * {@link Options#withMaxMacroDepth(int) Options} wins; otherwise this property, when set;
     * otherwise {@value #DEFAULT_MAX_MACRO_DEPTH}. The property is the process-wide default
     * for callers that cannot reach every {@code toSpecification} call; a value it holds that is
     * not a positive integer is a configuration error and throws a plain
     * {@link IllegalArgumentException} — it is not a refusal of the plan.
     */
    public static final String MAX_MACRO_DEPTH_PROPERTY =
            "dev.cerbos.queryplan.springdata.maxMacroDepth";

    /** Default value of {@link #MAX_MACRO_DEPTH_PROPERTY}. */
    public static final int DEFAULT_MAX_MACRO_DEPTH = 5;

    /**
     * Everything a caller tells the adapter about the translation.
     *
     * <p>Immutable: every collection is defensively copied on construction, and each
     * {@code with…} method returns a new instance, so an {@code Options} can be built once and
     * shared across calls and threads. Start from {@link #of(Map)} — the mapping is the one
     * declaration every plan needs — and add the rest as the application requires. The
     * positional {@code toSpecification} overloads are exactly this record with the remaining
     * components at their defaults.
     *
     * @param mapping maps each plan variable ({@code request.resource.attr.<name>},
     *        {@code request.resource.id}) to a JPA path or relation — see
     *        {@link AttributeMapping}; a variable the map does not cover throws
     *        {@link UnmappedAttributeException}
     * @param operatorOverrides per-operator replacement translations, keyed by Cerbos operator
     *        name and consulted only for resolved scalar (field, value) leaves — see
     *        {@link OperatorFunction} for exactly which translation sites are (and are not)
     *        overridable
     * @param nullAttributeRepresentation the caller's NULL-column convention for attributes
     *        whose mapping does not declare one — see
     *        {@link SpringDataQueryPlanAdapter#toSpecification(PlanResourcesResult, Map, Map,
     *        NullAttributeRepresentation)}
     * @param maxMacroDepth the collection-macro nesting bound, including literal-collection folds,
     *        for this call. This does not bound the total expression size. Empty falls
     *        back to {@link #MAX_MACRO_DEPTH_PROPERTY} and then {@link #DEFAULT_MAX_MACRO_DEPTH}
     */
    public record Options(
            Map<String, AttributeMapping> mapping,
            Map<String, OperatorFunction> operatorOverrides,
            NullAttributeRepresentation nullAttributeRepresentation,
            OptionalInt maxMacroDepth) {

        public Options {
            mapping = Map.copyOf(Objects.requireNonNull(mapping, "mapping"));
            operatorOverrides = Map.copyOf(
                    Objects.requireNonNull(operatorOverrides, "operatorOverrides"));
            Objects.requireNonNull(nullAttributeRepresentation, "nullAttributeRepresentation");
            Objects.requireNonNull(maxMacroDepth, "maxMacroDepth");
            if (maxMacroDepth.isPresent() && maxMacroDepth.getAsInt() < 1) {
                throw new IllegalArgumentException(
                        "maxMacroDepth must be a positive integer, got " + maxMacroDepth.getAsInt());
            }
        }

        /**
         * Options holding only a mapping: no overrides, the
         * {@link NullAttributeRepresentation#EXPLICIT} convention, and no macro-depth
         * declaration of their own.
         */
        public static Options of(Map<String, AttributeMapping> mapping) {
            return new Options(mapping, Map.of(), NullAttributeRepresentation.EXPLICIT,
                    OptionalInt.empty());
        }

        public Options withMapping(Map<String, AttributeMapping> mapping) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation, maxMacroDepth);
        }

        public Options withOperatorOverrides(Map<String, OperatorFunction> operatorOverrides) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation, maxMacroDepth);
        }

        public Options withNullAttributeRepresentation(
                NullAttributeRepresentation nullAttributeRepresentation) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation, maxMacroDepth);
        }

        /**
         * Bound collection-macro nesting for this call. An explicit value here wins over
         * {@link #MAX_MACRO_DEPTH_PROPERTY}; the property and then
         * {@link #DEFAULT_MAX_MACRO_DEPTH} apply only when none is declared.
         *
         * @param maxMacroDepth a positive integer
         * @throws IllegalArgumentException if {@code maxMacroDepth} is less than 1
         */
        public Options withMaxMacroDepth(int maxMacroDepth) {
            return new Options(mapping, operatorOverrides, nullAttributeRepresentation,
                    OptionalInt.of(maxMacroDepth));
        }

        /**
         * The macro-depth bound in force for a translation: the declared value, else the
         * system property, else the default. Read per translation, like the property always was.
         */
        int effectiveMaxMacroDepth() {
            return maxMacroDepth.orElseGet(SpringDataQueryPlanAdapter::readMaxMacroDepth);
        }
    }

    private SpringDataQueryPlanAdapter() {}

    // -- Options overloads --

    /**
     * Translates a Cerbos query plan (as returned by the Java SDK's
     * {@code CerbosBlockingClient.plan(...)}) into a Spring Data JPA {@link Specification}
     * under the caller's {@link Options}. Every positional overload delegates here.
     *
     * @param <T> the entity type the Specification will be executed against
     * @param planResult the SDK plan result ({@code KIND_ALWAYS_ALLOWED},
     *        {@code KIND_ALWAYS_DENIED}, or a conditional plan)
     * @param options the caller's declarations — mapping, overrides, NULL convention, macro
     *        depth
     * @return a SELECT-only Specification selecting exactly the rows the plan permits — see
     *         {@link #toSpecification(PlanResourcesResult, Map)}
     * @throws MalformedPlanException if the conditional plan carries no condition
     * @throws UnsupportedPlanShapeException if the plan carries a null comparison operand
     *         under {@link NullAttributeRepresentation#OMITTED}; every other refusal is
     *         deferred to the Specification's first evaluation — see the class documentation
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult, Options options) {
        Objects.requireNonNull(options, "options");
        if (planResult.isAlwaysAllowed()) {
            return alwaysAllowed();
        }
        if (planResult.isAlwaysDenied()) {
            return alwaysDenied();
        }
        Operand condition = planResult.getCondition()
                .orElseThrow(() -> Refusals.malformed("Conditional plan has no condition"));
        // Always: the call-level option is only the fallback now, and an attribute can declare
        // OMITTED while the call declares EXPLICIT.
        assertNoNullComparisonOperands(
                condition, options.mapping(), options.nullAttributeRepresentation());
        return conditional(condition, options);
    }

    /**
     * Translates a raw {@link PlanResourcesResponse} protobuf into a Spring Data JPA
     * {@link Specification} under the caller's {@link Options}. Every positional
     * {@code PlanResourcesResponse} overload delegates here.
     *
     * @param <T> the entity type the Specification will be executed against
     * @param response the raw {@code PlanResources} RPC response
     * @param options the caller's declarations — mapping, overrides, NULL convention, macro
     *        depth
     * @return a SELECT-only Specification selecting exactly the rows the plan permits — see
     *         {@link #toSpecification(PlanResourcesResult, Map)}
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter
     *         carries no condition
     * @throws UnsupportedPlanShapeException if the plan carries a null comparison operand
     *         under {@link NullAttributeRepresentation#OMITTED}; every other refusal is
     *         deferred to the Specification's first evaluation — see the class documentation
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response, Options options) {
        Objects.requireNonNull(options, "options");
        PlanResourcesFilter filter = response.getFilter();
        return switch (filter.getKind()) {
            case KIND_ALWAYS_ALLOWED -> alwaysAllowed();
            case KIND_ALWAYS_DENIED -> alwaysDenied();
            case KIND_CONDITIONAL -> {
                Operand cond = filter.getCondition();
                if (cond.getNodeCase() == Operand.NodeCase.NODE_NOT_SET) {
                    throw Refusals.malformed("Conditional plan has no condition");
                }
                assertNoNullComparisonOperands(
                        cond, options.mapping(), options.nullAttributeRepresentation());
                yield conditional(cond, options);
            }
            default -> throw Refusals.malformed("Unknown filter kind: " + filter.getKind());
        };
    }

    // -- PlanResourcesResult overloads --

    /**
     * Translates a Cerbos query plan (as returned by the Java SDK's
     * {@code CerbosBlockingClient.plan(...)}) into a Spring Data JPA
     * {@link Specification}, using the default operator translations.
     *
     * <p>Equivalent to {@link #toSpecification(PlanResourcesResult, Map, Map)} with no
     * operator overrides.
     *
     * <p><strong>The returned Specification is SELECT-only</strong> — see
     * {@link SpringDataQueryPlanAdapter the class documentation} for the corruption
     * mechanism this prevents and the select-ids-then-{@code deleteAllById} alternative.
     *
     * @param <T> the entity type the Specification will be executed against
     * @param planResult the SDK plan result ({@code KIND_ALWAYS_ALLOWED},
     *        {@code KIND_ALWAYS_DENIED}, or a conditional plan)
     * @param mapper maps each plan variable ({@code request.resource.attr.<name>},
     *        {@code request.resource.id}) to a JPA path or relation — see
     *        {@link AttributeMapping}
     * @return a Specification selecting exactly the rows the plan permits — every row for
     *         {@code KIND_ALWAYS_ALLOWED} ({@link Specification#unrestricted()}), no row for
     *         {@code KIND_ALWAYS_DENIED} ({@code 1=0}), the translated predicate tree
     *         otherwise
     * @throws MalformedPlanException if the conditional plan carries no condition.
     *         Translation of the condition itself is deferred: unsupported shapes
     *         ({@link UnsupportedPlanShapeException}), unmapped attributes
     *         ({@link UnmappedAttributeException}) and wire-contract violations
     *         ({@link MalformedPlanException}) throw (fail closed) when the Specification is
     *         first evaluated by the repository, not from this call. All three extend
     *         {@link IllegalArgumentException}.
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult, Map<String, AttributeMapping> mapper) {
        return toSpecification(planResult, mapper, Map.of());
    }

    /**
     * Translates a Cerbos query plan (as returned by the Java SDK's
     * {@code CerbosBlockingClient.plan(...)}) into a Spring Data JPA {@link Specification},
     * consulting {@code overrides} for scalar leaf translations.
     *
     * <p>Prefer this {@link PlanResourcesResult} entry point when using the Cerbos Java SDK
     * client. The {@link #toSpecification(PlanResourcesResponse, Map, Map)} overloads accept
     * the raw protobuf response instead — useful when the response was obtained without the
     * SDK client wrapper (e.g. deserialized, proxied, or hand-built in tests, since
     * {@code PlanResourcesResult} cannot be constructed outside the SDK package).
     *
     * @param <T> the entity type the Specification will be executed against
     * @param planResult the SDK plan result
     * @param mapper maps each plan variable to a JPA path or relation — see
     *        {@link AttributeMapping}
     * @param overrides per-operator replacement translations, keyed by Cerbos operator name;
     *        consulted only for resolved scalar (field, value) leaves — see
     *        {@link OperatorFunction} for exactly which translation sites are (and are not)
     *        overridable
     * @return a SELECT-only Specification selecting exactly the rows the plan permits — see
     *         {@link #toSpecification(PlanResourcesResult, Map)}
     * @throws MalformedPlanException if the conditional plan carries no condition; see
     *         {@link #toSpecification(PlanResourcesResult, Map)} for the deferred
     *         fail-closed contract covering the translation itself
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        return toSpecification(
                planResult, mapper, overrides, NullAttributeRepresentation.EXPLICIT);
    }

    /**
     * Translates a Cerbos query plan into a Spring Data JPA {@link Specification}, declaring
     * how the caller represents a NULL column in the attributes it sends to {@code check()}.
     *
     * <p>The planner emits the same {@code eq(attr, null)} node under both conventions, so the
     * plan cannot reveal which one is in use. Under
     * {@link NullAttributeRepresentation#OMITTED} a NULL column carries no attribute, CEL
     * raises a missing-attribute error, and {@code check()} denies the row — {@code IS NULL}
     * would return exactly the rows the PDP refuses. Every null comparison operand in the plan
     * is therefore rejected eagerly, from this call rather than at Specification evaluation.
     *
     * @param <T> the entity type the Specification will be executed against
     * @param planResult the SDK plan result
     * @param mapper maps each plan variable to a JPA path or relation
     * @param overrides per-operator replacement translations, keyed by Cerbos operator name
     * @param nullAttributeRepresentation the caller's NULL-column convention
     * @return a SELECT-only Specification selecting exactly the rows the plan permits — see
     *         {@link #toSpecification(PlanResourcesResult, Map)}
     * @throws MalformedPlanException if the conditional plan carries no condition
     * @throws UnsupportedPlanShapeException if the plan carries a null comparison operand
     *         under {@link NullAttributeRepresentation#OMITTED}
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResult planResult,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides,
            NullAttributeRepresentation nullAttributeRepresentation) {
        return toSpecification(planResult, Options.of(mapper)
                .withOperatorOverrides(overrides)
                .withNullAttributeRepresentation(nullAttributeRepresentation));
    }

    // -- PlanResourcesResponse overloads --

    /**
     * Translates a raw {@link PlanResourcesResponse} protobuf into a Spring Data JPA
     * {@link Specification}, using the default operator translations.
     *
     * <p>Equivalent to {@link #toSpecification(PlanResourcesResponse, Map, Map)} with no
     * operator overrides. Accepts the wire-level protobuf directly, so it works with
     * responses obtained without the SDK client wrapper; when calling the PDP through the
     * Cerbos Java SDK, the {@link #toSpecification(PlanResourcesResult, Map)} overloads are
     * the natural fit.
     *
     * @param <T> the entity type the Specification will be executed against
     * @param response the raw {@code PlanResources} RPC response
     * @param mapper maps each plan variable to a JPA path or relation — see
     *        {@link AttributeMapping}
     * @return a SELECT-only Specification selecting exactly the rows the plan permits — see
     *         {@link #toSpecification(PlanResourcesResult, Map)}
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter
     *         carries no condition; see {@link #toSpecification(PlanResourcesResult, Map)}
     *         for the deferred fail-closed contract covering the translation itself
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response, Map<String, AttributeMapping> mapper) {
        return toSpecification(response, mapper, Map.of());
    }

    /**
     * Translates a raw {@link PlanResourcesResponse} protobuf into a Spring Data JPA
     * {@link Specification}, consulting {@code overrides} for scalar leaf translations.
     *
     * @param <T> the entity type the Specification will be executed against
     * @param response the raw {@code PlanResources} RPC response
     * @param mapper maps each plan variable to a JPA path or relation — see
     *        {@link AttributeMapping}
     * @param overrides per-operator replacement translations, keyed by Cerbos operator name;
     *        consulted only for resolved scalar (field, value) leaves — see
     *        {@link OperatorFunction} for exactly which translation sites are (and are not)
     *        overridable
     * @return a SELECT-only Specification selecting exactly the rows the plan permits — see
     *         {@link #toSpecification(PlanResourcesResult, Map)}
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter
     *         carries no condition; see {@link #toSpecification(PlanResourcesResult, Map)}
     *         for the deferred fail-closed contract covering the translation itself
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        return toSpecification(
                response, mapper, overrides, NullAttributeRepresentation.EXPLICIT);
    }

    /**
     * Translates a raw {@link PlanResourcesResponse} protobuf into a Spring Data JPA
     * {@link Specification}, declaring how the caller represents a NULL column in the
     * attributes it sends to {@code check()}.
     *
     * <p>See {@link #toSpecification(PlanResourcesResult, Map, Map,
     * NullAttributeRepresentation)} for what the representation changes.
     *
     * @param <T> the entity type the Specification will be executed against
     * @param response the raw {@code PlanResources} RPC response
     * @param mapper maps each plan variable to a JPA path or relation
     * @param overrides per-operator replacement translations, keyed by Cerbos operator name
     * @param nullAttributeRepresentation the caller's NULL-column convention
     * @return a SELECT-only Specification selecting exactly the rows the plan permits — see
     *         {@link #toSpecification(PlanResourcesResult, Map)}
     * @throws MalformedPlanException if the filter kind is unknown or a conditional filter
     *         carries no condition
     * @throws UnsupportedPlanShapeException if the plan carries a null comparison operand
     *         under {@link NullAttributeRepresentation#OMITTED}
     */
    public static <T> Specification<T> toSpecification(
            PlanResourcesResponse response,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides,
            NullAttributeRepresentation nullAttributeRepresentation) {
        return toSpecification(response, Options.of(mapper)
                .withOperatorOverrides(overrides)
                .withNullAttributeRepresentation(nullAttributeRepresentation));
    }

    // -- The three plan kinds --

    /**
     * {@code KIND_ALWAYS_ALLOWED} — Spring Data's own "no restriction" Specification, whose
     * implementation is {@code (root, query, cb) -> null}. {@code SimpleJpaRepository} guards
     * with {@code if (predicate != null) query.where(predicate)}, so no {@code WHERE 1=1} is
     * emitted, and it is a true identity for {@code .and(...)} / {@code .or(...)}. Requires
     * spring-data-jpa 3.5.2 or later.
     */
    private static <T> Specification<T> alwaysAllowed() {
        return Specification.unrestricted();
    }

    /** {@code KIND_ALWAYS_DENIED} — an always-false predicate ({@code 1=0}). */
    private static <T> Specification<T> alwaysDenied() {
        return (root, query, cb) -> cb.disjunction();
    }

    /**
     * {@code KIND_CONDITIONAL} — a Specification whose lambda rebuilds the entire predicate
     * tree from the {@code Root}/{@code CriteriaQuery} it is handed, on every invocation. That
     * is required, not incidental: {@code JpaSpecificationExecutor.findAll(spec, Pageable)}
     * fires a separate {@code COUNT} query with its own {@code CriteriaQuery} and {@code Root},
     * and Hibernate 6 rejects a {@code Predicate} built against a different {@code Root}
     * ({@code SqlTreeCreationException: Could not locate TableGroup}).
     *
     * <p>The caller's maps were defensively copied when {@link Options} was built, because of
     * that re-invocation: capturing them by reference would let post-translation mutation
     * silently change which columns the authorization filter resolves.
     */
    private static <T> Specification<T> conditional(Operand condition, Options options) {
        return (root, query, cb) ->
                new PlanWalker(cb, options, isSelectInvocation(root, query))
                        .traverse(condition, Scope.root(root, query, options.mapping()));
    }

    // -- NULL representation guard --

    /**
     * Rejects every null literal operand in the plan under
     * {@link NullAttributeRepresentation#OMITTED}.
     *
     * <p>A NULL column then carries no attribute at all, so CEL raises a missing-attribute
     * error and {@code check()} denies the row — {@code IS NULL} would return exactly the rows
     * the PDP refuses (cerbos/query-plan-adapters#302).
     *
     * <p>The scan runs over the plan tree rather than at each emission site because the
     * translator lowers a null constant to {@code IS NULL} from several places (scalar leaf,
     * scalar {@code in} with null elements, relation membership, {@code hasIntersection}). For
     * the same reason it matches on the OPERAND and never on an allowlist of operators: a null
     * constant reaches a NULL-selecting predicate through more shapes than the obvious
     * {@code eq}/{@code ne}/{@code in}, and any operator added later would silently escape a
     * list that has to be maintained by hand.
     *
     * <p>The rejection is also deliberately wider than the over-granting shapes:
     * {@code ne(x, null)} on its own is aligned, but negation is applied around the built
     * predicate rather than pushed into the leaf, so a leaf cannot tell whether an enclosing
     * {@code not} will flip {@code IS NOT NULL} back into a NULL-selecting predicate. Rejecting
     * every null operand is correct under any nesting; narrowing it requires negation-parity
     * tracking.
     */
    private static void assertNoNullComparisonOperands(
            Operand operand, Map<String, AttributeMapping> mapper,
            NullAttributeRepresentation fallback) {
        if (operand.getNodeCase() != Operand.NodeCase.EXPRESSION) {
            return;
        }
        var expression = operand.getExpression();
        List<Operand> operands = expression.getOperandsList();

        // A comparison between a mapped attribute and a literal is decided by that attribute's
        // own declaration, which is what lets one call carry both conventions (#308). Confined
        // to that shape: a null buried in a macro over a literal list reaches a comparison long
        // after this scan, and nothing here can say which column it will land against, so those
        // keep using the call-level fallback.
        NullAttributeRepresentation declared =
                declaredForComparedAttribute(expression.getOperator(), operands, mapper);
        if (declared != null) {
            if (declared == NullAttributeRepresentation.OMITTED
                    && operands.stream().anyMatch(SpringDataQueryPlanAdapter::carriesNull)) {
                throw nullOperandUnderOmitted(expression.getOperator());
            }
            return;
        }

        if (fallback == NullAttributeRepresentation.OMITTED
                && operands.stream().anyMatch(SpringDataQueryPlanAdapter::carriesNull)) {
            throw nullOperandUnderOmitted(expression.getOperator());
        }
        operands.forEach(child -> assertNoNullComparisonOperands(child, mapper, fallback));
    }

    private static final Set<String> EQUALITY_FAMILY = Set.of("eq", "ne", "in");

    private static UnsupportedPlanShapeException nullOperandUnderOmitted(String operator) {
        return Refusals.nullOperandUnderOmitted(operator);
    }

    /**
     * The declared NULL convention of the attribute a binary comparison names, or {@code null}
     * when the node is not a comparison between one mapped attribute and one literal.
     */
    private static NullAttributeRepresentation declaredForComparedAttribute(
            String operator, List<Operand> operands, Map<String, AttributeMapping> mapper) {
        // The operators CEL evaluates to a definite boolean over a null value, and so the only
        // ones an attribute's declared convention can settle. Anything else — a collection macro,
        // hasIntersection, a string match — keeps using the call-level fallback, because the
        // declaration says nothing about what its null means there.
        if (!EQUALITY_FAMILY.contains(operator) || operands.size() != 2) {
            return null;
        }
        Operand left = operands.get(0);
        Operand right = operands.get(1);
        Operand variable = left;
        Operand literal = right;
        if (right.getNodeCase() == Operand.NodeCase.VARIABLE) {
            variable = right;
            literal = left;
        }
        if (variable.getNodeCase() != Operand.NodeCase.VARIABLE
                || literal.getNodeCase() != Operand.NodeCase.VALUE) {
            return null;
        }
        return mapper.get(variable.getVariable()) instanceof AttributeMapping.Field f
                ? f.nullAttributeRepresentation()
                : null;
    }

    private static boolean carriesNull(Operand operand) {
        if (operand.getNodeCase() != Operand.NodeCase.VALUE) {
            return false;
        }
        Value value = operand.getValue();
        return switch (value.getKindCase()) {
            case NULL_VALUE -> true;
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                    .anyMatch(element -> element.getKindCase() == Value.KindCase.NULL_VALUE);
            default -> false;
        };
    }

    // -- Evaluation context --

    /**
     * Detects whether the Specification is being evaluated for the {@code SELECT} query it was
     * handed: in every Spring Data SELECT path ({@code findAll}/{@code findOne}/{@code count}/
     * {@code exists}/pagination) the {@code Root} is created via {@code query.from(...)}, so it is
     * a member of {@code query.getRoots()}. In {@code SimpleJpaRepository.delete(Specification)}
     * the {@code Root} comes from a {@code CriteriaDelete} while the {@code CriteriaQuery}
     * argument is a fresh throwaway {@code createQuery(cls)} whose root set does not contain it
     * (and newer Spring Data versions pass {@code null} for the query). Correlated subqueries are
     * only sound in the first case — see {@link ChainSubqueries#chainSubquery}.
     */
    private static boolean isSelectInvocation(Root<?> root, CriteriaQuery<?> query) {
        return query != null && query.getRoots().contains(root);
    }

    /**
     * {@link #MAX_MACRO_DEPTH_PROPERTY} as an integer, or the default when unset. A value that
     * is not a positive integer is a misconfigured JVM, not a plan the adapter refuses, so it is
     * a plain {@link IllegalArgumentException} rather than one of the {@link Refusals}.
     */
    private static int readMaxMacroDepth() {
        String raw = System.getProperty(MAX_MACRO_DEPTH_PROPERTY);
        if (raw == null) {
            return DEFAULT_MAX_MACRO_DEPTH;
        }
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    MAX_MACRO_DEPTH_PROPERTY + " must be a positive integer, got '" + raw + "'", e);
        }
        if (value < 1) {
            throw new IllegalArgumentException(
                    MAX_MACRO_DEPTH_PROPERTY + " must be a positive integer, got " + value);
        }
        return value;
    }
}
