package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;

import java.util.List;
import java.util.Map;

/**
 * Translates a Cerbos {@code PlanResources} response into a Spring Data JPA
 * {@link org.springframework.data.jpa.domain.Specification} that can be executed by any
 * {@code JpaSpecificationExecutor}.
 */
public final class SpringDataQueryPlanAdapter {

    private SpringDataQueryPlanAdapter() {}

    // -- PlanResourcesResult overloads --

    public static <T> Result<T> toSpecification(
            PlanResourcesResult planResult, Map<String, AttributeMapping> mapper) {
        return toSpecification(planResult, mapper, Map.of());
    }

    public static <T> Result<T> toSpecification(
            PlanResourcesResult planResult,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        if (planResult.isAlwaysAllowed()) {
            return new Result.AlwaysAllowed<>();
        }
        if (planResult.isAlwaysDenied()) {
            return new Result.AlwaysDenied<>();
        }
        Operand condition = planResult.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Conditional plan has no condition"));
        return new Result.Conditional<>((root, query, cb) ->
                new Translator(cb, overrides).traverse(condition, Scope.root(root, query, mapper)));
    }

    // -- PlanResourcesResponse overloads --

    public static <T> Result<T> toSpecification(
            PlanResourcesResponse response, Map<String, AttributeMapping> mapper) {
        return toSpecification(response, mapper, Map.of());
    }

    public static <T> Result<T> toSpecification(
            PlanResourcesResponse response,
            Map<String, AttributeMapping> mapper,
            Map<String, OperatorFunction> overrides) {
        PlanResourcesFilter filter = response.getFilter();
        return switch (filter.getKind()) {
            case KIND_ALWAYS_ALLOWED -> new Result.AlwaysAllowed<>();
            case KIND_ALWAYS_DENIED -> new Result.AlwaysDenied<>();
            case KIND_CONDITIONAL -> {
                Operand cond = filter.getCondition();
                if (cond.getNodeCase() == Operand.NodeCase.NODE_NOT_SET) {
                    throw new IllegalArgumentException("Conditional plan has no condition");
                }
                yield new Result.Conditional<T>((root, query, cb) ->
                        new Translator(cb, overrides).traverse(cond, Scope.root(root, query, mapper)));
            }
            default -> throw new IllegalArgumentException("Unknown filter kind: " + filter.getKind());
        };
    }

    // -- Internal translator --

    private static final class Translator {
        private final CriteriaBuilder cb;
        private final Map<String, OperatorFunction> overrides;
        private final HierarchyTranslator hierarchy;

        Translator(CriteriaBuilder cb, Map<String, OperatorFunction> overrides) {
            this.cb = cb;
            this.overrides = overrides;
            this.hierarchy = new HierarchyTranslator(cb);
        }

        Predicate traverse(Operand operand, Scope scope) {
            return switch (operand.getNodeCase()) {
                case EXPRESSION -> traverseExpression(operand.getExpression(), scope);
                case VARIABLE -> handleBareVariable(operand.getVariable(), scope);
                default -> throw new IllegalArgumentException("Unexpected operand type: " + operand.getNodeCase());
            };
        }

        private Predicate handleBareVariable(String variable, Scope scope) {
            Path<?> path = scope.resolvePath(variable);
            return applyLeaf("eq", path, true);
        }

        /**
         * Logical negation with a junction barrier. Hibernate 6's SQM negation is stateful for
         * comparison predicates: {@code cb.not(cb.not(p))} stays negated instead of toggling
         * back (verified against Hibernate 6.6.18 — a double-negated {@code eq} still renders
         * a single {@code NOT}). Wrapping in a single-element conjunction gives each {@code not}
         * a fresh node to negate, so nested negations compose correctly.
         */
        private Predicate negate(Predicate p) {
            return cb.not(cb.and(p));
        }

        private Predicate traverseExpression(PlanResourcesFilter.Expression expression, Scope scope) {
            String op = expression.getOperator();
            List<Operand> operands = expression.getOperandsList();

            return switch (op) {
                case "and" -> cb.and(operands.stream()
                        .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
                case "or" -> cb.or(operands.stream()
                        .map(o -> traverse(o, scope)).toArray(Predicate[]::new));
                case "not" -> {
                    if (operands.size() != 1) {
                        throw new IllegalArgumentException("not requires exactly 1 operand");
                    }
                    yield negate(traverse(operands.get(0), scope));
                }
                case "exists", "exists_one", "all", "except", "filter" ->
                        handleCollectionOperator(op, operands, scope);
                // has_intersection is the deprecated pre-camelCase alias still accepted by the PDP.
                case "hasIntersection", "has_intersection" -> handleHasIntersection(operands, scope);
                case "isSet" -> handleIsSet(operands, scope);
                case "in" -> handleIn(operands, scope);
                case "overlaps" -> hierarchy.handleOverlaps(operands, scope);
                case "ancestorOf" -> hierarchy.handleAncestorDescendant(operands, scope, true);
                case "descendentOf" -> hierarchy.handleAncestorDescendant(operands, scope, false);
                default -> {
                    NormalizedBinary nb = NormalizedBinary.of(op, operands);
                    Predicate sizePred = trySizeComparison(nb.op(), nb.operands(), scope);
                    if (sizePred != null) {
                        yield sizePred;
                    }
                    yield handleLeafOperator(nb.op(), nb.operands(), scope);
                }
            };
        }

        /**
         * A binary expression normalized to field-side-first. The planner preserves policy source
         * order, so a constant may precede the field it constrains ({@code 5 < R.attr.x} arrives
         * as {@code lt(value(5), variable(x))}). Normalizing once here — most field-like operand
         * first (variable > nested expression > constant value), mirroring directional operators
         * when swapping — lets every downstream handler assume field-first order. A consequence
         * is that {@link OperatorFunction} overrides are consulted under the mirrored operator:
         * a value-first {@code lt} is looked up as {@code gt}.
         */
        private record NormalizedBinary(String op, List<Operand> operands) {

            static NormalizedBinary of(String op, List<Operand> operands) {
                if (operands.size() == 2 && rank(operands.get(0)) < rank(operands.get(1))) {
                    return new NormalizedBinary(mirror(op), List.of(operands.get(1), operands.get(0)));
                }
                return new NormalizedBinary(op, operands);
            }

            private static int rank(Operand o) {
                return switch (o.getNodeCase()) {
                    case VARIABLE -> 2;
                    case EXPRESSION -> 1;
                    default -> 0;
                };
            }

            /** lt/le/gt/ge mirror when their operands swap sides; symmetric operators are unchanged. */
            private static String mirror(String op) {
                return switch (op) {
                    case "lt" -> "gt";
                    case "gt" -> "lt";
                    case "le" -> "ge";
                    case "ge" -> "le";
                    default -> op;
                };
            }
        }

        // -- Leaf operators (eq/ne/lt/gt/le/ge/contains/startsWith/endsWith) --

        /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
        private Predicate handleLeafOperator(String op, List<Operand> operands, Scope scope) {
            // Detect leaf comparisons where one side is an 'add' expression (e.g. string
            // concatenation: `aString == "prefix:" + R.attr.id`). We fold constants and solve for
            // the field side when possible — same algorithm as the Prisma adapter.
            Operand addExprOperand = null;
            Operand otherOperand = null;
            for (Operand o : operands) {
                if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                        && "add".equals(o.getExpression().getOperator())) {
                    addExprOperand = o;
                } else {
                    otherOperand = o;
                }
            }
            if (addExprOperand != null) {
                if (otherOperand == null) {
                    throw new IllegalArgumentException("add comparison requires a second operand");
                }
                return handleAddComparison(op, addExprOperand.getExpression(), otherOperand, scope);
            }

            String variable = null;
            Object value = null;
            boolean valueSeen = false;
            for (Operand o : operands) {
                switch (o.getNodeCase()) {
                    case VARIABLE -> {
                        if (variable != null) {
                            // H1: field-to-field comparison is not expressible in JPA Criteria as a
                            // value-bound predicate. Surface this explicitly rather than the generic
                            // "Missing value operand" message the loop would otherwise produce.
                            throw new IllegalArgumentException(
                                    "Field-to-field comparison is not supported for operator '"
                                            + op + "': " + variable + " vs " + o.getVariable());
                        }
                        variable = o.getVariable();
                    }
                    case VALUE -> {
                        value = PlanValues.protoValueToJava(o.getValue());
                        valueSeen = true;
                    }
                    case EXPRESSION -> {
                        // H3: map() compositions are only accepted inside hasIntersection.
                        // A direct comparison like eq(map(...), [...]) reaches here; point users
                        // at the supported shape rather than throwing a generic operand error.
                        String innerOp = o.getExpression().getOperator();
                        if ("map".equals(innerOp)) {
                            throw new IllegalArgumentException(
                                    "Direct comparison of map(...) to a value is not supported "
                                            + "(operator: " + op + "). Wrap the map() expression in "
                                            + "hasIntersection(map(...), [...]) instead.");
                        }
                        throw new IllegalArgumentException(
                                "Unexpected " + innerOp + "() expression in leaf operand of " + op);
                    }
                    default -> throw new IllegalArgumentException(
                            "Unexpected operand type in leaf expression: " + o.getNodeCase());
                }
            }
            if (variable == null) {
                throw new IllegalArgumentException("Missing variable operand for " + op);
            }
            if (!valueSeen) {
                throw new IllegalArgumentException("Missing value operand for " + op);
            }

            Path<?> path = scope.resolvePath(variable);

            if (value == null) {
                // A registered override owns the operator's full translation, including a null RHS.
                OperatorFunction override = overrides.get(op);
                if (override != null) {
                    return override.apply(cb, path, null);
                }
                return switch (op) {
                    case "eq" -> cb.isNull(path);
                    case "ne" -> cb.isNotNull(path);
                    default -> throw new IllegalArgumentException(
                            "Null values are only supported with eq and ne operators (got " + op + ")");
                };
            }

            return applyLeaf(op, path, value);
        }

        /**
         * Apply a scalar leaf operator, consulting the per-operator {@code overrides} hook first so a
         * registered {@link OperatorFunction} wins on EVERY path that produces this operator — direct
         * comparison, {@code add}-folded comparison, and bare-boolean — not just the direct one.
         */
        private Predicate applyLeaf(String op, Path<?> path, Object value) {
            OperatorFunction override = overrides.get(op);
            if (override != null) {
                return override.apply(cb, path, value);
            }
            return defaultLeaf(op, path, value);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Predicate defaultLeaf(String op, Path<?> path, Object value) {
            // Fractional constants compare in double space: protoValueToJava yields Double only
            // for non-whole numbers, and Hibernate refuses to coerce e.g. 1.5 into an
            // Integer-typed path ("not a whole number") — but `intColumn >= 1.5` is legal CEL
            // that the planner emits verbatim.
            jakarta.persistence.criteria.Expression raw =
                    (value instanceof Double) ? path.as(Double.class) : path;
            return switch (op) {
                case "eq" -> cb.equal(raw, value);
                case "ne" -> cb.notEqual(raw, value);
                case "lt" -> cb.lessThan(raw, (Comparable) value);
                case "gt" -> cb.greaterThan(raw, (Comparable) value);
                case "le" -> cb.lessThanOrEqualTo(raw, (Comparable) value);
                case "ge" -> cb.greaterThanOrEqualTo(raw, (Comparable) value);
                case "contains" -> cb.like(path.as(String.class),
                        "%" + PlanValues.escapeLike(String.valueOf(value)) + "%", '\\');
                case "startsWith" -> cb.like(path.as(String.class),
                        PlanValues.escapeLike(String.valueOf(value)) + "%", '\\');
                case "endsWith" -> cb.like(path.as(String.class),
                        "%" + PlanValues.escapeLike(String.valueOf(value)), '\\');
                default -> throw new IllegalArgumentException("Unsupported operator: " + op);
            };
        }

        // -- add (fold + solve for string concat / numeric translation) --

        /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
        private Predicate handleAddComparison(String op, PlanResourcesFilter.Expression addExpr,
                                              Operand otherOperand, Scope scope) {
            List<Operand> addOperands = addExpr.getOperandsList();
            if (addOperands.size() != 2) {
                throw new IllegalArgumentException("add requires exactly 2 operands");
            }
            Operand addLeft = addOperands.get(0);
            Operand addRight = addOperands.get(1);

            // Case 1: add(value, value) — fold the two constants, then compare to the field.
            // Normalization guarantees the field variable sits on the left of the comparison,
            // so the folded constant compares as `field op folded`.
            if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                    && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
                Object folded = PlanValues.foldAdd(
                        PlanValues.protoValueToJava(addLeft.getValue()),
                        PlanValues.protoValueToJava(addRight.getValue()));
                if (otherOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                    throw new IllegalArgumentException(
                            "add(const, const) compared to a non-field operand is not supported");
                }
                Path<?> path = scope.resolvePath(otherOperand.getVariable());
                return applyLeaf(op, path, folded);
            }

            // Case 2: add(field, value) or add(value, field) — solve for the field.
            // Only eq/ne are supported; lt/gt/etc. against a synthesized expression would require
            // emitting more complex predicates we don't try to support here.
            if (!"eq".equals(op) && !"ne".equals(op)) {
                throw new IllegalArgumentException(
                        "add comparison with a field reference only supports eq/ne (got " + op + ")");
            }
            if (otherOperand.getNodeCase() != Operand.NodeCase.VALUE) {
                throw new IllegalArgumentException(
                        "add(field, value) requires a value on the other side of the comparison");
            }
            Object otherValue = PlanValues.protoValueToJava(otherOperand.getValue());

            Operand fieldOp;
            Object addConst;
            boolean fieldIsLeft;
            if (addLeft.getNodeCase() == Operand.NodeCase.VARIABLE
                    && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
                fieldOp = addLeft;
                addConst = PlanValues.protoValueToJava(addRight.getValue());
                fieldIsLeft = true;
            } else if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                    && addRight.getNodeCase() == Operand.NodeCase.VARIABLE) {
                fieldOp = addRight;
                addConst = PlanValues.protoValueToJava(addLeft.getValue());
                fieldIsLeft = false;
            } else {
                throw new IllegalArgumentException(
                        "add requires exactly one field reference and one value, or two values");
            }

            Object solved = PlanValues.solveAdd(otherValue, addConst, fieldIsLeft);
            if (solved == null) {
                // No solution exists (e.g. "projects:123" == "users:" + R.id can never be true).
                // eq → always-false; ne → always-true.
                return "eq".equals(op) ? cb.disjunction() : cb.conjunction();
            }
            Path<?> path = scope.resolvePath(fieldOp.getVariable());
            return applyLeaf(op, path, solved);
        }

        // -- isSet --

        private Predicate handleIsSet(List<Operand> operands, Scope scope) {
            if (operands.size() != 2) {
                throw new IllegalArgumentException("isSet requires exactly 2 operands");
            }
            String variable = null;
            Boolean flag = null;
            for (Operand o : operands) {
                if (o.getNodeCase() == Operand.NodeCase.VARIABLE) variable = o.getVariable();
                else if (o.getNodeCase() == Operand.NodeCase.VALUE) {
                    Object v = PlanValues.protoValueToJava(o.getValue());
                    if (!(v instanceof Boolean b)) {
                        throw new IllegalArgumentException("isSet second operand must be a boolean");
                    }
                    flag = b;
                }
            }
            if (variable == null || flag == null) {
                throw new IllegalArgumentException("Invalid isSet operands");
            }
            Path<?> path = scope.resolvePath(variable);
            OperatorFunction override = overrides.get("isSet");
            if (override != null) {
                return override.apply(cb, path, flag);
            }
            return flag ? cb.isNotNull(path) : cb.isNull(path);
        }

        // -- in (set membership or collection membership) --

        private Predicate handleIn(List<Operand> rawOperands, Scope scope) {
            if (rawOperands.size() != 2) {
                throw new IllegalArgumentException("in requires exactly 2 operands");
            }
            // Both shapes — `field in [values]` and `value in collection-field` — resolve the
            // same way once normalized field-first: the mapping kind (Relation vs Field) decides
            // whether this is collection membership or a scalar IN, not the operand order.
            List<Operand> operands = NormalizedBinary.of("in", rawOperands).operands();
            Operand fieldOp = operands.get(0);
            Operand valueOp = operands.get(1);
            if (fieldOp.getNodeCase() != Operand.NodeCase.VARIABLE
                    || valueOp.getNodeCase() != Operand.NodeCase.VALUE) {
                throw new IllegalArgumentException("Unsupported in operand combination: "
                        + rawOperands.get(0).getNodeCase() + "/" + rawOperands.get(1).getNodeCase());
            }
            String var = fieldOp.getVariable();
            Object val = PlanValues.protoValueToJava(valueOp.getValue());

            AttributeMapping mapping = scope.resolveMapping(var);
            if (mapping instanceof AttributeMapping.Relation rel) {
                List<?> values = (val instanceof List<?> l) ? l : List.of(val);
                return collectionContainsAny(scope, rel, values);
            }

            Path<?> path = scope.resolvePath(var);
            OperatorFunction override = overrides.get("in");
            if (override != null) {
                return override.apply(cb, path, val);
            }
            if (val instanceof List<?> list) {
                if (list.isEmpty()) {
                    return cb.disjunction();
                }
                return path.in(list);
            }
            return cb.equal(path, val);
        }

        // -- hasIntersection --

        private Predicate handleHasIntersection(List<Operand> rawOperands, Scope scope) {
            if (rawOperands.size() != 2) {
                throw new IllegalArgumentException("hasIntersection requires exactly 2 operands");
            }
            // Intersection is symmetric, and the planner preserves policy source order —
            // `hasIntersection(P.attr.tags, R.attr.tags)` folds the principal side to a value
            // list in the FIRST position. Normalization puts the field/map side first.
            List<Operand> operands = NormalizedBinary.of("hasIntersection", rawOperands).operands();
            Operand first = operands.get(0);
            Operand second = operands.get(1);

            if (first.getNodeCase() == Operand.NodeCase.VARIABLE
                    && second.getNodeCase() == Operand.NodeCase.VALUE) {
                String var = first.getVariable();
                Object val = PlanValues.protoValueToJava(second.getValue());
                List<?> values = (val instanceof List<?> l) ? l : List.of(val);

                AttributeMapping mapping = scope.resolveMapping(var);
                if (mapping instanceof AttributeMapping.Relation rel) {
                    return collectionContainsAny(scope, rel, values);
                }
                Path<?> path = scope.resolvePath(var);
                // hasIntersection(field, []) is always false; avoid a dialect-dependent empty `IN ()`.
                if (values.isEmpty()) {
                    return cb.disjunction();
                }
                return path.in(values);
            }

            if (first.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "map".equals(first.getExpression().getOperator())) {
                if (second.getNodeCase() != Operand.NodeCase.VALUE) {
                    throw new IllegalArgumentException(
                            "hasIntersection second operand must be a value list when used with map()");
                }
                Object val = PlanValues.protoValueToJava(second.getValue());
                List<?> values = (val instanceof List<?> l) ? l : List.of(val);
                return handleMapIntersection(first.getExpression(), values, scope);
            }

            throw new IllegalArgumentException(
                    "Unsupported hasIntersection operand shape: " + first.getNodeCase());
        }

        /** Translate {@code hasIntersection(map(collection, lambda), values)}. */
        private Predicate handleMapIntersection(PlanResourcesFilter.Expression mapExpr,
                                                List<?> values, Scope scope) {
            // hasIntersection(map(...), []) is always false; short-circuit before the subquery.
            if (values.isEmpty()) {
                return cb.disjunction();
            }

            List<Operand> mapOperands = mapExpr.getOperandsList();
            if (mapOperands.size() != 2) {
                throw new IllegalArgumentException("map requires exactly 2 operands");
            }
            Operand collectionOperand = mapOperands.get(0);
            Operand lambdaOperand = mapOperands.get(1);

            if (collectionOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException("map first operand must be a variable");
            }
            if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
                throw new IllegalArgumentException("map second operand must be a lambda");
            }

            String collectionVar = collectionOperand.getVariable();

            List<Operand> lambdaOps = lambdaOperand.getExpression().getOperandsList();
            if (lambdaOps.size() != 2) {
                throw new IllegalArgumentException("map lambda requires exactly 2 operands (body, variable)");
            }
            Operand projection = lambdaOps.get(0);
            Operand lambdaVar = lambdaOps.get(1);
            if (projection.getNodeCase() != Operand.NodeCase.VARIABLE
                    || lambdaVar.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException("map lambda body must be a simple variable projection");
            }
            String memberField = Scope.extractLambdaSuffix(projection.getVariable(), lambdaVar.getVariable());

            // Check whether the collection path resolves through one Relation or a chain.
            // A chain (e.g. "request.resource.attr.categories.subCategories") emits nested
            // EXISTS subqueries — one per hop.
            if (scope instanceof Scope.RootScope rootScope) {
                Scope.RelationChain chain = Scope.resolveRelationChain(rootScope.mapper(), collectionVar);
                if (chain != null && !chain.relations().isEmpty()) {
                    AttributeMapping.Relation tailRel = chain.relations().get(chain.relations().size() - 1);
                    return chainedExistsSubquery(scope, chain.relations(), (sub, joinFrom, correlated) ->
                            Scope.memberPath(joinFrom, tailRel, memberField).in(values));
                }
            }

            AttributeMapping mapping = scope.resolveMapping(collectionVar);
            if (mapping instanceof AttributeMapping.Relation rel) {
                return existsSubquery(scope, rel, (sub, joinFrom, correlated) ->
                        Scope.memberPath(joinFrom, rel, memberField).in(values));
            }
            throw new IllegalArgumentException(
                    "map can only be applied to a collection mapped as Relation: " + collectionVar);
        }

        private Predicate collectionContainsAny(Scope outerScope, AttributeMapping.Relation rel, List<?> values) {
            // Intersection with an empty value set is always false — and an EXISTS wrapping an
            // empty `IN ()` is dialect-dependent — so short-circuit before building the subquery.
            if (values.isEmpty()) {
                return cb.disjunction();
            }
            return existsSubquery(outerScope, rel, (sub, joinFrom, correlated) -> {
                Path<?> field = Scope.memberPath(joinFrom, rel, null);
                if (values.size() == 1) {
                    return cb.equal(field, values.get(0));
                }
                return field.in(values);
            });
        }

        // -- size(collection) <op> N --

        /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
        private Predicate trySizeComparison(String op, List<Operand> operands, Scope scope) {
            PlanResourcesFilter.Expression sizeExpr = null;
            Long numValue = null;
            for (Operand o : operands) {
                if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                        && "size".equals(o.getExpression().getOperator())) {
                    sizeExpr = o.getExpression();
                } else if (o.getNodeCase() == Operand.NodeCase.VALUE) {
                    Object v = PlanValues.protoValueToJava(o.getValue());
                    if (v instanceof Number n) numValue = n.longValue();
                }
            }
            if (sizeExpr == null || numValue == null) {
                return null;
            }
            List<Operand> sizeOps = sizeExpr.getOperandsList();
            if (sizeOps.size() != 1 || sizeOps.get(0).getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException("Unsupported size() expression");
            }
            String var = sizeOps.get(0).getVariable();
            AttributeMapping mapping = scope.resolveMapping(var);
            if (!(mapping instanceof AttributeMapping.Relation rel)) {
                throw new IllegalArgumentException("size() requires a collection (Relation) mapping for " + var);
            }

            boolean nonEmpty = ("gt".equals(op) && numValue == 0L) || ("ge".equals(op) && numValue == 1L);
            boolean empty = ("eq".equals(op) && numValue == 0L)
                    || ("le".equals(op) && numValue == 0L)
                    || ("lt".equals(op) && numValue == 1L);

            if (nonEmpty) {
                return existsSubquery(scope, rel, (sub, joinFrom, correlated) -> cb.conjunction());
            }
            if (empty) {
                return negate(existsSubquery(scope, rel, (sub, joinFrom, correlated) -> cb.conjunction()));
            }
            throw new IllegalArgumentException(
                    "Unsupported size comparison: size(" + var + ") " + op + " " + numValue
                            + ". Only emptiness checks (size > 0, size == 0) are supported.");
        }

        // -- exists / exists_one / all / except / filter --

        private Predicate handleCollectionOperator(String op, List<Operand> operands, Scope scope) {
            if (operands.size() != 2) {
                throw new IllegalArgumentException(op + " requires exactly 2 operands");
            }
            Operand listOperand = operands.get(0);
            Operand lambdaOperand = operands.get(1);

            if (listOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException(op + " first operand must be a variable");
            }
            if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
                throw new IllegalArgumentException(op + " second operand must be a lambda");
            }

            String collectionVar = listOperand.getVariable();
            AttributeMapping mapping = scope.resolveMapping(collectionVar);
            if (!(mapping instanceof AttributeMapping.Relation rel)) {
                throw new IllegalArgumentException(
                        op + " requires a Relation mapping for " + collectionVar);
            }

            List<Operand> lambdaOps = lambdaOperand.getExpression().getOperandsList();
            if (lambdaOps.size() != 2) {
                throw new IllegalArgumentException("lambda requires exactly 2 operands");
            }
            Operand body = lambdaOps.get(0);
            Operand lambdaVar = lambdaOps.get(1);
            if (lambdaVar.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw new IllegalArgumentException("lambda variable must be a variable operand");
            }
            String lambdaVarName = lambdaVar.getVariable();

            return switch (op) {
                case "exists", "filter" -> existsSubquery(scope, rel,
                        (sub, joinFrom, correlated) -> traverse(body,
                                Scope.lambda(joinFrom, sub, rel, lambdaVarName, Scope.rebase(scope, correlated, sub))));
                case "except" -> existsSubquery(scope, rel,
                        (sub, joinFrom, correlated) -> negate(traverse(body,
                                Scope.lambda(joinFrom, sub, rel, lambdaVarName, Scope.rebase(scope, correlated, sub)))));
                case "all" -> negate(existsSubquery(scope, rel,
                        (sub, joinFrom, correlated) -> negate(traverse(body,
                                Scope.lambda(joinFrom, sub, rel, lambdaVarName, Scope.rebase(scope, correlated, sub))))));
                case "exists_one" -> {
                    Subquery<Long> sub = scope.parentQuery().subquery(Long.class);
                    From<?, ?> correlated = correlate(sub, scope.from());
                    Join<?, ?> joinFrom = correlated.join(rel.joinAttribute());
                    sub.select(cb.count(joinFrom));
                    sub.where(traverse(body,
                            Scope.lambda(joinFrom, sub, rel, lambdaVarName, Scope.rebase(scope, correlated, sub))));
                    yield cb.equal(sub, 1L);
                }
                default -> throw new IllegalArgumentException("Unsupported collection operator: " + op);
            };
        }

        @FunctionalInterface
        private interface SubqueryBodyBuilder {
            /**
             * @param sub        the subquery being built
             * @param joinFrom   the join over the Relation's collection inside the subquery
             * @param correlated the outer entity correlated into the subquery — lambda bodies
             *                   resolve non-lambda variables (e.g. {@code request.resource.attr.x})
             *                   against this so outer references stay legal JPA correlation paths
             */
            Predicate build(Subquery<?> sub, From<?, ?> joinFrom, From<?, ?> correlated);
        }

        /** Correlate the current scope's {@code From} into {@code sub}. */
        @SuppressWarnings("unchecked")
        private static From<?, ?> correlate(Subquery<?> sub, From<?, ?> outerFrom) {
            if (outerFrom instanceof Root<?> r) {
                return sub.correlate(r);
            }
            if (outerFrom instanceof Join<?, ?> j) {
                return sub.correlate((Join<Object, Object>) j);
            }
            throw new IllegalArgumentException("Cannot correlate from non-Root, non-Join scope: " + outerFrom);
        }

        /**
         * Build nested EXISTS subqueries for a chain of Relations: the outermost EXISTS joins the
         * first Relation, an inner EXISTS correlates from that join through the next, and so on.
         * The {@code bodyBuilder} produces the leaf predicate against the innermost join.
         */
        private Predicate chainedExistsSubquery(Scope scope,
                                                List<AttributeMapping.Relation> chain,
                                                SubqueryBodyBuilder bodyBuilder) {
            if (chain.size() == 1) {
                return existsSubquery(scope, chain.get(0), bodyBuilder);
            }
            return existsSubquery(scope, chain.get(0), (sub, joinFrom, correlated) -> {
                // Recurse using an intermediate scope rooted at the current join + this subquery.
                // The lambda variable name is internal-only — `$` is not a valid CEL identifier
                // character, so this sentinel can never collide with a user-supplied lambda name.
                AttributeMapping.Relation thisRel = chain.get(0);
                Scope intermediate = Scope.lambda(joinFrom, sub, thisRel, "$$chain$$",
                        Scope.rebase(scope, correlated, sub));
                return chainedExistsSubquery(intermediate, chain.subList(1, chain.size()), bodyBuilder);
            });
        }

        private Predicate existsSubquery(Scope scope, AttributeMapping.Relation rel, SubqueryBodyBuilder bodyBuilder) {
            Subquery<Integer> sub = scope.parentQuery().subquery(Integer.class);
            From<?, ?> correlated = correlate(sub, scope.from());
            Join<?, ?> joinFrom = correlated.join(rel.joinAttribute());
            sub.select(cb.literal(1));
            sub.where(bodyBuilder.build(sub, joinFrom, correlated));
            return cb.exists(sub);
        }
    }
}
