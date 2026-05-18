package dev.cerbos.queryplan.springdata;

import com.google.protobuf.Value;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;
import dev.cerbos.api.v1.response.Response.PlanResourcesResponse;
import dev.cerbos.sdk.PlanResourcesResult;

import jakarta.persistence.criteria.AbstractQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.From;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Translates a Cerbos {@code PlanResources} response into a Spring Data JPA
 * {@link org.springframework.data.jpa.domain.Specification} that can be executed by any
 * {@code JpaSpecificationExecutor}.
 */
public final class SpringDataQueryPlanAdapter {

    // Alias for the deeply-nested protobuf type to avoid collision with jakarta.persistence.criteria.Expression
    private static final class PlanExpr {
        private PlanExpr() {}
    }

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
                new Translator(cb, mapper, overrides).traverse(condition, Scope.root(root, query, mapper)));
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
                        new Translator(cb, mapper, overrides).traverse(cond, Scope.root(root, query, mapper)));
            }
            default -> throw new IllegalArgumentException("Unknown filter kind: " + filter.getKind());
        };
    }

    // -- Internal translator --

    private static final class Translator {
        private final CriteriaBuilder cb;
        private final Map<String, AttributeMapping> topMapper;
        private final Map<String, OperatorFunction> overrides;

        Translator(CriteriaBuilder cb,
                   Map<String, AttributeMapping> topMapper,
                   Map<String, OperatorFunction> overrides) {
            this.cb = cb;
            this.topMapper = topMapper;
            this.overrides = overrides;
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
            OperatorFunction fn = overrides.get("eq");
            if (fn != null) {
                return fn.apply(cb, path, true);
            }
            return cb.equal(path, true);
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
                    yield cb.not(traverse(operands.get(0), scope));
                }
                case "exists", "exists_one", "all", "except", "filter" ->
                        handleCollectionOperator(op, operands, scope);
                case "hasIntersection" -> handleHasIntersection(operands, scope);
                case "isSet" -> handleIsSet(operands, scope);
                case "in" -> handleIn(operands, scope);
                default -> {
                    Predicate sizePred = trySizeComparison(op, operands, scope);
                    if (sizePred != null) {
                        yield sizePred;
                    }
                    yield handleLeafOperator(op, operands, scope);
                }
            };
        }

        // -- Leaf operators (eq/ne/lt/gt/le/ge/contains/startsWith/endsWith) --

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
                        value = protoValueToJava(o.getValue());
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
                return switch (op) {
                    case "eq" -> cb.isNull(path);
                    case "ne" -> cb.isNotNull(path);
                    default -> throw new IllegalArgumentException(
                            "Null values are only supported with eq and ne operators (got " + op + ")");
                };
            }

            OperatorFunction override = overrides.get(op);
            if (override != null) {
                return override.apply(cb, path, value);
            }

            return defaultLeaf(op, path, value);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Predicate defaultLeaf(String op, Path<?> path, Object value) {
            Path raw = path;
            return switch (op) {
                case "eq" -> cb.equal(path, value);
                case "ne" -> cb.notEqual(path, value);
                case "lt" -> cb.lessThan(raw, (Comparable) value);
                case "gt" -> cb.greaterThan(raw, (Comparable) value);
                case "le" -> cb.lessThanOrEqualTo(raw, (Comparable) value);
                case "ge" -> cb.greaterThanOrEqualTo(raw, (Comparable) value);
                case "contains" -> cb.like(path.as(String.class), "%" + escapeLike(String.valueOf(value)) + "%", '\\');
                case "startsWith" -> cb.like(path.as(String.class), escapeLike(String.valueOf(value)) + "%", '\\');
                case "endsWith" -> cb.like(path.as(String.class), "%" + escapeLike(String.valueOf(value)), '\\');
                default -> throw new IllegalArgumentException("Unsupported operator: " + op);
            };
        }

        private static String escapeLike(String s) {
            return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
        }

        // -- add (fold + solve for string concat / numeric translation) --

        private Predicate handleAddComparison(String op, PlanResourcesFilter.Expression addExpr,
                                              Operand otherOperand, Scope scope) {
            List<Operand> addOperands = addExpr.getOperandsList();
            if (addOperands.size() != 2) {
                throw new IllegalArgumentException("add requires exactly 2 operands");
            }
            Operand addLeft = addOperands.get(0);
            Operand addRight = addOperands.get(1);

            // Case 1: add(value, value) — fold the two constants, then compare to the field.
            if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                    && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
                Object folded = foldAdd(
                        protoValueToJava(addLeft.getValue()),
                        protoValueToJava(addRight.getValue()));
                if (otherOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                    throw new IllegalArgumentException(
                            "add(const, const) compared to a non-field operand is not supported");
                }
                Path<?> path = scope.resolvePath(otherOperand.getVariable());
                return defaultLeaf(op, path, folded);
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
            Object otherValue = protoValueToJava(otherOperand.getValue());

            Operand fieldOp;
            Object addConst;
            boolean fieldIsLeft;
            if (addLeft.getNodeCase() == Operand.NodeCase.VARIABLE
                    && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
                fieldOp = addLeft;
                addConst = protoValueToJava(addRight.getValue());
                fieldIsLeft = true;
            } else if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                    && addRight.getNodeCase() == Operand.NodeCase.VARIABLE) {
                fieldOp = addRight;
                addConst = protoValueToJava(addLeft.getValue());
                fieldIsLeft = false;
            } else {
                throw new IllegalArgumentException(
                        "add requires exactly one field reference and one value, or two values");
            }

            Object solved = solveAdd(otherValue, addConst, fieldIsLeft);
            if (solved == null) {
                // No solution exists (e.g. "projects:123" == "users:" + R.id can never be true).
                // eq → always-false; ne → always-true.
                return "eq".equals(op) ? cb.disjunction() : cb.conjunction();
            }
            Path<?> path = scope.resolvePath(fieldOp.getVariable());
            return defaultLeaf(op, path, solved);
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
                    Object v = protoValueToJava(o.getValue());
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
            return flag ? cb.isNotNull(path) : cb.isNull(path);
        }

        // -- in (set membership or collection membership) --

        private Predicate handleIn(List<Operand> operands, Scope scope) {
            if (operands.size() != 2) {
                throw new IllegalArgumentException("in requires exactly 2 operands");
            }
            Operand left = operands.get(0);
            Operand right = operands.get(1);

            if (left.getNodeCase() == Operand.NodeCase.VARIABLE
                    && right.getNodeCase() == Operand.NodeCase.VALUE) {
                String var = left.getVariable();
                Object val = protoValueToJava(right.getValue());

                AttributeMapping mapping = scope.resolveMapping(var);
                if (mapping instanceof AttributeMapping.Relation rel) {
                    List<?> values = (val instanceof List<?> l) ? l : List.of(val);
                    return collectionContainsAny(scope, rel, values);
                }

                Path<?> path = scope.resolvePath(var);
                if (val instanceof List<?> list) {
                    if (list.isEmpty()) {
                        return cb.disjunction();
                    }
                    return path.in(list);
                }
                return cb.equal(path, val);
            }

            if (left.getNodeCase() == Operand.NodeCase.VALUE
                    && right.getNodeCase() == Operand.NodeCase.VARIABLE) {
                Object val = protoValueToJava(left.getValue());
                String var = right.getVariable();

                AttributeMapping mapping = scope.resolveMapping(var);
                if (mapping instanceof AttributeMapping.Relation rel) {
                    return collectionContainsAny(scope, rel, List.of(val));
                }
                Path<?> path = scope.resolvePath(var);
                return cb.equal(path, val);
            }

            throw new IllegalArgumentException(
                    "Unsupported in operand combination: " + left.getNodeCase() + "/" + right.getNodeCase());
        }

        // -- hasIntersection --

        private Predicate handleHasIntersection(List<Operand> operands, Scope scope) {
            if (operands.size() != 2) {
                throw new IllegalArgumentException("hasIntersection requires exactly 2 operands");
            }
            Operand first = operands.get(0);
            Operand second = operands.get(1);

            if (first.getNodeCase() == Operand.NodeCase.VARIABLE
                    && second.getNodeCase() == Operand.NodeCase.VALUE) {
                String var = first.getVariable();
                Object val = protoValueToJava(second.getValue());
                List<?> values = (val instanceof List<?> l) ? l : List.of(val);

                AttributeMapping mapping = scope.resolveMapping(var);
                if (mapping instanceof AttributeMapping.Relation rel) {
                    return collectionContainsAny(scope, rel, values);
                }
                Path<?> path = scope.resolvePath(var);
                return path.in(values);
            }

            if (first.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "map".equals(first.getExpression().getOperator())) {
                if (second.getNodeCase() != Operand.NodeCase.VALUE) {
                    throw new IllegalArgumentException(
                            "hasIntersection second operand must be a value list when used with map()");
                }
                Object val = protoValueToJava(second.getValue());
                List<?> values = (val instanceof List<?> l) ? l : List.of(val);

                PlanResourcesFilter.Expression mapExpr = first.getExpression();
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

                PlanResourcesFilter.Expression lambdaExpr = lambdaOperand.getExpression();
                List<Operand> lambdaOps = lambdaExpr.getOperandsList();
                Operand projection = lambdaOps.get(0);
                Operand lambdaVar = lambdaOps.get(1);
                if (projection.getNodeCase() != Operand.NodeCase.VARIABLE
                        || lambdaVar.getNodeCase() != Operand.NodeCase.VARIABLE) {
                    throw new IllegalArgumentException("map lambda body must be a simple variable projection");
                }
                String memberField = extractLambdaSuffix(projection.getVariable(), lambdaVar.getVariable());

                // Check whether the collection path resolves through one Relation or a chain.
                // A chain (e.g. "request.resource.attr.categories.subCategories") emits nested
                // EXISTS subqueries — one per hop.
                if (scope instanceof Scope.RootScope rootScope) {
                    RelationChain chain = resolveRelationChain(rootScope.mapper(), collectionVar);
                    if (chain != null && !chain.relations().isEmpty()) {
                        AttributeMapping.Relation tailRel = chain.relations().get(chain.relations().size() - 1);
                        return chainedExistsSubquery(scope, chain.relations(), (sub, joinFrom) -> {
                            Path<?> field = resolveMemberPath(joinFrom, tailRel, memberField);
                            return field.in(values);
                        });
                    }
                }

                AttributeMapping mapping = scope.resolveMapping(collectionVar);
                if (mapping instanceof AttributeMapping.Relation rel) {
                    return existsSubquery(scope, rel, (sub, joinFrom) -> {
                        Path<?> field = resolveMemberPath(joinFrom, rel, memberField);
                        return field.in(values);
                    });
                }
                throw new IllegalArgumentException(
                        "map can only be applied to a collection mapped as Relation: " + collectionVar);
            }

            throw new IllegalArgumentException(
                    "Unsupported hasIntersection operand shape: " + first.getNodeCase());
        }

        private Predicate collectionContainsAny(Scope outerScope, AttributeMapping.Relation rel, List<?> values) {
            return existsSubquery(outerScope, rel, (sub, joinFrom) -> {
                Path<?> field;
                if (rel.defaultMemberField() != null && !rel.defaultMemberField().isEmpty()) {
                    field = joinFrom.get(rel.defaultMemberField());
                } else {
                    // @ElementCollection<primitive> - the join itself is the element value
                    field = (Path<?>) joinFrom;
                }
                if (values.size() == 1) {
                    return cb.equal(field, values.get(0));
                }
                return field.in(values);
            });
        }

        // -- size(collection) <op> N --

        private Predicate trySizeComparison(String op, List<Operand> operands, Scope scope) {
            PlanResourcesFilter.Expression sizeExpr = null;
            Long numValue = null;
            for (Operand o : operands) {
                if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                        && "size".equals(o.getExpression().getOperator())) {
                    sizeExpr = o.getExpression();
                } else if (o.getNodeCase() == Operand.NodeCase.VALUE) {
                    Object v = protoValueToJava(o.getValue());
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
                return existsSubquery(scope, rel, (sub, joinFrom) -> cb.conjunction());
            }
            if (empty) {
                return cb.not(existsSubquery(scope, rel, (sub, joinFrom) -> cb.conjunction()));
            }
            throw new IllegalArgumentException(
                    "Unsupported size comparison: size(" + var + ") " + op + " " + numValue
                            + ". Only emptiness checks (size > 0, size == 0) are supported.");
        }

        // -- exists / exists_one / all / except / filter --

        @SuppressWarnings("unchecked")
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

            PlanResourcesFilter.Expression lambdaExpr = lambdaOperand.getExpression();
            List<Operand> lambdaOps = lambdaExpr.getOperandsList();
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
                        (sub, joinFrom) -> traverse(body, Scope.lambda(joinFrom, sub, rel, lambdaVarName)));
                case "except" -> existsSubquery(scope, rel,
                        (sub, joinFrom) -> cb.not(traverse(body, Scope.lambda(joinFrom, sub, rel, lambdaVarName))));
                case "all" -> cb.not(existsSubquery(scope, rel,
                        (sub, joinFrom) -> cb.not(traverse(body, Scope.lambda(joinFrom, sub, rel, lambdaVarName)))));
                case "exists_one" -> {
                    Subquery<Long> sub = scope.parentQuery().subquery(Long.class);
                    From<?, ?> outerFrom = scope.from();
                    From<?, ?> correlated;
                    if (outerFrom instanceof Root<?> r) {
                        correlated = sub.correlate(r);
                    } else if (outerFrom instanceof Join<?, ?> j) {
                        correlated = sub.correlate((Join<Object, Object>) j);
                    } else {
                        throw new IllegalArgumentException("Cannot correlate scope: " + outerFrom);
                    }
                    Join<?, ?> joinFrom = correlated.join(rel.joinAttribute());
                    sub.select(cb.count(joinFrom));
                    sub.where(traverse(body, Scope.lambda(joinFrom, sub, rel, lambdaVarName)));
                    yield cb.equal(sub, 1L);
                }
                default -> throw new IllegalArgumentException("Unsupported collection operator: " + op);
            };
        }

        @FunctionalInterface
        private interface SubqueryBodyBuilder {
            Predicate build(Subquery<?> sub, From<?, ?> joinFrom);
        }

        /**
         * Build nested EXISTS subqueries for a chain of Relations: the outermost EXISTS joins the
         * first Relation, an inner EXISTS correlates from that join through the next, and so on.
         * The {@code bodyBuilder} produces the leaf predicate against the innermost join.
         */
        private Predicate chainedExistsSubquery(Scope scope,
                                                java.util.List<AttributeMapping.Relation> chain,
                                                SubqueryBodyBuilder bodyBuilder) {
            if (chain.size() == 1) {
                return existsSubquery(scope, chain.get(0), bodyBuilder);
            }
            return existsSubquery(scope, chain.get(0), (sub, joinFrom) -> {
                // Recurse using an intermediate scope rooted at the current join + this subquery.
                AttributeMapping.Relation thisRel = chain.get(0);
                Scope intermediate = Scope.lambda(joinFrom, sub, thisRel, "__chain__");
                return chainedExistsSubquery(intermediate, chain.subList(1, chain.size()), bodyBuilder);
            });
        }

        @SuppressWarnings("unchecked")
        private Predicate existsSubquery(Scope scope, AttributeMapping.Relation rel, SubqueryBodyBuilder bodyBuilder) {
            From<?, ?> outerFrom = scope.from();
            Subquery<Integer> sub = scope.parentQuery().subquery(Integer.class);
            From<?, ?> correlated;
            if (outerFrom instanceof Root<?> r) {
                correlated = sub.correlate(r);
            } else if (outerFrom instanceof Join<?, ?> j) {
                correlated = sub.correlate((Join<Object, Object>) j);
            } else {
                throw new IllegalArgumentException("Cannot correlate from non-Root, non-Join scope: " + outerFrom);
            }
            Join<?, ?> joinFrom = correlated.join(rel.joinAttribute());
            sub.select(cb.literal(1));
            Predicate body = bodyBuilder.build(sub, joinFrom);
            sub.where(body);
            return cb.exists(sub);
        }
    }

    // -- Scope --

    private sealed interface Scope permits Scope.RootScope, Scope.LambdaScope {
        Path<?> resolvePath(String cerbosVar);

        AttributeMapping resolveMapping(String cerbosVar);

        From<?, ?> from();

        AbstractQuery<?> parentQuery();

        static Scope root(From<?, ?> root, AbstractQuery<?> query, Map<String, AttributeMapping> mapper) {
            return new RootScope(root, query, mapper);
        }

        static Scope lambda(From<?, ?> from, AbstractQuery<?> parentQuery,
                            AttributeMapping.Relation relation, String lambdaVar) {
            return new LambdaScope(from, parentQuery, relation, lambdaVar);
        }

        record RootScope(From<?, ?> from, AbstractQuery<?> parentQuery, Map<String, AttributeMapping> mapper)
                implements Scope {
            @Override
            public Path<?> resolvePath(String cerbosVar) {
                AttributeMapping m = mapper.get(cerbosVar);
                if (m == null) {
                    throw new IllegalArgumentException("Unknown attribute: " + cerbosVar);
                }
                if (m instanceof AttributeMapping.Field f) {
                    return traversePath(from, f.jpaPath());
                }
                throw new IllegalArgumentException(
                        "Attribute " + cerbosVar + " is a Relation; cannot resolve as a scalar path");
            }

            @Override
            public AttributeMapping resolveMapping(String cerbosVar) {
                AttributeMapping m = mapper.get(cerbosVar);
                if (m != null) {
                    return m;
                }

                // Try resolving as a dotted suffix off a registered Relation prefix.
                // Example: mapper has "request.resource.attr.categories" → Relation("categories", fields={"subCategories": Relation(...)})
                // and we're asked for "request.resource.attr.categories.subCategories" — walk the chain.
                String[] parts = cerbosVar.split("\\.");
                for (int i = parts.length - 1; i > 0; i--) {
                    String prefix = String.join(".", java.util.Arrays.copyOfRange(parts, 0, i));
                    AttributeMapping prefixMapping = mapper.get(prefix);
                    if (prefixMapping instanceof AttributeMapping.Relation rel) {
                        AttributeMapping resolved = walkRelationChain(rel,
                                java.util.Arrays.copyOfRange(parts, i, parts.length));
                        if (resolved != null) {
                            return resolved;
                        }
                    }
                }

                throw new IllegalArgumentException("Unknown attribute: " + cerbosVar);
            }
        }

        record LambdaScope(From<?, ?> from, AbstractQuery<?> parentQuery,
                           AttributeMapping.Relation relation, String lambdaVar) implements Scope {
            @Override
            public Path<?> resolvePath(String cerbosVar) {
                String suffix = extractLambdaSuffix(cerbosVar, lambdaVar);
                if (suffix.isEmpty()) {
                    if (relation.defaultMemberField() != null && !relation.defaultMemberField().isEmpty()) {
                        return from.get(relation.defaultMemberField());
                    }
                    return (Path<?>) from;
                }
                AttributeMapping nested = relation.fields().get(suffix);
                if (nested instanceof AttributeMapping.Field f) {
                    return traversePath(from, f.jpaPath());
                }
                return traversePath(from, suffix);
            }

            @Override
            public AttributeMapping resolveMapping(String cerbosVar) {
                String suffix = extractLambdaSuffix(cerbosVar, lambdaVar);
                if (suffix.isEmpty()) {
                    return relation;
                }
                AttributeMapping nested = relation.fields().get(suffix);
                if (nested != null) {
                    return nested;
                }
                return AttributeMapping.field(suffix);
            }
        }
    }

    // -- helpers --

    /**
     * Fold {@code add(left, right)} where both operands are constants. Strings concatenate;
     * numbers add. Used when the planner emits e.g. {@code eq(field, add("prefix:", "123"))}.
     */
    static Object foldAdd(Object left, Object right) {
        if (left instanceof String || right instanceof String) {
            return String.valueOf(left) + String.valueOf(right);
        }
        if (left instanceof Number ln && right instanceof Number rn) {
            if (left instanceof Long && right instanceof Long) {
                return ln.longValue() + rn.longValue();
            }
            return ln.doubleValue() + rn.doubleValue();
        }
        throw new IllegalArgumentException(
                "add requires string or numeric operands, got " + left.getClass() + " + " + right.getClass());
    }

    /**
     * Solve {@code field + addConstant == comparisonValue} (or with operands swapped if
     * {@code !fieldIsLeft}). For strings: strip the prefix/suffix and return what the field must
     * equal; return {@code null} if the comparison value doesn't match the constant's
     * shape (which means no field value can satisfy the equation). For numbers: subtract.
     */
    static Object solveAdd(Object comparisonValue, Object addConstant, boolean fieldIsLeft) {
        if (comparisonValue instanceof String compStr && addConstant instanceof String constStr) {
            if (fieldIsLeft) {
                // field + const == comparison  →  field == comparison stripped-of-suffix
                if (!compStr.endsWith(constStr)) return null;
                return compStr.substring(0, compStr.length() - constStr.length());
            }
            // const + field == comparison  →  field == comparison stripped-of-prefix
            if (!compStr.startsWith(constStr)) return null;
            return compStr.substring(constStr.length());
        }
        if (comparisonValue instanceof Number compNum && addConstant instanceof Number constNum) {
            // Both orderings of numeric addition produce the same equation: field = comp - const
            if (comparisonValue instanceof Long && addConstant instanceof Long) {
                return compNum.longValue() - constNum.longValue();
            }
            return compNum.doubleValue() - constNum.doubleValue();
        }
        throw new IllegalArgumentException(
                "add comparison type mismatch: " + comparisonValue.getClass() + " vs " + addConstant.getClass());
    }

    /**
     * Walk a dotted suffix through a Relation's nested {@code fields()} map. Returns the leaf
     * mapping (Field or Relation) reached, or {@code null} if any segment doesn't resolve.
     */
    private static AttributeMapping walkRelationChain(AttributeMapping.Relation rel, String[] suffixParts) {
        AttributeMapping current = rel;
        for (String part : suffixParts) {
            if (!(current instanceof AttributeMapping.Relation r)) {
                return null;
            }
            AttributeMapping next = r.fields().get(part);
            if (next == null) {
                return null;
            }
            current = next;
        }
        return current;
    }

    /**
     * Resolve a dotted top-level Cerbos attribute to a chain of Relations, ending in either a
     * leaf Field or the final Relation. Used by {@code hasIntersection(map(...))} when the map's
     * collection operand is a dotted path through nested Relation mappings.
     */
    record RelationChain(List<AttributeMapping.Relation> relations, AttributeMapping.Field tail) {}

    private static RelationChain resolveRelationChain(Map<String, AttributeMapping> mapper, String cerbosVar) {
        AttributeMapping direct = mapper.get(cerbosVar);
        if (direct instanceof AttributeMapping.Relation rel) {
            return new RelationChain(List.of(rel), null);
        }
        String[] parts = cerbosVar.split("\\.");
        for (int i = parts.length - 1; i > 0; i--) {
            String prefix = String.join(".", java.util.Arrays.copyOfRange(parts, 0, i));
            AttributeMapping prefixMapping = mapper.get(prefix);
            if (!(prefixMapping instanceof AttributeMapping.Relation rel)) {
                continue;
            }
            String[] suffixParts = java.util.Arrays.copyOfRange(parts, i, parts.length);
            java.util.List<AttributeMapping.Relation> chain = new java.util.ArrayList<>();
            chain.add(rel);
            AttributeMapping current = rel;
            boolean ok = true;
            for (int s = 0; s < suffixParts.length; s++) {
                if (!(current instanceof AttributeMapping.Relation r)) {
                    ok = false;
                    break;
                }
                AttributeMapping next = r.fields().get(suffixParts[s]);
                if (next == null) {
                    ok = false;
                    break;
                }
                if (next instanceof AttributeMapping.Relation nextRel) {
                    chain.add(nextRel);
                    current = nextRel;
                } else if (next instanceof AttributeMapping.Field leafField && s == suffixParts.length - 1) {
                    return new RelationChain(chain, leafField);
                } else {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return new RelationChain(chain, null);
            }
        }
        return null;
    }

    private static Path<?> resolveMemberPath(From<?, ?> joinFrom, AttributeMapping.Relation rel, String memberField) {
        if (memberField == null || memberField.isEmpty()) {
            if (rel.defaultMemberField() != null && !rel.defaultMemberField().isEmpty()) {
                return joinFrom.get(rel.defaultMemberField());
            }
            return (Path<?>) joinFrom;
        }
        AttributeMapping nested = rel.fields().get(memberField);
        if (nested instanceof AttributeMapping.Field f) {
            return traversePath(joinFrom, f.jpaPath());
        }
        return traversePath(joinFrom, memberField);
    }

    private static Path<?> traversePath(From<?, ?> from, String dottedJpaPath) {
        String[] parts = dottedJpaPath.split("\\.");
        Path<?> p = from;
        for (String part : parts) {
            p = p.get(part);
        }
        return p;
    }

    private static String extractLambdaSuffix(String variable, String lambdaVar) {
        if (variable.equals(lambdaVar)) {
            return "";
        }
        String prefix = lambdaVar + ".";
        if (!variable.startsWith(prefix)) {
            throw new IllegalArgumentException(
                    "Variable '" + variable + "' does not start with lambda variable '" + lambdaVar + "'");
        }
        return variable.substring(prefix.length());
    }

    static Object protoValueToJava(Value value) {
        return switch (value.getKindCase()) {
            case STRING_VALUE -> value.getStringValue();
            case NUMBER_VALUE -> {
                double d = value.getNumberValue();
                if (d == Math.floor(d) && !Double.isInfinite(d)) {
                    yield (long) d;
                }
                yield d;
            }
            case BOOL_VALUE -> value.getBoolValue();
            case NULL_VALUE -> null;
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                    .map(SpringDataQueryPlanAdapter::protoValueToJava)
                    .toList();
            case STRUCT_VALUE -> value.getStructValue().getFieldsMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> protoValueToJava(e.getValue())));
            default -> throw new IllegalArgumentException(
                    "Unsupported protobuf value type: " + value.getKindCase());
        };
    }
}
