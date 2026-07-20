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
import java.util.Objects;
import java.util.Set;

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
                case "if" -> handleBareTernary(operands, scope);
                case "overlaps" -> hierarchy.handleOverlaps(operands, scope);
                case "ancestorOf" -> hierarchy.handleAncestorDescendant(operands, scope, true);
                case "descendentOf" -> hierarchy.handleAncestorDescendant(operands, scope, false);
                default -> {
                    Predicate ternaryPred = tryTernaryComparison(op, operands, scope);
                    if (ternaryPred != null) {
                        yield ternaryPred;
                    }
                    NormalizedBinary nb = NormalizedBinary.of(op, operands);
                    Predicate sizePred = trySizeComparison(nb.op(), nb.operands(), scope);
                    if (sizePred != null) {
                        yield sizePred;
                    }
                    Predicate arithPred = tryArithmeticComparison(nb.op(), nb.operands(), scope);
                    if (arithPred != null) {
                        yield arithPred;
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

        // -- if (CEL ternary) --

        /** Binary comparison operators that accept a ternary operand (see {@link #tryTernaryComparison}). */
        private static final Set<String> TERNARY_COMPARISONS =
                Set.of("eq", "ne", "lt", "gt", "le", "ge");

        /**
         * Rewrite a comparison wrapping a CEL ternary — {@code cmp(if(c, a, b), other)} — into a
         * pure predicate:
         *
         * <pre>{@code (pred(c) AND cmp(a, other)) OR (NOT pred(c) AND cmp(b, other))}</pre>
         *
         * We rewrite instead of emitting {@code CASE WHEN} ({@code cb.selectCase}) because this
         * translator is predicate-only: every existing typed leaf path — field-first
         * normalization, size() handling, add-fold, fractional double-space comparison — operates
         * on comparison predicates. Substituting each branch back into the comparison and
         * recursing through {@link #traverseExpression} routes the branches through those exact
         * paths, so a ternary branch behaves identically to the same comparison written directly.
         * Recursion also handles nested ternaries and a ternary on the other side for free.
         *
         * <p>Null semantics: under SQL three-valued logic a NULL condition column makes both
         * {@code pred(c)} and {@code NOT pred(c)} unknown, so the row is excluded from both
         * branches. This matches Cerbos: a null/missing condition in a CEL ternary is an
         * evaluation error and the check denies.
         *
         * @return the rewritten predicate, or {@code null} if this comparison involves no ternary
         */
        private Predicate tryTernaryComparison(String op, List<Operand> operands, Scope scope) {
            if (!TERNARY_COMPARISONS.contains(op) || operands.size() != 2) {
                return null;
            }
            int idx = -1;
            for (int i = 0; i < operands.size(); i++) {
                Operand o = operands.get(i);
                if (o.getNodeCase() == Operand.NodeCase.EXPRESSION
                        && "if".equals(o.getExpression().getOperator())) {
                    idx = i;
                    break;
                }
            }
            if (idx < 0) {
                return null;
            }
            List<Operand> ifOps = operands.get(idx).getExpression().getOperandsList();
            if (ifOps.size() != 3) {
                throw new IllegalArgumentException(
                        "if (ternary) requires exactly 3 operands (condition, then, else), got "
                                + ifOps.size());
            }
            Operand condition = ifOps.get(0);
            Operand thenBranch = ifOps.get(1);
            Operand elseBranch = ifOps.get(2);

            // A constant boolean condition folds to a single branch — translate only that branch
            // so an untranslatable dead branch cannot fail the whole plan.
            if (condition.getNodeCase() == Operand.NodeCase.VALUE) {
                Boolean known = constantBooleanOrNull(condition);
                if (known == null) {
                    throw new IllegalArgumentException(
                            "if (ternary) condition must be a boolean expression");
                }
                return traverseExpression(
                        substituteOperand(op, operands, idx, known ? thenBranch : elseBranch), scope);
            }

            Predicate thenCmp = traverseExpression(
                    substituteOperand(op, operands, idx, thenBranch), scope);
            Predicate elseCmp = traverseExpression(
                    substituteOperand(op, operands, idx, elseBranch), scope);
            // Translate the condition once per occurrence: Hibernate 6 negation is stateful (see
            // negate()), so sharing one Predicate node between the positive and negated arms is
            // unsafe.
            return cb.or(
                    cb.and(traverse(condition, scope), thenCmp),
                    cb.and(negate(traverse(condition, scope)), elseCmp));
        }

        /**
         * A CEL ternary in boolean position — {@code if(c, a, b)} used directly as a condition,
         * so both branches are themselves boolean. Same predicate rewrite (and same rationale and
         * null semantics) as {@link #tryTernaryComparison}:
         *
         * <pre>{@code (pred(c) AND pred(a)) OR (NOT pred(c) AND pred(b))}</pre>
         */
        private Predicate handleBareTernary(List<Operand> operands, Scope scope) {
            if (operands.size() != 3) {
                throw new IllegalArgumentException(
                        "if (ternary) requires exactly 3 operands (condition, then, else), got "
                                + operands.size());
            }
            Operand condition = operands.get(0);
            Operand thenBranch = operands.get(1);
            Operand elseBranch = operands.get(2);

            // A constant boolean condition folds to a single branch — translate only that branch
            // so an untranslatable dead branch cannot fail the whole plan.
            if (condition.getNodeCase() == Operand.NodeCase.VALUE) {
                Boolean known = constantBooleanOrNull(condition);
                if (known == null) {
                    throw new IllegalArgumentException(
                            "if (ternary) condition must be a boolean expression");
                }
                return booleanBranchPredicate(known ? thenBranch : elseBranch, scope);
            }

            // Translate the condition once per occurrence — see tryTernaryComparison.
            return cb.or(
                    cb.and(traverse(condition, scope), booleanBranchPredicate(thenBranch, scope)),
                    cb.and(negate(traverse(condition, scope)), booleanBranchPredicate(elseBranch, scope)));
        }

        /**
         * A ternary branch in boolean position: a boolean VALUE folds to the always-true /
         * always-false predicate (the same collapse the unsolvable add-solve cases use); anything
         * else translates as a normal boolean operand (bare variables become {@code path = true}).
         */
        private Predicate booleanBranchPredicate(Operand branch, Scope scope) {
            if (branch.getNodeCase() == Operand.NodeCase.VALUE) {
                Boolean constant = constantBooleanOrNull(branch);
                if (constant == null) {
                    throw new IllegalArgumentException(
                            "if (ternary) branch in boolean position must be a boolean");
                }
                return constant ? cb.conjunction() : cb.disjunction();
            }
            return traverse(branch, scope);
        }

        /** Rebuild {@code op(operands...)} with the operand at {@code idx} replaced. */
        private static PlanResourcesFilter.Expression substituteOperand(
                String op, List<Operand> operands, int idx, Operand replacement) {
            PlanResourcesFilter.Expression.Builder b =
                    PlanResourcesFilter.Expression.newBuilder().setOperator(op);
            for (int i = 0; i < operands.size(); i++) {
                b.addOperands(i == idx ? replacement : operands.get(i));
            }
            return b.build();
        }

        /** The operand's boolean constant, or {@code null} if it is not a boolean VALUE. */
        private static Boolean constantBooleanOrNull(Operand o) {
            return PlanValues.protoValueToJava(o.getValue()) instanceof Boolean b ? b : null;
        }

        // -- Leaf operators (eq/ne/lt/gt/le/ge/contains/startsWith/endsWith) --

        /** Operands must already be normalized field-first (see {@link NormalizedBinary}). */
        private Predicate handleLeafOperator(String op, List<Operand> operands, Scope scope) {
            // Constant-vs-constant comparisons are statically evaluated. The planner never emits
            // them directly, but ternary substitution produces them — the else branch of
            // `(aBool ? aNumber : 0) > 0` becomes gt(value(0), value(0)).
            if (TERNARY_COMPARISONS.contains(op)
                    && operands.size() == 2
                    && operands.get(0).getNodeCase() == Operand.NodeCase.VALUE
                    && operands.get(1).getNodeCase() == Operand.NodeCase.VALUE) {
                return constantComparison(op,
                        PlanValues.protoValueToJava(operands.get(0).getValue()),
                        PlanValues.protoValueToJava(operands.get(1).getValue()));
            }

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
            String secondVariable = null;
            Object value = null;
            boolean valueSeen = false;
            for (Operand o : operands) {
                switch (o.getNodeCase()) {
                    case VARIABLE -> {
                        if (variable != null) {
                            secondVariable = o.getVariable();
                        } else {
                            variable = o.getVariable();
                        }
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
            if (secondVariable != null) {
                return fieldToFieldComparison(op, variable, secondVariable, scope);
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
         * Statically evaluate a comparison between two plan constants and collapse it to an
         * always-true ({@code 1=1}) or always-false ({@code 1=0}) predicate — the same collapse
         * the unsolvable {@code add}-solve cases use. Numbers compare in double space: protobuf
         * {@code Value.getNumberValue()} is a double, and {@link PlanValues#protoValueToJava}
         * only splits Long/Double for whole-number cosmetics, not semantics. Strings compare
         * lexicographically; booleans (and mixed incomparable types) support eq/ne only —
         * eq → false, ne → true — while ordering them is a planner bug and throws.
         */
        private Predicate constantComparison(String op, Object left, Object right) {
            boolean result;
            if ("eq".equals(op) || "ne".equals(op)) {
                boolean equal = (left instanceof Number ln && right instanceof Number rn)
                        ? ln.doubleValue() == rn.doubleValue()
                        : Objects.equals(left, right);
                result = "eq".equals(op) == equal;
            } else {
                int cmp;
                if (left instanceof Number ln && right instanceof Number rn) {
                    cmp = Double.compare(ln.doubleValue(), rn.doubleValue());
                } else if (left instanceof String ls && right instanceof String rs) {
                    cmp = ls.compareTo(rs);
                } else {
                    throw new IllegalArgumentException(
                            "Cannot order constant operands of " + op + ": "
                                    + typeName(left) + " vs " + typeName(right));
                }
                result = switch (op) {
                    case "lt" -> cmp < 0;
                    case "gt" -> cmp > 0;
                    case "le" -> cmp <= 0;
                    case "ge" -> cmp >= 0;
                    default -> throw new IllegalArgumentException(
                            "Unsupported constant comparison operator: " + op);
                };
            }
            return result ? cb.conjunction() : cb.disjunction();
        }

        private static String typeName(Object o) {
            return o == null ? "null" : o.getClass().getSimpleName();
        }

        /**
         * Compare two mapped columns directly (eq/ne/lt/gt/le/ge) or pattern-match one column
         * against another (contains/startsWith/endsWith). Operand source order is preserved —
         * two variables rank equally, so {@link NormalizedBinary} never swaps them.
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private Predicate fieldToFieldComparison(String op, String leftVar, String rightVar,
                                                 Scope scope) {
            jakarta.persistence.criteria.Expression left = scope.resolvePath(leftVar);
            jakarta.persistence.criteria.Expression right = scope.resolvePath(rightVar);
            return switch (op) {
                case "eq" -> cb.equal(left, right);
                case "ne" -> cb.notEqual(left, right);
                case "lt" -> cb.lessThan(left, right);
                case "gt" -> cb.greaterThan(left, right);
                case "le" -> cb.lessThanOrEqualTo(left, right);
                case "ge" -> cb.greaterThanOrEqualTo(left, right);
                case "contains" -> fieldToFieldLike(left, right, true, true);
                case "startsWith" -> fieldToFieldLike(left, right, false, true);
                case "endsWith" -> fieldToFieldLike(left, right, true, false);
                default -> throw new IllegalArgumentException(
                        "Field-to-field comparison is not supported for operator '" + op + "': "
                                + leftVar + " vs " + rightVar);
            };
        }

        /**
         * {@code haystackColumn LIKE wildcards(escape(needleColumn))} — the column-to-column
         * analogue of the constant LIKE path in {@link #defaultLeaf}. The needle is data, so its
         * LIKE metacharacters are escaped dynamically with nested {@code REPLACE} (portable:
         * H2/Postgres/MySQL/Oracle/SQL Server): {@code \} first, then {@code %} and {@code _},
         * mirroring {@link PlanValues#escapeLike} and the same explicit {@code '\'} escape char.
         *
         * <p>The explicit {@code IS NOT NULL} guard on the needle matches CEL (a missing
         * attribute is an evaluation error → deny) and also defends against dialects whose
         * {@code CONCAT} treats NULL as {@code ''}, which would otherwise turn a NULL needle
         * into a match-anything {@code '%%'} pattern.
         */
        private Predicate fieldToFieldLike(jakarta.persistence.criteria.Expression<?> haystack,
                                           jakarta.persistence.criteria.Expression<?> needle,
                                           boolean leadingWildcard, boolean trailingWildcard) {
            jakarta.persistence.criteria.Expression<String> escaped =
                    needle.as(String.class);
            escaped = cb.function("replace", String.class,
                    escaped, cb.literal("\\"), cb.literal("\\\\"));
            escaped = cb.function("replace", String.class,
                    escaped, cb.literal("%"), cb.literal("\\%"));
            escaped = cb.function("replace", String.class,
                    escaped, cb.literal("_"), cb.literal("\\_"));
            jakarta.persistence.criteria.Expression<String> pattern = escaped;
            if (leadingWildcard) {
                pattern = cb.concat(cb.literal("%"), pattern);
            }
            if (trailingWildcard) {
                pattern = cb.concat(pattern, cb.literal("%"));
            }
            return cb.and(
                    cb.isNotNull(needle),
                    cb.like(haystack.as(String.class), pattern, '\\'));
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

        // -- arithmetic (add/sub/mult/div) as a comparison operand --

        /** CEL arithmetic operators that can appear as an operand of a comparison. */
        private static final Set<String> ARITHMETIC_OPS = Set.of("add", "sub", "mult", "div", "mod");

        /**
         * Translate {@code cmp(arith(...), other)} — e.g. {@code R.attr.aNumber + 1.0 > 2.0}
         * arriving as {@code gt(add(variable, value(1)), value(2))} — by emitting the arithmetic
         * on the SQL side ({@code cb.sum}/{@code diff}/{@code prod}/{@code quot}) and comparing.
         *
         * <p>Everything is computed and compared in DOUBLE space. This is not a convenience:
         * Cerbos attribute values are protobuf {@code Value} numbers, i.e. ALWAYS CEL doubles at
         * check time, so the only arithmetic that can evaluate without a no-overload error is
         * double-typed — verified against a live PDP: {@code R.attr.n + 1} (int literal) denies
         * every row, {@code + 1.0} works, and {@code / 2.0} is true double division
         * ({@code 5 / 2.0 == 2.5}). Integer truncation is therefore never observable through the
         * check API, and the wire plan erases the int/double distinction anyway (both arrive as
         * {@code number_value}). Emitting the arithmetic (rather than solving algebraically)
         * also means multiplication/division by negative constants needs no inequality flipping.
         *
         * <p>{@code mod} stays unsupported: CEL {@code %} has no double overload, so on
         * attribute values it always errors (deny) — translating it to SQL {@code MOD} would
         * fabricate rows the PDP denies.
         *
         * @return the predicate, or {@code null} if this comparison involves no arithmetic
         *         expression or the shape is owned by the {@code add} fold/solve path (which
         *         also handles string concatenation and the override hooks)
         */
        private Predicate tryArithmeticComparison(String op, List<Operand> operands, Scope scope) {
            if (!TERNARY_COMPARISONS.contains(op) || operands.size() != 2) {
                return null;
            }
            boolean hasArith = operands.stream().anyMatch(o ->
                    o.getNodeCase() == Operand.NodeCase.EXPRESSION
                            && ARITHMETIC_OPS.contains(o.getExpression().getOperator()));
            if (!hasArith) {
                return null;
            }
            if (addFoldSolveOwns(op, operands.get(0), operands.get(1))
                    || addFoldSolveOwns(op, operands.get(1), operands.get(0))) {
                return null;
            }
            jakarta.persistence.criteria.Expression<Double> left =
                    resolveNumericExpression(operands.get(0), scope);
            jakarta.persistence.criteria.Expression<Double> right =
                    resolveNumericExpression(operands.get(1), scope);
            return switch (op) {
                case "eq" -> cb.equal(left, right);
                case "ne" -> cb.notEqual(left, right);
                case "lt" -> cb.lt(left, right);
                case "gt" -> cb.gt(left, right);
                case "le" -> cb.le(left, right);
                case "ge" -> cb.ge(left, right);
                default -> throw new IllegalArgumentException(
                        "Unsupported arithmetic comparison operator: " + op);
            };
        }

        /**
         * Whether {@code cmp(candidate, other)} is a shape {@link #handleAddComparison} already
         * translates — those keep their existing path (constant folding, string concat
         * solving, and the {@link OperatorFunction} override hooks): {@code add(value, value)}
         * against a field for any operator, and {@code add} of one field and one value against
         * a value for eq/ne.
         */
        private static boolean addFoldSolveOwns(String op, Operand candidate, Operand other) {
            if (candidate.getNodeCase() != Operand.NodeCase.EXPRESSION
                    || !"add".equals(candidate.getExpression().getOperator())
                    || candidate.getExpression().getOperandsCount() != 2) {
                return false;
            }
            Operand l = candidate.getExpression().getOperands(0);
            Operand r = candidate.getExpression().getOperands(1);
            boolean bothValues = l.getNodeCase() == Operand.NodeCase.VALUE
                    && r.getNodeCase() == Operand.NodeCase.VALUE;
            if (bothValues && other.getNodeCase() == Operand.NodeCase.VARIABLE) {
                return true; // fold path
            }
            boolean oneFieldOneValue =
                    (l.getNodeCase() == Operand.NodeCase.VARIABLE
                            && r.getNodeCase() == Operand.NodeCase.VALUE)
                    || (l.getNodeCase() == Operand.NodeCase.VALUE
                            && r.getNodeCase() == Operand.NodeCase.VARIABLE);
            return ("eq".equals(op) || "ne".equals(op))
                    && oneFieldOneValue
                    && other.getNodeCase() == Operand.NodeCase.VALUE; // solve path
        }

        /**
         * Resolve a comparison operand to a numeric SQL expression in double space:
         * variable → column cast to double; value → double literal; nested
         * add/sub/mult/div → recursive {@code cb.sum}/{@code diff}/{@code prod}/{@code quot}.
         */
        private jakarta.persistence.criteria.Expression<Double> resolveNumericExpression(
                Operand operand, Scope scope) {
            switch (operand.getNodeCase()) {
                case VARIABLE -> {
                    return scope.resolvePath(operand.getVariable()).as(Double.class);
                }
                case VALUE -> {
                    Object v = PlanValues.protoValueToJava(operand.getValue());
                    if (!(v instanceof Number n)) {
                        throw new IllegalArgumentException(
                                "Arithmetic comparison requires numeric operands, got "
                                        + typeName(v));
                    }
                    return cb.literal(n.doubleValue());
                }
                case EXPRESSION -> {
                    PlanResourcesFilter.Expression expr = operand.getExpression();
                    String op = expr.getOperator();
                    if ("mod".equals(op)) {
                        throw new IllegalArgumentException(
                                "mod is not supported in comparisons: CEL % is integer-only and "
                                        + "attribute values are always doubles at check time, so "
                                        + "the condition can never be satisfied by the PDP");
                    }
                    if (!ARITHMETIC_OPS.contains(op)) {
                        throw new IllegalArgumentException(
                                "Unexpected " + op + "() expression inside an arithmetic "
                                        + "comparison operand");
                    }
                    if (expr.getOperandsCount() != 2) {
                        throw new IllegalArgumentException(op + " requires exactly 2 operands");
                    }
                    jakarta.persistence.criteria.Expression<Double> l =
                            resolveNumericExpression(expr.getOperands(0), scope);
                    jakarta.persistence.criteria.Expression<Double> r =
                            resolveNumericExpression(expr.getOperands(1), scope);
                    return switch (op) {
                        case "add" -> cb.sum(l, r);
                        case "sub" -> cb.diff(l, r);
                        case "mult" -> cb.prod(l, r);
                        case "div" -> cb.quot(l, r).as(Double.class);
                        default -> throw new IllegalArgumentException(
                                "Unsupported arithmetic operator: " + op);
                    };
                }
                default -> throw new IllegalArgumentException(
                        "Unexpected operand type in arithmetic comparison: "
                                + operand.getNodeCase());
            }
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
            if (sizeOps.size() != 1) {
                throw new IllegalArgumentException("Unsupported size() expression");
            }
            Operand sizeArg = sizeOps.get(0);
            String var;
            Operand lambdaBody = null;
            String lambdaVarName = null;
            if (sizeArg.getNodeCase() == Operand.NodeCase.VARIABLE) {
                var = sizeArg.getVariable();
            } else if (sizeArg.getNodeCase() == Operand.NodeCase.EXPRESSION
                    && "filter".equals(sizeArg.getExpression().getOperator())) {
                // size(coll.filter(x, pred)) — count only the elements matching the lambda.
                List<Operand> filterOps = sizeArg.getExpression().getOperandsList();
                if (filterOps.size() != 2
                        || filterOps.get(0).getNodeCase() != Operand.NodeCase.VARIABLE
                        || filterOps.get(1).getNodeCase() != Operand.NodeCase.EXPRESSION
                        || !"lambda".equals(filterOps.get(1).getExpression().getOperator())) {
                    throw new IllegalArgumentException("Unsupported size(filter(...)) expression");
                }
                var = filterOps.get(0).getVariable();
                List<Operand> lambdaOps = filterOps.get(1).getExpression().getOperandsList();
                if (lambdaOps.size() != 2
                        || lambdaOps.get(1).getNodeCase() != Operand.NodeCase.VARIABLE) {
                    throw new IllegalArgumentException("lambda requires exactly 2 operands");
                }
                lambdaBody = lambdaOps.get(0);
                lambdaVarName = lambdaOps.get(1).getVariable();
            } else {
                throw new IllegalArgumentException("Unsupported size() expression");
            }
            AttributeMapping mapping = scope.resolveMapping(var);
            if (mapping instanceof AttributeMapping.Field) {
                // size(string) — CEL string length → LENGTH(column) <op> N.
                if (lambdaBody != null) {
                    throw new IllegalArgumentException(
                            "size(filter(...)) requires a collection (Relation) mapping for " + var);
                }
                Path<?> path = scope.resolvePath(var);
                return compareCount(cb.length(path.as(String.class)), op, numValue.intValue());
            }
            if (!(mapping instanceof AttributeMapping.Relation rel)) {
                throw new IllegalArgumentException("size() requires a collection (Relation) mapping for " + var);
            }
            final Operand fBody = lambdaBody;
            final String fVar = lambdaVarName;
            SubqueryBodyBuilder bodyBuilder = (sub, joinFrom, correlated) ->
                    fBody == null ? cb.conjunction()
                            : traverse(fBody, Scope.lambda(joinFrom, sub, rel, fVar,
                                    Scope.rebase(scope, correlated, sub)));

            boolean nonEmpty = ("gt".equals(op) && numValue == 0L) || ("ge".equals(op) && numValue == 1L);
            boolean empty = ("eq".equals(op) && numValue == 0L)
                    || ("le".equals(op) && numValue == 0L)
                    || ("lt".equals(op) && numValue == 1L);

            if (nonEmpty) {
                return existsSubquery(scope, rel, bodyBuilder);
            }
            if (empty) {
                return negate(existsSubquery(scope, rel, bodyBuilder));
            }
            // Arbitrary N → correlated (SELECT COUNT(...)) <op> N, same shape as exists_one.
            Subquery<Long> sub = scope.parentQuery().subquery(Long.class);
            From<?, ?> correlated = correlate(sub, scope.from());
            Join<?, ?> joinFrom = correlated.join(rel.joinAttribute());
            sub.select(cb.count(joinFrom));
            if (fBody != null) {
                sub.where(bodyBuilder.build(sub, joinFrom, correlated));
            }
            return compareCount(sub, op, numValue);
        }

        /** Compare a numeric size expression (COUNT subquery or LENGTH) against a constant. */
        private <N extends Number & Comparable<N>> Predicate compareCount(
                jakarta.persistence.criteria.Expression<N> count, String op, N n) {
            return switch (op) {
                case "eq" -> cb.equal(count, n);
                case "ne" -> cb.notEqual(count, n);
                case "lt" -> cb.lessThan(count, n);
                case "gt" -> cb.greaterThan(count, n);
                case "le" -> cb.lessThanOrEqualTo(count, n);
                case "ge" -> cb.greaterThanOrEqualTo(count, n);
                default -> throw new IllegalArgumentException(
                        "Unsupported size comparison operator: " + op);
            };
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
