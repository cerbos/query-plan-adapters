package dev.cerbos.queryplan.springdata;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The comparison-translation module: every leaf comparison — plain {@code field op value},
 * field-to-field, constant-vs-constant, constant-receiver string matches, arithmetic,
 * ternary-wrapped and {@code size()} comparisons — enters through {@link #translate} and
 * nowhere else. Inside, one operand-resolution seam ({@link #resolve}) classifies each
 * operand into a {@link Resolved} shape, and {@link #dispatch} translates the resolved
 * pair; predicate-level rewrites (the CEL ternary, the eq/ne string-concat solve) are
 * explicit steps in {@code translate}/{@code dispatch}, ordered by code structure. What
 * this replaces: a chain of order-dependent probes (ternary → size → arithmetic → a leaf
 * collector loop) where each probe re-scanned the raw operands and an ownership referee
 * decided whether the {@code add} fold/solve path or the arithmetic path translated a
 * given shape — the ordering was the specification, and it lived in comments.
 *
 * <p>Two of the pipeline's steps are collaborators of their own because their shapes are not
 * operand resolutions: the ternary rewrite ({@link TernaryTranslator}) substitutes branches
 * back into the RAW comparison and walks again, and {@code size()}
 * ({@link SizeTranslator}) emits subqueries. The double-space lowering that
 * {@link #dispatch} routes every remaining arithmetic-rooted pair to is
 * {@link ArithmeticTranslator}, which reaches back here for the constant fold and the raw
 * expression comparison so those are spelled once.
 *
 * <p>Design note — rejected alternative: an eagerly-converting resolver
 * ({@code resolve(operand) -> Constant(javaValue) | Column(path) | NumericSql(expr)})
 * that folds {@code add(value, value)} with {@link PlanValues#foldAdd} and converts
 * VALUES/paths at classification time was sketched first. It was rejected because
 * conversion errors are part of the observable contract: WHICH message a malformed
 * operand raises depends on the whole comparison's shape (a boolean inside {@code add}
 * is a foldAdd type error against a field but "Arithmetic comparison requires numeric
 * operands" against a constant; an unknown attribute must not preempt an "Unexpected
 * X() expression" on the sibling operand), so eager conversion either re-orders pinned
 * messages or forces the resolver to take a context parameter — which reintroduces the
 * caller-knows-best coupling the seam exists to remove. The chosen shape classifies
 * structurally and converts lazily at the dispatch site that consumes the operand.
 *
 * <p><b>Extension recipe — adding a new comparison-operand type</b>. The
 * {@code timestamp()} support is the worked example, implemented exactly this way:
 * <ol>
 *   <li>Add {@code Resolved} cases: {@link Resolved.TimestampField} /
 *       {@link Resolved.TimestampConstant}, the latter with a lazy accessor that
 *       parses the argument (its errors are then part of the contract);</li>
 *   <li>Classify them in {@link #resolve}'s EXPRESSION arm (before the {@code Opaque}
 *       fallback); a pure-constant argument folds in the accessor — never in
 *       dispatch;</li>
 *   <li>Handle the new pairings in {@link #dispatch} next to the existing typed cases
 *       ({@link #timestampLeaf} compares the column against the parsed instant via
 *       {@link LeafTranslator#withOverride} so {@link OperatorFunction} overrides keep
 *       working).</li>
 * </ol>
 * Nothing else changes: no new probe, no re-scan, no ordering decision — unmatched
 * pairings still fall through to {@link #leafOperandError}, whose "Unexpected
 * X() expression in leaf operand of Y" message stays the pinned fail-closed behavior.
 */
final class ComparisonTranslator {

    /**
     * The orderable/equality comparison operators (eq/ne/lt/gt/le/ge) — shared by the
     * ternary rewrite, the arithmetic path, and the constant-vs-constant fold.
     */
    static final Set<String> COMPARISON_OPS =
            Set.of("eq", "ne", "lt", "gt", "le", "ge");

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final LeafTranslator leaf;
    private final TernaryTranslator ternary;
    private final SizeTranslator sizes;
    private final ArithmeticTranslator arithmetic;

    ComparisonTranslator(CriteriaBuilder cb, TriPredicate tri, LeafTranslator leaf,
                         TernaryTranslator ternary, SizeTranslator sizes) {
        this.cb = cb;
        this.tri = tri;
        this.leaf = leaf;
        this.ternary = ternary;
        this.sizes = sizes;
        this.arithmetic = new ArithmeticTranslator(cb, tri, leaf, this);
    }

    /**
     * The single entry point for the {@code default} arm of
     * {@link PlanWalker#traverseExpression}: translate {@code op(operands...)} where
     * {@code op} is not one of the structural operators handled by name. The pipeline is
     * fixed by code order, not by probe-chain position:
     * <ol>
     *   <li><b>Ternary rewrite</b> on the RAW operands — a {@code cmp(if(...), other)}
     *       substitutes each branch back into the comparison and recurses, so it must see
     *       source order before any mirroring;</li>
     *   <li><b>Normalization</b> to field-first form (mirroring directional operators —
     *       see {@link NormalizedBinary}); every later stage assumes it;</li>
     *   <li><b>size() comparisons</b> as a dedicated step: the emptiness shortcuts
     *       (EXISTS / NOT EXISTS), the COUNT/LENGTH shapes and the tri-state
     *       {@code size(filter(...))} guard are subquery translations, not operand
     *       resolutions, and their SQL shapes are pinned by the differential oracle;</li>
     *   <li><b>Operand resolution</b> — each operand through the single {@link #resolve}
     *       seam;</li>
     *   <li><b>Dispatch</b> on the resolved pair ({@link #dispatch}).</li>
     * </ol>
     */
    Predicate translate(String op, List<Operand> operands, Scope scope) {
        Predicate ternaryPred = ternary.tryTernaryComparison(op, operands, scope);
        if (ternaryPred != null) {
            return ternaryPred;
        }
        NormalizedBinary nb = NormalizedBinary.of(op, operands);
        // Every leaf operator is binary. Extra operands are a malformed plan and must
        // fail loudly rather than silently dropping one — BEFORE the size() probe,
        // whose last-match-wins operand scan would otherwise translate a partial
        // comparison (e.g. eq(size(coll), variable, value) as COUNT = value, silently
        // discarding the variable constraint).
        if (nb.operands().size() != 2) {
            throw Refusals.malformed(
                    nb.op() + " requires exactly 2 operands, got " + nb.operands().size());
        }
        Predicate sizePred = sizes.trySizeComparison(nb.op(), nb.operands(), scope);
        if (sizePred != null) {
            return sizePred;
        }
        return dispatch(nb.op(),
                resolve(nb.operands().get(0)),
                resolve(nb.operands().get(1)),
                nb.operands(), scope);
    }

    // -- the operand-resolution seam --

    /**
     * A comparison operand resolved to its translation-relevant shape — the single seam
     * every leaf comparison goes through ({@link #resolve}). Resolution is purely
     * structural: values convert and constants fold LAZILY (at the dispatch site that
     * consumes them), because WHICH error a malformed operand raises depends on the shape
     * of the whole comparison — e.g. a non-numeric constant inside {@code add} is a
     * type-mismatch when solved against a field but an
     * "Arithmetic comparison requires numeric operands" when lowered to SQL arithmetic —
     * and eager conversion here would re-order those pinned messages.
     */
    private sealed interface Resolved {
        /** A plan constant (raw VALUE node); {@link #value()} converts on demand. */
        record Constant(Operand operand) implements Resolved {
            Object value() {
                return PlanValues.protoValueToJava(operand.getValue());
            }
        }

        /** A mapped column reference; the path resolves at the consuming dispatch site. */
        record Field(String variable) implements Resolved {}

        /**
         * {@code add(value, value)} — a pure-constant subtree. {@link #fold()} folds it
         * with {@link PlanValues#foldAdd} (strings concatenate, numbers add), so by the
         * time the resolved pair is dispatched no "who owns the fold" question exists.
         */
        record ConstantAdd(Operand left, Operand right) implements Resolved {
            Object fold() {
                return PlanValues.foldAdd(
                        PlanValues.protoValueToJava(left.getValue()),
                        PlanValues.protoValueToJava(right.getValue()));
            }
        }

        /**
         * {@code add(field, value)} / {@code add(value, field)} — solvable for the field
         * under eq/ne against a constant ({@link PlanValues#solveAdd}) when the solve is
         * algebraically exact (string concatenation, in-range long/long integers); every
         * other pairing — including fractional doubles, which IEEE subtraction cannot
         * invert — lowers to SQL arithmetic.
         */
        record FieldPlusConstant(String fieldVariable, Operand constant, boolean fieldIsLeft)
                implements Resolved {}

        /**
         * Any other arithmetic-rooted expression ({@code sub}/{@code mult}/{@code div}/
         * {@code mod}, or {@code add} in a shape with nested expressions or wrong arity) —
         * lowered to double-space SQL by {@link ArithmeticTranslator}.
         */
        record Arithmetic(String operator) implements Resolved {}

        /**
         * {@code timestamp(variable)} — a temporal column wrapped in the CEL
         * {@code timestamp()} cast. The path resolves at the consuming dispatch site
         * ({@link #timestampLeaf}), which also owns the column-type contract.
         */
        record TimestampField(String variable) implements Resolved {}

        /**
         * {@code timestamp(value)} — a constant instant. The planner constant-folds
         * {@code now()}/{@code now() - duration(...)} arithmetic and re-wraps the result
         * in {@code timestamp("<RFC-3339>")} on the wire (PDP-verified), so both policy
         * literals and folded relative windows arrive in this shape. {@link #instant()}
         * parses lazily: {@link Instant#parse} first, {@link OffsetDateTime#parse} as
         * the fallback for non-UTC offsets (Cerbos emits literals verbatim, including
         * offsets and nanosecond precision) — normalizing to the absolute instant,
         * matching CEL timestamp equality across offsets.
         */
        record TimestampConstant(Operand operand) implements Resolved {
            Instant instant() {
                Object raw = PlanValues.protoValueToJava(operand.getValue());
                // CEL's own timestamp() rejects a non-string or unparseable literal, so
                // the planner cannot emit one: both are malformed, not unsupported.
                if (!(raw instanceof String s)) {
                    throw Refusals.malformed(
                            "timestamp() constant must be an RFC-3339 string, got "
                                    + (raw == null ? "null" : raw.getClass().getSimpleName()));
                }
                try {
                    return Instant.parse(s);
                } catch (DateTimeParseException e) {
                    try {
                        return OffsetDateTime.parse(s).toInstant();
                    } catch (DateTimeParseException e2) {
                        throw Refusals.malformed(
                                "timestamp() constant could not be parsed as an RFC-3339 instant", e2);
                    }
                }
            }
        }

        /**
         * {@code string(variable)} — CEL's string conversion over a mapped column. Only a
         * BOOLEAN column has a lowering ({@link #booleanStringComparison}); the column's type is
         * read at the consuming dispatch site, and every other type is refused there with the
         * leaf-operand message a {@code string()} operand has always raised.
         */
        record StringOfField(String variable) implements Resolved {}

        /**
         * An operand no leaf comparison understands ({@code map()}, {@code lambda},
         * {@code timestamp()} over a nested expression, an unset node...). Dispatch
         * routes these to {@link #leafOperandError}, which reports from the RAW operands
         * so each shape keeps its exact message.
         */
        record Opaque() implements Resolved {}
    }

    /**
     * THE operand-resolution seam: classify one comparison operand. Adding a new operand
     * type starts here — see the extension recipe on {@link ComparisonTranslator}.
     */
    private Resolved resolve(Operand o) {
        return switch (o.getNodeCase()) {
            case VALUE -> new Resolved.Constant(o);
            case VARIABLE -> new Resolved.Field(o.getVariable());
            case EXPRESSION -> {
                PlanResourcesFilter.Expression e = o.getExpression();
                String exprOp = e.getOperator();
                // timestamp(variable) / timestamp(value) — the only shapes the planner
                // emits for temporal comparisons (PDP-verified: the folded now()-duration
                // constant is re-wrapped in timestamp(), never a bare string). A nested
                // expression inside timestamp() has no verified translation and stays
                // Opaque → leafOperandError.
                if ("timestamp".equals(exprOp) && e.getOperandsCount() == 1) {
                    Operand arg = e.getOperands(0);
                    if (arg.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        yield new Resolved.TimestampField(arg.getVariable());
                    }
                    if (arg.getNodeCase() == Operand.NodeCase.VALUE) {
                        yield new Resolved.TimestampConstant(arg);
                    }
                    yield new Resolved.Opaque();
                }
                // string(variable). Anything else inside string() — a nested expression, a
                // constant — stays Opaque → leafOperandError.
                if ("string".equals(exprOp) && e.getOperandsCount() == 1
                        && e.getOperands(0).getNodeCase() == Operand.NodeCase.VARIABLE) {
                    yield new Resolved.StringOfField(e.getOperands(0).getVariable());
                }
                if (!ArithmeticTranslator.ARITHMETIC_OPS.contains(exprOp)) {
                    yield new Resolved.Opaque();
                }
                if ("add".equals(exprOp) && e.getOperandsCount() == 2) {
                    Operand l = e.getOperands(0);
                    Operand r = e.getOperands(1);
                    boolean lValue = l.getNodeCase() == Operand.NodeCase.VALUE;
                    boolean rValue = r.getNodeCase() == Operand.NodeCase.VALUE;
                    if (lValue && rValue) {
                        yield new Resolved.ConstantAdd(l, r);
                    }
                    if (l.getNodeCase() == Operand.NodeCase.VARIABLE && rValue) {
                        yield new Resolved.FieldPlusConstant(l.getVariable(), r, true);
                    }
                    if (lValue && r.getNodeCase() == Operand.NodeCase.VARIABLE) {
                        yield new Resolved.FieldPlusConstant(r.getVariable(), l, false);
                    }
                }
                yield new Resolved.Arithmetic(exprOp);
            }
            default -> new Resolved.Opaque();
        };
    }

    /** Whether this operand resolved to an {@code add}-rooted expression (any shape). */
    private static boolean isAddRooted(Resolved r) {
        return r instanceof Resolved.ConstantAdd
                || r instanceof Resolved.FieldPlusConstant
                || (r instanceof Resolved.Arithmetic a && "add".equals(a.operator()));
    }

    /** Whether this operand resolved to any arithmetic-rooted expression. */
    private static boolean isArithmeticRooted(Resolved r) {
        return r instanceof Resolved.ConstantAdd
                || r instanceof Resolved.FieldPlusConstant
                || r instanceof Resolved.Arithmetic;
    }

    // -- dispatch on the resolved pair --

    /**
     * Translate one leaf comparison from its resolved operand pair. Cases are ordered by
     * code structure, top to bottom; {@code operands} is the (normalized) raw operand list,
     * kept only for the paths that must see raw shapes — SQL arithmetic lowering
     * ({@link ArithmeticTranslator} walks subtrees) and error reporting
     * ({@link #leafOperandError} pins per-shape messages).
     */
    private Predicate dispatch(String op, Resolved left, Resolved right,
                               List<Operand> operands, Scope scope) {
        // Constant-vs-constant comparisons are statically evaluated. The planner never emits
        // them directly, but ternary substitution produces them — the else branch of
        // `(aBool ? aNumber : 0) > 0` becomes gt(value(0), value(0)).
        if (COMPARISON_OPS.contains(op)
                && left instanceof Resolved.Constant lc
                && right instanceof Resolved.Constant rc) {
            return constantComparison(op, lc.value(), rc.value());
        }

        // Constant-receiver string matches: `"a,b".contains(R.attr.x)` arrives as
        // contains(value, variable) — the CONSTANT is the haystack and the COLUMN the
        // needle (NormalizedBinary deliberately leaves these in source order). An unfolded
        // concat receiver (`("a" + "b").contains(R.attr.x)`) folds here too — NOT into the
        // add-solve path, which would translate the INVERTED column-haystack LIKE.
        StringMatch match = StringMatch.of(op);
        if (match != null && right instanceof Resolved.Field needleField) {
            Object receiver = left instanceof Resolved.Constant c ? c.value()
                    : left instanceof Resolved.ConstantAdd ca ? ca.fold()
                    : null;
            if (receiver != null) {
                if (!(receiver instanceof String haystack)) {
                    // `5.contains(x)` has no overload in CEL; the planner never folds one.
                    throw Refusals.malformed(
                            op + " requires a string receiver, got "
                                    + PlanValues.typeName(receiver));
                }
                Path<?> needle = scope.path(needleField.variable());
                // The needle is a column, so it is escaped dynamically; a NULL needle is
                // a missing attribute → CEL error → deny (fieldToFieldLike guards it).
                return fieldToFieldLike(cb.literal(haystack), needle, match);
            }
            // A NULL receiver constant is not a haystack; fall through so the null-RHS
            // leaf branch below owns the error message.
        }

        if (COMPARISON_OPS.contains(op)) {
            // timestamp(field) vs timestamp(constant) — the wire shape of every
            // time-window / retention-cutoff policy (`timestamp(R.attr.createdAt) <
            // now() - duration("24h")` folds its RHS to timestamp("<instant>")).
            // NormalizedBinary cannot reorder these (both operands are EXPRESSION
            // nodes, equal rank), so the value-first form is MIRRORED here — never
            // inverted: the planner preserves policy source order.
            if (left instanceof Resolved.TimestampField tsField
                    && right instanceof Resolved.TimestampConstant tsConst) {
                return timestampLeaf(op, tsField, tsConst, scope);
            }
            if (left instanceof Resolved.TimestampConstant tsConst
                    && right instanceof Resolved.TimestampField tsField) {
                return timestampLeaf(NormalizedBinary.mirror(op), tsField, tsConst, scope);
            }
            // Two constant instants — reachable through ternary substitution, like the
            // numeric constant-vs-constant fold above; instant comparison is exact.
            if (left instanceof Resolved.TimestampConstant lts
                    && right instanceof Resolved.TimestampConstant rts) {
                return timestampConstantComparison(op, lts.instant(), rts.instant());
            }
            // string(column) eq/ne a string constant. NormalizedBinary puts the string()
            // operand first (an EXPRESSION outranks a VALUE), so the value-first spelling
            // arrives here too. Ordering operators and non-string constants fall through to
            // leafOperandError, as does every column that is not a boolean.
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.StringOfField sf
                    && right instanceof Resolved.Constant c
                    && c.value() instanceof String text) {
                return booleanStringComparison(op, sf, text, operands, scope);
            }
            // Fold: `field op add(value, value)` — the folded constant compares like any
            // plan constant (normalization guarantees the field arrives first). Strings
            // concatenate here, matching CEL — this shape never enters double space.
            if (left instanceof Resolved.Field f && right instanceof Resolved.ConstantAdd ca) {
                return leaf.applyLeaf(op, scope.path(f.variable()), ca.fold());
            }
            // Solve: `add(field, const) eq/ne constant` — for the ALGEBRAICALLY EXACT
            // shapes only (string concatenation, in-range long/long integers).
            // Fractional/oversized numeric pairs fall through to numericComparison:
            // IEEE subtraction does not invert IEEE addition (fl(fl(t-c)+c) != t), so
            // a Java-side solve would return rows the PDP's check() denies — the SQL
            // side must compute fl(field + const) and compare it to the target in
            // double space, sharing IEEE semantics with the ordering operators.
            if (("eq".equals(op) || "ne".equals(op))
                    && left instanceof Resolved.FieldPlusConstant fpc
                    && right instanceof Resolved.Constant other
                    && !PlanValues.requiresSqlLowering(
                            other.value(),
                            PlanValues.protoValueToJava(fpc.constant().getValue()))) {
                return solveAddComparison(op, fpc, other, scope);
            }
            // Everything else arithmetic-rooted lowers to SQL-side double-space arithmetic.
            if (isArithmeticRooted(left) || isArithmeticRooted(right)) {
                return arithmetic.numericComparison(op, operands, scope);
            }
        } else if (isAddRooted(left) || isAddRooted(right)) {
            // add under a non-comparison operator (string matches, unknown operators):
            // only the constant fold against a field translates; everything else reports
            // the add-specific shape errors.
            return addFoldOrError(op, operands, scope);
        }

        if (left instanceof Resolved.Field a && right instanceof Resolved.Field b) {
            return fieldToFieldComparison(op, a.variable(), b.variable(), scope);
        }

        // The ordinary scalar leaf: one mapped column against one plan constant. Order-
        // insensitive on purpose — receiver-sensitive operators are never normalized, so a
        // null receiver arrives value-first and must still reach the null-RHS message.
        Resolved.Field field = left instanceof Resolved.Field lf ? lf
                : right instanceof Resolved.Field rf ? rf : null;
        Resolved.Constant constant = left instanceof Resolved.Constant lc2 ? lc2
                : right instanceof Resolved.Constant rc2 ? rc2 : null;
        if (field != null && constant != null) {
            return leafFieldValue(op, field, constant, scope);
        }

        throw leafOperandError(op, operands);
    }

    /** `field op value` (or value-first for non-normalized operators): the scalar leaf. */
    private Predicate leafFieldValue(String op, Resolved.Field field,
                                     Resolved.Constant constant, Scope scope) {
        Object value = constant.value();

        // A structured constant — a CEL list literal (`R.attr.tags == ["a", "b"]`
        // arrives as eq(variable, value-list) verbatim; PDP-verified in both operand
        // orders) or, defensively, a struct VALUE (protoValueToJava can produce a Map,
        // though the planner emits map literals as struct() expressions, which
        // leafOperandError already names). No scalar-column comparison exists for
        // these: letting the value through dies inside Hibernate with a raw coercion
        // error ("Could not convert ... ListN to java.lang.String") instead of the
        // adapter's named-IllegalArgumentException contract. Checked BEFORE path
        // resolution so a Relation-mapped attribute reports this shape too, not the
        // generic "is a Relation" resolution error. Reports the shape only — element
        // values never leak into the message.
        if (value instanceof List<?> || value instanceof Map<?, ?>) {
            throw Refusals.unsupported(
                    op + " comparison against a " + constantShape(value)
                            + " constant is not supported for attribute "
                            + field.variable() + ". Whole-" + kindWord(value)
                            + " equality is not translatable to a scalar column"
                            + " comparison; map the attribute as a Relation and use"
                            + " in/hasIntersection, or compare elements individually.");
        }

        Path<?> path = scope.path(field.variable());

        if (value == null) {
            // A registered override owns the operator's full translation, including a null RHS.
            return leaf.withOverride(op, path, null, () -> switch (op) {
                case "eq" -> cb.isNull(path);
                case "ne" -> cb.isNotNull(path);
                // `x < null` is legal CEL over a dyn attribute (it errors at check time,
                // which denies); no ordering predicate reproduces that, so refuse it.
                default -> throw Refusals.unsupported(
                        "Null values are only supported with eq and ne operators (got " + op + ")");
            });
        }

        // An attribute the caller sends as an explicit null holds a null VALUE in CEL,
        // so equality against a non-null operand is definite. An operator the caller
        // overrode is left to the override: replacing it would make this declaration
        // silently discard the caller's own translation (#308).
        if (("eq".equals(op) || "ne".equals(op))
                && !leaf.overridden(op)
                && leaf.isExplicitNull(field.variable(), scope)) {
            return leaf.definiteEquality(op, path, cb.literal(value), true, false);
        }

        return leaf.applyLeaf(op, path, value);
    }

    /**
     * {@code timestamp(field) op timestamp(constant)}: compare a temporal column against
     * a parsed constant instant. {@code op} is already field-first (the dispatch mirrors
     * value-first forms before calling here).
     *
     * <p><b>Column-type contract.</b> Only column types that unambiguously denote an
     * absolute instant are translated:
     * <ul>
     *   <li>{@link Instant} — bound as-is;</li>
     *   <li>{@link OffsetDateTime} — bound as the instant at UTC. Hibernate 6 stores
     *       both with {@code SqlTypes.TIMESTAMP_UTC} (normalized to UTC before
     *       binding), so the database comparison is an instant comparison regardless
     *       of the bound offset.</li>
     * </ul>
     * {@code LocalDateTime} (no zone — the stored wall-clock time could mean any
     * instant), {@code java.util.Date} (JDBC binding routes through zone conversions),
     * {@code String} (format- and offset-dependent lexicographic order) and everything
     * else throw a NAMED error instead of guessing: a wrong zone assumption here would
     * silently include rows the PDP's {@code check()} denies (or vice versa) — an
     * authorization-relevant divergence, so the adapter fails closed. A registered
     * {@link OperatorFunction} override is consulted FIRST (with the parsed
     * {@link Instant} as the value), so callers who know their column's zone semantics
     * can translate those types themselves.
     *
     * <p>A NULL column value makes every comparison UNKNOWN under SQL three-valued
     * logic → the row is excluded, matching CEL: a missing attribute is an evaluation
     * error and {@code check()} denies (PDP-verified for eq/ne/lt and mirrored forms).
     */
    private Predicate timestampLeaf(String op, Resolved.TimestampField field,
                                    Resolved.TimestampConstant constant, Scope scope) {
        Instant instant = constant.instant();
        Path<?> path = scope.path(field.variable());
        return leaf.withOverride(op, path, instant, () -> {
            Class<?> javaType = path.getJavaType();
            Object bound;
            if (Instant.class.equals(javaType)) {
                bound = instant;
            } else if (OffsetDateTime.class.equals(javaType)) {
                bound = instant.atOffset(ZoneOffset.UTC);
            } else {
                // Unmapped rather than unsupported: the plan is fine and the Criteria
                // API could compare the column, but the MAPPING does not say which
                // instant a LocalDateTime/Date/String holds. The caller resolves it by
                // remapping the column or registering an override — a declaration.
                throw Refusals.unmapped(
                        "timestamp() comparison requires a column mapped to java.time.Instant "
                                + "or java.time.OffsetDateTime, but '" + field.variable()
                                + "' maps to " + javaType.getSimpleName()
                                + ". Other temporal representations (LocalDateTime, "
                                + "java.util.Date, String) are ambiguous about the absolute "
                                + "instant they store; remap the column or register an "
                                + "OperatorFunction override for '" + op + "'.");
            }
            return leaf.defaultLeaf(op, path, bound);
        });
    }

    /**
     * Statically evaluate a comparison between two constant instants — reachable via
     * ternary substitution, mirroring {@link #constantComparison}. Instant comparison
     * is total and exact, so the collapse is oracle-faithful.
     */
    private Predicate timestampConstantComparison(String op, Instant left, Instant right) {
        return constant(holds(op, left.compareTo(right)));
    }

    /**
     * {@code string(boolColumn) eq/ne "text"}: CEL's string conversion of a boolean column,
     * compared with a string constant.
     *
     * <p>CEL renders a bool as exactly {@code "true"} or {@code "false"}, so the comparison is
     * decided HERE, byte for byte, and the store is only ever asked about the boolean column:
     * {@code string(x) == "true"} is {@code x == true}, {@code string(x) == "false"} is
     * {@code x == false}, and any other constant matches no value at all. Both SQL spellings of
     * the conversion are wrong somewhere. A {@code CAST} renders {@code 'true'} on H2 and
     * PostgreSQL and {@code '1'} on MySQL, which stores a boolean as a number. And
     * {@code CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END} compared
     * with the constant puts two LITERALS on the comparison, which a store compares in its
     * CONNECTION collation rather than a column's: MySQL Connector/J leaves that at
     * {@code utf8mb4_0900_ai_ci} even on a server whose columns are case-sensitive, and there
     * the CASE form returned every true row for {@code string(x) == "TRUE"}, all of which CEL
     * denies (measured on this repository's MySQL leg; cerbos/query-plan-adapters#418 proposed
     * the CASE).
     *
     * <p>A NULL column is a missing attribute, or an explicit null, on the check side, and CEL
     * has no {@code string()} for either: it raises, and the PDP denies the row under both
     * polarities. The two word arms keep that by construction ({@code NULL = true} is UNKNOWN);
     * the no-match arm states it, as {@link #solveAddComparison} does for an unsolvable
     * concatenation. That is also why an explicit-null declaration does not make this equality
     * definite the way it does a plain column's.
     *
     * <p>The two word arms go through {@link LeafTranslator#applyLeaf} with the boolean the
     * constant names, as the bare boolean attribute does, so an {@code eq}/{@code ne} override
     * sees them. Every other column type keeps the refusal a {@code string()} operand has always
     * raised: a number's or an instant's text form is what the dialects render on their own
     * terms, and nothing here reproduces CEL's. {@link Boolean} only, not the primitive: the
     * leaf's type check would read a {@code boolean} path as incompatible with a
     * {@link Boolean} value and fold the comparison to a constant.
     */
    private Predicate booleanStringComparison(String op, Resolved.StringOfField field,
                                              String value, List<Operand> operands, Scope scope) {
        Path<?> path = scope.path(field.variable());
        if (!Boolean.class.equals(path.getJavaType())) {
            throw leafOperandError(op, operands);
        }
        if ("true".equals(value) || "false".equals(value)) {
            return leaf.applyLeaf(op, path, Boolean.valueOf(value));
        }
        return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                () -> cb.isNull(path));
    }

    /**
     * Shape description for a structured constant in an error message: size and kind
     * only — never element values, matching the adapter's no-value-leak discipline
     * (see {@link PlanValues#typeName}).
     */
    private static String constantShape(Object value) {
        if (value instanceof List<?> l) {
            return "list of " + l.size() + " element" + (l.size() == 1 ? "" : "s");
        }
        Map<?, ?> m = (Map<?, ?>) value;
        return "map of " + m.size() + " entr" + (m.size() == 1 ? "y" : "ies");
    }

    private static String kindWord(Object value) {
        return value instanceof List<?> ? "list" : "map";
    }

    /**
     * Solve {@code add(field, const) eq/ne constant} for the field — only reached for
     * algebraically exact solves (string concatenation, in-range long/long integers;
     * {@link #dispatch} routes fractional doubles to
     * {@link ArithmeticTranslator#numericComparison} because IEEE subtraction does not
     * invert IEEE addition). When no solution exists (e.g.
     * {@code "projects:123" == "users:" + R.id} can never be true), eq is always-false;
     * ne is NOT always-true — a missing attribute makes the concatenation a CEL
     * evaluation error ({@code "users:" + null}) → deny, so NULL rows must stay
     * excluded: IS NOT NULL, never an unconditional {@code 1=1} (which would leak exactly
     * the rows the PDP denies).
     */
    private Predicate solveAddComparison(String op, Resolved.FieldPlusConstant fpc,
                                         Resolved.Constant other, Scope scope) {
        Object otherValue = other.value();
        Object addConst = PlanValues.protoValueToJava(fpc.constant().getValue());
        Object solved = PlanValues.solveAdd(otherValue, addConst, fpc.fieldIsLeft());
        if (solved == null) {
            return tri.baseUnlessUnknown("ne".equals(op) ? cb.conjunction() : cb.disjunction(),
                    () -> cb.isNull(scope.path(fpc.fieldVariable())));
        }
        return leaf.applyLeaf(op, scope.path(fpc.fieldVariable()), solved);
    }

    /**
     * {@code add} under a non-comparison operator. The only translatable shape is the
     * constant fold against a field ({@code ("a" + "b") op field} with the fold as the
     * VALUE side); the rest report the add-specific shape errors, matching the raw
     * operand layout (either side may hold the {@code add}).
     */
    private Predicate addFoldOrError(String op, List<Operand> operands, Scope scope) {
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
        if (otherOperand == null) {
            // Both operands are add() expressions — there IS a second operand, it just
            // isn't a scalar to fold against, so say that instead of misreporting arity.
            throw Refusals.unsupported(
                    op + " between two add() expressions is not supported: got "
                            + Refusals.describeOperand(operands.get(0)) + " and "
                            + Refusals.describeOperand(operands.get(1))
                            + "; one operand must be a mapped attribute or constant");
        }
        List<Operand> addOperands = addExprOperand.getExpression().getOperandsList();
        if (addOperands.size() != 2) {
            throw Refusals.malformed("add requires exactly 2 operands");
        }
        Operand addLeft = addOperands.get(0);
        Operand addRight = addOperands.get(1);
        if (addLeft.getNodeCase() == Operand.NodeCase.VALUE
                && addRight.getNodeCase() == Operand.NodeCase.VALUE) {
            Object folded = PlanValues.foldAdd(
                    PlanValues.protoValueToJava(addLeft.getValue()),
                    PlanValues.protoValueToJava(addRight.getValue()));
            if (otherOperand.getNodeCase() != Operand.NodeCase.VARIABLE) {
                throw Refusals.unsupported(
                        "add(const, const) compared to a non-field operand is not supported");
            }
            return leaf.applyLeaf(op, scope.path(otherOperand.getVariable()), folded);
        }
        throw Refusals.unsupported(
                "add comparison with a field reference only supports eq/ne (got " + op + ")");
    }

    /**
     * Report an operand shape no leaf case accepts. Reads the RAW operands in order so
     * each malformed shape keeps its exact message: {@code map()} points at the supported
     * {@code hasIntersection} wrapping, other expressions name themselves, unset nodes
     * report their node case, and an all-constant pair reports the missing variable.
     */
    private IllegalArgumentException leafOperandError(String op, List<Operand> operands) {
        String variable = null;
        for (Operand o : operands) {
            switch (o.getNodeCase()) {
                case VARIABLE -> variable = o.getVariable();
                // Conversion can itself reject a malformed VALUE — same order as reading
                // the operands left to right.
                case VALUE -> PlanValues.protoValueToJava(o.getValue());
                case EXPRESSION -> {
                    // H3: map() compositions are only accepted inside hasIntersection.
                    // A direct comparison like eq(map(...), [...]) reaches here; point users
                    // at the supported shape rather than throwing a generic operand error.
                    String innerOp = o.getExpression().getOperator();
                    if ("map".equals(innerOp)) {
                        throw Refusals.unsupported(
                                "Direct comparison of map(...) to a value is not supported "
                                        + "(operator: " + op + "). Wrap the map() expression in "
                                        + "hasIntersection(map(...), [...]) instead.");
                    }
                    // eq(except(variable, value-list), value-list) — the comparison
                    // form of the two-list except() (PDP-verified wire shape).
                    if ("except".equals(innerOp)) {
                        throw Refusals.exceptUnsupported();
                    }
                    // A computed operand — a cast, an index, a lambda, a nested
                    // timestamp(): legal CEL the leaf cases have no column shape for.
                    throw Refusals.unsupported(
                            "Unexpected " + innerOp + "() expression in leaf operand of " + op);
                }
                default -> throw Refusals.malformed(
                        "Unexpected operand type in leaf expression: " + o.getNodeCase());
            }
        }
        // Two constants under an operator the constant fold does not cover: a
        // comparison the planner evaluates itself and never ships.
        if (variable == null) {
            return Refusals.malformed("Missing variable operand for " + op);
        }
        return Refusals.malformed("Missing value operand for " + op);
    }

    /**
     * Statically evaluate a comparison between two plan constants and collapse it to an
     * always-true ({@code 1=1}) or always-false ({@code 1=0}) predicate — the same collapse
     * the unsolvable {@code add}-solve cases use. Numbers compare in double space: protobuf
     * {@code Value.getNumberValue()} is a double, and {@link PlanValues#protoValueToJava}
     * only splits Long/Double for whole-number cosmetics, not semantics. Strings compare
     * lexicographically; booleans (and mixed incomparable types) support eq/ne only —
     * eq → false, ne → true — while ordering them is a planner bug and throws.
     *
     * <p>Numeric ordering uses the primitive IEEE operators, NOT {@link Double#compare}:
     * the total order ranks {@code NaN} above every number (and {@code -0.0} below
     * {@code 0.0}), so {@code Double.compare} would collapse {@code gt}/{@code ge}
     * against a NaN constant — reachable via an unfolded {@code div(0,0)}, e.g. the
     * else arm of {@code (aBool ? 1.0 : 0.0/0.0) > 0.5} — to always-true, returning
     * rows the PDP denies. Cerbos 0.55 uses IEEE false for an unordered NaN pair, so the primitive
     * comparison also preserves its result under negation.
     */
    Predicate constantComparison(String op, Object left, Object right) {
        if ("eq".equals(op) || "ne".equals(op)) {
            boolean equal = (left instanceof Number ln && right instanceof Number rn)
                    ? ln.doubleValue() == rn.doubleValue()
                    : Objects.equals(left, right);
            return constant("eq".equals(op) == equal);
        }
        if (left instanceof Number ln && right instanceof Number rn) {
            double l = ln.doubleValue();
            double r = rn.doubleValue();
            return constant(switch (op) {
                case "lt" -> l < r;
                case "gt" -> l > r;
                case "le" -> l <= r;
                case "ge" -> l >= r;
                default -> throw Refusals.internal(
                        "Unsupported constant comparison operator: " + op);
            });
        }
        if (left instanceof String ls && right instanceof String rs) {
            return constant(holds(op, ls.compareTo(rs)));
        }
        // Ordering two booleans, or a string against a number, has no CEL
        // overload: the planner would have rejected the policy.
        throw Refusals.malformed(
                "Cannot order constant operands of " + op + ": "
                        + PlanValues.typeName(left) + " vs " + PlanValues.typeName(right));
    }

    /**
     * Whether {@code op} holds for a {@link Comparable#compareTo} result — the total orders
     * (strings, instants). Numbers never come through here: see {@link #constantComparison}.
     */
    private static boolean holds(String op, int cmp) {
        return switch (op) {
            case "eq" -> cmp == 0;
            case "ne" -> cmp != 0;
            case "lt" -> cmp < 0;
            case "gt" -> cmp > 0;
            case "le" -> cmp <= 0;
            case "ge" -> cmp >= 0;
            default -> throw Refusals.internal(
                    "Unsupported constant comparison operator: " + op);
        };
    }

    /** A statically decided comparison: always-true ({@code 1=1}) or always-false ({@code 1=0}). */
    private Predicate constant(boolean result) {
        return result ? cb.conjunction() : cb.disjunction();
    }

    /**
     * Compare two mapped columns directly (eq/ne/lt/gt/le/ge) or pattern-match one column
     * against another (contains/startsWith/endsWith). Operand source order is preserved —
     * two variables rank equally, so {@link NormalizedBinary} never swaps them.
     */
    private Predicate fieldToFieldComparison(String op, String leftVar, String rightVar,
                                             Scope scope) {
        Expression<?> left = scope.path(leftVar);
        Expression<?> right = scope.path(rightVar);
        if (Temporal.class.isAssignableFrom(left.getJavaType())
                || Temporal.class.isAssignableFrom(right.getJavaType())) {
            throw Refusals.unsupported("Bare temporal comparison cannot preserve CEL string "
                    + "equality; use timestamp() explicitly");
        }
        boolean leftExplicit = leaf.isExplicitNull(leftVar, scope);
        boolean rightExplicit = leaf.isExplicitNull(rightVar, scope);
        if (!LeafTranslator.compatibleTypes(left.getJavaType(), right.getJavaType())) {
            return "eq".equals(op) || "ne".equals(op)
                    ? leaf.definiteEquality(op, left, right, leftExplicit, rightExplicit)
                    : tri.unknown();
        }
        // Mixing the two conventions across one comparison has no faithful rendering.
        // The declared side needs a definite answer for its NULL (CEL holds a null
        // VALUE); the undeclared side needs UNKNOWN for its NULL (a missing attribute,
        // which CEL denies under both polarities). A definite predicate returns rows the
        // PDP refuses; a plain one drops rows the PDP allows. Refuse it rather than pick
        // a direction — declare both attributes, or neither.
        if (("eq".equals(op) || "ne".equals(op)) && leftExplicit != rightExplicit) {
            // Unmapped: the two declarations conflict, and the message tells the
            // caller which declaration to change.
            throw Refusals.unmapped(
                    "Cannot translate `" + op + "` between two columns under mixed null"
                            + " conventions: cannot compare an attribute declared"
                            + " explicit-null with one on the omitted convention: the"
                            + " omitted side is UNKNOWN for a NULL column while the"
                            + " declared side is definite, and no single predicate is"
                            + " both. Declare the convention on both mappings, or on"
                            + " neither.");
        }
        if (("eq".equals(op) || "ne".equals(op)) && leftExplicit && rightExplicit) {
            return leaf.definiteEquality(op, left, right, leftExplicit, rightExplicit);
        }
        if (COMPARISON_OPS.contains(op)) {
            return comparePredicate(op, left, right);
        }
        StringMatch match = StringMatch.of(op);
        if (match != null) {
            return fieldToFieldLike(left, right, match);
        }
        throw Refusals.unsupported(
                "Field-to-field comparison is not supported for operator '" + op + "': "
                        + leftVar + " vs " + rightVar);
    }

    /**
     * Raw-typed comparison of two SQL expressions — the shared dispatch of field-to-field
     * comparisons and arithmetic expression-vs-expression comparisons. Constant-RHS shapes
     * do NOT route here: they bind through the plain-value overloads on purpose (double
     * bind parameters — see {@link ArithmeticTranslator#numericComparison}).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    Predicate comparePredicate(String op,
                               Expression left,
                               Expression right) {
        return switch (op) {
            case "eq" -> cb.equal(left, right);
            case "ne" -> cb.notEqual(left, right);
            case "lt" -> cb.lessThan(left, right);
            case "gt" -> cb.greaterThan(left, right);
            case "le" -> cb.lessThanOrEqualTo(left, right);
            case "ge" -> cb.greaterThanOrEqualTo(left, right);
            default -> throw Refusals.internal(
                    "Unsupported arithmetic comparison operator: " + op);
        };
    }

    /** Escaped by {@link #fieldToFieldLike} in this order: the escape character itself first. */
    private static final List<String> LIKE_METACHARACTERS = List.of("\\", "%", "_", "[");

    /**
     * {@code haystackColumn LIKE wildcards(escape(needleColumn))} — the column-to-column
     * analogue of the constant LIKE path in {@link LeafTranslator#defaultLeaf}. The needle
     * is data, so its LIKE metacharacters are escaped dynamically with nested
     * {@code REPLACE} (portable: H2/Postgres/MySQL/Oracle/SQL Server): {@code \} first,
     * then {@code %}, {@code _}, and {@code [}, mirroring {@link PlanValues#escapeLike} and
     * the same explicit {@code '\'} escape char. {@code [} is escaped because SQL Server
     * LIKE treats {@code [...]} as a character class even under an ESCAPE clause;
     * {@code \[} is a literal {@code [} on every targeted dialect ({@code ]} needs no
     * escaping once no {@code [} can open a class — see {@link PlanValues#escapeLike}).
     *
     * <p>A NULL needle must make the whole predicate UNKNOWN, not FALSE. CEL raises a
     * missing-attribute error, which denies under BOTH polarities, and only UNKNOWN
     * reproduces that: this used to be spelled {@code needle IS NOT NULL AND haystack LIKE
     * pattern}, which is definite-FALSE for a NULL needle, and {@code NOT FALSE} is TRUE —
     * so every negated column-needle match returned exactly the rows whose needle is NULL,
     * which the PDP denies (cerbos/query-plan-adapters#387). Nesting the guard in a CASE
     * that yields a NULL PATTERN keeps the LIKE itself UNKNOWN, and still defends against
     * dialects whose {@code CONCAT} treats NULL as {@code ''} and would otherwise build a
     * match-anything {@code '%%'}.
     */
    private Predicate fieldToFieldLike(Expression<?> haystack, Expression<?> needle,
                                       StringMatch match) {
        Expression<String> pattern = needle.as(String.class);
        for (String metacharacter : LIKE_METACHARACTERS) {
            pattern = cb.function("replace", String.class,
                    pattern, cb.literal(metacharacter), cb.literal("\\" + metacharacter));
        }
        if (match.leadingWildcard) {
            pattern = cb.concat(cb.literal("%"), pattern);
        }
        if (match.trailingWildcard) {
            pattern = cb.concat(pattern, cb.literal("%"));
        }
        Expression<String> guardedPattern =
                cb.<String>selectCase()
                        .when(cb.isNull(needle), cb.nullLiteral(String.class))
                        .otherwise(pattern);
        return cb.like(haystack.as(String.class), guardedPattern, '\\');
    }
}
