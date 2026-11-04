/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

package dev.cerbos.queryplan.jpa;

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter;
import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;

import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Translates the operators CEL applies to a map-valued attribute as a whole: {@code has(obj)},
 * {@code "key" in obj}, and {@code obj.exists(k, ...)}, {@code obj.exists(k, v, ...)},
 * {@code obj.all(k, ...)}, {@code obj.all(k, v, ...)}, which range over the map's keys.
 *
 * <p>An object-valued attribute is a plan variable the mapping does not name but whose fields it
 * does: {@code request.resource.attr.parent} when the mapping holds
 * {@code request.resource.attr.parent.aBool} and siblings. Its key set is the mapped field
 * names, so the caller must send {@code check()} exactly those keys, each absent when its
 * mapping says so ({@link NullAttributeRepresentation#OMITTED}). A key the mapping does not
 * name cannot be tested, and a macro over the object is refused when a key is a relation,
 * whose rows say nothing about whether the caller sent the key.
 *
 * <p>The object is present exactly when the to-one hop its fields are read through is present;
 * an object whose fields sit on the row itself is present on every row. Every operator here is
 * a CEL error on a missing object, so it is UNKNOWN under both polarities, as a missing to-one
 * hop is elsewhere.
 */
final class ObjectAttributeTranslator {

    private static final Set<String> LAMBDA_BINDING_OPERATORS =
            Set.of("exists", "exists_one", "all", "filter", "map", "except");

    private final CriteriaBuilder cb;
    private final TriPredicate tri;
    private final PresenceTranslator presence;
    private final PlanWalker walker;

    ObjectAttributeTranslator(CriteriaBuilder cb, TriPredicate tri, PresenceTranslator presence,
                              PlanWalker walker) {
        this.cb = cb;
        this.tri = tri;
        this.presence = presence;
        this.walker = walker;
    }

    /** Whether {@code variable} is an object-valued attribute: unmapped, with mapped fields. */
    static boolean isObject(Scope scope, String variable) {
        return keysOf(scope, variable) != null;
    }

    /** The mapped keys of {@code variable}, or null when it is not an object-valued attribute. */
    private static List<String> keysOf(Scope scope, String variable) {
        Map<String, AttributeMapping> mapping = scope.rootMapping();
        if (mapping.containsKey(variable)
                || Scope.resolveRelationChain(mapping, variable) != null) {
            return null;
        }
        String prefix = variable + ".";
        TreeSet<String> keys = new TreeSet<>();
        for (String mapped : mapping.keySet()) {
            if (mapped.startsWith(prefix)) {
                String rest = mapped.substring(prefix.length());
                int dot = rest.indexOf('.');
                keys.add(dot < 0 ? rest : rest.substring(0, dot));
            }
        }
        return keys.isEmpty() ? null : List.copyOf(keys);
    }

    /** {@code has(variable)}: the object's hop is present. */
    Predicate present(Scope scope, String variable) {
        Join<?, ?> hop = hop(scope, variable);
        if (hop == null) {
            return cb.conjunction();
        }
        Join<?, ?> enclosing = enclosingJoin(scope, hop);
        Predicate present = hop.isNotNull();
        if (enclosing == null) {
            return present;
        }
        return cb.or(present, cb.and(enclosing.isNull(), tri.unknown()));
    }

    /** {@code key in variable}: whether the caller sent the key, UNKNOWN on a missing object. */
    Predicate keyMembership(Scope scope, String variable, Value needle) {
        if (needle.getKindCase() != Value.KindCase.STRING_VALUE) {
            throw Refusals.unsupported("in over the map-valued attribute " + variable
                    + " tests a string key; a " + needle.getKindCase()
                    + " needle has no key to test");
        }
        String key = needle.getStringValue();
        requireKey(scope, variable, key);
        return guarded(scope, variable, () -> keyPresent(scope, variable, key));
    }

    /**
     * {@code exists} or {@code all} over the object's keys, with one lambda variable (the key)
     * or two (key, then value). Each mapped key is substituted into the body as a string
     * constant, and the value variable is renamed to the key's own attribute; the terms are
     * joined as the planner folds a short list, guarded by the key's presence.
     */
    Predicate macro(String op, String variable, Operand lambdaOperand, Scope scope) {
        if (!"exists".equals(op) && !"all".equals(op)) {
            throw Refusals.unsupported(op + " over the map-valued attribute " + variable
                    + " is not supported: only exists() and all() fold over its keys");
        }
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            throw Refusals.malformed(op + " second operand must be a lambda");
        }
        List<Operand> lambdaOps = lambdaOperand.getExpression().getOperandsList();
        if (lambdaOps.size() < 2 || lambdaOps.size() > 3 || lambdaOps.subList(1, lambdaOps.size())
                .stream().anyMatch(v -> v.getNodeCase() != Operand.NodeCase.VARIABLE)) {
            throw Refusals.malformed("lambda requires a body and one or two variables");
        }
        Operand body = lambdaOps.get(0);
        String keyVar = lambdaOps.get(1).getVariable();
        String valueVar = lambdaOps.size() == 3 ? lambdaOps.get(2).getVariable() : null;
        if (keyVar.equals(valueVar)) {
            throw Refusals.malformed("Two-variable lambda binds " + keyVar + " twice");
        }
        List<String> keys = keysOf(scope, variable);
        for (String key : keys) {
            requireNotRelation(scope, variable, key);
        }
        boolean exists = "exists".equals(op);
        return walker.enterMacro(op, () -> guarded(scope, variable, () -> {
            List<Predicate> terms = new ArrayList<>();
            for (String key : keys) {
                Operand rewritten = substitute(body, keyVar, v -> keyConstant(variable, v, key));
                if (valueVar != null) {
                    String child = variable + "." + key;
                    rewritten = substitute(rewritten, valueVar,
                            suffix -> Operand.newBuilder().setVariable(child + suffix).build());
                }
                Predicate term = walker.traverse(rewritten, scope);
                terms.add(exists
                        ? cb.and(keyPresent(scope, variable, key), term)
                        : cb.or(tri.not(keyPresent(scope, variable, key)), term));
            }
            return exists ? cb.or(terms.toArray(Predicate[]::new))
                    : cb.and(terms.toArray(Predicate[]::new));
        }));
    }

    private static Operand keyConstant(String variable, String reference, String key) {
        if (!reference.isEmpty()) {
            throw Refusals.unsupported("Cannot resolve \"" + reference.substring(1) + "\" of a key"
                    + " of " + variable + ": a map key is a string, which has no fields");
        }
        return Operand.newBuilder().setValue(Value.newBuilder().setStringValue(key)).build();
    }

    /** {@code (present AND body) OR (NOT present AND UNKNOWN)}, each predicate built fresh. */
    private Predicate guarded(Scope scope, String variable, Supplier<Predicate> body) {
        return cb.or(cb.and(present(scope, variable), body.get()),
                cb.and(tri.not(present(scope, variable)), tri.unknown()));
    }

    private Predicate keyPresent(Scope scope, String variable, String key) {
        String child = variable + "." + key;
        AttributeMapping mapping = scope.rootMapping().get(child);
        if (mapping instanceof AttributeMapping.Field) {
            return presence.handleHas(
                    List.of(Operand.newBuilder().setVariable(child).build()), scope);
        }
        requireNotRelation(scope, variable, key);
        return present(scope, child);
    }

    private static void requireKey(Scope scope, String variable, String key) {
        if (!keysOf(scope, variable).contains(key)) {
            throw Refusals.unmapped("Unknown attribute: " + variable + "." + key
                    + " (the mapped fields of " + variable + " are its key set)");
        }
    }

    private static void requireNotRelation(Scope scope, String variable, String key) {
        if (scope.rootMapping().get(variable + "." + key) instanceof AttributeMapping.Relation) {
            throw Refusals.unsupported("The key " + key + " of the map-valued attribute "
                    + variable + " is a Relation: its rows cannot say whether the caller sent"
                    + " the key, so the object's key set cannot be enumerated");
        }
    }

    /**
     * The LEFT join every mapped field of {@code variable} is read through, or null when they
     * sit on the scope's own row. Fields that disagree about the join have no one object.
     */
    private Join<?, ?> hop(Scope scope, String variable) {
        List<String> signature = null;
        Join<?, ?> hop = null;
        for (String key : keysOf(scope, variable)) {
            String child = variable + "." + key;
            AttributeMapping mapping = scope.rootMapping().get(child);
            Join<?, ?> candidate;
            if (mapping instanceof AttributeMapping.Field) {
                candidate = enclosingJoin(scope, scope.path(child));
            } else if (mapping == null) {
                Join<?, ?> nested = hop(scope, child);
                candidate = nested == null ? null : enclosingJoin(scope, nested);
            } else {
                continue;
            }
            List<String> candidateSignature = signatureOf(scope, candidate);
            if (signature == null) {
                signature = candidateSignature;
                hop = candidate;
            } else if (!signature.equals(candidateSignature)) {
                throw Refusals.unsupported("The fields of the map-valued attribute " + variable
                        + " are read through different associations, so no one hop holds"
                        + " the object");
            }
        }
        if (signature == null) {
            throw Refusals.unsupported("The map-valued attribute " + variable
                    + " maps only Relations, which say nothing about its presence");
        }
        return hop;
    }

    private static Join<?, ?> enclosingJoin(Scope scope, Path<?> path) {
        for (Path<?> p = path.getParentPath(); p != null && p != scope.from();
                p = p.getParentPath()) {
            if (p instanceof Join<?, ?> join) {
                return join;
            }
        }
        return null;
    }

    private static List<String> signatureOf(Scope scope, Join<?, ?> hop) {
        List<String> names = new ArrayList<>();
        for (Join<?, ?> j = hop; j != null; j = enclosingJoin(scope, j)) {
            names.add(j.getAttribute().getName());
        }
        return names;
    }

    /**
     * Rewrites every read of {@code varName} ({@code var} or {@code var.f}) with
     * {@code replace.apply(suffix)}, where the suffix is empty or starts with a dot. A nested
     * macro that rebinds the name shadows it, so only its collection operand is rewritten.
     */
    private static Operand substitute(Operand operand, String varName, Replacement replace) {
        switch (operand.getNodeCase()) {
            case VARIABLE -> {
                String name = operand.getVariable();
                if (name.equals(varName)) {
                    return replace.apply("");
                }
                if (name.startsWith(varName + ".")) {
                    return replace.apply(name.substring(varName.length()));
                }
                return operand;
            }
            case EXPRESSION -> {
                PlanResourcesFilter.Expression expr = operand.getExpression();
                List<Operand> ops = expr.getOperandsList();
                PlanResourcesFilter.Expression.Builder rebuilt = expr.toBuilder();
                if (LAMBDA_BINDING_OPERATORS.contains(expr.getOperator()) && ops.size() == 2
                        && bindsVariable(ops.get(1), varName)) {
                    rebuilt.setOperands(0, substitute(ops.get(0), varName, replace));
                    return Operand.newBuilder().setExpression(rebuilt).build();
                }
                for (int i = 0; i < ops.size(); i++) {
                    rebuilt.setOperands(i, substitute(ops.get(i), varName, replace));
                }
                return Operand.newBuilder().setExpression(rebuilt).build();
            }
            default -> {
                return operand;
            }
        }
    }

    private static boolean bindsVariable(Operand lambdaOperand, String varName) {
        if (lambdaOperand.getNodeCase() != Operand.NodeCase.EXPRESSION
                || !"lambda".equals(lambdaOperand.getExpression().getOperator())) {
            return false;
        }
        List<Operand> ops = lambdaOperand.getExpression().getOperandsList();
        return ops.subList(1, ops.size()).stream().anyMatch(v ->
                v.getNodeCase() == Operand.NodeCase.VARIABLE && varName.equals(v.getVariable()));
    }

    /** Maps a read of the substituted variable, by its suffix ("" or {@code .f}), to an operand. */
    @FunctionalInterface
    private interface Replacement {
        Operand apply(String suffix);
    }
}
