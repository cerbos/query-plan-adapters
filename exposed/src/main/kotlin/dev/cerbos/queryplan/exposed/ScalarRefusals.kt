package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand

/**
 * The scalar side's named refusals: the ones raised from more than one place, and the ones whose
 * wording is load-bearing because `conformance/actions.json` pins it and a harness asserts it.
 *
 * Every one of them delegates to [Refusals], so the classification (malformed plan, mapping
 * shortfall, inexpressible shape) still belongs to the factory rather than to the text. Spelling
 * a message once here is what keeps the corpus pin and the code from drifting apart.
 */
internal object ScalarRefusals {

    /**
     * An operator no leaf case knows. `matches` is the policy-reachable one and gets its own
     * wording, because CEL's regex dialect is RE2 and no SQL engine implements it: a translated
     * pattern would accept or reject strings the PDP does not.
     */
    fun unsupportedOperator(operator: String): UnsupportedPlanShapeException =
        if (operator == "matches") {
            Refusals.unsupported(
                "Unsupported operator: matches. CEL matches its pattern with RE2, which no SQL " +
                    "engine implements: LIKE has no alternation or anchors, and each engine's own " +
                    "regular expression operator differs from RE2 in what it accepts and in how it " +
                    "treats anchors, so a translated pattern would match strings the PDP does not.",
            )
        } else {
            Refusals.unsupported("Unsupported operator: $operator")
        }

    /**
     * A null comparison operand under [NullAttributeRepresentation.OMITTED]: a NULL column then
     * sends no attribute, so CEL raises a missing-attribute error and `check()` denies the row,
     * while a NULL-selecting filter would return it
     * (https://github.com/cerbos/query-plan-adapters/issues/302).
     */
    fun nullOperandUnderOmitted(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate `$operator` against a null operand under " +
            "NullAttributeRepresentation.OMITTED: a NULL column sends no attribute, so Cerbos " +
            "evaluates the comparison as a missing-attribute error (deny) while a NULL-selecting " +
            "filter would return those rows. Send NULL columns as explicit nulls and declare " +
            "EXPLICIT, or keep this shape out of the policy.",
    )

    /**
     * Equality between one attribute that declares the explicit-null convention and one that does
     * not. The declared side needs a definite answer for its NULL (CEL holds a null VALUE there);
     * the undeclared side needs UNKNOWN (a missing attribute, which CEL denies under both
     * polarities). A definite predicate returns rows the PDP refuses, a plain one drops rows the
     * PDP allows, and no single predicate is both.
     */
    fun mixedNullConventions(operator: String): UnmappedAttributeException = Refusals.unmapped(
        "Cannot translate `$operator` between two columns under mixed null conventions: one " +
            "attribute declares NullAttributeRepresentation.EXPLICIT and the other does not, so " +
            "the declared side needs a definite answer for its NULL while the undeclared side " +
            "needs UNKNOWN, and no single predicate is both. Declare the convention on both " +
            "mappings, or on neither.",
    )

    /**
     * CEL's `int()` and `double()`. SQL `CAST` is not a CEL conversion in either direction: CEL
     * reads a WHOLE string or raises (and an error denies the row) while `CAST` reads whatever
     * numeric prefix parses, and CEL's `int()` truncates toward zero where PostgreSQL and MySQL
     * round to nearest.
     */
    fun numericCastUnsupported(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate $operator(): SQL CAST reads the numeric prefix of a string where CEL " +
            "requires the whole string and raises otherwise, and PostgreSQL and MySQL round where " +
            "CEL truncates toward zero, so the cast would admit rows the PDP denies.",
    )

    /**
     * `string()` over a column whose SQL text rendering is not CEL's. A boolean column is lowered
     * portably (see [LeafTranslator]); this covers the rest — a DECIMAL renders its declared
     * scale (`1.50`) where CEL renders the shortest round-tripping decimal (`1.5`), and a binary
     * or temporal column has no CEL text form at all.
     */
    fun textCastUnsupported(columnType: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate string() over a $columnType column: CEL renders the shortest decimal " +
            "that round-trips and no SQL CAST reproduces that for this column type. Only text, " +
            "integer, floating-point and boolean columns are translatable.",
    )

    /**
     * CEL `%` has no double overload, so on attribute values — which Cerbos always transports as
     * doubles — a bare `attr % n` always errors and denies. A satisfiable policy has to cast with
     * `int()` first, and that cast is itself unlowerable, which is why the rejection stands either
     * way (https://github.com/cerbos/query-plan-adapters/issues/387).
     */
    fun modUnsupported(): UnsupportedPlanShapeException = Refusals.unsupported(
        "mod is not supported in comparisons: CEL % is integer-only while attribute values are " +
            "always doubles at check time, so a satisfiable policy must cast with int() first — " +
            "and int() has no faithful SQL lowering, because CAST rounds where CEL truncates " +
            "toward zero.",
    )

    /**
     * Cerbos `except()` is a two-list function whose list-difference result has no SQL shape. The
     * PDP-verified arrival shapes are inside `size()` and as a comparison operand; no lambda form
     * appears on the wire.
     */
    fun exceptUnsupported(): UnsupportedPlanShapeException = Refusals.unsupported(
        "except is not supported: Cerbos except(list, list) computes a list difference, which has " +
            "no SQL translation. Rewrite the policy with a collection macro instead — e.g. " +
            "size(R.attr.tags.except([\"x\"])) > 0 is equivalent to " +
            "R.attr.tags.exists(t, !(t in [\"x\"])).",
    )

    /** A cast, a `size()` or a macro inside an arithmetic operand: legal CEL, no double-space form. */
    fun unexpectedInsideArithmetic(operator: String): UnsupportedPlanShapeException =
        if (operator == "int" || operator == "double") {
            numericCastUnsupported(operator)
        } else {
            Refusals.unsupported("Unexpected $operator() expression inside an arithmetic comparison operand")
        }

    /**
     * `+` between two columns whose types cannot settle CEL's overload. Guessing numeric is wrong
     * in the dangerous direction: `text + text` is a hard error on PostgreSQL, 0 on SQLite and 0
     * on MySQL, where the string constant on the other side coerces to 0 with them and the filter
     * matches almost every row
     * (https://github.com/cerbos/query-plan-adapters/issues/391).
     */
    fun ambiguousAddition(): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot tell numeric addition from string concatenation in `+`: CEL overloads `+` on " +
            "strings and the plan carries no operand types, so the mapped column types have to " +
            "settle it and neither of these is text or numeric.",
    )

    /**
     * A CEL list or map literal compared against a scalar column — `R.attr.tags == ["a", "b"]`
     * arrives as `eq(variable, value-list)` verbatim. No scalar column comparison exists for it,
     * and binding the structure would die inside the JDBC driver with a coercion error instead of
     * this adapter's named contract. Reports the SHAPE only: element values never leak.
     */
    fun structuredConstant(operator: String, variable: String, value: Any): UnsupportedPlanShapeException {
        val shape = when (value) {
            is List<*> -> "list of ${value.size} element${if (value.size == 1) "" else "s"}"
            is Map<*, *> -> "map of ${value.size} ${if (value.size == 1) "entry" else "entries"}"
            else -> "structured"
        }
        val kind = if (value is List<*>) "list" else "map"
        return Refusals.unsupported(
            "$operator comparison against a $shape constant is not supported for attribute " +
                "$variable. Whole-$kind equality is not translatable to a scalar column " +
                "comparison; map the attribute as a relation and use in/hasIntersection, or " +
                "compare elements individually.",
        )
    }

    /** Shape-only description of an operand for an error message; constant VALUES never leak. */
    fun describeOperand(operand: Operand): String = when (operand.nodeCase) {
        Operand.NodeCase.VARIABLE -> "VARIABLE '${operand.variable}'"
        Operand.NodeCase.EXPRESSION -> "EXPRESSION ${operand.expression.operator}()"
        // The protobuf kind, not the converted value: conversion can itself throw on a malformed
        // VALUE, and this helper has to stay safe inside an error path.
        Operand.NodeCase.VALUE -> "VALUE (${operand.value.kindCase})"
        else -> operand.nodeCase.toString()
    }
}
