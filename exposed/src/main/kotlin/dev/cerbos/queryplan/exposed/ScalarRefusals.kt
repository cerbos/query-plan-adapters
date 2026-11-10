package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import org.jetbrains.exposed.v1.core.Column

/** The scalar side's named refusals: raised from more than one place, or pinned by the corpus. */
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
     * A null comparison operand under [NullAttributeRepresentation.OMITTED]: CEL denies a missing
     * attribute, which a NULL-selecting filter would return (#302).
     */
    fun nullOperandUnderOmitted(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate `$operator` against a null operand under " +
            "NullAttributeRepresentation.OMITTED: a NULL column sends no attribute, so Cerbos " +
            "evaluates the comparison as a missing-attribute error (deny) while a NULL-selecting " +
            "filter would return those rows. Send NULL columns as explicit nulls and declare " +
            "EXPLICIT, or keep this shape out of the policy.",
    )

    /**
     * A column operand of a string match or of `size()` whose declared type is not text. CEL has no
     * such overload and denies, while MySQL and SQLite coerce and match.
     */
    fun textColumnRequired(operator: String, variable: String, column: Column<*>): UnmappedAttributeException =
        Refusals.unmapped(
            "$operator over '$variable' requires a text column, but it maps to a " +
                "${ScalarColumnTypes.describe(column)} column. CEL has no $operator overload for " +
                "that type, so the expression raises a no-overload error and denies, while SQL " +
                "coerces the column — MySQL and SQLite match and PostgreSQL aborts the statement.",
        )

    /**
     * A plan constant whose type the mapped column does not hold. MySQL coerces the column
     * (`'abc' = 0` is TRUE). Reports the constant's type only.
     */
    fun constantTypeMismatch(
        operator: String,
        variable: String,
        column: Column<*>,
        value: Any,
    ): UnmappedAttributeException = Refusals.unmapped(
        "$operator compares '$variable' against a ${PlanValues.typeName(value)} constant, but it " +
            "maps to a ${ScalarColumnTypes.describe(column)} column. CEL decides a comparison " +
            "between those from the values alone — equality is false and an ordering raises a " +
            "no-overload error — while SQL coerces one side, and MySQL coerces the column, so the " +
            "filter returns rows the PDP denies. $TRANSLATABLE_KINDS",
    )

    /**
     * `timestamp(a) op timestamp(b)` where one column carries an offset and the other does not: the
     * store resolves that through the session time zone.
     */
    fun timestampRepresentationMismatch(
        operator: String,
        leftVariable: String,
        leftColumn: Column<*>,
        rightVariable: String,
        rightColumn: Column<*>,
    ): UnmappedAttributeException = Refusals.unmapped(
        "$operator compares timestamp('$leftVariable') with timestamp('$rightVariable'), which map " +
            "to a ${ScalarColumnTypes.describe(leftColumn)} and a " +
            "${ScalarColumnTypes.describe(rightColumn)} column. Both store an absolute instant, but " +
            "one carries a zone offset and the other does not, and a store resolves that pairing " +
            "through the SESSION's time zone — so the same filter selects different rows in two " +
            "sessions. Map both attributes onto columns of one representation: two timestamp() " +
            "columns, or two timestampWithTimeZone() ones.",
    )

    /** [constantTypeMismatch] between two mapped columns: the same coercion, neither side constant. */
    fun columnTypeMismatch(
        operator: String,
        leftVariable: String,
        leftColumn: Column<*>,
        rightVariable: String,
        rightColumn: Column<*>,
    ): UnmappedAttributeException = Refusals.unmapped(
        "$operator compares '$leftVariable' with '$rightVariable', which map to a " +
            "${ScalarColumnTypes.describe(leftColumn)} and a " +
            "${ScalarColumnTypes.describe(rightColumn)} column. CEL decides a comparison between " +
            "those from the values alone — equality is false and an ordering raises a no-overload " +
            "error — while SQL coerces one side, and MySQL coerces the text one, so the filter " +
            "returns rows the PDP denies. $TRANSLATABLE_KINDS",
    )

    private const val TRANSLATABLE_KINDS =
        "Compare it against a value of the column's own type, or map the attribute onto one of the " +
            "kinds this adapter compares: text, integer, floating-point, decimal or boolean. A " +
            "temporal column is compared by wrapping both sides in timestamp()."

    /** CEL's `int()` and `double()`: SQL `CAST` reads numeric prefixes and rounds where CEL raises or truncates. */
    fun numericCastUnsupported(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate $operator(): SQL CAST reads the numeric prefix of a string where CEL " +
            "requires the whole string and raises otherwise, and PostgreSQL and MySQL round where " +
            "CEL truncates toward zero, so the cast would admit rows the PDP denies.",
    )

    /**
     * `string()` over a column whose SQL text is not CEL's: DECIMAL scale, double formatting and `-0`,
     * binary and temporal columns. Booleans are lowered in [LeafTranslator].
     */
    fun textCastUnsupported(columnType: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate string() over a $columnType column: CEL renders a number as Go's " +
            "shortest round-tripping form (1e+06, 2, -0) and no SQL CAST reproduces that for this " +
            "column type, nor reads the sign of a stored -0.0. Text, integer and boolean " +
            "columns are translatable, and a floating-point column only under == or != against " +
            "a string literal other than \"0\" and \"-0\".",
    )

    /**
     * An ordering against a literal holding a UTF-16 code unit at or above 0xD800, rendered on a
     * dialect whose string order this adapter does not know to be code point order.
     */
    fun codeUnitOrdering(operator: String, variable: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "$operator orders $variable against a literal holding a character at or above U+D800 " +
            "(an astral character or U+E000–U+FFFF): CEL orders strings by code point, and this " +
            "dialect is not one whose code point ordering the adapter knows how to spell (H2, " +
            "PostgreSQL, MySQL, MariaDB, SQLite); a store that compares UTF-16 code units puts a " +
            "surrogate pair before U+E000–U+FFFF.",
    )

    /** A string match whose constant needle holds a NUL character. */
    fun nulNeedle(operator: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate $operator() with a needle holding a NUL character: PostgreSQL text " +
            "cannot hold one, and MySQL's LIKE stops reading the pattern at it.",
    )

    /**
     * A division whose denominator is a floating-point column or computed arithmetic: its zero may
     * be `-0.0`, which CEL divides into the opposite infinity and SQL cannot tell from `0.0`.
     */
    fun signedZeroDivisor(): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate a division by a floating-point column or by computed arithmetic: its " +
            "zero may be -0.0, which CEL divides a non-zero number into the opposite infinity " +
            "from 0.0, while SQL compares -0.0 equal to 0.0 and has no portable way to read the " +
            "sign bit. Divide by an integer or decimal column, or by a constant.",
    )

    /** CEL `%` has no double overload, and attribute numbers are always doubles (#387). */
    fun modUnsupported(): UnsupportedPlanShapeException = Refusals.unsupported(
        "mod is not supported in comparisons: CEL % is integer-only while attribute values are " +
            "always doubles at check time, so a satisfiable policy must cast with int() first — " +
            "and int() has no faithful SQL lowering, because CAST rounds where CEL truncates " +
            "toward zero.",
    )

    /**
     * A NaN or infinity reaching the binder. SQL has no literal for it; PostgreSQL orders `'NaN'` above
     * every number and MySQL's driver rejects it.
     */
    fun nonFiniteConstant(): UnsupportedPlanShapeException = Refusals.unsupported(
        "a NaN or infinity constant cannot be bound into SQL: no store has a literal for either, " +
            "PostgreSQL orders NaN above every number where CEL orders it below none, and MySQL's " +
            "driver rejects the parameter outright.",
    )

    /**
     * A NaN or infinity combined with a column by arithmetic: no literal to bind, and its result depends
     * on the column's sign.
     */
    fun nonFiniteInArithmetic(): UnsupportedPlanShapeException = Refusals.unsupported(
        "arithmetic between a column and the NaN or infinity a zero denominator produces is not " +
            "supported: SQL has no literal for either value, and neither folds to one constant " +
            "against a column, because an infinity times or divided by a column depends on that " +
            "column's sign. Compare the division itself, or keep the composition constant.",
    )

    /** Cerbos `except()`, with the same message [RelationRefusals.exceptUnsupported] uses. */
    fun exceptUnsupported(): UnsupportedPlanShapeException = RelationRefusals.exceptUnsupported()

    /** A cast, a `size()` or a macro inside an arithmetic operand: legal CEL, no double-space form. */
    fun unexpectedInsideArithmetic(operator: String): UnsupportedPlanShapeException =
        if (operator == "int" || operator == "double") {
            numericCastUnsupported(operator)
        } else {
            Refusals.unsupported("Unexpected $operator() expression inside an arithmetic comparison operand")
        }

    /** A list or map literal compared against a scalar column. Reports the shape only. */
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
