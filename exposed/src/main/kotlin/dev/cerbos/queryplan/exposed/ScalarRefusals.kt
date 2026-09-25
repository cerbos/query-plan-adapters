package dev.cerbos.queryplan.exposed

import dev.cerbos.api.v1.engine.Engine.PlanResourcesFilter.Expression.Operand
import dev.cerbos.queryplan.exposed.sql.ScalarColumnTypes
import org.jetbrains.exposed.v1.core.Column

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
     * A column operand of a string match or of `size()` whose declared type is not text.
     *
     * THE ONE FACTORY for that requirement: the haystack and the column needle of `contains`,
     * `startsWith` and `endsWith`, the column a hierarchy path is read from, and the argument of
     * `size()` all raise it, so the reasoning is stated once.
     *
     * CEL has no such overload for a number, a boolean or a timestamp, so at check time the
     * expression raises a no-overload error and `check()` DENIES. SQL does not agree and does not
     * agree with itself: MySQL and SQLite coerce the column and MATCH (`123 LIKE '%2%'` is TRUE,
     * `CHAR_LENGTH(1)` is 1) while PostgreSQL aborts the statement, so the same policy is a silent
     * over-grant on one store and a runtime failure on another. [ArithmeticTranslator] refuses the
     * symmetric case — a non-numeric column in arithmetic — for the same reason.
     */
    fun textColumnRequired(operator: String, variable: String, column: Column<*>): UnmappedAttributeException =
        Refusals.unmapped(
            "$operator over '$variable' requires a text column, but it maps to a " +
                "${ScalarColumnTypes.describe(column)} column. CEL has no $operator overload for " +
                "that type, so the expression raises a no-overload error and denies, while SQL " +
                "coerces the column — MySQL and SQLite match and PostgreSQL aborts the statement.",
        )

    /**
     * A plan constant whose TYPE is not one the mapped column holds.
     *
     * `R.attr.aString == P.attr.level` is legal CEL and arrives as `eq(variable, value)` with
     * nothing in it naming a type. CEL answers it from the VALUES alone — equality is a definite
     * FALSE for a present attribute and every ordering is a no-overload error — where SQL has to
     * coerce one side, and MySQL coerces the COLUMN: `'abc' = 0` is TRUE there, so the filter
     * returns every row the PDP denies. Reports the constant's TYPE only; values never leak.
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
     * `timestamp(a) op timestamp(b)` over two columns that both pin an absolute instant but SPELL
     * it differently: one carries an offset, the other does not.
     *
     * Deliberately not [columnTypeMismatch], which this branch used to borrow. Nothing here is a
     * coercion and nothing here is a CEL problem: CEL compares two timestamps with no overload
     * error at all, there is no text operand for MySQL to coerce, and its advice — wrap both sides
     * in `timestamp()` — is what the caller has already done. The divergence is the STORE's own:
     * a `timestamp` compared with a `timestamptz` is resolved through the session's time zone, so
     * the same filter selects different rows in two sessions of one application, and a session
     * setting is not a property of the filter.
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

    /**
     * What to do about a type mismatch, in terms of the kinds this adapter actually compares.
     *
     * "Map the attribute onto a column of the constant's type" was not actionable for a column
     * whose kind is unrecognised — a `UUIDTable` id is an `EntityIDColumnType(UUIDColumnType)`, so
     * `request.resource.id == "…"` refuses and there is no "column of the constant's type" to reach
     * for. Naming the kinds says which remappings exist, the way [textCastUnsupported] does.
     */
    private const val TRANSLATABLE_KINDS =
        "Compare it against a value of the column's own type, or map the attribute onto one of the " +
            "kinds this adapter compares: text, integer, floating-point, decimal or boolean. A " +
            "temporal column is compared by wrapping both sides in timestamp()."

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
     * scale (`1.50`) where CEL renders the shortest round-tripping decimal (`1.5`), a
     * floating-point column renders `1000000.0` or `1.0E6` where CEL prints Go's shortest `%g`
     * form (`1e+06`, `2`) and prints a stored `-0.0` as `-0`, which SQL cannot tell from `0.0`
     * (equality against a non-zero literal is solved for the column instead), and a binary or
     * temporal column has no CEL text form at all.
     */
    fun textCastUnsupported(columnType: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "Cannot translate string() over a $columnType column: CEL renders a number as Go's " +
            "shortest round-tripping form (1e+06, 2, -0) and no SQL CAST reproduces that for this " +
            "column type, nor reads the sign of a stored -0.0. Text, integer and boolean " +
            "columns are translatable, and a floating-point column only under == or != against " +
            "a string literal other than \"0\" and \"-0\".",
    )

    /**
     * An ordering of a string attribute against a literal holding a UTF-16 code unit at or above
     * 0xD800, where code point order (CEL) and code unit order (H2) disagree.
     */
    fun codeUnitOrdering(operator: String, variable: String): UnsupportedPlanShapeException = Refusals.unsupported(
        "$operator orders '$variable' against a literal holding a character at or above U+D800 " +
            "(an astral character or U+E000–U+FFFF): CEL orders strings by code point, and a " +
            "store that compares UTF-16 code units (H2, Java's String.compareTo) puts a " +
            "surrogate pair before U+E000–U+FFFF.",
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
     * A NaN or an infinity that reached the one place this adapter binds a constant.
     *
     * The backstop under [nonFiniteInArithmetic], which is where a non-finite value is actually
     * produced. This one makes "no statement this adapter emits binds a non-finite double" true at
     * the binder rather than only along the paths that were audited: SQL has no literal for either
     * value, PostgreSQL accepts `'NaN'` and then orders it ABOVE every number where CEL orders it
     * below nothing, and MySQL's driver rejects the parameter and fails the query outright.
     */
    fun nonFiniteConstant(): UnsupportedPlanShapeException = Refusals.unsupported(
        "a NaN or infinity constant cannot be bound into SQL: no store has a literal for either, " +
            "PostgreSQL orders NaN above every number where CEL orders it below none, and MySQL's " +
            "driver rejects the parameter outright.",
    )

    /**
     * A NaN or a signed infinity — what CEL division by zero produces — that would have to be
     * combined with a COLUMN by `+`, `-`, `*` or `/`.
     *
     * The division itself stays symbolic ([ArithmeticValues]), and every arm folds while the other
     * operand is a constant: `NaN + 1.0` is NaN, `+Infinity + 1.0` is +Infinity. A column operand
     * ends that. There is no literal to bind, and no single constant to fold to either — an
     * infinity times a column is +Infinity, -Infinity or NaN according to that column's sign, and
     * the sign is not known until the row is read.
     */
    fun nonFiniteInArithmetic(): UnsupportedPlanShapeException = Refusals.unsupported(
        "arithmetic between a column and the NaN or infinity a zero denominator produces is not " +
            "supported: SQL has no literal for either value, and neither folds to one constant " +
            "against a column, because an infinity times or divided by a column depends on that " +
            "column's sign. Compare the division itself, or keep the composition constant.",
    )

    /**
     * Cerbos `except()`, wherever the scalar side meets it: in condition position, and as a
     * comparison operand. Delegates, because [RelationRefusals.exceptUnsupported] raises it for
     * `size(x.except(y))` and one shape must not have three spellings — the message is what a
     * caller matches on, and a reader comparing two of them would conclude the adapter distinguishes
     * cases it does not.
     */
    fun exceptUnsupported(): UnsupportedPlanShapeException = RelationRefusals.exceptUnsupported()

    /** A cast, a `size()` or a macro inside an arithmetic operand: legal CEL, no double-space form. */
    fun unexpectedInsideArithmetic(operator: String): UnsupportedPlanShapeException =
        if (operator == "int" || operator == "double") {
            numericCastUnsupported(operator)
        } else {
            Refusals.unsupported("Unexpected $operator() expression inside an arithmetic comparison operand")
        }

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
