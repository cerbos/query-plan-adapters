package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.ids
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.render
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.translate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * `size()` over a string, over a relation and over `size(filter(...))`.
 *
 * Two hazards live here and nowhere else: a FRACTIONAL threshold against an integral count, which
 * has to round rather than truncate, and the emptiness shortcuts, which are two-valued and are
 * therefore only available where there is no leading hop to require.
 */
class SizeComparisonTest {

    @Test
    fun `a count over a direct relation compares exactly`() {
        // `size(tags) > 1`: r5 has two tags, r6 three (one of them hidden from the application, but
        // this mapping declares no visibility predicate, so the subquery sees it).
        assertEquals(listOf("r5", "r6"), ids(translate("size/greater-than/collection-above-one")))
    }

    @Test
    fun `emptiness over a direct relation takes the EXISTS shortcut`() {
        // `!(size(tags) == 0)`: a direct relation's emptiness is already exact under both
        // polarities, so the shortcut is safe and the emitted SQL carries no COUNT.
        val op = translate("size/equals/negated-collection-zero")
        assertEquals(listOf("r2", "r3", "r4", "r5", "r6"), ids(op))
        assertTrue(render(op).contains("EXISTS"), render(op))
        assertTrue(!render(op).contains("COUNT"), render(op))
    }

    @Test
    fun `a value-first emptiness check is mirrored rather than read backwards`() {
        // `0 < size(tags)` is the same question as `size(tags) > 0`; reading the operands in wire
        // order would ask for a negative count and refuse a supported shape.
        assertEquals(listOf("r2", "r3", "r4", "r5", "r6"), ids(translate("size/less-than/value-first")))
    }

    @Test
    fun `a fractional threshold rounds up rather than truncating`() {
        // `size(tags) >= 1.5` is `>= 2`. Truncating to `>= 1` would return r2, r3 and r4 as well,
        // each of which the PDP denies.
        assertEquals(listOf("r5", "r6"), ids(translate("size/greater-or-equal/fractional-threshold")))
    }

    @Test
    fun `a count over a chain requires its leading hop under every threshold`() {
        // r2 and r3 hold one sub-category, r5 two, r7 none; r1, r4, r6 and r8 have no category at
        // all, so the chain has to read UNKNOWN rather than 0.
        assertEquals(listOf("r2", "r3", "r5"), ids(translate("relation/size/non-empty-to-one-chain")))
        assertEquals(listOf("r7"), ids(translate("relation/size/zero-to-one-chain")))
        // `>= 0` holds for every count, so only the hop requirement keeps the parentless rows out.
        assertEquals(listOf("r2", "r3", "r5", "r7"), ids(translate("relation/size/non-negative-to-one-chain")))
    }

    @Test
    fun `the negated count spelling inherits the hop requirement`() {
        // `!(size(chain) > 0)` takes a different branch from `size(chain) == 0`, because the
        // planner emits the negation verbatim rather than normalising it. Guarding the count
        // EXPRESSION is what makes both spellings agree (cerbos/query-plan-adapters#316).
        assertEquals(listOf("r7"), ids(translate("relation/size/negated-non-empty-to-one-chain")))
        assertEquals(ids(translate("relation/size/zero-to-one-chain")), ids(translate("relation/size/negated-non-empty-to-one-chain")))
    }

    @Test
    fun `a fractional threshold over a chain rounds and still requires the hop`() {
        // `>= 1.5` rounds to `>= 2`, which a count of zero fails whether or not the hop is
        // required — so this action pins the rounding, not the hop.
        assertEquals(listOf("r5"), ids(translate("size/greater-or-equal/fractional-threshold-on-relation")))
        // `<= 1.5` rounds to `<= 1`, which a count of zero PASSES: here the hop is the whole test.
        assertEquals(listOf("r2", "r3", "r7"), ids(translate("size/less-or-equal/fractional-threshold-on-relation")))
    }

    @Test
    fun `size of a filtered collection is strict, so one undetermined element denies`() {
        // `size(tags.filter(t, t.name == "public")) == 1`. r5 would count one match, but its
        // NULL-named sibling errors the whole expression; r6 counts two.
        assertEquals(listOf("r2"), ids(translate("size/equals/filtered-collection")))
    }

    @Test
    fun `a string length counts characters rather than bytes`() {
        // `size(aString) > 4`: "three", "seven" and "eight" are the only names past four characters.
        assertEquals(listOf("r3", "r7", "r8"), ids(translate("size/greater-than/string-length")))
        assertTrue(render(translate("size/greater-than/string-length")).contains("CHAR_LENGTH"), render(translate("size/greater-than/string-length")))
    }

    @Test
    fun `a string length is never negative, so a lower bound holds for every present string`() {
        assertEquals(listOf("r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"), ids(translate("size/greater-than/string-non-empty")))
    }

    @Test
    fun `a threshold outside int range is decided statically rather than wrapped`() {
        // A narrowing cast would turn 4294967296 into 0 and `size(s) > 4294967296` into an
        // always-true filter, returning every row while check() denies them all.
        assertEquals(emptyList<String>(), ids(translate("size/greater-than/huge-threshold")))
        assertEquals(listOf("r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"), ids(translate("size/less-than/huge-threshold")))
    }

    @Test
    fun `size of a column with no relation mapping is refused`() {
        val scalarOnly = cerbosMapping {
            "request.resource.attr.tags" to SubquerySupportTest.Companion.RelResources.aString
        }
        val error = runCatching { translate("size/equals/filtered-collection", scalarOnly) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals(
            "size(filter(...)) counts the elements of request.resource.attr.tags, which resolves to " +
                "a scalar column; counting needs a relation mapping",
            error?.message,
        )
    }
}
