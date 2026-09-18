package dev.cerbos.queryplan.exposed

import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.MAPPING
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelCategories
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelResources
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelSubCategories
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.RelTags
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.render
import dev.cerbos.queryplan.exposed.SubquerySupportTest.Companion.translate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * How a plan variable is resolved: the longest mapped prefix, the remainder walked through nested
 * fields, and — inside a lambda — which scope owns what.
 *
 * Owner anchoring is the assertion worth having. A subquery anchored to the wrong scope still
 * builds and still returns rows, the wrong ones, so there is no error to catch: the only way to see
 * it is to name the column the correlation has to read.
 */
class ScopeResolutionTest {

    @Test
    fun `a dotted reference matches the longest mapped prefix, not the first one that fits`() {
        // `request.resource.attr.mainCategory.subCategories` is mapped nowhere as a whole; the
        // prefix `…mainCategory` is, and `subCategories` is walked through its nested fields.
        val scope = RootScope(Translation(Options.of(MAPPING)))
        val resolved = scope.resolve("request.resource.attr.mainCategory.subCategories")
        assertTrue(resolved is Resolution.Collection, resolved.toString())
        val hops = (resolved as Resolution.Collection).hops
        assertEquals(listOf(RelCategories, RelSubCategories), hops.map { it.table })
        // The mapped prefix is itself the first hop; dropping it would correlate the remainder
        // straight off the root and skip the intermediate table.
        assertEquals(listOf(RelCategories), resolved.leadingHops.map { it.table })
        assertSame(scope, resolved.owner)
    }

    @Test
    fun `resolution is idempotent, so a variable asked for twice numbers its aliases once`() {
        val scope = RootScope(Translation(Options.of(MAPPING)))
        assertSame(
            scope.resolve("request.resource.attr.tags"),
            scope.resolve("request.resource.attr.tags"),
        )
    }

    @Test
    fun `a relation declared inside a lambda correlates against that lambda's alias`() {
        // `categories.exists(c, c.subCategories.exists(s, …))`. `c.subCategories`'s `from` column
        // lives on the categories table, so it has to be read through the categories ALIAS. Read
        // bare it would join against whichever categories row the outer scan happened to be on.
        val sql = render(translate("outer-attr-depth2"))
        assertTrue(sql.contains("cerbos_2.CATEGORY_ID = cerbos_1.ID"), sql)
    }

    @Test
    fun `a non-lambda variable inside a lambda is owned by the scope that declared it`() {
        // `categories.exists(c, size(c.subCategories) == 1 && R.attr.tags.exists(t, …))`. The tags
        // relation belongs to the ROOT even though it is referenced from inside the categories
        // lambda, so its subquery correlates the resource row rather than the categories alias.
        val sql = render(translate("w2-outer-relation"))
        assertTrue(sql.contains("REL_TAGS cerbos_3 WHERE cerbos_3.RESOURCE_ID = REL_RESOURCES.ID"), sql)
    }

    @Test
    fun `an outer scalar inside a lambda reaches the subquery as a correlation`() {
        val sql = render(translate("outer-attr-depth2"))
        assertTrue(sql.contains("REL_RESOURCES.A_BOOL"), sql)
    }

    @Test
    fun `the bare lambda variable denotes the relation's element column`() {
        // `size(R.attr.tags) > 1` reaches the relation itself; the element column is what a body
        // that names the variable alone reads, and a relation that declares none is refused.
        val noElement = cerbosMapping {
            "request.resource.attr.tagNames" to many(RelTags, from = RelResources.id, to = RelTags.resourceId)
        }
        val error = runCatching { translate("in-null-elem-rel", noElement) }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertTrue(error?.message.orEmpty().contains("declares no element column"), error?.message.orEmpty())
    }

    @Test
    fun `an unmapped variable is refused rather than resolved by stripping a prefix`() {
        // An adapter that resolved by stripping `request.resource.attr.` would silently accept a
        // name nothing maps, and the failure would look like an empty result rather than a bug.
        val scope = RootScope(Translation(Options.of(MAPPING)))
        val error = runCatching { scope.resolve("request.resource.attr.nothingMapsThis") }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals("Unknown attribute: request.resource.attr.nothingMapsThis", error?.message)
    }

    @Test
    fun `a relation asked for as a scalar is refused`() {
        val scope = RootScope(Translation(Options.of(MAPPING)))
        val error = runCatching { scope.scalar("request.resource.attr.tags") }.exceptionOrNull()
        assertEquals(UnmappedAttributeException::class.java, error?.javaClass)
        assertEquals(
            "request.resource.attr.tags is mapped as a relation, but this position needs a scalar column",
            error?.message,
        )
    }

    @Test
    fun `a function-style resolver is asked for progressively shorter prefixes`() {
        // The mapping may be a function rather than a table, so the prefixes cannot be enumerated:
        // the resolver is probed, longest first, and this records the exact questions it is asked.
        val asked = mutableListOf<String>()
        val resolver = AttributeResolver { reference ->
            asked += reference
            MAPPING.entries[reference]
        }
        val scope = RootScope(Translation(Options.of(resolver)))
        // The walk ends in a refusal — `tags.name` is one value per element — but the probe order
        // is what this pins, and it is the same order a resolvable name would have produced.
        runCatching { scope.resolve("request.resource.attr.tags.name") }
        assertEquals(
            listOf(
                "request.resource.attr.tags.name",
                "request.resource.attr.tags",
            ),
            asked,
        )
    }
}
