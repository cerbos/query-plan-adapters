package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.Table
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * [Options] is a caller-supplied argument, and the corpus structurally cannot vary it: `actions.json`
 * classifies each action against ONE mapping and one set of options per adapter, so the NULL
 * convention a call chooses and the macro-depth bound it sets have no corpus spelling
 * (`CLAUDE.md`, "What a translator unit test may pin", kind 2). This is where they are pinned.
 */
class OptionsTest {

    private object Docs : Table("options_docs") {
        val id = varchar("id", 32)
        val owner = varchar("owner", 32)
    }

    private val mapping = cerbosMapping { "request.resource.id" to Docs.id }
    private val other = cerbosMapping { "request.resource.attr.owner" to Docs.owner }

    @Test
    fun `the defaults are the conventions the README states`() {
        val options = Options.of(mapping)
        assertSame(mapping, options.mapping)
        assertEquals(NullAttributeRepresentation.EXPLICIT, options.nullAttributeRepresentation)
        assertEquals(Options.DEFAULT_MAX_MACRO_DEPTH, options.maxMacroDepth)
        assertEquals(5, Options.DEFAULT_MAX_MACRO_DEPTH)
    }

    @Test
    fun `a wither returns a new instance and leaves the original alone`() {
        val options = Options.of(mapping)

        val omitted = options.withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
        assertNotSame(options, omitted)
        assertEquals(NullAttributeRepresentation.OMITTED, omitted.nullAttributeRepresentation)
        // Immutability is what lets one Options be shared across concurrent requests, so the
        // original must be untouched rather than merely equal to something.
        assertEquals(NullAttributeRepresentation.EXPLICIT, options.nullAttributeRepresentation)

        val deeper = options.withMaxMacroDepth(9)
        assertEquals(9, deeper.maxMacroDepth)
        assertEquals(Options.DEFAULT_MAX_MACRO_DEPTH, options.maxMacroDepth)

        val remapped = options.withMapping(other)
        assertSame(other, remapped.mapping)
        assertSame(mapping, options.mapping)
    }

    @Test
    fun `a wither carries every other setting across`() {
        val options = Options.of(mapping)
            .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
            .withMaxMacroDepth(2)

        val remapped = options.withMapping(other)
        assertEquals(NullAttributeRepresentation.OMITTED, remapped.nullAttributeRepresentation)
        assertEquals(2, remapped.maxMacroDepth)

        val flipped = options.withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)
        assertEquals(2, flipped.maxMacroDepth)
        assertSame(mapping, flipped.mapping)
    }

    @Test
    fun `a macro depth below one is refused`() {
        // A bound of 0 refuses every collection macro, which is not a cost guard but a silent
        // capability change, so it is rejected where it is set rather than surfacing as a refusal
        // the caller cannot explain.
        listOf(0, -1, Int.MIN_VALUE).forEach { depth ->
            val error = assertThrows(IllegalArgumentException::class.java) {
                Options.of(mapping).withMaxMacroDepth(depth)
            }
            assertEquals("maxMacroDepth must be at least 1, got $depth", error.message)
        }
        Options.of(mapping).withMaxMacroDepth(1)
    }

    @Test
    fun `a resolver function is an Options mapping like any other`() {
        // AttributeResolver is a fun interface, so a caller with a rule rather than a table hands
        // in a lambda. The corpus only ever exercises the static form.
        val byRule = Options.of { reference -> if (reference == "request.resource.id") AttributeMapping.field(Docs.id) else null }
        assertEquals(Docs.id, (byRule.mapping.resolve("request.resource.id") as AttributeMapping.Field).column)
        assertEquals(null, byRule.mapping.resolve("request.resource.attr.other"))
    }

    @Test
    fun `toString names the settings and not the mapping`() {
        val described = Options.of(mapping).withMaxMacroDepth(3).toString()
        assertTrue(described.contains("nullAttributeRepresentation=EXPLICIT"), described)
        assertTrue(described.contains("maxMacroDepth=3"), described)
    }
}
