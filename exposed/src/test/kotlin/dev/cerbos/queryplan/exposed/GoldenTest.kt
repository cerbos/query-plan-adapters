package dev.cerbos.queryplan.exposed

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.jetbrains.exposed.v1.core.EqOp
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.stringParam
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * The golden asset's own contract: what [Golden] refuses to read, what it refuses to write, and what
 * survives a rewrite.
 *
 * Every case here works on a temp directory. Pointing them at `exposed/golden/expectations.json`
 * would make the suite rewrite the very asset the translator test asserts against — and a suite that
 * regenerates what it compares is the failure mode `conformance/README.md` spends a section on. The
 * one thing this suite asks of the committed asset is that it parses.
 */
class GoldenTest {

    private val json = ObjectMapper()

    private fun conditional(sql: String): ObjectNode = Golden.entry(
        OfflineRenderer.DIALECTS.associateWith {
            Rendered("$sql /* $it */", listOf(RenderedParam.of("VarCharColumnType", "one")))
        },
    )

    private fun golden(directory: Path, exposed: String = Golden.EXPOSED_MINOR): Golden =
        Golden(directory.resolve("expectations.json"), exposed)

    private fun writeRaw(directory: Path, body: String): Path =
        directory.resolve("expectations.json").also { Files.writeString(it, body) }

    // -- round trip -----------------------------------------------------------------------------

    @Test
    fun `a rendering round-trips through the asset`(@TempDir directory: Path) {
        val written = mapOf(
            "cs-eq" to conditional("a_string = ?"),
            "always-allow" to Golden.entry(Golden.KIND_ALWAYS_ALLOWED),
            "always-deny" to Golden.entry(Golden.KIND_ALWAYS_DENIED),
        )
        val golden = golden(directory)
        golden.write(written)

        val read = golden.read()
        assertEquals(written.keys.sorted(), read.keys.toList())
        written.forEach { (action, entry) -> assertEquals(entry, read.getValue(action)) }
    }

    @Test
    fun `a missing file is written rather than refused`(@TempDir directory: Path) {
        val golden = golden(directory)
        assertFalse(Files.exists(golden.file))
        golden.write(mapOf("cs-eq" to conditional("a_string = ?")))
        assertTrue(Files.exists(golden.file))
    }

    @Test
    fun `the asset is two-space indented and ends in one newline`(@TempDir directory: Path) {
        golden(directory).write(mapOf("cs-eq" to conditional("a_string = ?")))
        val body = Files.readString(directory.resolve("expectations.json"))
        assertTrue(body.contains("\n  \"adapter\": \"exposed\","), body)
        assertFalse(body.contains("\r"), "line endings must be LF")
        assertTrue(body.endsWith("}\n") && !body.endsWith("\n\n"), "one trailing newline")
    }

    // -- notes ----------------------------------------------------------------------------------

    @Test
    fun `a note survives regeneration and is never compared`(@TempDir directory: Path) {
        val golden = golden(directory)
        golden.write(mapOf("cs-eq" to conditional("a_string = ?")))

        // A reviewer writes commentary into the asset by hand; the next regeneration must keep it.
        val annotated = json.readTree(Files.readString(golden.file)) as ObjectNode
        (annotated.get("expectations").get("cs-eq") as ObjectNode).put(Golden.NOTE_KEY, "why this shape")
        Files.writeString(golden.file, annotated.toPrettyString())

        golden.write(mapOf("cs-eq" to conditional("a_string <> ?")))

        val rewritten = json.readTree(Files.readString(golden.file))
        assertEquals("why this shape", rewritten.get("expectations").get("cs-eq").get(Golden.NOTE_KEY).asText())
        // …and the note is the ONE key `read` strips, so it can never take part in a comparison.
        assertNull(golden.read().getValue("cs-eq").get(Golden.NOTE_KEY))
    }

    @Test
    fun `a note is written before the expectation it annotates`(@TempDir directory: Path) {
        val golden = golden(directory)
        golden.write(mapOf("cs-eq" to conditional("a_string = ?")))
        val annotated = json.readTree(Files.readString(golden.file)) as ObjectNode
        (annotated.get("expectations").get("cs-eq") as ObjectNode).put(Golden.NOTE_KEY, "read me first")
        Files.writeString(golden.file, annotated.toPrettyString())
        golden.write(mapOf("cs-eq" to conditional("a_string = ?")))

        val body = Files.readString(golden.file)
        assertTrue(body.indexOf(Golden.NOTE_KEY) < body.indexOf(Golden.KIND_KEY), body)
    }

    // -- sort order -----------------------------------------------------------------------------

    @Test
    fun `entries are written sorted whatever order they arrive in`(@TempDir directory: Path) {
        val golden = golden(directory)
        golden.write(
            linkedMapOf(
                "zz-last" to conditional("c = ?"),
                "aa-first" to conditional("a = ?"),
                "mm-middle" to conditional("b = ?"),
            ),
        )
        assertEquals(listOf("aa-first", "mm-middle", "zz-last"), golden.read().keys.toList())
    }

    @Test
    fun `an asset listed out of order is refused`(@TempDir directory: Path) {
        writeRaw(
            directory,
            """
            {
              "adapter": "exposed",
              "exposed": "1.5",
              "regenerate": "gradle goldenUpdate",
              "expectations": {
                "zz-last": { "kind": "KIND_ALWAYS_DENIED" },
                "aa-first": { "kind": "KIND_ALWAYS_DENIED" }
              }
            }
            """.trimIndent(),
        )
        val error = assertThrows(IllegalStateException::class.java) { golden(directory).read() }
        assertTrue(error.message!!.contains("out of order"), error.message)
    }

    // -- the header is checked, not ignored -------------------------------------------------------

    @Test
    fun `an asset copied from another adapter is refused`(@TempDir directory: Path) {
        writeRaw(
            directory,
            """
            {
              "adapter": "drizzle",
              "exposed": "1.5",
              "regenerate": "gradle goldenUpdate",
              "expectations": {}
            }
            """.trimIndent(),
        )
        val error = assertThrows(IllegalStateException::class.java) { golden(directory).read() }
        assertTrue(error.message!!.contains("declares adapter \"drizzle\", not \"exposed\""), error.message)
    }

    @Test
    fun `an asset rendered by another Exposed is refused`(@TempDir directory: Path) {
        writeRaw(
            directory,
            """
            {
              "adapter": "exposed",
              "exposed": "1.4",
              "regenerate": "gradle goldenUpdate",
              "expectations": {}
            }
            """.trimIndent(),
        )
        val error = assertThrows(IllegalStateException::class.java) { golden(directory).read() }
        assertTrue(error.message!!.contains("declares exposed \"1.4\", not \"1.5\""), error.message)
    }

    @Test
    fun `an asset naming another regeneration command is refused`(@TempDir directory: Path) {
        writeRaw(
            directory,
            """
            {
              "adapter": "exposed",
              "exposed": "1.5",
              "regenerate": "npm run golden:update",
              "expectations": {}
            }
            """.trimIndent(),
        )
        assertThrows(IllegalStateException::class.java) { golden(directory).read() }
    }

    // -- regeneration refuses under another renderer ----------------------------------------------

    @Test
    fun `regenerating under another Exposed is refused before the write`(@TempDir directory: Path) {
        val baseline = golden(directory)
        baseline.write(mapOf("cs-eq" to conditional("a_string = ?")))
        val before = Files.readString(baseline.file)

        val floor = golden(directory, exposed = "1.0.0")
        assertFalse(floor.rendersUnderRunningExposed)
        val error = assertThrows(IllegalStateException::class.java) {
            floor.write(mapOf("cs-eq" to conditional("something else entirely")))
        }
        assertTrue(error.message!!.contains("1.0.0 is on the classpath"), error.message)
        // Before the write, not after it: a half-rewritten asset labelled 1.5 is the thing rule 2
        // of "When the generator is an input" exists to stop.
        assertEquals(before, Files.readString(baseline.file))
    }

    @Test
    fun `a patch release of the recorded minor still regenerates`(@TempDir directory: Path) {
        assertTrue(golden(directory, exposed = "1.5.7").rendersUnderRunningExposed)
        golden(directory, exposed = "1.5.7").write(mapOf("cs-eq" to conditional("a_string = ?")))
    }

    // -- an entry's shape --------------------------------------------------------------------------

    @Test
    fun `a folded plan carries no rendering and a conditional one must`(@TempDir directory: Path) {
        val golden = golden(directory)
        val foldedWithSql = Golden.entry(Golden.KIND_ALWAYS_ALLOWED)
            .set<ObjectNode>(Golden.RENDERED_KEY, json.createObjectNode())
        assertThrows(IllegalStateException::class.java) { golden.write(mapOf("bad" to foldedWithSql)) }

        val conditionalWithoutSql = Golden.entry(Golden.KIND_CONDITIONAL)
        assertThrows(IllegalStateException::class.java) { golden.write(mapOf("bad" to conditionalWithoutSql)) }
    }

    @Test
    fun `an entry recording a dialect the adapter does not claim is refused`(@TempDir directory: Path) {
        writeRaw(
            directory,
            """
            {
              "adapter": "exposed",
              "exposed": "1.5",
              "regenerate": "gradle goldenUpdate",
              "expectations": {
                "cs-eq": {
                  "kind": "KIND_CONDITIONAL",
                  "rendered": { "oracle": { "sql": "1 = 1", "params": [] } }
                }
              }
            }
            """.trimIndent(),
        )
        val error = assertThrows(IllegalStateException::class.java) { golden(directory).read() }
        assertTrue(error.message!!.contains("records dialects [oracle]"), error.message)
    }

    @Test
    fun `an entry carrying a key the format does not define is refused`(@TempDir directory: Path) {
        writeRaw(
            directory,
            """
            {
              "adapter": "exposed",
              "exposed": "1.5",
              "regenerate": "gradle goldenUpdate",
              "expectations": {
                "cs-eq": { "kind": "KIND_ALWAYS_DENIED", "comment": "notes are called note" }
              }
            }
            """.trimIndent(),
        )
        val error = assertThrows(IllegalStateException::class.java) { golden(directory).read() }
        assertTrue(error.message!!.contains("unknown keys [comment]"), error.message)
    }

    @Test
    fun `a bind is recorded with both its column type and its value`(@TempDir directory: Path) {
        val golden = golden(directory)
        golden.write(mapOf("cs-eq" to conditional("a_string = ?")))
        val param = json.readTree(Files.readString(golden.file))
            .get("expectations").get("cs-eq").get("rendered").get("sqlite").get("params").get(0)
        assertEquals("VarCharColumnType", param.get(Golden.PARAM_TYPE_KEY).asText())
        assertEquals("one", param.get(Golden.PARAM_VALUE_KEY).asText())
    }

    // -- the entry a translator unit test writes ----------------------------------------------------

    @Test
    fun `an entry is built from the filter the translator returned, whichever kind it is`(@TempDir directory: Path) {
        val docs = object : Table("golden_docs") {
            val aString = varchar("a_string", 64)
        }
        val conditional = QueryPlanFilter.Conditional(EqOp(docs.aString, stringParam("one")))

        val entries = linkedMapOf(
            "cs-eq" to Golden.entryFor { conditional },
            "p-has" to Golden.entryFor { QueryPlanFilter.AlwaysAllowed },
            "in-empty" to Golden.entryFor { QueryPlanFilter.AlwaysDenied },
        )
        assertEquals(Golden.KIND_CONDITIONAL, entries.getValue("cs-eq").get(Golden.KIND_KEY).asText())
        assertEquals(Golden.KIND_ALWAYS_ALLOWED, entries.getValue("p-has").get(Golden.KIND_KEY).asText())
        assertEquals(Golden.KIND_ALWAYS_DENIED, entries.getValue("in-empty").get(Golden.KIND_KEY).asText())
        assertEquals(
            OfflineRenderer.DIALECTS,
            entries.getValue("cs-eq").get(Golden.RENDERED_KEY).fieldNames().asSequence().toList(),
        )

        // …and what it builds is exactly what the asset accepts, which is the whole handover.
        val golden = golden(directory)
        golden.write(entries)
        assertEquals(entries.keys.sorted(), golden.read().keys.toList())
    }

    // -- the committed asset -----------------------------------------------------------------------

    @Test
    fun `the committed asset parses, and declares the Exposed the tests are running`() {
        val golden = Golden()
        assertTrue(Files.exists(golden.file), "${golden.file} is missing")
        golden.read()
        assertEquals(
            Golden.runningExposedVersion().startsWith("${Golden.EXPOSED_MINOR}."),
            golden.rendersUnderRunningExposed,
        )
    }
}
