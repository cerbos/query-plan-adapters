package dev.cerbos.queryplan.exposed

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.name
import kotlin.io.path.readText

/**
 * No doc comment in this module documents something else.
 *
 * Kotlin binds only the LAST doc comment before a declaration, so two in a row leave the first one
 * orphaned: it renders nowhere, the declaration it was written for silently has none, and a reader
 * scrolling past finds a paragraph sitting over a function it is not about. It has happened twice
 * here, both times when something new was inserted between an existing KDoc and its declaration,
 * and nothing says so — an orphaned doc comment is legal Kotlin and no linter here objects.
 *
 * Stated as a property over the whole module rather than at the places it has happened, for the
 * reason `ComparisonSqlShapeTest`'s sweeps are corpus-quantified: a rule that names its instances
 * stops covering the next one.
 */
class KdocPlacementTest {

    @Test
    fun `no doc comment is immediately followed by another doc comment`() {
        val offenders = mutableListOf<String>()
        var scanned = 0
        var docComments = 0

        sources().forEach { file ->
            scanned++
            val lines = file.readText().lines()
            var openedAt = -1
            lines.forEachIndexed { index, raw ->
                val line = raw.trim()
                if (openedAt < 0 && line.startsWith("/**")) openedAt = index
                if (openedAt >= 0 && line.endsWith("*/")) {
                    docComments++
                    val next = (index + 1 until lines.size).firstOrNull { lines[it].isNotBlank() }
                    if (next != null && lines[next].trim().startsWith("/**")) {
                        offenders.add(
                            "${file.name}: the doc comment at line ${openedAt + 1} is followed by " +
                                "another at line ${next + 1}, so it documents nothing",
                        )
                    }
                    openedAt = -1
                }
            }
        }

        assertEquals(emptyList<String>(), offenders)

        // Anti-vacuity: the scan really reaches this module's sources and really recognises a doc
        // comment, so a wrong path or a detector that matched nothing could not report clean.
        assertTrue(scanned > 60, "only $scanned Kotlin files were scanned")
        assertTrue(docComments > 400, "only $docComments doc comments were found")
    }

    private fun sources(): List<Path> = listOf("src/main/kotlin", "src/test/kotlin")
        .map { Path.of(System.getProperty("user.dir"), it) }
        .flatMap { root ->
            Files.walk(root).use { walk -> walk.filter { it.extension == "kt" }.toList() }
        }
}
