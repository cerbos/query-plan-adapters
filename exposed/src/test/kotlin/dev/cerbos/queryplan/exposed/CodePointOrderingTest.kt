package dev.cerbos.queryplan.exposed

import org.jetbrains.exposed.v1.core.VarCharColumnType
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * The store fact `CodePointOrdering` rests on for H2: comparing two strings as their UTF-8 bytes
 * (`STRINGTOUTF8`) orders them by code point, which needs H2 to compare binary values as UNSIGNED
 * bytes. The corpus's astral case only separates a surrogate pair from U+FFFD, where signed and
 * unsigned byte order happen to agree (0xF0 and 0xEF are both negative), so it cannot see a
 * signed comparison; `"e" < "é"` (0x65 against 0xC3) and `"z" < "é"` can.
 */
class CodePointOrderingTest {

    private val database by lazy {
        Database.connect("jdbc:h2:mem:cerbos_code_point;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
    }

    private fun below(left: String, right: String): Boolean = transaction(database) {
        exec(
            "SELECT STRINGTOUTF8(?) < STRINGTOUTF8(?)",
            listOf(
                VarCharColumnType() to left,
                VarCharColumnType() to right,
            ),
        ) { rs -> rs.next() && rs.getBoolean(1) }
    } ?: error("no row")

    @Test
    fun `H2 orders UTF-8 bytes unsigned, which is code point order`() {
        assertEquals(true, below("e", "é"), "ASCII before a two-byte character")
        assertEquals(true, below("z", "é"))
        assertEquals(true, below("�", "🚀"), "U+FFFD before an astral character")
        assertEquals(false, below("🚀", "�"))
        assertEquals(true, below("héllo", "héllo�"), "a prefix before its extension")
    }
}
