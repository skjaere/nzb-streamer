package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.nzb.FilenameResolver
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertNull

class FilenameResolverTest {

    // --- extractSubjectFilename --------------------------------------------------------

    @ParameterizedTest
    @CsvSource(
        // Standard `[X/Y] "name.ext" yEnc (M/N)` shape — by far the most common.
        "'[1/8] - \"Release.Name.r00\" yEnc 12345 (1/54321)', Release.Name.r00",
        "'[01/15] \"Release.Name.2024.1080p.mkv\" yEnc (1/100)', Release.Name.2024.1080p.mkv",
        // Just the quoted name, no surrounding tokens.
        "'\"Release.Name.mkv\"', Release.Name.mkv",
    )
    fun `quoted filename in subject is preferred`(subject: String, expected: String) {
        assertEquals(expected, FilenameResolver.extractSubjectFilename(subject))
    }

    @Test
    fun `subject with no quotes falls back to SABnzbd regex`() {
        val subject = "Release.Name.r00 - yEnc (1/100)"
        assertEquals("Release.Name.r00", FilenameResolver.extractSubjectFilename(subject))
    }

    @Test
    fun `subject with neither quotes nor a matching filename returns null`() {
        assertNull(FilenameResolver.extractSubjectFilename("just some random subject text"))
    }

    @Test
    fun `quoted path-like value is rejected but fallback may still find the basename`() {
        // If the quoted match contains a path separator we refuse it (better than
        // surfacing half a path). But the SABnzbd fallback regex doesn't traverse `/`
        // so it can still recover the bare basename — that's a feature, not a bug.
        // Forward slash: fallback recovers `name.mkv`.
        assertEquals(
            "name.mkv",
            FilenameResolver.extractSubjectFilename("[1/8] \"some/path/name.mkv\" yEnc")
        )
        // Backslash: SABnzbd's fallback regex uses `\w` which doesn't match `\` either,
        // so we also recover the basename.
        assertEquals(
            "name.mkv",
            FilenameResolver.extractSubjectFilename("[1/8] \"some\\path\\name.mkv\" yEnc")
        )
    }

    @Test
    fun `subject with only a path returns null`() {
        // No bare filename anywhere — both the quoted and fallback regexes refuse to
        // produce a value containing a separator, so the result is null.
        assertNull(FilenameResolver.extractSubjectFilename("\"only/a/path/no/file/at/end\""))
    }

    // --- bestFilename: priority order --------------------------------------------------

    @Test
    fun `par2 wins when all three sources are clean`() {
        val best = FilenameResolver.bestFilename(
            yencName = "Release.Name.r00",
            subject = "[1/8] \"Release.Name.r00\" yEnc",
            par2Filename = "Release.Name.r00",
        )
        // All three resolve to the same clean name — par2 wins on equal priority.
        assertEquals("Release.Name.r00", best)
    }

    @Test
    fun `par2 wins over an obfuscated yenc name`() {
        val best = FilenameResolver.bestFilename(
            yencName = "b082fa0beaa644d3aa01045d5b8d0b36.r00", // obfuscated 32-hex
            subject = "[1/8] \"b082fa0beaa644d3aa01045d5b8d0b36.r00\" yEnc",
            par2Filename = "Release.Name.r00",
        )
        assertEquals("Release.Name.r00", best)
    }

    @Test
    fun `subject is used when par2 is missing`() {
        val best = FilenameResolver.bestFilename(
            yencName = "b082fa0beaa644d3aa01045d5b8d0b36.r00",
            subject = "[1/8] \"Release.Name.r00\" yEnc",
            par2Filename = null,
        )
        assertEquals("Release.Name.r00", best)
    }

    @Test
    fun `yenc is used when par2 and subject are blank or unparseable`() {
        val best = FilenameResolver.bestFilename(
            yencName = "Release.Name.mkv",
            subject = "no filename here at all just words",
            par2Filename = null,
        )
        assertEquals("Release.Name.mkv", best)
    }

    @Test
    fun `obfuscated par2 still wins over obfuscated subject and yenc on priority alone`() {
        // All three obfuscated → same -1000 penalty applied to each, so the priority
        // ordering (par2 > subject > yenc) decides.
        val best = FilenameResolver.bestFilename(
            yencName = "ccc12345678901234567890123456789.mkv",
            subject = "[1/1] \"bbb12345678901234567890123456789.mkv\" yEnc",
            par2Filename = "aaa12345678901234567890123456789.mkv",
        )
        assertEquals("aaa12345678901234567890123456789.mkv", best)
    }

    @Test
    fun `clean yenc beats obfuscated par2`() {
        // Real-world case: uploader's par2 set carries the on-wire (obfuscated) name
        // because the par2 set was generated from the already-obscured files. In that
        // case the yenc-header name is sometimes cleaner — e.g. when the uploader
        // wrote a clean filename to the yenc header but produced par2 against the
        // obscured wire bytes. Rare; we honour the cleaner source.
        val par2 = "b082fa0beaa644d3aa01045d5b8d0b36.mkv" // obscured
        val best = FilenameResolver.bestFilename(
            yencName = "Release.Name.mkv",
            subject = "[1/1] yEnc",
            par2Filename = par2,
        )
        // par2 priority=3-1000=-997; yenc priority=1+0+50(important)+10(ext-len)=61.
        // yenc wins.
        assertEquals("Release.Name.mkv", best)
    }

    // --- bestFilename: empty / blank handling ------------------------------------------

    @Test
    fun `all blank sources returns empty string`() {
        assertEquals("", FilenameResolver.bestFilename("", "", null))
    }

    @Test
    fun `null yenc and blank subject and missing par2 returns empty`() {
        assertEquals("", FilenameResolver.bestFilename(null, "", null))
    }

    @Test
    fun `whitespace-only candidates are treated as blank`() {
        // Even with a blank-but-not-null yenc, the resolver should return "" rather
        // than the whitespace string. Whitespace-only sneaking into StreamableFile.path
        // would surface as a confusing empty-looking file.
        assertEquals("", FilenameResolver.bestFilename("   ", "  ", "  "))
    }

    // --- bestFilename: extension / important-file scoring ------------------------------

    @Test
    fun `important file extension bonus boosts yenc over subject`() {
        // par2=null. subject candidate has an unusual extension (.dat, 3 chars but not
        // an "important" type per IMPORTANT_FILE_EXTENSIONS). yenc has .mkv (important +
        // 2-4 char ext). The bonuses tip the scale toward yenc.
        // subject prio = 2 + 0(not obscured) + 0(.dat not important) + 10(ext-len) = 12
        // yenc prio = 1 + 0 + 50(important) + 10 = 61
        val best = FilenameResolver.bestFilename(
            yencName = "Release.Name.mkv",
            subject = "[1/1] \"Release.Name.dat\" yEnc",
            par2Filename = null,
        )
        assertEquals("Release.Name.mkv", best)
    }

    @Test
    fun `long extension does not earn the 2-to-4 char bonus`() {
        // 5-char ext bypasses the +10. Doesn't change the priority order here since
        // par2 is present; primarily documenting the boundary.
        val best = FilenameResolver.bestFilename(
            yencName = "Release.Name.weird5",
            subject = "[1/1] \"Release.Name.weird5\" yEnc",
            par2Filename = "Release.Name.weird5",
        )
        assertEquals("Release.Name.weird5", best)
    }

    // --- hash16kHex --------------------------------------------------------------------

    @Test
    fun `hash16kHex returns null for null input`() {
        assertNull(FilenameResolver.hash16kHex(null))
    }

    @Test
    fun `hash16kHex returns null for too-short input`() {
        assertNull(FilenameResolver.hash16kHex(ByteArray(100)))
    }

    @Test
    fun `hash16kHex output shape`() {
        val hex = FilenameResolver.hash16kHex(ByteArray(16 * 1024))!!
        // MD5 in lowercase hex = 32 chars in [0-9a-f].
        assertEquals(32, hex.length)
        assertEquals(true, hex.all { it in "0123456789abcdef" }, hex)
    }

    @Test
    fun `hash16kHex is deterministic for the same input`() {
        val data = ByteArray(16 * 1024) { (it % 251).toByte() }
        assertEquals(FilenameResolver.hash16kHex(data), FilenameResolver.hash16kHex(data))
    }

    @Test
    fun `hash16kHex ignores bytes past the first 16KB`() {
        val short = ByteArray(16 * 1024) { (it % 251).toByte() }
        val long = ByteArray(20 * 1024) { i ->
            if (i < 16 * 1024) (i % 251).toByte() else 0xFF.toByte()
        }
        // First 16KB is identical; trailing bytes differ. Should produce the same hash.
        assertEquals(FilenameResolver.hash16kHex(short), FilenameResolver.hash16kHex(long))
    }

    @Test
    fun `hash16kHex differs when first-16KB content differs`() {
        val a = ByteArray(16 * 1024)
        val b = ByteArray(16 * 1024).apply { this[0] = 1 }
        assertEquals(false, FilenameResolver.hash16kHex(a) == FilenameResolver.hash16kHex(b))
    }

    // --- indexPar2ByHash16k ------------------------------------------------------------

    @Test
    fun `indexPar2ByHash16k returns empty for null bytes`() {
        assertEquals(emptyMap(), FilenameResolver.indexPar2ByHash16k(null))
    }

    @Test
    fun `indexPar2ByHash16k returns empty for malformed bytes`() {
        // Random bytes — Par2Parser may throw or just produce no descriptors. Either
        // way we get an empty map, never a crash.
        assertEquals(emptyMap(), FilenameResolver.indexPar2ByHash16k("not a par2 file".toByteArray()))
    }

    // Full ObfuscationHeuristic corpus lives in ObfuscationHeuristicTest — no parity
    // smoke tests here now that both files are in the same module.
}
