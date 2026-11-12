package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.nzb.FilenameResolver
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
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
        "'[01/15] \"Harbor.Lights.2019.mkv\" yEnc (1/100)', Harbor.Lights.2019.mkv",
        // A quoted name is kept as it is, extension or not.
        "'\"Tg4LmW9qZr2K\" yEnc (1/9)', Tg4LmW9qZr2K",
    )
    fun `quoted filename in subject is preferred`(subject: String, expected: String) {
        assertEquals(expected, FilenameResolver.extractSubjectFilename(subject))
    }

    @Test
    fun `subject with no quotes names the file before yEnc`() {
        val subject = "Release.Name.r00 - yEnc (1/100)"
        assertEquals("Release.Name.r00", FilenameResolver.extractSubjectFilename(subject))
    }

    @Test
    fun `subject with neither quotes nor a matching filename returns null`() {
        assertNull(FilenameResolver.extractSubjectFilename("just some random subject text"))
    }

    @Test
    fun `quoted path-like value is rejected but fallback may still find the basename`() {
        // A quoted value holding a path separator is refused (better than surfacing
        // half a path). The unquoted rule splits the subject at separators, so it still
        // finds the bare name after the last one.
        // Forward slash.
        assertEquals(
            "name.mkv",
            FilenameResolver.extractSubjectFilename("[1/8] \"some/path/name.mkv\" yEnc")
        )
        // Backslash.
        assertEquals(
            "name.mkv",
            FilenameResolver.extractSubjectFilename("[1/8] \"some\\path\\name.mkv\" yEnc")
        )
    }

    @Test
    fun `subject with only a path returns null`() {
        // No bare filename anywhere: the quoted value holds a separator, and no piece
        // between the separators has an extension, so the result is null.
        assertNull(FilenameResolver.extractSubjectFilename("\"only/a/path/no/file/at/end\""))
    }

    @ParameterizedTest
    @MethodSource("unquotedSubjects")
    fun `an unquoted subject names the file nearest yEnc or its part counter`(subject: String, expected: String) {
        assertEquals(expected, FilenameResolver.extractSubjectFilename(subject), subject)
    }

    /**
     * A volume posted under `.bin`, a disc image and its cue sheet are files a release holds: one
     * named nearest yEnc wins over an NFO named before it, and over a web address beside it.
     */
    @ParameterizedTest
    @CsvSource(
        "'Movie.2020.nfo - Movie.2020.r00.bin yEnc (1/50)', Movie.2020.r00.bin",
        "'Movie.2020.nfo - Movie.2020.img yEnc (1/50)', Movie.2020.img",
        "'Movie.2020.nfo - Movie.2020.cue yEnc (1/1)', Movie.2020.cue",
        "'Movie.2020.r00.bin - example.org - yEnc (1/50)', Movie.2020.r00.bin",
    )
    fun `a bin, img or cue file in an unquoted subject is a file a release holds`(subject: String, expected: String) {
        assertEquals(expected, FilenameResolver.extractSubjectFilename(subject), subject)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Harbor.Lights.2019.1080p yEnc (1/50)",
            "Movie.2020 yEnc (1/50)",
            "Harbor.Lights.(2019).mkv yEnc (1/9)",
            "a8f3c1b2e9d4 yEnc (1/50)",
            "1.37GB yEnc (1/1)",
            ".mkv yEnc (1/2)",
            "\"only/a/path/no/file/at/end\"",
            "just some random subject text",
            "test file (1/1)",
            "test file",
            "test",
            "s",
            "file1",
            "file\r\nINFO forged line",
        ]
    )
    fun `an unquoted subject without a file name piece names nothing`(subject: String) {
        assertNull(FilenameResolver.extractSubjectFilename(subject), subject)
    }

    /**
     * Unquoted, nothing marks where a name with spaces starts: the piece before yEnc joined to a
     * word by a single space is only the end of it, so the subject names nothing.
     */
    @ParameterizedTest
    @ValueSource(strings = ["Harbor Lights 2019.mkv yEnc (1/50)", "Copper Crown S01E02.mkv yEnc (1/30)", "yEnc (1/5) Harbor Lights.mkv"])
    fun `an unquoted name with spaces names nothing`(subject: String) {
        assertNull(FilenameResolver.extractSubjectFilename(subject), subject)
    }

    /** A release name used as the subject ends in a codec or source tag, not an extension: it names no file. */
    @ParameterizedTest
    @ValueSource(
        strings = [
            "Harbor.Lights.2019.1080p.WEB.x264 yEnc (1/50)",
            "Harbor.Lights.2019.1080p.WEB.H.264 yEnc (1/50)",
            "Harbor.Lights.2019.2160p.HEVC yEnc (1/50)",
            "Harbor.Lights.2019.1080p.WEB yEnc (1/50)",
            "Copper.Crown.S01E02.720p.HDTV yEnc (1/30)",
            "Harbor.Lights.2003.DVDRip.XviD yEnc (1/50)",
            "www.example.org yEnc (1/50)",
        ],
    )
    fun `a release name or a web address in an unquoted subject names nothing`(subject: String) {
        assertNull(FilenameResolver.extractSubjectFilename(subject), subject)
    }

    @Test
    fun `the yEnc name wins over a release name used as the subject`() {
        val best = FilenameResolver.bestFilename(
            yencName = "a8f7d6e5.mkv",
            subject = "Harbor.Lights.2019.1080p.WEB.x264 yEnc (1/50)",
            par2Filename = null,
        )
        assertEquals("a8f7d6e5.mkv", best)
    }

    @Test
    fun `the full yEnc name wins over an unquoted subject that holds a name with spaces`() {
        val best = FilenameResolver.bestFilename(
            yencName = "Harbor Lights 2019.mkv",
            subject = "Harbor Lights 2019.mkv yEnc (1/50)",
            par2Filename = null,
        )
        assertEquals("Harbor Lights 2019.mkv", best)
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

    /** VIDEO_TS.BUP is a copy of VIDEO_TS.IFO: their shared hash names neither. */
    @Test
    fun `indexPar2ByHash16k names nothing by a hash that descriptions with different names share`() {
        val ifo = kotlin.random.Random(1).nextBytes(20_000)
        val vob = kotlin.random.Random(2).nextBytes(20_000)
        val index = FilenameResolver.indexPar2ByHash16k(
            buildPar2File(listOf("VIDEO_TS.IFO" to ifo, "VIDEO_TS.BUP" to ifo.copyOf(), "VTS_01_1.VOB" to vob))
        )

        assertEquals(mapOf(FilenameResolver.hash16kHex(vob)!! to "VTS_01_1.VOB"), index)
    }

    // Full ObfuscationHeuristic corpus lives in ObfuscationHeuristicTest — no parity
    // smoke tests here now that both files are in the same module.

    companion object {
        @JvmStatic
        fun unquotedSubjects(): List<Arguments> = listOf(
            Arguments.of("Harbor.Lights.2019.1080p.mkv yEnc (1/50)", "Harbor.Lights.2019.1080p.mkv"),
            Arguments.of("[01/15] - Harbor.Lights.2019.part01.rar yEnc (1/80) 52428800", "Harbor.Lights.2019.part01.rar"),
            Arguments.of("Copper Crown [04/12] - Copper.Crown.S01E04.part3.rar (12/90)", "Copper.Crown.S01E04.part3.rar"),
            Arguments.of("Copper.Crown.S01E02.720p.mkv (1/30)", "Copper.Crown.S01E02.720p.mkv"),
            Arguments.of("(1/30) Copper.Crown.S01E02.mkv", "Copper.Crown.S01E02.mkv"),
            Arguments.of("yEnc (1/5) Harbor.Lights.mkv", "Harbor.Lights.mkv"),
            Arguments.of("Harbor.Lights.mkv yEnc(1/50)", "Harbor.Lights.mkv"),
            Arguments.of("[1/5]Harbor.Lights.mkv yEnc (1/9)", "Harbor.Lights.mkv"),
            Arguments.of("HARBOR.LIGHTS.2019.MKV YENC (1/50)", "HARBOR.LIGHTS.2019.MKV"),
            Arguments.of("Movie.2020.7z.001 yEnc (1/40)", "Movie.2020.7z.001"),
            Arguments.of("[1/5] Harbor.Lights.2019.mkv.002 yEnc (3/60)", "Harbor.Lights.2019.mkv.002"),
            Arguments.of("Show.S01E03.vol00+01.par2 yEnc (1/4)", "Show.S01E03.vol00+01.par2"),
            Arguments.of("Movie.2020.1080p.WEB-GROUP.mkv yEnc (1/50)", "Movie.2020.1080p.WEB-GROUP.mkv"),
            Arguments.of("Harbor.Lights.2019.mkv - 1.37GB - yEnc (1/50)", "Harbor.Lights.2019.mkv"),
            Arguments.of("Harbor.Lights.2019.mkv, 1.4 GB yEnc (1/50)", "Harbor.Lights.2019.mkv"),
            Arguments.of("Copper.Crown.S01E01.mkv (1/30) 734003200 bytes", "Copper.Crown.S01E01.mkv"),
            // A number with a unit apart, or grouped by dots, is no name: a name has a letter.
            Arguments.of("Harbor.Lights.2019.mkv - 1.234 GB - yEnc (1/50)", "Harbor.Lights.2019.mkv"),
            Arguments.of("Harbor.Lights.2019.part01.rar - 734.003.200 bytes - yEnc (1/80)", "Harbor.Lights.2019.part01.rar"),
            Arguments.of("Show - [2/9] - Show.S01E02.mkv  700 MB (1/9)", "Show.S01E02.mkv"),
            Arguments.of("Harbor.Lights.2019.nfo Harbor.Lights.2019.mkv yEnc (1/50)", "Harbor.Lights.2019.mkv"),
            Arguments.of("Movie.2020.mkv yEnc (1/50) Movie.2020.nfo", "Movie.2020.mkv"),
            Arguments.of("[3/7] - Copper.Crown.S02E05.mkv. yEnc (1/50)", "Copper.Crown.S02E05.mkv"),
            Arguments.of("Re: Harbor.Lights.nfo", "Harbor.Lights.nfo"),
            Arguments.of("C:\\Posts\\Harbor.Lights.mkv yEnc (1/2)", "Harbor.Lights.mkv"),
            // One quote only: no quoted name, so the unquoted rule reads past the quote.
            Arguments.of("\"Harbor.Lights.mkv yEnc (1/50)", "Harbor.Lights.mkv"),
            Arguments.of("data.bin", "data.bin"),
            Arguments.of("release.part01.rar", "release.part01.rar"),
            Arguments.of("release.part1.rar", "release.part1.rar"),
            // A file type a release holds wins over a nearer piece of another type, such as a site's address.
            Arguments.of("Harbor.Lights.2019.mkv [www.example.org] yEnc (1/50)", "Harbor.Lights.2019.mkv"),
            Arguments.of("Harbor.Lights.2019.mkv - example.org - yEnc (1/50)", "Harbor.Lights.2019.mkv"),
            Arguments.of("Harbor.Lights.2019.dat - example.org - yEnc (1/50)", "example.org"),
            // A split part numbered like a codec is still one.
            Arguments.of("Movie.2020.7z.264 yEnc (1/400)", "Movie.2020.7z.264"),
            // A count is no part of the name after it.
            Arguments.of("01/50 Harbor.Lights.mkv yEnc (1/50)", "Harbor.Lights.mkv"),
        )
    }
}
