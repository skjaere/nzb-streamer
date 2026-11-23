package io.skjaere.nzbstreamer.play

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * A file the player can't read as every source says it: plain words by how the probe found it unreadable,
 * naming the file's type where its name gives one fit to show, never Media3's own.
 */
class UnreadableFailureTest {
    private fun failed(kind: UnreadableFile?) = ProbeResult.Failed(UsenetError.Unknown(null), kind)

    @Test
    fun `the file's type is its extension in capitals, where it is two to five letters or digits`() {
        val names = listOf("Copper.Crown.2019.mkv", "00001.m2ts", "Harbor.Lights.S01E01.Ts", "README", "a.toolongext", "a.", "a.x", "Copper.Crown.2019.m-v")
        assertEquals(
            mapOf(
                "Copper.Crown.2019.mkv" to "MKV", "00001.m2ts" to "M2TS", "Harbor.Lights.S01E01.Ts" to "TS",
                "README" to null, "a.toolongext" to null, "a." to null, "a.x" to null, "Copper.Crown.2019.m-v" to null,
            ),
            names.associateWith { unreadableFailure(failed(UnreadableFile.UnknownFormat), it)?.extension },
        )
    }

    @Test
    fun `each kind is said in plain words, with the file's type or without it`() {
        assertEquals(
            listOf(
                AttemptFailure.Unreadable(UnreadableFile.UnknownFormat, "MKV") to "The player can't read this MKV file",
                AttemptFailure.Unreadable(UnreadableFile.UnknownFormat, null) to "The player can't read this file's format",
                AttemptFailure.Unreadable(UnreadableFile.Broken, "M2TS") to "The player can't read this M2TS file: it may be damaged",
                AttemptFailure.Unreadable(UnreadableFile.Broken, null) to "The player can't read this file: it may be damaged",
                AttemptFailure.Unreadable(UnreadableFile.ReaderCrashed, "MKV") to "Reading the release failed",
                AttemptFailure.Unreadable(UnreadableFile.ReaderCrashed, null) to "Reading the release failed",
            ),
            listOf(
                UnreadableFile.UnknownFormat to "Copper.Crown.2019.mkv", UnreadableFile.UnknownFormat to "README",
                UnreadableFile.Broken to "00001.m2ts", UnreadableFile.Broken to "a.toolongext",
                UnreadableFile.ReaderCrashed to "Copper.Crown.2019.mkv", UnreadableFile.ReaderCrashed to "a.",
            ).map { (kind, name) -> unreadableFailure(failed(kind), name).let { it to it?.text } },
        )
    }

    /** A probe that failed for another reason (a missing article, its own time-out) is said as that reason says. */
    @Test
    fun `a probe that failed otherwise is none of them`() {
        assertNull(unreadableFailure(failed(null), "Copper.Crown.2019.mkv"))
        assertNull(unreadableFailure(ProbeResult.Failed(UsenetError.MissingArticle("430 No Such Article")), "Copper.Crown.2019.mkv"))
        assertNull(unreadableFailure(ProbeResult.Failed(UsenetError.Timeout("No track list within 20 s")), "Copper.Crown.2019.mkv"))
    }
}
