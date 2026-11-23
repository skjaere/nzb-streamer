package io.skjaere.nzbstreamer.play

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** The container types a video file's extension names, for the player and for a server streaming to a Cast device, and which a Cast device takes. */
class ContainerTypesTest {
    @Test
    fun `every type is as the player had it`() {
        val types = listOf(
            "a.avi", "B.AVI", "c.mkv", "d.mp4", "e.m4v", "f.ts", "g.m2ts", "h.webm", "i.mov", "j.wmv", "k.mpg", "l.mpeg",
            "m.srt", "README", "n.avi.part", "dir.mkv/o", ".mkv",
        ).associateWith { ContainerTypes.mimeOf(it) }
        assertEquals(
            mapOf(
                "a.avi" to "video/x-msvideo", "B.AVI" to "video/x-msvideo", "c.mkv" to "video/x-matroska",
                "d.mp4" to "video/mp4", "e.m4v" to "video/mp4", "f.ts" to "video/mp2t", "g.m2ts" to "video/mp2t",
                "h.webm" to "video/webm", "i.mov" to "video/quicktime", "j.wmv" to "video/x-ms-wmv",
                "k.mpg" to "video/mp2p", "l.mpeg" to "video/mp2p",
                "m.srt" to null, "README" to null, "n.avi.part" to null, "dir.mkv/o" to null, ".mkv" to "video/x-matroska",
            ),
            types,
        )
    }

    /** As measured on a receiver: MKV, WebM, MP4, M4V and MOV play; TS and AVI are refused. */
    @Test
    fun `a Cast device takes mkv, webm, mp4, m4v and mov, in any case`() {
        for (name in listOf("a.mkv", "a.webm", "a.mp4", "a.m4v", "a.mov", "Copper.Crown.2021.1080p.WEB.MKV", "x.Mp4", "x.MoV")) {
            assertTrue(ContainerTypes.castable(name), name)
        }
        for (name in listOf("a.ts", "a.m2ts", "a.avi", "a.wmv", "a.mpg", "a.mpeg", "A.AVI", "a.M2TS", "a.srt", "README", "a.mkv.part", "")) {
            assertFalse(ContainerTypes.castable(name), name)
        }
    }

    @Test
    fun `the extension is the name's last, lowercased`() {
        assertEquals("avi", ContainerTypes.extension("Copper.Crown.2021.AVI"))
        assertEquals("mkv", ContainerTypes.extension("dir/a.b.mkv"))
        assertEquals("", ContainerTypes.extension("README"))
    }
}
