package io.skjaere.nzbstreamer.play

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * A copy of a disc told by its files once they are listed: a Blu-ray's by its `BDMV` folder or its navigation
 * files, a DVD's by its `VIDEO_TS` folder or its own files, an image by `.iso`; never by the release's name, and never
 * where a video of the release's own is beside it, which plays.
 */
class DiscFilesTest {
    private data class File(override val index: Int, override val path: String, override val size: Long) : ListedFile

    private fun kindOf(vararg files: Pair<String, Long>): DiscKind? = DiscFiles.kindOf(files.mapIndexed { i, (path, size) -> File(i, path, size) })

    /** A UHD disc as a RAR set lists it, its folders kept under the release's own, with the backup copies a disc holds. */
    private val disc = "Harbor.Lights.2024.2160p.UHD.BluRay.HEVC-GRP".let { root ->
        arrayOf(
            "$root/BDMV/index.bdmv" to 120L, "$root/BDMV/MovieObject.bdmv" to 300L, "$root/BDMV/PLAYLIST/00800.mpls" to 2_000L,
            "$root/BDMV/CLIPINF/00001.clpi" to 4_000L, "$root/BDMV/STREAM/00001.m2ts" to 60_000_000_000L, "$root/BDMV/STREAM/00002.m2ts" to 900_000_000L,
            "$root/BDMV/BACKUP/index.bdmv" to 120L, "$root/BDMV/BACKUP/PLAYLIST/00800.mpls" to 2_000L, "$root/BDMV/BACKUP/CLIPINF/00001.clpi" to 4_000L,
            "$root/CERTIFICATE/id.bdmv" to 100L, "$root/$root.nfo" to 3_000L,
        )
    }

    @Test
    fun `a Blu-ray disc in its folders is a disc copy`() {
        assertEquals(DiscKind.BluRay, kindOf(*disc))
        // Whatever the separator (a 7z made on Windows may keep its own) and whatever the case.
        assertEquals(DiscKind.BluRay, kindOf("Harbor.Lights.2024\\BDMV\\STREAM\\00001.m2ts" to 900L))
        assertEquals(DiscKind.BluRay, kindOf("HARBOR.LIGHTS.2024/bdmv/stream/00001.M2TS" to 900L, "HARBOR.LIGHTS.2024/bdmv/PLAYLIST/00800.MPLS" to 2L))
        // The folder alone tells: a stream left with no navigation file beside it in the listing.
        assertEquals(DiscKind.BluRay, kindOf("Copper.Crown.2019/BDMV/STREAM/00001.m2ts" to 900L, "Copper.Crown.2019/Copper.Crown.2019.nfo" to 1L))
    }

    /** Posted as they are, a disc's files have no folders (a subject's name can't hold one): the navigation files tell it. */
    @Test
    fun `a Blu-ray disc's files without their folders are a disc copy by its navigation files`() {
        assertEquals(DiscKind.BluRay, kindOf("00001.m2ts" to 900L, "00800.mpls" to 2L, "00001.clpi" to 4L, "index.bdmv" to 1L))
        assertEquals(DiscKind.BluRay, kindOf("00001.M2TS" to 900L, "INDEX.BDMV" to 1L))
        assertEquals(DiscKind.BluRay, kindOf("00001.ssif" to 900L, "00001.clpi" to 4L))
        assertEquals(DiscKind.BluRay, kindOf("00001.m2ts" to 900L, "00000.bdjo" to 4L))
    }

    @Test
    fun `a DVD's files or a disc image are a disc copy`() {
        assertEquals(DiscKind.Dvd, kindOf("Quiet.Orbit.1999.DVD/VIDEO_TS/VTS_01_1.VOB" to 900L, "Quiet.Orbit.1999.DVD/VIDEO_TS/VTS_01_0.IFO" to 4L, "Quiet.Orbit.1999.DVD/VIDEO_TS/VTS_01_0.BUP" to 4L))
        assertEquals(DiscKind.Dvd, kindOf("Quiet.Orbit.1999/video_ts/menu.dat" to 4L))
        assertEquals(DiscKind.Dvd, kindOf("VTS_01_1.vob" to 900L))
        assertEquals(DiscKind.Image, kindOf("Quiet.Orbit.1999.iso" to 900L, "Quiet.Orbit.1999.nfo" to 1L))
        assertEquals(DiscKind.Image, kindOf("Quiet.Orbit.1999.ISO" to 900L))
    }

    /** A copy holding files of two kinds is the kind of its largest one. */
    @Test
    fun `a disc copy is the kind of its largest disc file`() {
        assertEquals(DiscKind.Image, kindOf("Copper.Crown.2019/BDMV/STREAM/00001.m2ts" to 300L, "Copper.Crown.2019/Copper.Crown.2019.iso" to 900L))
        assertEquals(DiscKind.BluRay, kindOf("Copper.Crown.2019/BDMV/STREAM/00001.m2ts" to 900L, "Copper.Crown.2019/Copper.Crown.2019.iso" to 300L))
        assertEquals(DiscKind.Dvd, kindOf("Copper.Crown.2019/VIDEO_TS/VTS_01_1.VOB" to 900L, "Copper.Crown.2019.iso" to 300L))
    }

    /** A sample clip or an extra beside a disc is no video of the release's own: it is still a disc copy. */
    @Test
    fun `samples and extras beside a disc leave it a disc copy`() {
        assertEquals(DiscKind.BluRay, kindOf(*disc, "Harbor.Lights.2024.2160p.UHD.BluRay.HEVC-GRP/Sample/sample.m2ts" to 40_000_000L, "sample.mkv" to 30_000_000L))
        assertEquals(DiscKind.BluRay, kindOf(*disc, "Harbor.Lights.2024.2160p.UHD.BluRay.HEVC-GRP/Extras/Trailer.mkv" to 200_000_000L))
        assertEquals(DiscKind.Image, kindOf("Quiet.Orbit.1999.iso" to 900L, "Quiet.Orbit.1999-sample.mkv" to 10L))
        // Whatever the separator, as the disc's own folders are told (a 7z made on Windows may keep its own): a sample clip
        // named plainly, or a trailer in the extras' folder, is still no film of the release's own.
        val windows = arrayOf("Copper.Crown.2019\\BDMV\\index.bdmv" to 1L, "Copper.Crown.2019\\BDMV\\STREAM\\00001.m2ts" to 40_000_000_000L)
        assertEquals(DiscKind.BluRay, kindOf(*windows, "Copper.Crown.2019\\Sample\\sample.mkv" to 50_000_000L))
        assertEquals(DiscKind.BluRay, kindOf(*windows, "Copper.Crown.2019\\Extras\\Trailer.mkv" to 200_000_000L))
    }

    /**
     * Not a disc copy: a lone stream file with no navigation file and no `BDMV` folder (it is probed, and said as a file the
     * player can't read); a disc beside a video of the release's own, which plays; a folder whose name only holds
     * "BDMV"; nothing of a disc at all.
     */
    @Test
    fun `a lone stream file, a disc beside a video of its own, or a name that only says BDMV is no disc copy`() {
        assertEquals(null, kindOf("Copper.Crown.2019.2160p.BluRay-LONE.m2ts" to 900L))
        assertEquals(null, kindOf("00001.m2ts" to 900L, "00002.m2ts" to 300L, "Copper.Crown.2019.nfo" to 1L))
        assertEquals(null, kindOf(*disc, "Copper.Crown.2019.2160p.BluRay.REMUX.mkv" to 30_000_000_000L))
        assertEquals(null, kindOf("00001.m2ts" to 900L, "index.bdmv" to 1L, "Copper.Crown.2019.mkv" to 100L))
        assertEquals(null, kindOf("Quiet.Orbit.1999.iso" to 900L, "Quiet.Orbit.1999.mkv" to 800L))
        assertEquals(null, kindOf("Harbor.Lights.2024.BDMV-GRP/Harbor.Lights.2024.1080p.mkv" to 900L, "Harbor.Lights.2024.BDMV-GRP/info.nfo" to 1L))
        assertEquals(null, kindOf("Copper.Crown.2019.1080p.WEB-DL.mkv" to 900L, "Copper.Crown.2019.nfo" to 1L))
        assertEquals(null, kindOf())
    }

    @Test
    fun `each kind is said in plain words`() {
        assertEquals(
            listOf(
                "A copy of a Blu-ray disc: only single video files can be played",
                "A copy of a DVD: only single video files can be played",
                "A disc image (ISO): only single video files can be played",
            ),
            DiscKind.entries.map { AttemptFailure.DiscCopy(it).text },
        )
    }
}
