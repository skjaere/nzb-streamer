package io.skjaere.nzbstreamer.play

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A release's files as any source lists them, by path and size: what plays from it is picked as from a prepared NZB of the
 * same files, so a source other than Usenet picks with the same rules. The comparisons with a prepared NZB of the same
 * files are the app's that prepares NZBs: these are a plain listing's halves of them.
 */
class FileListingTest {

    /** A file listed by its path and size alone, as no NZB lists it. */
    private data class PlainFile(override val index: Int, override val path: String, override val size: Long) : ListedFile

    private class PlainListing(override val title: String, files: List<Pair<String, Long>>) : FileListing<PlainFile> {
        override val id = "plain"
        override val files = files.mapIndexed { i, (path, size) -> PlainFile(i, path, size) }
    }

    private fun plain(title: String, vararg files: Pair<String, Long>) = PlainListing(title, files.toList())

    private val film = arrayOf(
        "Quiet.Orbit.2019.1080p.WEB-GRP/Quiet.Orbit.2019.1080p.WEB-GRP.mkv" to 4_000L,
        "Quiet.Orbit.2019.1080p.WEB-GRP/sample.mkv" to 40L,
        "Quiet.Orbit.2019.1080p.WEB-GRP/info.nfo" to 1L,
    )
    private val pack = arrayOf(
        "Harbor.Lights.S01E01.mkv" to 900L, "Harbor.Lights.S01E02.mkv" to 950L, "Harbor.Lights.S01E03.mkv" to 920L, "Harbor.Lights.S01.nfo" to 1L,
    )
    private val unnamed = arrayOf("Show.1_6.mp4" to 400L, "Show.2-6.mp4" to 500L, "Show.3_6.mp4" to 300L, "Show.sample.mp4" to 10L)

    @Test
    fun `an episode is picked from a plain listing as from a prepared NZB`() {
        val listing = plain("Harbor.Lights.S01.1080p.WEB-GRP", *pack)
        assertEquals("Harbor.Lights.S01E02.mkv", FilePicker.Episode(1, 2, testEpisodeNames).pick(listing)?.name)
        assertNull(FilePicker.Episode(1, 4, testEpisodeNames).pick(listing))
        assertEquals("Quiet.Orbit.2019.1080p.WEB-GRP.mkv", FilePicker.Largest.pick(plain("Quiet Orbit", *film))?.name)
        // Where no name tells which video is the episode, they are the choices to ask about, samples among them.
        val choices = FilePicker.Episode(1, 2, testEpisodeNames).choices(plain("Show.S01.1080p.WEB-GRP", *unnamed))
        assertEquals(unnamed.map { it.first }, choices.map { it.path })
    }

    /**
     * A torrent whose video is only in RAR volumes: a service lists the volumes as they are, and streams no file out
     * of them, so it can't be played from there. A video beside volumes plays.
     */
    @Test
    fun `a listing whose video is only in RAR volumes can't be streamed`() {
        val folder = "Copper.Crown.2019.720p.HDTV.x264-PACKED"
        val packed = plain(folder, "$folder/packed.rar" to 500L, "$folder/packed.r00" to 500L, "$folder/packed.r01" to 200L, "$folder/packed.nfo" to 1L)
        assertEquals("Can't be streamed: its video is packed in RAR archives", packed.unstreamable()?.text)
        assertNull(plain("Quiet Orbit", *film).unstreamable())
        assertNull(plain("Quiet Orbit", *film, "Quiet.Orbit.2019.1080p.WEB-GRP/Subs.rar" to 10L).unstreamable())
        // Nothing playable and nothing packed: no video, as a pick says it.
        assertNull(plain("Notes", "notes.nfo" to 1L).unstreamable())
    }

    /**
     * A scene torrent keeps a sample clip beside its RAR volumes, which counts as no video of its own: the sample
     * would play as the film, or as the episode it is named for.
     */
    @Test
    fun `a sample beside the RAR volumes is no video of the release's, which still can't be streamed`() {
        val folder = "Copper.Crown.2019.1080p.BluRay.x264-GRP"
        val volumes = arrayOf("$folder/grp-cc.rar" to 500L, "$folder/grp-cc.r00" to 500L, "$folder/grp-cc.r01" to 500L, "$folder/grp-cc.nfo" to 1L)
        val film = plain(folder, *volumes, "$folder/Sample/grp-cc-sample.mkv" to 60L)
        assertEquals("Can't be streamed: its video is packed in RAR archives", film.unstreamable()?.text)
        val episode = plain("Harbor.Lights.S01E02.1080p.WEB-DL.H.264-GRP", *volumes, "Sample/Harbor.Lights.S01E02.1080p.WEB-DL.H.264-GRP.mkv" to 40L)
        assertEquals("Can't be streamed: its video is packed in RAR archives", episode.unstreamable()?.text)
        // A video of its own beside them plays.
        assertNull(plain(folder, *volumes, "$folder/Sample/grp-cc-sample.mkv" to 60L, "$folder/grp-cc.mkv" to 6_000L).unstreamable())
    }

    /**
     * A disc copy is told from any listing's files alike, a prepared NZB's as a service's, before anything is
     * picked from them: the play then fails at once, its words the disc's kind.
     */
    @Test
    fun `a listing of a disc's files is a disc copy, as a prepared NZB of the same files is`() {
        val root = "Harbor.Lights.2024.2160p.UHD.BluRay.HEVC-GRP"
        val bluRay = arrayOf("$root/BDMV/index.bdmv" to 1L, "$root/BDMV/PLAYLIST/00800.mpls" to 2L, "$root/BDMV/CLIPINF/00001.clpi" to 4L, "$root/BDMV/STREAM/00001.m2ts" to 9_000L)
        val flat = arrayOf("00001.m2ts" to 9_000L, "00800.mpls" to 2L, "index.bdmv" to 1L)
        val dvd = arrayOf("VIDEO_TS/VTS_01_1.VOB" to 9_000L, "VIDEO_TS/VTS_01_0.IFO" to 4L, "VIDEO_TS/VTS_01_0.BUP" to 4L)
        val image = arrayOf("Quiet.Orbit.1999.iso" to 9_000L)
        // A 7z made on Windows may keep its own separator; a plainly named sample clip beside the disc plays nothing instead.
        val windows = arrayOf("Copper.Crown.2019\\BDMV\\index.bdmv" to 1L, "Copper.Crown.2019\\BDMV\\STREAM\\00001.m2ts" to 9_000L, "Copper.Crown.2019\\Sample\\sample.mkv" to 50L)
        listOf(bluRay to DiscKind.BluRay, flat to DiscKind.BluRay, dvd to DiscKind.Dvd, image to DiscKind.Image, windows to DiscKind.BluRay).forEach { (files, kind) ->
            assertEquals(AttemptFailure.DiscCopy(kind), plain("Release", *files).discCopy(), files.first().first)
        }
        listOf(film, pack, unnamed, arrayOf("Copper.Crown.2019.2160p.BluRay-LONE.m2ts" to 9_000L), arrayOf(*bluRay, "Copper.Crown.2019.mkv" to 10L)).forEach { files ->
            assertNull(plain("Release", *files).discCopy(), files.first().first)
        }
    }

    /**
     * Inside a disc's `BDMV` folder nothing is a video: a disc beside a video of the release's own plays that video,
     * however much larger the disc's stream is, and the pickers and the chooser never offer the stream.
     */
    @Test
    fun `a disc beside a video of the release's own plays the video, however large the disc's stream`() {
        val root = "Copper.Crown.2019.2160p.BluRay-GRP"
        val files = arrayOf(
            "$root/BDMV/index.bdmv" to 1L, "$root/BDMV/PLAYLIST/00800.mpls" to 2L, "$root/BDMV/STREAM/00001.m2ts" to 60_000L,
            "$root/BDMV/STREAM/00002.m2ts" to 50_000L, "$root/Copper.Crown.2019.2160p.BluRay.REMUX.mkv" to 30_000L,
        )
        listOf(plain(root, *files)).forEach { listing ->
            assertEquals("Copper.Crown.2019.2160p.BluRay.REMUX.mkv", listing.defaultPick?.name)
            assertEquals("Copper.Crown.2019.2160p.BluRay.REMUX.mkv", FilePicker.Largest.pick(listing)?.name)
            assertTrue(listing.oneVideo)
            assertEquals(listOf(false, false, false, false, true), listing.files.map { it.isVideo })
        }
        // A pack of the season's episodes beside its disc's: the episode is the one named for it, and nothing is asked.
        val pack = plain(
            "Harbor.Lights.S01.1080p.BluRay-GRP",
            "Harbor.Lights.S01.1080p.BluRay-GRP/BDMV/STREAM/00001.m2ts" to 9_000L, "Harbor.Lights.S01.1080p.BluRay-GRP/BDMV/STREAM/00002.m2ts" to 9_000L,
            "Harbor.Lights.S01.1080p.BluRay-GRP/Harbor.Lights.S01E01.mkv" to 900L, "Harbor.Lights.S01.1080p.BluRay-GRP/Harbor.Lights.S01E02.mkv" to 900L,
        )
        assertEquals("Harbor.Lights.S01E02.mkv", FilePicker.Episode(1, 2, testEpisodeNames).pick(pack)?.name)
        // Where no name tells, the user is asked among the release's own videos alone.
        val unnamedPack = plain("Show.S01.1080p.BluRay-GRP", "Show.S01/BDMV/STREAM/00001.m2ts" to 9_000L, "Show.S01/Show.1_6.mp4" to 400L, "Show.S01/Show.2-6.mp4" to 500L)
        assertEquals(listOf("Show.1_6.mp4", "Show.2-6.mp4"), FilePicker.Episode(1, 2, testEpisodeNames).choices(unnamedPack).map { it.name })
    }
}
