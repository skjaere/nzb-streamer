package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * prepare fetches the first article of only the files it needs to open a release (its archive
 * volumes, or its videos and other content when it has no archive), and a file it doesn't need
 * (a PAR2 file, an NFO, an SFV, a sample, another episode's files) that lacks its first article
 * doesn't fail the release. A needed file that lacks it still does.
 */
class PrepareNeededFilesTest {
    private val articles = mutableMapOf<String, ByteArray>()
    private val files = mutableListOf<String>()
    private val missing = mutableSetOf<String>()
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    /** Posts [data] as [name], in articles of [partSize], and returns their ids (in brackets). */
    private fun post(name: String, data: ByteArray, partSize: Int = 32 * 1024): List<String> {
        val parts = YencEncoder.encodeMultiPart(data, name, partSize)
        val ids = parts.indices.map { "<$name-${it + 1}@test>" }
        ids.zip(parts) { id, part -> articles[id] = part.data }
        val segments = ids.withIndex().joinToString("") { (i, id) ->
            """<segment bytes="${parts[i].data.size}" number="${i + 1}">${id.removeSurrounding("<", ">")}</segment>"""
        }
        files += """<file poster="t" date="0" subject="&quot;$name&quot; yEnc (1/${parts.size})">""" +
            """<groups><group>alt.binaries.test</group></groups><segments>$segments</segments></file>"""
        return ids
    }

    /** Posts [payload] packed as [inner] in a RAR5 set of volumes named [base].partN.rar. */
    private fun postRarSet(base: String, inner: String, payload: ByteArray) {
        ArchiveGenerator.generate(payload, 64L * 1024, ContainerType.RAR5, inner)
            .forEachIndexed { v, volume -> post("$base.part${v + 1}.rar", volume.data) }
    }

    private fun firstArticle(name: String) = "<$name-1@test>"

    private fun nzb() = ("""<?xml version="1.0" encoding="UTF-8"?>""" +
        """<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""").toByteArray()

    private fun <T> withStreamer(block: suspend (NzbStreamer, FakeNntpServer) -> T): T {
        val server = FakeNntpServer(body = { id -> articles[id].takeUnless { id in missing } }).also { servers += it }
        return NzbStreamer.fromConfig(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4))
            .use { runBlocking { block(it, server) } }
    }

    private fun bytes(name: String, size: Int) = Random(name.hashCode()).nextBytes(size)

    /** A PAR2 index (FileDesc packets) of about [size] bytes, as a poster's `.par2` file. */
    private fun par2Index(size: Int): ByteArray {
        val packet = buildPar2IndexFile("Movie.2020.mkv", 200_000)
        return ByteArray(0).let { var all = it; while (all.size < size) all += packet; all }
    }

    @Test
    fun `a file the release doesn't need that lacks its first article doesn't fail it`() {
        val payload = bytes("movie", 200_000)
        postRarSet("Movie.2020", "Movie.2020.mkv", payload)
        listOf("Movie.2020.par2", "Movie.2020.vol00+01.par2", "Movie.2020.nfo", "Movie.2020.sfv", "Movie.2020-sample.mkv")
            .forEach { name -> missing += post(name, bytes(name, 1_000)).first() }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            val movie = streamer.resolveStreamableFiles(prepared.metadata).single()
            assertEquals("Movie.2020.mkv", movie.path)
            var streamed = ByteArray(0)
            streamer.streamFile(prepared.metadata, movie) { streamed = it.toByteArray() }
            assertContentEquals(payload, streamed)
        }
    }

    @Test
    fun `prepare fetches the first article of only the files it needs to open the release`() {
        postRarSet("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        post("Movie.2020.par2", par2Index(2_000))
        val unneeded = listOf("Movie.2020.vol00+01.par2", "Movie.2020.vol01+02.par2", "Movie.2020.nfo", "Movie.2020.sfv", "Movie.2020-sample.mkv")
        unneeded.forEach { name -> post(name, bytes(name, 1_000)) }

        withStreamer { streamer, server ->
            assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(emptyList(), unneeded.map(::firstArticle).filter { it in server.bodiesRead }, "fetched though not needed")
            assertTrue(firstArticle("Movie.2020.par2") in server.bodiesRead, "the PAR2 index wasn't fetched")
        }
    }

    @Test
    fun `an archive volume that lacks its first article still fails the release`() {
        postRarSet("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        missing += firstArticle("Movie.2020.part2.rar")

        withStreamer { streamer, _ ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
        }
    }

    @Test
    fun `another episode's video that lacks its first article doesn't fail a season pack`() {
        val episodes = (1..3).associate { e -> "Show.S01E0$e.mkv" to bytes("episode $e", 50_000) }
        episodes.forEach { (name, data) -> post(name, data) }
        missing += firstArticle("Show.S01E02.mkv")
        missing += post("Show.S01E02.srt", bytes("subtitles", 1_000)).first()
        post("Show.S01E01.srt", bytes("subtitles 1", 1_000))

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            val listed = streamer.resolveStreamableFiles(prepared.metadata)
            assertEquals(listOf("Show.S01E01.mkv", "Show.S01E03.mkv", "Show.S01E01.srt"), listed.map { it.path })
            var streamed = ByteArray(0)
            streamer.streamFile(prepared.metadata, listed.single { it.path == "Show.S01E03.mkv" }) { streamed = it.toByteArray() }
            assertContentEquals(episodes.getValue("Show.S01E03.mkv"), streamed)
        }
    }

    @Test
    fun `another episode's RAR set that lacks a first article doesn't fail a season pack`() {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) -> postRarSet(base, "$base.mkv", data) }
        missing += firstArticle("Show.S01E02.part2.rar")

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            val listed = streamer.resolveStreamableFiles(prepared.metadata)
            assertEquals(listOf("Show.S01E01.mkv", "Show.S01E03.mkv"), listed.map { it.path })
            var streamed = ByteArray(0)
            streamer.streamFile(prepared.metadata, listed.single { it.path == "Show.S01E03.mkv" }) { streamed = it.toByteArray() }
            assertContentEquals(episodes.getValue("Show.S01E03"), streamed)
        }
    }

    /** Not a subtitle archive in place of the movie: what stands in for a set must be of its size. */
    @Test
    fun `a small archive set doesn't open a release whose main set lacks a first article`() {
        postRarSet("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        post("Movie.2020.subs.rar", ArchiveGenerator.generate(bytes("subs", 5_000), 1, ContainerType.RAR5, "Movie.2020.srt").single().data)
        missing += firstArticle("Movie.2020.part2.rar")

        withStreamer { streamer, _ ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
        }
    }

    @Test
    fun `a trailer doesn't open a release whose movie lacks its first article`() {
        missing += post("Movie.2020.mkv", bytes("movie", 200_000)).first()
        post("Movie.2020.Trailer.mkv", bytes("trailer", 20_000))

        withStreamer { streamer, _ ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
        }
    }

    @Test
    fun `the only video that lacks its first article still fails the release`() {
        missing += post("Movie.2020.mkv", bytes("movie", 50_000)).first()
        post("Movie.2020.srt", bytes("subtitles", 1_000))

        withStreamer { streamer, _ ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
        }
    }

    @Test
    fun `a file of a release without video that lacks its first article still fails it`() {
        post("Album.01.flac", bytes("track 1", 20_000))
        missing += post("Album.02.flac", bytes("track 2", 20_000)).first()

        withStreamer { streamer, _ ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
        }
    }

    @Test
    fun `a PAR2 index that lacks a later article doesn't fail the release`() {
        postRarSet("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        missing += post("Movie.2020.par2", par2Index(48 * 1024))[1]

        withStreamer { streamer, server ->
            assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertTrue("<Movie.2020.par2-2@test>" in server.bodiesRead, "the PAR2 index wasn't downloaded")
        }
    }
}
