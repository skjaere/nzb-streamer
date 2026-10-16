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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * prepare fetches the first article of only the files it needs to open a release (its archive
 * volumes, or its videos and other content when it has no archive), and a file it doesn't need
 * (a PAR2 file, an NFO, an SFV, a sample, another episode's files) that lacks its first article
 * doesn't fail the release. A needed file that lacks it still does. A volume posted under another
 * extension is still one of its archive's, and an archive that lacks one doesn't open (N52).
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
        volumes(base, inner, payload).forEach { (name, data) -> post(name, data) }
    }

    /**
     * The volumes of [payload] packed as [inner] in an archive of [type], named as its set [base]
     * is: [base].partN.rar (RAR5), [base].rar, .r00, .r01, ... (RAR4) or [base].7z.001, ... (7z).
     */
    private fun volumes(base: String, inner: String, payload: ByteArray, type: ContainerType = ContainerType.RAR5) =
        ArchiveGenerator.generate(payload, 64L * 1024, type, inner).mapIndexed { v, volume ->
            when (type) {
                ContainerType.RAR5 -> "$base.part${v + 1}.rar"
                ContainerType.RAR4 -> if (v == 0) "$base.rar" else "$base.r%02d".format(v - 1)
                ContainerType.SEVENZIP -> "$base.7z.%03d".format(v + 1)
            } to volume.data
        }

    /** The bytes of [file], or of the release's only file, streamed whole. */
    private suspend fun NzbStreamer.streamed(prepared: PrepareResult.Success, file: String? = null): ByteArray {
        val listed = resolveStreamableFiles(prepared.metadata)
        var streamed = ByteArray(0)
        streamFile(prepared.metadata, file?.let { path -> listed.single { it.path == path } } ?: listed.single()) {
            streamed = it.toByteArray()
        }
        return streamed
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

    /**
     * Volumes obfuscated one by one ("a8f7d6e5.rar", "b91c2d3e.rar", ...) don't group into a set,
     * yet are one archive: none stands in for another (library issue N36).
     */
    @ParameterizedTest
    @ValueSource(strings = ["rar", "7z"])
    fun `a volume of an archive obfuscated volume by volume that lacks its first article still fails the release`(extension: String) {
        val type = if (extension == "rar") ContainerType.RAR5 else ContainerType.SEVENZIP
        val volumes = ArchiveGenerator.generate(bytes("movie", 200_000), 64L * 1024, type, "Movie.2020.mkv")
        val names = volumes.indices.map { v -> "${OBFUSCATED[v]}.$extension" }
        volumes.forEachIndexed { v, volume -> post(names[v], volume.data) }
        missing += firstArticle(names[1])

        withStreamer { streamer, _ ->
            val result = streamer.prepare(nzb())
            assertIs<PrepareResult.MissingArticles>(result, "prepare: $result")
        }
    }

    /**
     * A release without a PAR2 index names its obfuscated volumes from a recovery volume: prepare
     * fetches the smallest (library issue N36).
     */
    @Test
    fun `a recovery volume names obfuscated volumes when the release has no PAR2 index`() {
        val payload = bytes("movie", 200_000)
        val volumes = ArchiveGenerator.generate(payload, 64L * 1024, ContainerType.RAR5, "Movie.2020.mkv")
        val named = volumes.mapIndexed { v, volume -> "Movie.2020.part${v + 1}.rar" to volume.data }
        // Posted under names that tell nothing, in another order than the archive's.
        volumes.indices.reversed().forEach { v -> post(OBFUSCATED[v], volumes[v].data) }
        post("0b1c2d3e.vol01+02.par2", buildPar2File(named, recoveryBlock = 8 * 1024))
        post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 4 * 1024))

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            val movie = streamer.resolveStreamableFiles(prepared.metadata).single()
            var streamed = ByteArray(0)
            streamer.streamFile(prepared.metadata, movie) { streamed = it.toByteArray() }
            assertContentEquals(payload, streamed)
            assertTrue(firstArticle("0b1c2d3e.vol01+02.par2") !in server.bodiesRead, "the larger recovery volume was fetched")
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

    /**
     * A volume posted under another extension than its archive's is one of its volumes all the
     * same: the PAR2 index names it by its first 16 KB or, without one, it starts with the archive's
     * signature and its name but for that extension gives its place (library issue N52).
     */
    @ParameterizedTest
    @CsvSource(
        "RAR5, Movie.2020.part2.bin, true", "RAR5, Movie.2020.part2.bin, false",
        "RAR5, Movie.2020.part2.avi, true", "RAR5, Movie.2020.part2.avi, false",
        "RAR5, x7f3k9q2.avi, true", "RAR4, Movie.2020.r00.bin, true", "RAR4, Movie.2020.r00.bin, false",
        "RAR5, Movie.2020.part2, false", "SEVENZIP, Movie.2020.7z.002.bin, true",
    )
    fun `a volume posted under another extension opens with its archive`(type: ContainerType, postedAs: String, withIndex: Boolean) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, type)
        named.forEachIndexed { v, (name, data) -> post(if (v == 1) postedAs else name, data) }
        if (withIndex) post("Movie.2020.par2", buildPar2File(named))
        post("Movie.2020.nfo", bytes("nfo", 1_000))

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /** Without a PAR2 index nothing tells where such a volume goes: the release isn't opened in a guessed order (N52). */
    @Test
    fun `a volume posted under a name that doesn't give its place fails a release without a PAR2 index`() {
        volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
            .forEachIndexed { v, (name, data) -> post(if (v == 1) "x7f3k9q2.avi" else name, data) }

        withStreamer { streamer, _ ->
            val result = assertIs<PrepareResult.Failure>(streamer.prepare(nzb()), "prepare")
            assertTrue("x7f3k9q2.avi" in result.message, result.message)
        }
    }

    /**
     * A 7z set's volume past the first has no signature: without a PAR2 index its content can't
     * tell it is one. The set lacks it, and its end header lies past the end of what it holds, so
     * the release fails rather than opening without it (N52).
     */
    @Test
    fun `a 7z volume posted under another extension fails a release without a PAR2 index`() {
        volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000), ContainerType.SEVENZIP)
            .forEachIndexed { v, (name, data) -> post(if (v == 1) "Movie.2020.7z.002.bin" else name, data) }

        withStreamer { streamer, _ ->
            assertIs<PrepareResult.Failure>(streamer.prepare(nzb()), "prepare")
        }
    }

    /**
     * A volume the PAR2 index names that the release doesn't hold, or holds under a name prepare
     * doesn't fetch (an NFO's): the archive can't open whole (N52).
     */
    @ParameterizedTest
    @ValueSource(strings = ["", "Movie.2020.part2.nfo"])
    fun `an archive volume the PAR2 index names that the release doesn't hold fails it`(postedAs: String) {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        named.forEachIndexed { v, (name, data) ->
            if (v != 1) post(name, data) else if (postedAs.isNotEmpty()) post(postedAs, data)
        }
        post("Movie.2020.par2", buildPar2File(named))

        withStreamer { streamer, server ->
            val result = assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
            assertTrue("Movie.2020.part2.rar" in result.message, result.message)
            assertTrue(server.bodiesRead.none { "nfo" in it }, "an NFO was fetched")
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `a volume posted under another extension that lacks its first article fails the release`(withIndex: Boolean) {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        named.forEachIndexed { v, (name, data) -> if (v == 1) missing += post("Movie.2020.part2.bin", data).first() else post(name, data) }
        if (withIndex) post("Movie.2020.par2", buildPar2File(named))

        withStreamer { streamer, _ ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
        }
    }

    /** Each episode has its PAR2 index: the second's names the volume of its set posted under another extension (N52). */
    @Test
    fun `a season pack opens an episode whose volume is posted under another extension`() {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) ->
            val named = volumes(base, "$base.mkv", data)
            named.forEach { (name, volume) -> post(if (name == "Show.S01E02.part2.rar") "Show.S01E02.part2.bin" else name, volume) }
            post("$base.par2", buildPar2File(named))
        }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            episodes.forEach { (base, data) -> assertContentEquals(data, streamer.streamed(prepared, "$base.mkv"), base) }
        }
    }

    /** As another episode's set that lacks a first article, one that lacks a volume its PAR2 index names is left out (N52). */
    @Test
    fun `a season pack leaves out an episode that lacks a volume its PAR2 index names`() {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) ->
            val named = volumes(base, "$base.mkv", data)
            named.forEach { (name, volume) -> if (name != "Show.S01E02.part2.rar") post(name, volume) }
            post("$base.par2", buildPar2File(named))
        }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(listOf("Show.S01E01.mkv", "Show.S01E03.mkv"), streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            assertContentEquals(episodes.getValue("Show.S01E03"), streamer.streamed(prepared, "Show.S01E03.mkv"))
        }
    }

    /**
     * Guards for N52: ordinary releases open as before, and prepare fetches no file beside their
     * volumes when their PAR2 index names every volume. Without one, a video or other file beside
     * the archive is fetched to tell by its content that it isn't a volume.
     */
    @ParameterizedTest
    @CsvSource("RAR5, true", "RAR5, false", "RAR4, true", "RAR4, false", "SEVENZIP, true", "SEVENZIP, false")
    fun `an ordinary archive release opens as before`(type: ContainerType, withIndex: Boolean) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, type)
        named.forEach { (name, data) -> post(name, data) }
        if (withIndex) post("Movie.2020.par2", buildPar2File(named))
        // Without an index, the recovery volume is fetched in its place (N36).
        val unneeded = listOf("Movie.2020.nfo", "Movie.2020.sfv", "Movie.2020-sample.mkv") +
            listOfNotNull("Movie.2020.vol00+01.par2".takeIf { withIndex })
        (unneeded + "Movie.2020.vol00+01.par2").distinct().forEach { name -> post(name, bytes(name, 1_000)) }
        val beside = listOf("Movie.2020.Proof.jpg", "Movie.2020.Featurette.mkv")
        beside.forEach { name -> post(name, bytes(name, 20_000)) }

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
            assertEquals(emptyList(), unneeded.map(::firstArticle).filter { it in server.bodiesRead }, "fetched though not needed")
            assertEquals(!withIndex, beside.map(::firstArticle).all { it in server.bodiesRead }, "files beside the archive fetched")
        }
    }

    /** Guard for N52: volumes obfuscated one by one, named by the PAR2 index by their first 16 KB, open as before. */
    @ParameterizedTest
    @ValueSource(strings = ["", ".rar"])
    fun `volumes obfuscated one by one open with a PAR2 index`(extension: String) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.forEachIndexed { v, (_, data) -> post("${OBFUSCATED[v]}$extension", data) }
        post("0b1c2d3e.par2", buildPar2File(named))

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /**
     * A recovery volume's file descriptions often follow its recovery block, past its first 16 KB:
     * it is read as it streams, its block skipped, until they are (library issue N53).
     */
    @Test
    fun `a recovery volume whose file descriptions follow its recovery block names obfuscated volumes`() {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 64 * 1024))

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /** A PAR2 index that lacks an article names nothing: the smallest recovery volume does in its place (N53). */
    @ParameterizedTest
    @ValueSource(ints = [0, 1])
    fun `a recovery volume names obfuscated volumes when the PAR2 index lacks an article`(lacking: Int) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        missing += post("0b1c2d3e.par2", buildPar2File(named), partSize = 256)[lacking]
        post("0b1c2d3e.vol01+02.par2", buildPar2File(named, recoveryBlock = 8 * 1024))
        post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 4 * 1024))

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
            assertTrue(firstArticle("0b1c2d3e.vol01+02.par2") !in server.bodiesRead, "the larger recovery volume was fetched")
        }
    }

    /**
     * A recovery volume that names its files before its recovery block is read no further than
     * them: its block, which can be tens of MB, is neither held nor downloaded (N53).
     */
    @Test
    fun `a recovery volume is read no further than its file descriptions`() {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        val ids = post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 1024 * 1024, descriptionsFirst = true))

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            val read = ids.count { it in server.bodiesRead }
            assertTrue(read <= 4, "$read of the recovery volume's ${ids.size} articles were downloaded")
        }
    }

    /**
     * Guard for N53: a recovery volume whose block, ahead of its file descriptions, is bigger than
     * prepare reads of it is left after its first article.
     */
    @Test
    fun `a recovery volume too big to read to its file descriptions is left after its first article`() {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        named.indices.forEach { v -> post(OBFUSCATED[v], named[v].second) }
        val ids = post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 17 * 1024 * 1024), partSize = 512 * 1024)

        withStreamer { streamer, server ->
            assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(listOf(ids.first()), ids.filter { it in server.bodiesRead }, "articles of the recovery volume downloaded")
        }
    }

    /**
     * A recovery volume posted under a name that tells nothing is fetched (it may be a volume) and,
     * naming its files first, passes for an index: it too is read no further than its file
     * descriptions (N53).
     */
    @Test
    fun `an obfuscated recovery volume is read no further than its file descriptions`() {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.forEach { (name, data) -> post(name, data) }
        val ids = post("f1e2d3c4b5a6f7e8d9c0b1a2f3e4d5c6", buildPar2File(named, recoveryBlock = 1024 * 1024, descriptionsFirst = true))

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertContentEquals(payload, streamer.streamed(prepared))
            val read = ids.count { it in server.bodiesRead }
            assertTrue(read <= 4, "$read of the recovery volume's ${ids.size} articles were downloaded")
        }
    }

    /**
     * Whether a PAR2 file names files is told from its first 16 KB without taking a packet's length
     * on trust: a recovery block's is its size, which can be tens of MB, here 2 GB (N53).
     */
    @Test
    fun `a PAR2 packet's length isn't allocated before it is read`() {
        val payload = bytes("movie", 200_000)
        volumes("Movie.2020", "Movie.2020.mkv", payload).forEach { (name, data) -> post(name, data) }
        val header = java.nio.ByteBuffer.allocate(64).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .put("PAR2".toByteArray()).put(0).put("PKT".toByteArray()).putLong(Int.MAX_VALUE - 64L)
            .put(ByteArray(32)).put("PAR 2.0".toByteArray()).put(0).put("RecvSlic".toByteArray()).array()
        post("f1e2d3c4b5a6f7e8d9c0b1a2f3e4d5c6", header + bytes("block", 20_000))

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    private companion object {
        /** Names that look like an obfuscating poster's, one per volume. */
        val OBFUSCATED = listOf("a8f7d6e5", "b91c2d3e", "c07e4f1a", "d2b5a9c4", "e6f3c8b1", "f4a1d7e9")
    }
}
