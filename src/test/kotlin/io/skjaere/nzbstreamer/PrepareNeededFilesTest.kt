package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.compressionutils.MalformedSevenZipArchiveException
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.ArchiveVolumeNotInNzbException
import io.skjaere.nzbstreamer.enrichment.UnplacedArchiveVolumeException
import io.skjaere.nzbstreamer.enrichment.firstCauseOfType
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
    // Articles whose answer the connection drops in (after 100 bytes), every time one is asked for.
    private val dropped = mutableSetOf<String>()
    // Articles whose answer waits until another, by id, has been asked for (and a moment more).
    private val answerAfter = mutableMapOf<String, String>()
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    /**
     * Posts [data] as [name], in articles of [partSize], and returns their ids (in brackets). Its
     * NZB subject names it [subject]: an indexer may rewrite subjects, the yEnc names left as posted.
     */
    private fun post(name: String, data: ByteArray, partSize: Int = 32 * 1024, subject: String = name): List<String> {
        val parts = YencEncoder.encodeMultiPart(data, name, partSize)
        val ids = parts.indices.map { "<$name-${it + 1}@test>" }
        ids.zip(parts) { id, part -> articles[id] = part.data }
        val segments = ids.withIndex().joinToString("") { (i, id) ->
            """<segment bytes="${parts[i].data.size}" number="${i + 1}">${id.removeSurrounding("<", ">")}</segment>"""
        }
        files += """<file poster="t" date="0" subject="&quot;$subject&quot; yEnc (1/${parts.size})">""" +
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
        lateinit var server: FakeNntpServer
        server = FakeNntpServer(
            body = { id ->
                answerAfter[id]?.let { other ->
                    val until = System.nanoTime() + 5_000_000_000L
                    while (other !in server.bodiesRead && System.nanoTime() < until) Thread.sleep(10)
                    Thread.sleep(200)
                }
                articles[id].takeUnless { id in missing }
            },
            dropAfter = { id -> 100.takeIf { id in dropped } },
        ).also { servers += it }
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

    /** Each episode's RAR4 set (.rar, .r00, ...) is opened from its own volumes, none from another's (NR-2). */
    @Test
    fun `a RAR4 season pack lists every episode and streams each whole`() {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) -> volumes(base, "$base.mkv", data, ContainerType.RAR4).forEach { (name, volume) -> post(name, volume) } }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(episodes.keys.map { "$it.mkv" }, streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            episodes.forEach { (base, data) -> assertContentEquals(data, streamer.streamed(prepared, "$base.mkv"), base) }
        }
    }

    /** A subtitle archive posted after the film's RAR4 set is no volume of it (NR-2). */
    @Test
    fun `a RAR4 film with a subtitle archive posted after it streams whole`() {
        val payload = bytes("movie", 200_000)
        volumes("Movie.2020", "Movie.2020.mkv", payload, ContainerType.RAR4).forEach { (name, data) -> post(name, data) }
        post("Movie.2020.subs.rar", ArchiveGenerator.generate(bytes("subs", 5_000), 1, ContainerType.RAR4, "Movie.2020.srt").single().data)

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertContentEquals(payload, streamer.streamed(prepared, "Movie.2020.mkv"))
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

    /**
     * Without a PAR2 index, a file posted under another extension that starts with an archive's
     * signature joins the release's archive only when its name places it in a set the release
     * opens: the volume of another archive (here the first of an extra's two, the second not
     * posted) is none of the film's, which opens without it (NR-14).
     */
    @ParameterizedTest
    @ValueSource(strings = ["RAR4", "RAR5"])
    fun `an archive of another set posted under another extension doesn't join the release's`(type: ContainerType) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, type)
        named.forEach { (name, data) -> post(name, data) }
        post("Extras.part1.bin", ArchiveGenerator.generate(bytes("extras", 100_000), 64L * 1024, ContainerType.RAR5, "Extras.mkv").first().data)

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /**
     * The volumes are ordered by the names their yEnc headers give, which an indexer that rewrites
     * subjects (either way) leaves as posted: without a PAR2 index, a volume posted under another
     * extension joins the set those names make, by its own yEnc name, whatever the subjects say (NR-14).
     */
    @ParameterizedTest
    @CsvSource("RAR5, Movie.2020, abc123", "RAR4, Movie.2020, abc123", "RAR5, abc123, Movie.2020", "RAR4, abc123, Movie.2020")
    fun `a volume posted under another extension joins its set by its yEnc name when subjects are rewritten`(
        type: ContainerType,
        subjects: String,
        posted: String,
    ) {
        val payload = bytes("movie", 200_000)
        val named = volumes(posted, "Movie.2020.mkv", payload, type)
        named.forEachIndexed { v, (name, data) ->
            val subject = subjects + name.removePrefix(posted)
            if (v == 1) post("$name.bin", data, subject = "$subject.bin") else post(name, data, subject = subject)
        }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /**
     * Beside volumes obfuscated one by one, with no PAR2 data, a volume posted under another
     * extension whose name places it in a set of its own may be one of theirs, under a name of its
     * own as theirs are: nothing tells where it goes, so the release fails rather than open
     * without it (NR-14).
     */
    @ParameterizedTest
    @CsvSource("RAR5, x7f3k9q2.part2.avi", "RAR4, x7f3k9q2.r00.bin")
    fun `a volume under a name of its own beside volumes obfuscated one by one fails a release without PAR2 data`(
        type: ContainerType,
        postedAs: String,
    ) {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000), type)
        named.forEachIndexed { v, (_, data) -> post(if (v == 1) postedAs else "${OBFUSCATED[v]}.rar", data) }

        withStreamer { streamer, _ ->
            val result = streamer.prepare(nzb())
            val failure = assertIs<PrepareResult.Failure>(result, "prepare: $result")
            assertEquals(postedAs, assertIs<UnplacedArchiveVolumeException>(failure.cause).volume)
        }
    }

    /**
     * A season pack leaves out an episode whose set lacks a volume, the volume found posted under
     * another extension with it, also when that one was found by its yEnc name, which the
     * subjects don't give (NR-14).
     */
    @Test
    fun `a season pack leaves out an episode that lacks a volume, with one found by its yEnc name`() {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 200_000) }
        episodes.forEach { (base, data) ->
            val posted = if (base == "Show.S01E02") "abc123" else base
            volumes(posted, "$base.mkv", data).forEachIndexed { v, (name, volume) ->
                val subject = base + name.removePrefix(posted)
                when {
                    posted == base -> post(name, volume)
                    v == 1 -> post("$name.bin", volume, subject = "$subject.bin")
                    v != 2 -> post(name, volume, subject = subject)
                }
            }
        }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(listOf("Show.S01E01.mkv", "Show.S01E03.mkv"), streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            assertContentEquals(episodes.getValue("Show.S01E03"), streamer.streamed(prepared, "Show.S01E03.mkv"))
        }
    }

    /**
     * Without a PAR2 index nothing tells where such a volume goes: the release isn't opened in a
     * guessed order (N52). It is broken as posted, which its cause's type says (NR-18).
     */
    @Test
    fun `a volume posted under a name that doesn't give its place fails a release without a PAR2 index`() {
        volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
            .forEachIndexed { v, (name, data) -> post(if (v == 1) "x7f3k9q2.avi" else name, data) }

        withStreamer { streamer, _ ->
            val result = assertIs<PrepareResult.Failure>(streamer.prepare(nzb()), "prepare")
            assertTrue("x7f3k9q2.avi" in result.message, result.message)
            assertEquals("x7f3k9q2.avi", assertIs<UnplacedArchiveVolumeException>(result.cause).volume)
        }
    }

    /**
     * A 7z set's volume past the first has no signature: without a PAR2 index its content can't
     * tell it is one. The set lacks it, and its end header lies past the end of what it holds, so
     * the release fails rather than opening without it (N52), as a malformed archive (NR-20).
     */
    @Test
    fun `a 7z volume posted under another extension fails a release without a PAR2 index`() {
        volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000), ContainerType.SEVENZIP)
            .forEachIndexed { v, (name, data) -> post(if (v == 1) "Movie.2020.7z.002.bin" else name, data) }

        withStreamer { streamer, _ ->
            val result = assertIs<PrepareResult.Failure>(streamer.prepare(nzb()), "prepare")
            assertTrue(result.cause.firstCauseOfType<MalformedSevenZipArchiveException>() != null, "${result.cause}")
        }
    }

    /**
     * A volume the PAR2 index names that the release doesn't hold, or holds under a name prepare
     * doesn't fetch (an NFO's): the archive can't open whole (N52). No provider can supply it, which
     * its cause's type says (NR-18).
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
            assertIs<ArchiveVolumeNotInNzbException>(result.cause)
            assertTrue(server.bodiesRead.none { "nfo" in it }, "an NFO was fetched")
        }
    }

    /** A provider may have its first article: it isn't a volume the NZB doesn't hold (NR-18). */
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `a volume posted under another extension that lacks its first article fails the release`(withIndex: Boolean) {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        named.forEachIndexed { v, (name, data) -> if (v == 1) missing += post("Movie.2020.part2.bin", data).first() else post(name, data) }
        if (withIndex) post("Movie.2020.par2", buildPar2File(named))

        withStreamer { streamer, _ ->
            val result = assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
            assertEquals(null, result.cause.firstCauseOfType<ArchiveVolumeNotInNzbException>(), "${result.cause}")
        }
    }

    /**
     * Without PAR2 data the files beside the archive are fetched to tell by their content whether
     * one is a volume posted under another extension. One that lacks its first article fails the
     * release only when it may be a volume: a set's volumes but its last are as big as its biggest,
     * so a proof picture, a subtitle or a featurette bigger than any volume can't be one (NR-3).
     */
    @ParameterizedTest
    @ValueSource(strings = ["Movie.2020.Proof.jpg", "Movie.2020.eng.srt", "Movie.2020.Featurette.mkv"])
    fun `a file beside the archive that can't be a volume and lacks its first article doesn't fail the release`(beside: String) {
        val payload = bytes("movie", 200_000)
        volumes("Movie.2020", "Movie.2020.mkv", payload).forEach { (name, data) -> post(name, data) }
        missing += post(beside, bytes(beside, if (beside.endsWith(".mkv")) 300_000 else 20_000)).first()
        post("Movie.2020.nfo", bytes("nfo", 1_000))

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertContentEquals(payload, streamer.streamed(prepared))
            assertTrue(firstArticle(beside) in server.bodiesRead, "the file beside the archive wasn't fetched")
        }
    }

    /**
     * In a season pack whose sets open on their own, a file beside them that lacks its first
     * article and may be a volume leaves out only the set its name places it in, and one that can't
     * be a volume nothing; one that may be a volume whose name places it nowhere fails the pack (NR-16).
     */
    @ParameterizedTest
    @CsvSource(
        "RAR5, Show.S01E02.part2.bin, Show.S01E01.mkv|Show.S01E03.mkv", "RAR4, Show.S01E02.r00.bin, Show.S01E01.mkv|Show.S01E03.mkv",
        "RAR5, Show.S01E02.eng.srt, Show.S01E01.mkv|Show.S01E02.mkv|Show.S01E03.mkv", "RAR5, x7f3k9q2.avi, ",
    )
    fun `a season pack leaves out only the set a lacking file beside it may be a volume of`(type: ContainerType, beside: String, listed: String?) {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) ->
            volumes(base, "$base.mkv", data, type).forEachIndexed { v, (name, volume) ->
                if (base == "Show.S01E02" && v == 1 && !beside.endsWith(".srt")) missing += post(beside, volume).first() else post(name, volume)
            }
        }
        if (beside.endsWith(".srt")) missing += post(beside, bytes(beside, 20_000)).first()

        withStreamer { streamer, _ ->
            val result = streamer.prepare(nzb())
            if (listed == null) {
                assertIs<PrepareResult.MissingArticles>(result, "prepare")
            } else {
                val prepared = assertIs<PrepareResult.Success>(result, "prepare")
                assertEquals(listed.split("|"), streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
                assertContentEquals(episodes.getValue("Show.S01E03"), streamer.streamed(prepared, "Show.S01E03.mkv"))
            }
        }
    }

    /**
     * Each episode has its PAR2 index: the second's names the volume of its set posted under another
     * extension (N52). An index made over the season's folder names each file with its path in it,
     * matched by the name it ends in (NR-11).
     */
    @ParameterizedTest
    @ValueSource(strings = ["", "Show.S01/", "Show.S01\\"])
    fun `a season pack opens an episode whose volume is posted under another extension`(folder: String) {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) ->
            val named = volumes(base, "$base.mkv", data)
            named.forEach { (name, volume) -> post(if (name == "Show.S01E02.part2.rar") "Show.S01E02.part2.bin" else name, volume) }
            post("$base.par2", buildPar2File(named.map { (name, volume) -> "$folder$name" to volume }))
        }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            episodes.forEach { (base, data) -> assertContentEquals(data, streamer.streamed(prepared, "$base.mkv"), base) }
        }
    }

    /**
     * As another episode's set that lacks a first article, one that lacks a volume its PAR2 index
     * names is left out (N52), also where the index names it with its path in the season's folder (NR-11).
     */
    @ParameterizedTest
    @ValueSource(strings = ["", "Show.S01/", "Show.S01\\"])
    fun `a season pack leaves out an episode that lacks a volume its PAR2 index names`(folder: String) {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) ->
            val named = volumes(base, "$base.mkv", data)
            named.forEach { (name, volume) -> if (name != "Show.S01E02.part2.rar") post(name, volume) }
            post("$base.par2", buildPar2File(named.map { (name, volume) -> "$folder$name" to volume }))
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
     * Volumes obfuscated one by one under names that keep their extension (a8f7d6e5.rar, each a
     * first volume's name) are placed by the names the PAR2 index gives them, in whatever order the
     * NZB lists them, and so is one found posted under another extension (NR-1).
     */
    @ParameterizedTest
    @CsvSource("true, false", "true, true", "false, true")
    fun `volumes obfuscated one by one under their extension open in the order the PAR2 index gives`(reversed: Boolean, hidden: Boolean) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        val order = if (reversed) named.indices.reversed() else named.indices
        order.forEach { v -> post(if (hidden && v == 1) "x7f3k9q2.avi" else "${OBFUSCATED[v]}.rar", named[v].second) }
        post("0b1c2d3e.par2", buildPar2File(named))

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /**
     * RAR5 volumes posted under names that give no place, with no PAR2 data to name them, open in
     * the order their main archive headers give, whatever order the NZB lists them in (NR-21).
     */
    @ParameterizedTest
    @CsvSource("reversed, ''", "shuffled, ''", "reversed, .rar", "shuffled, .rar")
    fun `volumes posted under names that give no place open in the order their headers give`(order: String, extension: String) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        assertEquals(4, named.size)
        val listed = if (order == "reversed") named.indices.reversed() else listOf(2, 0, 3, 1)
        listed.forEach { v -> post("${OBFUSCATED[v]}$extension", named[v].second) }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /** Their headers show such a set lacks a volume the NZB doesn't hold: it doesn't open without it (NR-21). */
    @ParameterizedTest
    @ValueSource(ints = [0, 1])
    fun `a set posted under names that give no place that lacks a volume fails the release`(absent: Int) {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        named.indices.reversed().filter { it != absent }.forEach { v -> post(OBFUSCATED[v], named[v].second) }

        withStreamer { streamer, _ ->
            val result = assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
            assertTrue("volume ${absent + 1}" in result.message, result.message)
        }
    }

    /**
     * Without PAR2 data to name its volumes, a set ordered by its names (or by its RAR5 headers)
     * whose volumes skip one, or don't start at its first, lacks one the NZB doesn't hold: the
     * release doesn't open without it (NR-13). A missing last volume can't be seen this way. The
     * cause's type says no provider can supply it (NR-18).
     */
    @ParameterizedTest
    @CsvSource("RAR4, 2", "RAR4, 0", "SEVENZIP, 1", "SEVENZIP, 0", "RAR5, 1", "RAR5, 0")
    fun `a set whose volumes skip one fails a release without PAR2 data`(type: ContainerType, absent: Int) {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000), type)
        assertEquals(4, named.size)
        named.forEachIndexed { v, (name, data) -> if (v != absent) post(name, data) }
        post("Movie.2020.nfo", bytes("nfo", 1_000))

        withStreamer { streamer, _ ->
            val result = assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()), "prepare")
            assertTrue("volume ${absent + 1}" in result.message, result.message)
            assertIs<ArchiveVolumeNotInNzbException>(result.cause)
        }
    }

    /**
     * A volume the PAR2 index names into a set, posted under a name that tells nothing or obfuscated
     * by itself beside the set's named volumes, is one of the set's: its volumes don't skip it, by
     * their RAR5 headers or otherwise, and the release opens in the order the index gives (NR-13).
     */
    @ParameterizedTest
    @CsvSource("RAR5, x7f3k9q2", "RAR5, a8f7d6e5.rar", "RAR4, x7f3k9q2", "RAR4, a8f7d6e5.rar")
    fun `a volume the PAR2 index names beside its set's named ones opens with them`(type: ContainerType, postedAs: String) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, type)
        named.forEachIndexed { v, (name, data) -> post(if (v == 1) postedAs else name, data) }
        post("Movie.2020.par2", buildPar2File(named))

        withStreamer { streamer, _ ->
            val result = streamer.prepare(nzb())
            val prepared = assertIs<PrepareResult.Success>(result, "prepare: $result")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /**
     * A RAR volume starts with its archive's signature: without PAR2 data, a file named like the
     * volume a set's names skip that isn't an archive doesn't stand in for it (NR-13). A 7z volume
     * past the first has none, so its name alone counts.
     */
    @Test
    fun `a file named like a missing RAR volume that isn't one doesn't hide the gap`() {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000), ContainerType.RAR4)
        assertEquals(4, named.size)
        named.forEachIndexed { v, (name, data) -> if (v == 2) post("$name.bin", bytes("not a volume", data.size)) else post(name, data) }

        withStreamer { streamer, _ ->
            val result = streamer.prepare(nzb())
            val missingArticles = assertIs<PrepareResult.MissingArticles>(result, "prepare: $result")
            assertTrue("volume 3" in missingArticles.message, missingArticles.message)
        }
    }

    /** As one that lacks a first article, another episode's set whose volumes skip one is left out (NR-13). */
    @ParameterizedTest
    @ValueSource(strings = ["RAR4", "RAR5"])
    fun `a season pack leaves out an episode whose volumes skip one`(type: ContainerType) {
        val episodes = (1..3).associate { e -> "Show.S01E0$e" to bytes("episode $e", 150_000) }
        episodes.forEach { (base, data) ->
            val named = volumes(base, "$base.mkv", data, type)
            assertEquals(3, named.size)
            named.forEachIndexed { v, (name, volume) -> if (base != "Show.S01E02" || v != 1) post(name, volume) }
        }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(listOf("Show.S01E01.mkv", "Show.S01E03.mkv"), streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            assertContentEquals(episodes.getValue("Show.S01E03"), streamer.streamed(prepared, "Show.S01E03.mkv"))
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
     * par2cmdline spreads a recovery volume's critical packets (its Main packet, then its file
     * descriptions and their slices' checksums, by file id) evenly between its slices: a volume of
     * three, the smallest when the volumes are sized alike, names some files after its first slice
     * and the rest after its second. It is read past a slice until its descriptions cover every
     * file its Main packet lists, so every obfuscated volume is named, and in its place (NR-4, NR-6).
     */
    @ParameterizedTest
    @ValueSource(strings = ["RAR4", "RAR5"])
    fun `a recovery volume whose file descriptions are spread between its slices names every volume`(type: ContainerType) {
        val payload = bytes("movie", 400_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, type)
        assertEquals(7, named.size)
        named.forEachIndexed { v, (_, data) -> post(OBFUSCATED[v], data) }
        post("0b1c2d3e.vol000+003.par2", buildPar2File(named, recoveryBlock = 8 * 1024, slices = 3))
        post("0b1c2d3e.vol003+004.par2", buildPar2File(named, recoveryBlock = 8 * 1024, slices = 4))

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
            assertTrue(firstArticle("0b1c2d3e.vol003+004.par2") !in server.bodiesRead, "the larger recovery volume was fetched")
        }
    }

    /**
     * Obfuscated volumes the PAR2 data names beside one it doesn't: ordered by those names, the
     * unnamed one would follow them wherever it goes. Without RAR5 headers to place it, the release
     * fails rather than open out of order; RAR5 volumes open in the order their headers give (NR-4).
     */
    @ParameterizedTest
    @ValueSource(strings = ["RAR4", "RAR5"])
    fun `volumes the PAR2 data names beside one it doesn't open only where headers place them`(type: ContainerType) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, type)
        named.forEachIndexed { v, (_, data) -> post(OBFUSCATED[v], data) }
        post("0b1c2d3e.par2", buildPar2File(named.filterIndexed { v, _ -> v != 1 }))

        withStreamer { streamer, _ ->
            val result = streamer.prepare(nzb())
            if (type == ContainerType.RAR4) {
                val failure = assertIs<PrepareResult.Failure>(result, "prepare")
                assertTrue(OBFUSCATED[1] in failure.message, failure.message)
            } else {
                assertContentEquals(payload, streamer.streamed(assertIs<PrepareResult.Success>(result, "prepare")))
            }
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
     * A recovery volume whose block, ahead of its file descriptions, is bigger than prepare reads of
     * a PAR2 file is read from past it: the block's length is in the first 16 KB, so its articles are
     * never fetched, and it names the volumes however big it is (NR-7).
     */
    @Test
    fun `a recovery volume whose block is bigger than prepare reads names the volumes, its block not fetched`() {
        val named = volumes("Movie.2020", "Movie.2020.mkv", bytes("movie", 200_000))
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        val ids = post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 17 * 1024 * 1024), partSize = 512 * 1024)

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertEquals(listOf(ids.first(), ids.last()), ids.filter { it in server.bodiesRead }, "articles of the recovery volume downloaded")
        }
    }

    /**
     * The recovery volume standing in for the PAR2 index is read from past its block, so an article
     * of the block that no provider has doesn't keep it from naming the volumes (NR-7).
     */
    @Test
    fun `a recovery volume's block isn't fetched, so an article of it no provider has doesn't lose its names`() {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        val ids = post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 1024 * 1024))
        missing += ids[5]

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
            assertEquals(listOf(ids.first(), ids.last()), ids.filter { it in server.bodiesRead }, "articles of the recovery volume downloaded")
        }
    }

    /**
     * A recovery volume read in place of the PAR2 index only names files: one whose read fails any
     * way (here the connection drops in an article of it every time, the one with its descriptions,
     * or its first when it is fetched because the index lacks its own) names nothing, and the
     * release opens as it would without it, here in the order its RAR5 headers give (NR-7).
     */
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `a recovery volume whose read fails names nothing and fails nothing`(indexLacking: Boolean) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        if (indexLacking) missing += post("0b1c2d3e.par2", buildPar2File(named)).first()
        val ids = post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 64 * 1024))
        val failing = if (indexLacking) ids.first() else ids.last()
        dropped += failing

        withStreamer { streamer, server ->
            val result = streamer.prepare(nzb())
            assertContentEquals(payload, streamer.streamed(assertIs<PrepareResult.Success>(result, "prepare: $result")))
            assertTrue(failing in server.bodiesRead, "the recovery volume wasn't read")
        }
    }

    /**
     * A recovery volume read until its file descriptions are whole is read no further, and what
     * fails past them changes nothing, whenever it fails: here its block's first article, which no
     * provider has, is answered before the one its descriptions end in (NR-15).
     */
    @Test
    fun `a recovery volume's descriptions are kept whatever fails past them`() {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        val par2 = buildPar2File(named, recoveryBlock = 64 * 1024, descriptionsFirst = true)
        val ids = post("0b1c2d3e.vol00+01.par2", par2, partSize = 1024)
        // The article the header of the slice after the descriptions ends in, and the next, the block's.
        val slice = par2.indexOfBytes("PAR 2.0\u0000RecvSlic".toByteArray(Charsets.ISO_8859_1)) - 48
        val last = (slice + 63) / 1024
        assertTrue(last in 1 until ids.size - 1, "the descriptions end in article ${last + 1} of ${ids.size}")
        missing += ids[last + 1]
        answerAfter[ids[last]] = ids[last + 1]

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /**
     * A recovery volume stands in for the PAR2 index of its own set: the smallest of those named
     * like it, the next-smallest when that names nothing, for a set whose index names nothing or
     * that has none. Another set's index (a sample's), which names only its own files, doesn't stand
     * in for it, nor does another set's smaller recovery volume (NR-5).
     */
    @ParameterizedTest
    @ValueSource(strings = ["its index lacking beside a sample's", "no index beside a sample's", "no index, its smallest volume lacking"])
    fun `a recovery volume of the set's own names its volumes`(shape: String) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, ContainerType.RAR4)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        if (shape.startsWith("its index lacking")) missing += post("Movie.2020.par2", buildPar2File(named)).first()
        if (shape.endsWith("beside a sample's")) {
            val sample = listOf("Movie.2020-sample.mkv" to bytes("sample", 20_000))
            if (shape.startsWith("its index lacking")) post("Movie.2020-sample.par2", buildPar2File(sample))
            post("Movie.2020-sample.vol00+01.par2", buildPar2File(sample, recoveryBlock = 4 * 1024))
        }
        val smallest = post("Movie.2020.vol00+01.par2", buildPar2File(named, recoveryBlock = 16 * 1024))
        if (shape.endsWith("smallest volume lacking")) {
            missing += smallest.first()
            post("Movie.2020.vol01+02.par2", buildPar2File(named, recoveryBlock = 32 * 1024))
        }

        withStreamer { streamer, _ ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
        }
    }

    /**
     * A PAR2 index over more than about a thousand files starts with a Main packet that lists them
     * all, past its first 16 KB: it is read all the same, as far as prepare reads a PAR2 file, and
     * names the volumes without a recovery volume read in its place (NR-9).
     */
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `a PAR2 index whose file descriptions start past its first 16 KB names the volumes`(withRecoveryVolume: Boolean) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload, ContainerType.RAR4)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        val described = named + (1..1_100).map { n -> "Movie.2020.Photos/Photo%04d.jpg".format(n) to bytes("photo $n", 64) }
        val index = buildPar2File(described)
        assertTrue(index.indexOfBytes("PAR 2.0\u0000FileDesc".toByteArray(Charsets.ISO_8859_1)) > 16 * 1024, "the index names files in its first 16 KB")
        post("0b1c2d3e.par2", index)
        val recovery = if (withRecoveryVolume) post("0b1c2d3e.vol00+01.par2", buildPar2File(described, recoveryBlock = 16 * 1024)) else emptyList()

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertContentEquals(payload, streamer.streamed(prepared))
            assertEquals(emptyList(), recovery.filter { it in server.bodiesRead }, "the recovery volume was fetched")
        }
    }

    /**
     * A release of files posted under their names, with no archive, needs no names from the PAR2
     * data: no recovery volume is read in place of its index when that lacks its first article
     * (NR-10).
     */
    @Test
    fun `a release of files posted under their names reads no recovery volume in place of its index`() {
        val movie = bytes("movie", 200_000)
        post("Movie.2020.1080p.mkv", movie)
        val described = listOf("Movie.2020.1080p.mkv" to movie)
        post("Movie.2020.1080p.nfo", bytes("nfo", 1_000))
        missing += post("Movie.2020.1080p.par2", buildPar2File(described)).first()
        val recovery = post("Movie.2020.1080p.vol000+001.par2", buildPar2File(described, recoveryBlock = 64 * 1024)) +
            post("Movie.2020.1080p.vol001+002.par2", buildPar2File(described, recoveryBlock = 64 * 1024, slices = 2))

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(listOf("Movie.2020.1080p.mkv"), streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            assertContentEquals(movie, streamer.streamed(prepared))
            assertEquals(emptyList(), recovery.filter { it in server.bodiesRead }, "recovery volume articles fetched")
        }
    }

    /**
     * The recovery volume read in place of the PAR2 index is read from the first article fetched of
     * it, whole, not that article fetched again: here its descriptions follow its block in that
     * article, past its first 16 KB. So with it fetched with the rest (no index) and after them (an
     * index that lacks its first article) (NR-10).
     */
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `a recovery volume's first article is fetched once`(indexLacking: Boolean) {
        val payload = bytes("movie", 200_000)
        val named = volumes("Movie.2020", "Movie.2020.mkv", payload)
        named.indices.reversed().forEach { v -> post(OBFUSCATED[v], named[v].second) }
        if (indexLacking) missing += post("0b1c2d3e.par2", buildPar2File(named)).first()
        val ids = post("0b1c2d3e.vol00+01.par2", buildPar2File(named, recoveryBlock = 20 * 1024), partSize = 64 * 1024)
        assertEquals(1, ids.size)

        withStreamer { streamer, server ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
            assertEquals(named.map { it.first }, prepared.metadata.response.volumes)
            assertEquals(1, server.bodiesRead.count { it == ids.single() }, "times the recovery volume's first article was fetched")
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

    /** Where [bytes] first appear in these, or -1. */
    private fun ByteArray.indexOfBytes(bytes: ByteArray): Int =
        (0..size - bytes.size).firstOrNull { at -> bytes.indices.all { this[at + it] == bytes[it] } } ?: -1

    private companion object {
        /** Names that look like an obfuscating poster's, one per volume. */
        val OBFUSCATED = listOf("a8f7d6e5", "b91c2d3e", "c07e4f1a", "d2b5a9c4", "e6f3c8b1", "f4a1d7e9", "a3e9c2f7", "b8d4e1a6")
    }
}
