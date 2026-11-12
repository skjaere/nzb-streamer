package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readFully
import io.ktor.utils.io.toByteArray
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.YencDecodingException
import io.skjaere.nzbstreamer.config.MissingArticlePolicy
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.nzb.NzbParser
import io.skjaere.nzbstreamer.seekable.SegmentRandomAccessStream
import io.skjaere.nzbstreamer.stream.Gap
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentCache
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import io.skjaere.nzbstreamer.stream.toSplits
import io.skjaere.yenc.YencEncoder
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * StreamingConfig.missingArticles: by default (FAIL) a streamed article no provider has fails the stream there, as it
 * always has. Under ZERO_FILL, zeros of its size take its place where the layout gives that size exactly, each stretch
 * reported once, in the file's offsets, before its zeros are read; never cached. What prepare reads, random access and
 * the availability check stay strict.
 */
class MissingArticleToleranceTest {
    private val partSize = 16 * 1024
    private val articles = ConcurrentHashMap<String, ByteArray>()

    /** Articles every provider answers 430 for (to BODY and STAT). */
    private val missing: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /** Articles every provider serves with a =ybegin size that can't be read. */
    private val malformed: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val files = mutableListOf<String>()
    private val servers = mutableListOf<FakeNntpServer>()
    private val gate = CountDownLatch(1)

    @AfterEach
    fun tearDown() {
        gate.countDown()
        servers.forEach { it.close() }
    }

    private fun id(name: String, part: Int) = "<$name-$part@test>"

    /** Posts [data] as [name] in parts of [partSize]; its NZB lists all but the parts in [unlisted] (from 1). */
    private fun post(name: String, data: ByteArray, unlisted: Set<Int> = emptySet()) {
        val parts = YencEncoder.encodeMultiPart(data, name, partSize)
        parts.forEachIndexed { i, part -> articles[id(name, i + 1)] = part.data }
        val segments = parts.indices.filterNot { it + 1 in unlisted }.joinToString("") { i ->
            """<segment bytes="${parts[i].data.size}" number="${i + 1}">$name-${i + 1}@test</segment>"""
        }
        files += """<file poster="t" date="0" subject="&quot;$name&quot; yEnc (1/${parts.size})">""" +
            """<groups><group>alt.binaries.test</group></groups><segments>$segments</segments></file>"""
    }

    private fun nzb() = ("""<?xml version="1.0" encoding="UTF-8"?>""" +
        """<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""").toByteArray()

    private fun malformedCopy(article: ByteArray): ByteArray =
        String(article, Charsets.ISO_8859_1).replaceFirst(Regex(" size=\\d+ "), " size= ").toByteArray(Charsets.ISO_8859_1)

    /** A provider of what was posted, but [missing] and [malformed]; [lacks] says 430 for more, [held] answers once the gate opens. */
    private fun server(lacks: (String) -> Boolean = { false }, held: (String) -> Boolean = { false }) = FakeNntpServer(
        stat = { id -> articles.containsKey(id) && id !in missing && !lacks(id) },
        body = { id ->
            if (held(id)) gate.await(60, TimeUnit.SECONDS)
            when {
                id in missing || lacks(id) -> null
                id in malformed -> malformedCopy(articles.getValue(id))
                else -> articles[id]
            }
        },
    ).also { servers += it }

    private fun config(server: FakeNntpServer) = NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)

    private fun streamer(
        policy: MissingArticlePolicy = MissingArticlePolicy.ZERO_FILL,
        providers: List<FakeNntpServer> = listOf(server()),
        cache: SegmentCache? = null,
        segmentFetchTimeoutMs: Long = 5_000,
    ) = NzbStreamer.fromConfig(
        providers.map(::config),
        StreamingConfig(missingArticles = policy, segmentFetchTimeoutMs = segmentFetchTimeoutMs),
        segmentCache = cache,
        closeSegmentCache = false,
    )

    /** What a stream gave: its bytes, the gaps it reported, and the failure it ended with (null at a plain end). */
    private class Streamed(val bytes: ByteArray, val gaps: List<Gap>, val failure: Throwable?)

    private suspend fun ByteReadChannel.drainTo(out: ByteArrayOutputStream) {
        val buffer = ByteArray(8192)
        while (true) {
            val n = readAvailable(buffer)
            if (n == -1) break
            out.write(buffer, 0, n)
        }
    }

    /** Prepares the release and streams its one file (or [range] of it). */
    private suspend fun NzbStreamer.stream(range: LongRange? = null, onGap: ((Gap) -> Unit)? = null): Streamed {
        val prepared = assertIs<PrepareResult.Success>(prepare(nzb()), "prepare")
        val file = resolveStreamableFiles(prepared.metadata).single()
        val gaps = CopyOnWriteArrayList<Gap>()
        val out = ByteArrayOutputStream()
        val failure = try {
            streamFile(prepared.metadata, file, range, onGap = { gap -> gaps += gap; onGap?.invoke(gap) }) { it.drainTo(out) }
            null
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            e
        }
        return Streamed(out.toByteArray(), gaps.toList(), failure)
    }

    private fun Throwable?.chain() = generateSequence(this) { it.cause }

    /** [data] with zeros over [ranges]. */
    private fun zeroed(data: ByteArray, vararg ranges: LongRange) = data.copyOf().also { copy ->
        ranges.forEach { range -> range.forEach { copy[it.toInt()] = 0 } }
    }

    /**
     * That [actual] is the start of [expected], [upTo] bytes at most: a stream that fails delivers what is in front of the
     * failure, but Ktor drops what its reader hadn't taken yet when the channel closed.
     */
    private fun assertStartOf(expected: ByteArray, upTo: Int, actual: ByteArray) {
        assertTrue(actual.size <= upTo, "streamed ${actual.size} bytes, past $upTo")
        assertContentEquals(expected.copyOf(actual.size), actual)
    }

    /** 5 whole parts and a sixth of 5000 bytes. */
    private val movie = Random(1).nextBytes(5 * partSize + 5000)
    private val name = "Movie.2020.mkv"

    /** Part [part]'s bytes in the file (from 1). */
    private fun partRange(part: Int) = (part - 1L) * partSize until minOf(part.toLong() * partSize, movie.size.toLong())

    @Test
    fun `by default a missing article fails the stream there, as before, and nothing is reported`() = runBlocking<Unit> {
        assertEquals(MissingArticlePolicy.FAIL, StreamingConfig().missingArticles)
        post(name, movie)
        missing += id(name, 3)
        streamer(MissingArticlePolicy.FAIL).use { streamer ->
            assertEquals(MissingArticlePolicy.FAIL, streamer.getMissingArticles())
            val streamed = streamer.stream()
            assertTrue(streamed.failure.chain().any { it is ArticleNotFoundException }, "ended with: ${streamed.failure}")
            assertStartOf(movie, 2 * partSize, streamed.bytes)
            assertEquals(emptyList(), streamed.gaps)
        }
    }

    @Test
    fun `a missing article mid-file streams as zeros of its size, reported once in the file's offsets`() = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 3)
        streamer().use { streamer ->
            val streamed = streamer.stream()
            assertNull(streamed.failure, "ended with: ${streamed.failure}")
            assertEquals(listOf(Gap(partRange(3), Gap.Cause.Missing)), streamed.gaps)
            assertContentEquals(zeroed(movie, partRange(3)), streamed.bytes)
        }
    }

    @Test
    fun `a range of the file reports the gap in the file's offsets, from inside it too`() = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 3)
        streamer().use { streamer ->
            val range = partSize + 100L..movie.size - 51L
            val streamed = streamer.stream(range)
            assertNull(streamed.failure, "ended with: ${streamed.failure}")
            assertEquals(listOf(Gap(partRange(3), Gap.Cause.Missing)), streamed.gaps)
            assertContentEquals(zeroed(movie, partRange(3)).copyOfRange(range.first.toInt(), range.last.toInt() + 1), streamed.bytes)

            // A stream that starts in the gap fills from there.
            val inside = 2L * partSize + 500..movie.size - 1L
            val from = streamer.stream(inside)
            assertEquals(listOf(Gap(inside.first..partRange(3).last, Gap.Cause.Missing)), from.gaps)
            assertContentEquals(zeroed(movie, partRange(3)).copyOfRange(inside.first.toInt(), movie.size), from.bytes)
        }
    }

    @Test
    fun `a missing last article streams as zeros of the file's remainder`() = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 6)
        streamer().use { streamer ->
            val streamed = streamer.stream()
            assertNull(streamed.failure, "ended with: ${streamed.failure}")
            assertEquals(listOf(Gap(5L * partSize until movie.size, Gap.Cause.Missing)), streamed.gaps)
            assertEquals(movie.size, streamed.bytes.size)
            assertContentEquals(zeroed(movie, partRange(6)), streamed.bytes)
        }
    }

    /** Parts the NZB lacks can be placed by the yEnc sizes; the missing one beside them is then exact too. */
    @Test
    fun `a missing article in a posting whose NZB lacks parts is filled where the layout places it`() = runBlocking<Unit> {
        post(name, movie, unlisted = setOf(6))
        missing += id(name, 2)
        streamer().use { streamer ->
            val streamed = streamer.stream()
            assertEquals(listOf(Gap(partRange(2), Gap.Cause.Missing)), streamed.gaps)
            // The part the NZB lists no article for still fails, after the zeros in front of it.
            assertTrue(streamed.failure.chain().any { it is ArticleNotFoundException && "part 6 of 6" in it.message.orEmpty() }, "ended with: ${streamed.failure}")
            assertStartOf(zeroed(movie, partRange(2)), 5 * partSize, streamed.bytes)
        }
    }

    /** When the parts' sizes can't be told, the last listed article takes what's left: a guess zeros mustn't stand on. */
    @Test
    fun `an article whose size the layout only guesses still fails`() = runBlocking<Unit> {
        post(name, movie, unlisted = setOf(6))
        // A yEnc total that isn't the parts the size implies: the NZB's five articles are laid out in order, the last
        // one taking the rest of the file.
        val first = id(name, 1)
        articles[first] = String(articles.getValue(first), Charsets.ISO_8859_1).replaceFirst(" total=6 ", " total=7 ")
            .toByteArray(Charsets.ISO_8859_1)
        missing += id(name, 3)
        streamer().use { streamer ->
            val streamed = streamer.stream()
            assertTrue(streamed.failure.chain().any { it is ArticleNotFoundException }, "ended with: ${streamed.failure}")
            assertEquals(emptyList(), streamed.gaps)
            assertStartOf(movie, 2 * partSize, streamed.bytes)
        }
    }

    @Test
    fun `zeros are never cached, and a later play asks the providers again`(@TempDir tmp: Path) = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 3)
        val provider = server()
        SegmentCache(SegmentCacheConfig(tmp, maxBytes = 100L * 1024 * 1024)).use { cache ->
            streamer(providers = listOf(provider), cache = cache).use { streamer ->
                repeat(2) {
                    val streamed = streamer.stream()
                    assertContentEquals(zeroed(movie, partRange(3)), streamed.bytes)
                    assertEquals(1, streamed.gaps.size)
                }
                assertNull(cache.get("$name-3@test"), "the zeros were cached")
                assertEquals(2, provider.bodiesRead.count { it == id(name, 3) }, "bodies asked: ${provider.bodiesRead}")
                assertEquals(1, provider.bodiesRead.count { it == id(name, 4) }, "the article after it wasn't cached")
            }
        }
    }

    @Test
    fun `a gap found ahead is reported although the reader stops before it`() = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 5)
        streamer().use { streamer ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()))
            val gaps = CopyOnWriteArrayList<Gap>()
            streamer.streamFile(prepared.metadata, streamer.resolveStreamableFiles(prepared.metadata).single(), onGap = { gaps += it }) { channel ->
                channel.readFully(ByteArray(partSize))
                withTimeout(5_000) { while (gaps.isEmpty()) delay(10) }
            }
            assertEquals(listOf(Gap(partRange(5), Gap.Cause.Missing)), gaps.toList())
        }
    }

    /**
     * The reader learns of a stretch before it can read the first of its zeros, so it can stop there and never hand them
     * on. The listener takes its time: were the zeros written before it was told, the reader would have the first of
     * them by then, on another thread.
     */
    @Test
    fun `a gap is reported before the first of its zeros can be read`() = runBlocking<Unit>(Dispatchers.Default) {
        post(name, movie)
        missing += id(name, 3)
        streamer().use { streamer ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()))
            val gaps = CopyOnWriteArrayList<Gap>()
            var known: List<Gap>? = null
            val file = streamer.resolveStreamableFiles(prepared.metadata).single()
            streamer.streamFile(prepared.metadata, file, onGap = { gap -> Thread.sleep(300); gaps += gap }) { channel ->
                channel.readFully(ByteArray(partRange(3).first.toInt()))
                assertEquals(0.toByte(), channel.readByte(), "the gap's first byte")
                known = gaps.toList()
            }
            assertEquals(listOf(Gap(partRange(3), Gap.Cause.Missing)), known, "the gaps known as its first zero was read")
        }
    }

    @Test
    fun `an article no provider has a readable copy of streams as zeros, reported as malformed`() = runBlocking<Unit> {
        post(name, movie)
        malformed += id(name, 3)
        streamer().use { streamer ->
            val streamed = streamer.stream()
            assertNull(streamed.failure, "ended with: ${streamed.failure}")
            assertEquals(listOf(Gap(partRange(3), Gap.Cause.Malformed)), streamed.gaps)
            assertContentEquals(zeroed(movie, partRange(3)), streamed.bytes)
        }
        streamer(MissingArticlePolicy.FAIL).use { streamer ->
            val streamed = streamer.stream()
            assertTrue(streamed.failure.chain().any { it is YencDecodingException }, "ended with: ${streamed.failure}")
        }
    }

    /** A pool that timed out may have it: that is no gap. */
    @Test
    fun `an article one provider lacks and another times out on fails with the timeout`() = runBlocking<Unit> {
        post(name, movie)
        val lacking = server(lacks = { it == id(name, 3) })
        val slow = server(held = { it == id(name, 3) })
        streamer(providers = listOf(lacking, slow), segmentFetchTimeoutMs = 300).use { streamer ->
            val streamed = streamer.stream()
            assertTrue(streamed.failure.chain().any { it is SegmentTimeoutException }, "ended with: ${streamed.failure}")
            assertEquals(emptyList(), streamed.gaps)
        }
    }

    /** A pool passed over by its breaker is asked before zeros stand in for what it may have. */
    @Test
    fun `a pool its breaker passed over is asked before a gap is filled`() = runBlocking<Unit> {
        post(name, movie)
        val skipped = server()
        val lacking = server(lacks = { it == id(name, 3) })
        streamer(providers = listOf(skipped, lacking)).use { streamer ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()))
            streamer.breakerOf(skipped).transitionToOpenState()
            val gaps = CopyOnWriteArrayList<Gap>()
            var bytes = ByteArray(0)
            streamer.streamFile(prepared.metadata, streamer.resolveStreamableFiles(prepared.metadata).single(), onGap = { gaps += it }) {
                bytes = it.toByteArray()
            }
            assertContentEquals(movie, bytes)
            assertEquals(emptyList(), gaps.toList())
            assertTrue(id(name, 3) in skipped.bodiesRead, "the passed-over pool wasn't asked")
        }
    }

    @Test
    fun `a listener that throws doesn't fail the stream`() = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 3)
        streamer().use { streamer ->
            val streamed = streamer.stream(onGap = { error("the listener's own failure") })
            assertNull(streamed.failure, "ended with: ${streamed.failure}")
            assertContentEquals(zeroed(movie, partRange(3)), streamed.bytes)
        }
    }

    @Test
    fun `the builder and the setter set the policy for the streams opened after`() = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 3)
        val provider = server()
        val built = NzbStreamer {
            nntp {
                host = "127.0.0.1"
                port = provider.port
                useTls = false
            }
            missingArticles = MissingArticlePolicy.ZERO_FILL
        }
        built.use { streamer ->
            assertEquals(MissingArticlePolicy.ZERO_FILL, streamer.getMissingArticles())
            assertEquals(1, streamer.stream().gaps.size)

            streamer.setMissingArticles(MissingArticlePolicy.FAIL)
            val strict = streamer.stream()
            assertTrue(strict.failure.chain().any { it is ArticleNotFoundException }, "ended with: ${strict.failure}")
            assertEquals(emptyList(), strict.gaps)
        }
    }

    /** Callers stream so: streamName named, consume a trailing lambda. */
    @Test
    fun `a call with a trailing lambda binds it to consume, as a caller's does`() = runBlocking<Unit> {
        post(name, movie)
        streamer(MissingArticlePolicy.FAIL).use { streamer ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()))
            val nzbDocument = prepared.metadata.orderedArchiveNzb
            val streamableFile = streamer.resolveStreamableFiles(prepared.metadata).single()
            val longRange = 0L until movie.size
            var bytes = ByteArray(0)
            streamer.streamFile(nzbDocument, streamableFile, longRange, streamName = "test label") { channel ->
                bytes = channel.toByteArray()
            }
            assertContentEquals(movie, bytes)
        }
    }

    // What prepare reads, random access and the availability check stay strict under ZERO_FILL.

    @Test
    fun `a release whose first article is missing still fails to prepare`() = runBlocking<Unit> {
        post(name, movie)
        missing += id(name, 1)
        streamer().use { streamer ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()))
        }
    }

    @Test
    fun `an archive whose headers lie in a missing article still fails to prepare`() = runBlocking<Unit> {
        // A 7z keeps its headers at the end of its last volume.
        val volumes = ArchiveGenerator.generate(Random(2).nextBytes(150_000), 64L * 1024, ContainerType.SEVENZIP, name)
        volumes.forEach { post(it.filename, it.data) }
        val last = volumes.last()
        missing += id(last.filename, (last.data.size + partSize - 1) / partSize)
        streamer().use { streamer ->
            assertIs<PrepareResult.MissingArticles>(streamer.prepare(nzb()))
        }
    }

    /** Archive headers (and PAR2 files) are read a segment at a time, so: fetchSegment and the random-access stream. */
    @Test
    fun `random access to a missing article still fails`() = runBlocking<Unit> {
        post(name, movie)
        val nzb = streamer().use { assertIs<EnrichmentResult.Success>(it.enrich(nzb())).enrichedNzb }
        missing += id(name, 3)
        NntpStreamingService(listOf(config(server())), StreamingConfig(missingArticles = MissingArticlePolicy.ZERO_FILL)).use { service ->
            service.connect()
            assertIs<ArticleNotFoundException>(runCatching { service.fetchSegment("$name-3@test") }.exceptionOrNull())
            val read = SegmentRandomAccessStream(nzb, service).use { stream ->
                stream.seek(2L * partSize + 10)
                runCatching { stream.read(ByteArray(16), 0, 16) }.exceptionOrNull()
            }
            assertTrue(read.chain().any { it is ArticleNotFoundException }, "read: $read")
        }
    }

    /**
     * A PAR2 index is read a segment at a time too: one that lacks an article past its first is gone without, as under
     * FAIL, the release opening all the same. Its packets are never read on through zeros, which would end them early.
     */
    @Test
    fun `a PAR2 index that lacks an article is read strictly, not through zeros`() = runBlocking<Unit> {
        post(name, movie)
        val index = buildPar2IndexFile(name, movie.size.toLong()).let { packet -> generateSequence { packet }.take(300).reduce(ByteArray::plus) }
        check(index.size > 2 * partSize) { "test setup: the index's size" }
        post("Movie.2020.par2", index)
        missing += id("Movie.2020.par2", 2)
        val provider = server()
        streamer(providers = listOf(provider)).use { streamer ->
            val enriched = assertIs<EnrichmentResult.Success>(streamer.enrich(nzb())).enrichedNzb
            assertTrue(id("Movie.2020.par2", 2) in provider.bodiesRead, "the index's read didn't reach the missing article")
            val par2 = enriched.files.single { it.yencHeaders?.name == "Movie.2020.par2" }
            assertNull(par2.par2Data, "the index was read on past its missing article")
        }
    }

    @Test
    fun `the availability check still finds a missing article missing`() = runBlocking<Unit> {
        post(name, movie)
        streamer().use { streamer ->
            val nzb = assertIs<EnrichmentResult.Success>(streamer.enrich(nzb())).enrichedNzb
            missing += id(name, 3)
            assertIs<VerificationResult.MissingArticles>(streamer.verifySegments(nzb))
        }
    }

    @Test
    fun `a part the NZB lists no article for still fails`() = runBlocking<Unit> {
        post(name, movie, unlisted = setOf(3))
        streamer().use { streamer ->
            val streamed = streamer.stream()
            assertTrue(streamed.failure.chain().any { it is ArticleNotFoundException && "part 3 of 6" in it.message.orEmpty() }, "ended with: ${streamed.failure}")
            assertEquals(emptyList(), streamed.gaps)
            assertStartOf(movie, 2 * partSize, streamed.bytes)
        }
    }

    /** A stored RAR's file is its volumes' data areas: zeros go where the missing article's bytes of it were. */
    @Test
    fun `a stored RAR passes zeros through at the file's offsets, across volumes`() = runBlocking<Unit> {
        val payload = Random(7).nextBytes(200_000)
        val volumes = ArchiveGenerator.generate(payload, 48L * 1024, ContainerType.RAR5, name)
        volumes.forEachIndexed { v, volume -> post("Movie.2020.part${v + 1}.rar", volume.data) }
        streamer().use { streamer ->
            val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()))
            val nzb = prepared.metadata.orderedArchiveNzb
            val file = streamer.resolveStreamableFiles(prepared.metadata).single()
            // The second volume's middle article, and the first volume's last (its data's end and the end of archive).
            val gone = listOf(1 to 2, 0 to (volumes[0].data.size + partSize - 1) / partSize)
            gone.forEach { (v, part) -> missing += id("Movie.2020.part${v + 1}.rar", part) }

            // Where the parser puts the file's bytes in the volumes, laid end to end, and where those articles are.
            val volumeStarts = nzb.files.runningFold(0L) { at, f -> at + f.yencHeaders!!.size }
            val splits = file.toSplits(nzb)
            val expected = gone.map { (v, part) ->
                val article = volumeStarts[v] + (part - 1L) * partSize until minOf(volumeStarts[v] + part.toLong() * partSize, volumeStarts[v + 1])
                var fileAt = 0L
                splits.firstNotNullOf { split ->
                    val from = maxOf(article.first, split.dataStartPosition)
                    val to = minOf(article.last + 1, split.dataStartPosition + split.dataSize)
                    (if (from < to) fileAt + (from - split.dataStartPosition) until fileAt + (to - split.dataStartPosition) else null)
                        .also { fileAt += split.dataSize }
                }
            }.sortedBy { it.first }

            val gaps = CopyOnWriteArrayList<Gap>()
            var bytes = ByteArray(0)
            streamer.streamFile(prepared.metadata, file, onGap = { gaps += it }) { bytes = it.toByteArray() }
            assertEquals(expected.map { Gap(it, Gap.Cause.Missing) }, gaps.sortedBy { it.fileRange.first })
            assertContentEquals(zeroed(payload, *expected.toTypedArray()), bytes)
        }
    }

    private fun NzbStreamer.breakerOf(server: FakeNntpServer) = run {
        val service = NzbStreamer::class.java.getDeclaredField("streamingService").apply { isAccessible = true }.get(this)
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry").apply { isAccessible = true }
        (field.get(service) as CircuitBreakerRegistry).circuitBreaker("127.0.0.1:${server.port}")
    }
}
