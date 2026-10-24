package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.ktor.utils.io.toByteArray
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * N93: a pool whose circuit breaker is open is passed over, so it was never asked. An article the pools that were asked
 * lack is therefore asked of it once (its breaker aside, within the segment timeout as any pool) before it is called
 * missing: an article is missing only when every pool answered 430. When the skipped pool fails instead, the article is
 * unavailable, not missing. Prepare is then no worse off than before that pool was asked: a file the release opens
 * without is left out, as for a 430, and one it needs fails as unavailable.
 */
class SkippedPoolTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val gate = CountDownLatch(1)

    @AfterEach
    fun tearDown() {
        gate.countDown()
        servers.forEach { it.close() }
    }

    private val segments = (0 until 4).map { Random(it).nextBytes(16 * 1024) }

    private fun String.segment() = removePrefix("<seg").substringBefore('@').toInt()

    private fun article(i: Int) = YencEncoder.encodeSinglePart(segments[i], "data.bin").data

    private fun server(has: (Int) -> Boolean, dropAfter: Int? = null, held: Boolean = false) = FakeNntpServer(
        body = { id ->
            if (held) gate.await(60, TimeUnit.SECONDS)
            id.segment().takeIf(has)?.let(::article)
        },
        dropAfter = { dropAfter },
    ).also { servers += it }

    private fun service(vararg servers: FakeNntpServer, config: StreamingConfig = StreamingConfig()) =
        NntpStreamingService(
            servers.map { NntpConfig("127.0.0.1", it.port, "", "", useTls = false, maxConnections = 2) },
            config,
        ).also { runBlocking { it.connect() } }

    private fun NntpStreamingService.breakerOf(server: FakeNntpServer): CircuitBreaker {
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        field.isAccessible = true
        return (field.get(this) as CircuitBreakerRegistry).circuitBreaker("127.0.0.1:${server.port}")
    }

    /** Opens [server]'s breaker, as failures would: its pool is then passed over. */
    private fun NntpStreamingService.open(server: FakeNntpServer) = breakerOf(server).transitionToOpenState()

    private fun file() = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = segments.indices.map { NzbSegment(segments[it].size.toLong(), it + 1, "seg$it@test") },
        yencHeaders = YencHeaders(line = 128, size = segments.sumOf { it.size.toLong() }, name = "data.bin", partEnd = segments[0].size.toLong()),
    )

    /** An NZB of one file whose first article is seg0's. */
    private fun nzb() = NzbDocument(
        listOf(
            NzbFile(
                poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
                segments = listOf(NzbSegment(segments[0].size.toLong(), 1, "seg0@test")),
            )
        )
    )

    private fun Throwable?.chain() = generateSequence(this) { it.cause }

    @Test
    fun `a segment the pools asked lack is fetched from a pool its breaker passed over`() = runBlocking<Unit> {
        val skipped = server(has = { true })
        val lacking = server(has = { it != 2 })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            assertContentEquals(segments[2], service.fetchSegment("seg2@test"))
            assertEquals(listOf("<seg2@test>"), skipped.bodiesRead)
            assertEquals(CircuitBreaker.State.OPEN, service.breakerOf(skipped).state, "the passed-over pool's breaker")
        }
    }

    @Test
    fun `a stream through a segment the pools asked lack is whole`() = runBlocking<Unit> {
        val skipped = server(has = { true })
        val lacking = server(has = { it != 2 })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            var bytes = ByteArray(0)
            service.streamSegments(SegmentQueueService.createFileQueue(file(), 0, 0L), name = "test") { bytes = it.toByteArray() }
            assertContentEquals(segments.reduce { a, b -> a + b }, bytes)
            assertEquals(listOf("<seg2@test>"), skipped.bodiesRead, "only the segment the others lack is asked of the passed-over pool")
        }
    }

    @Test
    fun `a segment every pool lacks, the one passed over asked too, is missing`() = runBlocking<Unit> {
        val skipped = server(has = { it != 2 })
        val lacking = server(has = { it != 2 })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            val failure = runCatching { service.fetchSegment("seg2@test") }.exceptionOrNull()
            assertIs<ArticleNotFoundException>(failure, "failed with: $failure")
            assertEquals(listOf("<seg2@test>"), skipped.bodiesRead, "the passed-over pool should be asked")
        }
    }

    @Test
    fun `a segment the passed-over pool drops the connection on is unavailable, not missing`() = runBlocking<Unit> {
        val skipped = server(has = { true }, dropAfter = 2_000)
        val lacking = server(has = { it != 2 })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            val failure = runCatching { service.fetchSegment("seg2@test") }.exceptionOrNull()
            assertTrue(failure != null && failure.chain().none { it is ArticleNotFoundException }, "failed with: $failure")
        }
    }

    /** One bounded try: the passed-over pool gets the segment timeout as any pool does. */
    @Test
    fun `a segment the passed-over pool never answers times out, not missing`() = runBlocking<Unit> {
        val skipped = server(has = { true }, held = true)
        val lacking = server(has = { it != 2 })
        service(skipped, lacking, config = StreamingConfig(segmentFetchTimeoutMs = 300)).use { service ->
            service.open(skipped)

            val failure = withTimeout(5_000) { runCatching { service.fetchSegment("seg2@test") }.exceptionOrNull() }
            assertIs<SegmentTimeoutException>(failure, "failed with: $failure")
        }
    }

    @Test
    fun `a segment another pool has is not asked of the pool passed over`() = runBlocking<Unit> {
        val skipped = server(has = { true })
        val other = server(has = { true })
        service(skipped, other).use { service ->
            service.open(skipped)

            assertContentEquals(segments[2], service.fetchSegment("seg2@test"))
            assertEquals(emptyList(), skipped.bodiesRead)
        }
    }

    /** Prepare's reads go through the same fallback (its first articles). */
    @Test
    fun `a first article the pools asked lack is prepared from a pool its breaker passed over`() = runBlocking<Unit> {
        val skipped = server(has = { true })
        val lacking = server(has = { false })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            val result = NzbEnrichmentService(service).enrich(nzb())

            assertIs<EnrichmentResult.Success>(result, "got $result")
            assertEquals(listOf("<seg0@test>"), skipped.bodiesRead)
        }
    }

    @Test
    fun `a first article every pool lacks, the one passed over asked too, is missing`() = runBlocking<Unit> {
        val skipped = server(has = { false })
        val lacking = server(has = { false })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            val result = NzbEnrichmentService(service).enrich(nzb())

            assertIs<EnrichmentResult.MissingArticles>(result, "got $result")
            assertEquals(listOf("<seg0@test>"), skipped.bodiesRead, "the passed-over pool should be asked")
        }
    }

    @Test
    fun `a first article the passed-over pool drops the connection on fails to prepare, not as missing`() = runBlocking<Unit> {
        val skipped = server(has = { true }, dropAfter = 2_000)
        val lacking = server(has = { false })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            val result = NzbEnrichmentService(service).enrich(nzb())

            assertIs<EnrichmentResult.Failure>(result, "got $result")
        }
    }

    /** Prepare's ask of a pool passed over is bounded as a segment's is: one that never answers fails it as unavailable, soon. */
    @Test
    fun `a first article the passed-over pool never answers fails to prepare within the segment timeout, not as missing`() = runBlocking<Unit> {
        val skipped = server(has = { true }, held = true)
        val lacking = server(has = { false })
        service(skipped, lacking, config = StreamingConfig(segmentFetchTimeoutMs = 300)).use { service ->
            service.open(skipped)

            val result = withTimeout(5_000) { NzbEnrichmentService(service).enrich(nzb()) }

            val failure = assertIs<EnrichmentResult.Failure>(result, "got $result")
            assertTrue(failure.cause.chain().any { it is SegmentTimeoutException }, "failed with: ${failure.cause}")
        }
    }

    // A film and its PAR2 index, which the release opens without: asking the pool passed over for the index's first
    // article, which the others lack, can only find it there; that pool failing leaves the index out, as before it was
    // asked, rather than fail the release.

    private val film = Random(10).nextBytes(16 * 1024)
    private val index = Random(11).nextBytes(4 * 1024)

    private fun posted(id: String): ByteArray? = when (id) {
        "<film@test>" -> YencEncoder.encodeSinglePart(film, "Movie.2020.mkv").data
        "<index@test>" -> YencEncoder.encodeSinglePart(index, "Movie.2020.par2").data
        else -> null
    }

    /** A provider of the film and its index, but for what it [lacks]; [held]: its answers wait for the gate; [dropAfter]: as [server]'s. */
    private fun provider(lacks: (String) -> Boolean = { false }, dropAfter: Int? = null, held: Boolean = false) = FakeNntpServer(
        body = { id ->
            if (held) gate.await(60, TimeUnit.SECONDS)
            posted(id).takeUnless { lacks(id) }
        },
        dropAfter = { dropAfter },
    ).also { servers += it }

    private fun filmNzb() = NzbDocument(
        listOf("Movie.2020.mkv" to film to "film@test", "Movie.2020.par2" to index to "index@test").map { (named, id) ->
            val (name, data) = named
            NzbFile(
                poster = "test", date = 0, subject = "\"$name\" yEnc (1/1)", groups = listOf("alt.binaries.test"),
                segments = listOf(NzbSegment(data.size.toLong(), 1, id)),
            )
        }
    )

    @Test
    fun `a file the release opens without, whose first article the pools asked lack, is left out when the pool passed over fails`() = runBlocking<Unit> {
        val skipped = provider(dropAfter = 2_000)
        val lacking = provider(lacks = { it == "<index@test>" })
        service(skipped, lacking).use { service ->
            service.open(skipped)

            val result = NzbEnrichmentService(service).enrich(filmNzb())

            val success = assertIs<EnrichmentResult.Success>(result, "got $result")
            assertEquals(listOf("Movie.2020.mkv"), success.enrichedNzb.files.map { it.yencHeaders?.name })
            assertTrue("<index@test>" in skipped.bodiesRead, "the passed-over pool wasn't asked")
        }
    }

    @Test
    fun `a file the release opens without, whose first article the pools asked lack, is left out soon when the pool passed over never answers`() = runBlocking<Unit> {
        val skipped = provider(held = true)
        val lacking = provider(lacks = { it == "<index@test>" })
        service(skipped, lacking, config = StreamingConfig(segmentFetchTimeoutMs = 300)).use { service ->
            service.open(skipped)

            val result = withTimeout(5_000) { NzbEnrichmentService(service).enrich(filmNzb()) }

            val success = assertIs<EnrichmentResult.Success>(result, "got $result")
            assertEquals(listOf("Movie.2020.mkv"), success.enrichedNzb.files.map { it.yencHeaders?.name })
        }
    }
}
