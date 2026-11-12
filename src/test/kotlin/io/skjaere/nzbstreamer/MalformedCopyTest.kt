package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.ktor.utils.io.toByteArray
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nntp.YencDecodingException
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
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
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * A copy of an article whose yEnc header can't be read (an empty or non-numeric number) is a bad copy on that
 * provider, not the provider failing: the next pool is tried, and the first pool's circuit breaker isn't told. With no
 * readable copy on any pool, the read fails as a malformed article (corrupt, to a caller), never as a missing one.
 */
class MalformedCopyTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val gate = CountDownLatch(1)

    @AfterEach
    fun tearDown() {
        gate.countDown()
        servers.forEach { it.close() }
    }

    private val segments = (0 until 12).map { Random(it).nextBytes(16 * 1024) }

    private fun String.segment() = removePrefix("<seg").substringBefore('@').toInt()

    private fun good(data: ByteArray) = YencEncoder.encodeSinglePart(data, "data.bin").data

    /** [data]'s article with its =ybegin line's size left empty, as a provider was seen to serve one. */
    private fun malformed(data: ByteArray): ByteArray {
        val article = String(good(data), Charsets.ISO_8859_1)
        val size = " size=${data.size} name="
        check(size in article) { "test setup: the article's =ybegin size" }
        return article.replaceFirst(size, " size= name=").toByteArray(Charsets.ISO_8859_1)
    }

    private fun server(copy: (Int) -> ByteArray?) =
        FakeNntpServer(body = { id -> copy(id.segment()) }).also { servers += it }

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

    private fun file(count: Int) = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = (0 until count).map { NzbSegment(segments[it].size.toLong(), it + 1, "seg$it@test") },
        yencHeaders = YencHeaders(line = 128, size = segments.take(count).sumOf { it.size.toLong() }, name = "data.bin", partEnd = segments[0].size.toLong()),
    )

    private suspend fun NntpStreamingService.streamed(count: Int): ByteArray {
        var bytes = ByteArray(0)
        streamSegments(SegmentQueueService.createFileQueue(file(count), 0, 0L), name = "test") { bytes = it.toByteArray() }
        return bytes
    }

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
    fun `a malformed copy on one pool is fetched from the next, without the first pool's breaker hearing of it`() = runBlocking<Unit> {
        val primary = server { malformed(segments[it]) }
        val fill = server { good(segments[it]) }
        service(primary, fill).use { service ->
            segments.indices.forEach { assertContentEquals(segments[it], service.fetchSegment("seg$it@test")) }

            val breaker = service.breakerOf(primary)
            assertEquals(0, breaker.metrics.numberOfFailedCalls, "failures recorded on the first pool's breaker")
            assertEquals(CircuitBreaker.State.CLOSED, breaker.state, "the first pool's breaker")
            assertEquals(segments.size, primary.bodiesRead.size, "the first pool should be asked for every segment: ${primary.bodiesRead}")
        }
    }

    @Test
    fun `a segment no pool has a readable copy of fails the stream as malformed, not missing`() = runBlocking<Unit> {
        val primary = server { if (it == 2) malformed(segments[it]) else good(segments[it]) }
        val fill = server { if (it == 2) malformed(segments[it]) else good(segments[it]) }
        service(primary, fill).use { service ->
            val failure = runCatching { service.streamed(4) }.exceptionOrNull()
            assertTrue(failure.chain().any { it is YencDecodingException }, "failed with: $failure")
            assertTrue(failure.chain().none { it is ArticleNotFoundException }, "called missing: $failure")
            assertEquals(0, service.breakerOf(primary).metrics.numberOfFailedCalls, "failures recorded on the first pool's breaker")
        }
    }

    /** A provider that lacks the article and one whose copy can't be read: no readable copy, but not a missing article. */
    @Test
    fun `a segment one pool lacks and another has only a malformed copy of is malformed, not missing`() = runBlocking<Unit> {
        val lacking = server { if (it == 2) null else good(segments[it]) }
        val bad = server { malformed(segments[it]) }
        service(lacking, bad).use { service ->
            val failure = runCatching { service.fetchSegment("seg2@test") }.exceptionOrNull()
            assertIs<YencDecodingException>(failure, "failed with: $failure")
        }
    }

    /** A pool that timed out may have a good copy: that says more than another's malformed one. */
    @Test
    fun `a segment malformed on one pool and timed out on another fails with the timeout`() = runBlocking<Unit> {
        val bad = server { malformed(segments[it]) }
        val slow = server {
            gate.await(60, TimeUnit.SECONDS)
            good(segments[it])
        }
        service(bad, slow, config = StreamingConfig(segmentFetchTimeoutMs = 300)).use { service ->
            val failure = runCatching { service.fetchSegment("seg2@test") }.exceptionOrNull()
            assertIs<SegmentTimeoutException>(failure, "failed with: $failure")
        }
    }

    /** Prepare reads a file's name and size from its first article's headers: those are the ones that can't be read. */
    @Test
    fun `a first article malformed on one pool is prepared from the next, without counting against the first`() = runBlocking<Unit> {
        val primary = server { malformed(segments[it]) }
        val fill = server { good(segments[it]) }
        service(primary, fill).use { service ->
            val result = NzbEnrichmentService(service).enrich(nzb())

            val success = assertIs<EnrichmentResult.Success>(result, "got $result")
            assertEquals("data.bin", success.enrichedNzb.files.single().yencHeaders?.name)
            assertEquals(0, service.breakerOf(primary).metrics.numberOfFailedCalls, "failures recorded on the first pool's breaker")
            assertTrue("<seg0@test>" in fill.bodiesRead)
        }
    }

    /**
     * A provider whose connection dropped may have a good copy: that says more than another's malformed one, whichever
     * pool comes first, as for a segment. The failure is then the provider's (a caller may recheck its providers), not a
     * corrupt release.
     */
    @Test
    fun `a first article malformed on one pool and dropped on another fails to prepare with the dropped connection, in either order`() =
        runBlocking<Unit> {
            for (droppingFirst in listOf(true, false)) {
                val bad = server { malformed(segments[it]) }
                val dropping = FakeNntpServer(body = { id -> good(segments[id.segment()]) }, dropAfter = { 2_000 }).also { servers += it }
                val pools = if (droppingFirst) arrayOf(dropping, bad) else arrayOf(bad, dropping)
                service(*pools).use { service ->
                    val result = NzbEnrichmentService(service).enrich(nzb())

                    val order = if (droppingFirst) "dropping, malformed" else "malformed, dropping"
                    val failure = assertIs<EnrichmentResult.Failure>(result, "$order: got $result")
                    assertTrue(failure.cause.chain().none { it is YencDecodingException }, "$order: failed with ${failure.cause}")
                    assertTrue(failure.cause.chain().any { it is NntpConnectionException }, "$order: failed with ${failure.cause}")
                }
            }
        }

    @Test
    fun `a release whose first article no pool has a readable copy of fails to prepare as malformed`() = runBlocking<Unit> {
        // In either order, beside a pool that lacks it: still no readable copy, and not a missing article.
        for (order in listOf(true, false)) {
            val bad = server { malformed(segments[it]) }
            val lacking = server { null }
            val pools = if (order) arrayOf(bad, lacking) else arrayOf(lacking, bad)
            service(*pools).use { service ->
                val result = NzbEnrichmentService(service).enrich(nzb())

                val failure = assertIs<EnrichmentResult.Failure>(result, "got $result")
                assertTrue(failure.cause.chain().any { it is YencDecodingException }, "failed with: ${failure.cause}")
            }
        }
    }
}
