package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.ktor.utils.io.toByteArray
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.skjaere.nzbstreamer.config.CrcMismatchPolicy
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentCache
import io.skjaere.nntp.YencCrcMismatchException
import io.skjaere.nntp.YencHeaders
import io.skjaere.yenc.RapidYenc
import io.skjaere.yenc.YencEncoder
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * A copy of an article whose yEnc CRC isn't the one its =yend line gives is a damaged copy on that
 * provider, not the provider failing: the next pool is tried, and the first pool's circuit breaker
 * isn't told. When no pool has a good copy, the first damaged one is used, with a warning (the
 * default, StreamingConfig.crcMismatch TOLERATE), and never cached, so a later play tries again.
 */
class CrcMismatchTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val registry = SimpleMeterRegistry().also { Metrics.addRegistry(it) }

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
        Metrics.removeRegistry(registry)
    }

    private val segments = (0 until 12).map { Random(it).nextBytes(16 * 1024) }

    private fun String.segment() = removePrefix("<seg").substringBefore('@').toInt()

    private fun good(data: ByteArray) = YencEncoder.encodeSinglePart(data, "data.bin").data

    /** [data]'s article with the CRC its =yend line gives a bit off: a damaged copy, as a provider may hold one. */
    private fun damaged(data: ByteArray): ByteArray {
        fun field(crc: UInt) = "crc32=" + crc.toString(16).padStart(8, '0')
        val crc = RapidYenc.crc32(data)
        val article = String(good(data), Charsets.ISO_8859_1)
        check(field(crc) in article) { "test setup: the article's crc32" }
        return article.replace(field(crc), field(crc xor 1u)).toByteArray(Charsets.ISO_8859_1)
    }

    private fun server(copy: (ByteArray) -> ByteArray) =
        FakeNntpServer(body = { id -> copy(segments[id.segment()]) }).also { servers += it }

    private fun service(vararg servers: FakeNntpServer, config: StreamingConfig = StreamingConfig(), cache: SegmentCache? = null) =
        NntpStreamingService(
            servers.map { NntpConfig("127.0.0.1", it.port, "", "", useTls = false, maxConnections = 2) },
            config,
            cache,
            closeSegmentCache = false,
        ).also { runBlocking { it.connect() } }

    private fun cache(dir: Path) = SegmentCache(SegmentCacheConfig(dir, maxBytes = 100L * 1024 * 1024))

    private fun file(count: Int) = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = (0 until count).map { NzbSegment(segments[it].size.toLong(), it + 1, "seg$it@test") },
        yencHeaders = YencHeaders(line = 128, size = segments.take(count).sumOf { it.size.toLong() }, name = "data.bin", partEnd = segments[0].size.toLong()),
    )

    private fun firstSegments(count: Int) = segments.take(count).reduce { a, b -> a + b }

    private suspend fun NntpStreamingService.streamed(count: Int): ByteArray {
        var bytes = ByteArray(0)
        streamSegments(SegmentQueueService.createFileQueue(file(count), 0, 0L), name = "test") { bytes = it.toByteArray() }
        return bytes
    }

    private fun NntpStreamingService.breakerOf(server: FakeNntpServer): CircuitBreaker {
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        field.isAccessible = true
        return (field.get(this) as CircuitBreakerRegistry).circuitBreaker("127.0.0.1:${server.port}")
    }

    /** Segments used despite a CRC mismatch, as the metric counts them. */
    private fun usedDespiteMismatch() = registry.find("nzb.segments.crc_mismatch_used").counter()?.count() ?: 0.0

    /** An NZB of one file whose first article is seg0's. */
    private fun nzb() = NzbDocument(
        listOf(
            NzbFile(
                poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
                segments = listOf(NzbSegment(segments[0].size.toLong(), 1, "seg0@test")),
            )
        )
    )

    @Test
    fun `a damaged copy on one pool is fetched from the next, without opening the first pool's breaker`() = runBlocking<Unit> {
        val primary = server(::damaged)
        val fill = server(::good)
        service(primary, fill).use { service ->
            segments.indices.forEach { assertContentEquals(segments[it], service.fetchSegment("seg$it@test")) }

            val breaker = service.breakerOf(primary)
            assertEquals(CircuitBreaker.State.CLOSED, breaker.state, "the first pool's breaker")
            assertEquals(0, breaker.metrics.numberOfFailedCalls, "failures recorded on the first pool's breaker")
            assertEquals(segments.size, primary.bodiesRead.size, "the first pool should be asked for every segment: ${primary.bodiesRead}")
            assertEquals(0.0, usedDespiteMismatch())
        }
    }

    @Test
    fun `a segment no pool has a good copy of is used, and not cached`(@TempDir tmp: Path) = runBlocking<Unit> {
        val primary = server(::damaged)
        val fill = server(::damaged)
        cache(tmp).use { cache ->
            service(primary, fill, cache = cache).use { service ->
                assertContentEquals(firstSegments(4), service.streamed(4))
                (0 until 4).forEach { assertNull(cache.get("seg$it@test"), "seg$it's damaged copy was cached") }
                assertEquals(4.0, usedDespiteMismatch())

                // Not cached: a later play asks the providers again.
                assertContentEquals(firstSegments(4), service.streamed(4))
                assertEquals(8, primary.bodiesRead.size, "bodies asked of the first pool: ${primary.bodiesRead}")
            }
        }
    }

    /** STRICT: a segment no pool has a good copy of fails as a timeout or a dropped connection does, reported. */
    @Test
    fun `with crcMismatch STRICT a segment no pool has a good copy of fails the stream with the mismatch`(@TempDir tmp: Path) =
        runBlocking<Unit> {
            val primary = server { if (it === segments[2]) damaged(it) else good(it) }
            val fill = server { if (it === segments[2]) damaged(it) else good(it) }
            cache(tmp).use { cache ->
                service(primary, fill, config = StreamingConfig(crcMismatch = CrcMismatchPolicy.STRICT), cache = cache).use { service ->
                    val failure = assertFailsWith<YencCrcMismatchException> { service.streamed(4) }
                    assertNotNull(failure.article, "the mismatch should carry the copy it found")
                    assertNull(cache.get("seg2@test"), "the damaged copy was cached")
                    assertEquals(0.0, usedDespiteMismatch())
                    assertEquals(0, service.breakerOf(primary).metrics.numberOfFailedCalls, "failures recorded on the first pool's breaker")
                }
            }
        }

    /** A fetch that waits on another's for the same segment (the segment cache's singleflight) gets the same bytes. */
    @Test
    fun `a damaged copy another fetch waited on is its bytes too, and not cached`(@TempDir tmp: Path) = runBlocking<Unit> {
        val asked = CompletableDeferred<Unit>()
        val answer = CountDownLatch(1)
        val provider = FakeNntpServer(body = { id ->
            asked.complete(Unit)
            answer.await(10, TimeUnit.SECONDS)
            damaged(segments[id.segment()])
        }).also { servers += it }
        cache(tmp).use { cache ->
            service(provider, cache = cache).use { service ->
                val leader = async { service.fetchSegment("seg0@test") }
                asked.await()
                // Undispatched, it runs until it waits on the leader's fetch.
                val follower = async(start = CoroutineStart.UNDISPATCHED) { service.fetchSegment("seg0@test") }
                answer.countDown()

                assertContentEquals(segments[0], leader.await())
                assertContentEquals(segments[0], follower.await())
                assertEquals(listOf("<seg0@test>"), provider.bodiesRead, "the follower should wait on the leader's fetch")
                assertNull(cache.get("seg0@test"), "the damaged copy was cached")
            }
        }
    }

    @Test
    fun `good copies stream and are cached as before`(@TempDir tmp: Path) = runBlocking<Unit> {
        val primary = server(::good)
        val fill = server(::good)
        cache(tmp).use { cache ->
            service(primary, fill, cache = cache).use { service ->
                assertContentEquals(firstSegments(4), service.streamed(4))
                (0 until 4).forEach { assertContentEquals(segments[it], cache.get("seg$it@test")) }
                assertEquals(emptyList(), fill.bodiesRead)
                assertEquals(0.0, usedDespiteMismatch())
            }
        }
    }

    /** Prepare reads a file's name and size from its first article: a damaged copy of it still has its headers. */
    @Test
    fun `a release whose first article no pool has a good copy of is prepared with it, not cached`(@TempDir tmp: Path) =
        runBlocking<Unit> {
            val provider = server(::damaged)
            cache(tmp).use { cache ->
                service(provider, cache = cache).use { service ->
                    val result = NzbEnrichmentService(service).enrich(nzb())

                    val success = assertIs<EnrichmentResult.Success>(result, "got $result")
                    assertEquals("data.bin", success.enrichedNzb.files.single().yencHeaders?.name)
                    assertNull(service.cachedArticle("seg0@test"), "the damaged copy was cached")
                    assertEquals(1.0, usedDespiteMismatch())
                }
            }
        }

    @Test
    fun `with crcMismatch STRICT a release whose first article no pool has a good copy of fails to prepare`() = runBlocking<Unit> {
        val provider = server(::damaged)
        service(provider, config = StreamingConfig(crcMismatch = CrcMismatchPolicy.STRICT)).use { service ->
            val result = NzbEnrichmentService(service).enrich(nzb())

            val failure = assertIs<EnrichmentResult.Failure>(result, "got $result")
            assertIs<YencCrcMismatchException>(failure.cause)
        }
    }

    @Test
    fun `a first article damaged on one pool is prepared from the next, without counting against the first`(@TempDir tmp: Path) =
        runBlocking<Unit> {
            val primary = server(::damaged)
            val fill = server(::good)
            cache(tmp).use { cache ->
                service(primary, fill, cache = cache).use { service ->
                    val result = NzbEnrichmentService(service).enrich(nzb())

                    assertIs<EnrichmentResult.Success>(result, "got $result")
                    assertContentEquals(segments[0], assertNotNull(service.cachedArticle("seg0@test")).second)
                    assertEquals(0, service.breakerOf(primary).metrics.numberOfFailedCalls, "failures recorded on the first pool's breaker")
                    assertTrue("<seg0@test>" in fill.bodiesRead)
                    assertEquals(0.0, usedDespiteMismatch())
                }
            }
        }
}
