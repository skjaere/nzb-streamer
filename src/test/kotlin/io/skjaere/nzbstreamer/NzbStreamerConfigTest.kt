package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.PoolTuning
import io.skjaere.nzbstreamer.config.PrepareConfig
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.stream.SegmentCache
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull

/** [NzbStreamer.fromConfig] builds a streamer with the configs it is given, every setting kept. */
class NzbStreamerConfigTest {
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    private fun config(server: FakeNntpServer, maxConnections: Int = 2, priority: Int = 0) =
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = maxConnections, priority = priority)

    private fun server(body: (String) -> ByteArray? = { null }) = FakeNntpServer(body = body).also { servers += it }

    @Test
    fun `fromConfig keeps every streaming setting`() {
        val streaming = StreamingConfig(
            concurrency = 3,
            verificationConcurrency = 5,
            readAheadSegments = 7,
            segmentFetchTimeoutMs = 1_234,
            circuitBreakerFailureThreshold = 9,
            circuitBreakerCooldownMs = 4_321,
            initialReadAheadSegments = 3,
        )
        NzbStreamer.fromConfig(config(server()), streaming).use { streamer ->
            assertEquals(
                mapOf(
                    "concurrency" to 3L,
                    "verificationConcurrency" to 5L,
                    "readAheadSegments" to 7L,
                    "segmentFetchTimeoutMs" to 1_234L,
                    "circuitBreakerFailureThreshold" to 9L,
                    "circuitBreakerCooldownMs" to 4_321L,
                    "initialReadAheadSegments" to 3L,
                ),
                mapOf(
                    "concurrency" to streamer.getStreamingConcurrency().toLong(),
                    "verificationConcurrency" to streamer.getVerificationConcurrency().toLong(),
                    "readAheadSegments" to streamer.getReadAheadSegments().toLong(),
                    "segmentFetchTimeoutMs" to streamer.getSegmentFetchTimeoutMs(),
                    "circuitBreakerFailureThreshold" to streamer.getCircuitBreakerFailureThreshold().toLong(),
                    "circuitBreakerCooldownMs" to streamer.getCircuitBreakerCooldownMs(),
                    "initialReadAheadSegments" to streamer.getInitialReadAheadSegments()?.toLong(),
                ),
            )
        }
    }

    /** A consumer that compares its configs with the running pools' must find them equal. */
    @Test
    fun `fromConfig keeps the pool configs it is given`() {
        val configs = listOf(config(server(), maxConnections = 3, priority = 1), config(server(), priority = 2))
        NzbStreamer.fromConfig(configs).use { streamer ->
            assertEquals(configs, streamer.getPoolConfigs())
        }
    }

    @Test
    fun `fromConfig keeps the enrichment concurrency`() = runBlocking<Unit> {
        val active = AtomicInteger()
        val peak = AtomicInteger()
        val provider = server { _ ->
            peak.accumulateAndGet(active.incrementAndGet(), ::maxOf)
            Thread.sleep(100)
            active.decrementAndGet()
            YencEncoder.encodeSinglePart(ByteArray(64) { 0x42 }, "data.bin").data
        }
        val files = (1..6).joinToString("") { i ->
            """<file poster="t" date="0" subject="file $i"><groups><group>a.b</group></groups>""" +
                """<segments><segment bytes="100" number="1">f$i@test</segment></segments></file>"""
        }
        val nzb = """<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">$files</nzb>"""

        NzbStreamer.fromConfig(
            config(provider, maxConnections = 4),
            prepareConfig = PrepareConfig(enrichmentConcurrency = 1),
        ).use { streamer ->
            assertIs<PrepareResult.Success>(streamer.prepare(nzb.toByteArray()))
        }
        assertEquals(1, peak.get(), "BODYs at once while preparing")
    }

    /**
     * A cache the caller keeps outlives the streamers it lends it to: an app that builds a new
     * streamer on every settings change keeps one cache (two instances on one directory would
     * each take the other's files for their own).
     */
    @Test
    fun `a streamer's close leaves a cache it was lent open`(@TempDir tmp: Path) {
        runBlocking {
            val cache = SegmentCache(SegmentCacheConfig(tmp, maxBytes = 1_000_000L))
            cache.getOrFetch("a@id") { ByteArray(64) { 1 } }
            val server = server()

            NzbStreamer.fromConfig(listOf(config(server)), segmentCache = cache, closeSegmentCache = false).close()
            NzbStreamer {
                nntp {
                    host = "127.0.0.1"
                    port = server.port
                    useTls = false
                }
                segmentCache = cache
                closeSegmentCache = false
            }.close()

            assertContentEquals(ByteArray(64) { 1 }, cache.get("a@id"))
        }
    }

    /** By default a streamer closes the cache it is given, saving its index (a server's shutdown relies on it). */
    @Test
    fun `a streamer's close closes its cache by default`(@TempDir tmp: Path) {
        runBlocking {
            val cache = SegmentCache(SegmentCacheConfig(tmp, maxBytes = 1_000_000L))
            cache.getOrFetch("a@id") { ByteArray(64) { 1 } }

            NzbStreamer.fromConfig(listOf(config(server())), segmentCache = cache).close()

            assertNull(cache.get("a@id"))
        }
    }

    /**
     * A data class's toString prints every value: a config logged, or put in an exception's message,
     * carried the provider's password with it.
     */
    @Test
    fun `an NNTP config's toString leaves out the password`() {
        val config = NntpConfig("news.example.com", 563, "alice", "correct horse, battery", useTls = true, maxConnections = 4, priority = 1)
        assertEquals(
            "NntpConfig(host=news.example.com, port=563, username=alice, useTls=true, maxConnections=4, priority=1, verifyHostname=true)",
            config.toString(),
        )
        assertFalse("horse" in "$config")
    }

    /**
     * Pools built from a config that says nothing of them (debridav's) keep the NNTP client's own timings: a DATE a
     * minute to the connections idle that long, given 10 s to answer, and no check before a connection is lent.
     */
    @Test
    fun `a streaming config's pool tuning is the NNTP client's own by default`() {
        assertEquals(
            PoolTuning(keepaliveIntervalMs = 60_000, keepaliveTimeoutMs = 10_000, validateAfterIdleMs = 0, validateTimeoutMs = 2_000),
            StreamingConfig().poolTuning,
        )
    }

    /** A config that says nothing of it (debridav's) fails a segment at its first timeout, as before. */
    @Test
    fun `a streaming config tries no timed-out segment again by default`() {
        assertEquals(0, StreamingConfig().segmentTimeoutRetries)
        assertFailsWith<IllegalArgumentException> { StreamingConfig(segmentTimeoutRetries = -1) }
    }

    /** setReadAheadBytes refused a budget of no bytes, and a config took one: it streamed a segment at a time. */
    @Test
    fun `a streaming config refuses a read-ahead of no bytes`() {
        listOf(0L, -1L).forEach { bytes -> assertFailsWith<IllegalArgumentException>("$bytes bytes") { StreamingConfig(readAheadBytes = bytes) } }
        assertEquals(1L, StreamingConfig(readAheadBytes = 1).readAheadBytes)
        assertNull(StreamingConfig().readAheadBytes)
    }
}
