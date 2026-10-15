package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.PrepareConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertIs

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
                ),
                mapOf(
                    "concurrency" to streamer.getStreamingConcurrency().toLong(),
                    "verificationConcurrency" to streamer.getVerificationConcurrency().toLong(),
                    "readAheadSegments" to streamer.getReadAheadSegments().toLong(),
                    "segmentFetchTimeoutMs" to streamer.getSegmentFetchTimeoutMs(),
                    "circuitBreakerFailureThreshold" to streamer.getCircuitBreakerFailureThreshold().toLong(),
                    "circuitBreakerCooldownMs" to streamer.getCircuitBreakerCooldownMs(),
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
}
