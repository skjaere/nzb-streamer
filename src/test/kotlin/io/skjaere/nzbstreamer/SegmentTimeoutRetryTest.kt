package io.skjaere.nzbstreamer

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/**
 * A segment whose fetch timed out on a connection failed, and with one pool its stream did: a connection that died in
 * use cost the player a reopen. With [StreamingConfig.segmentTimeoutRetries] it is fetched again on another connection,
 * within the time the fetch already had.
 */
class SegmentTimeoutRetryTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> })

    // Holds back the answers a test wants never to come; opened in tearDown.
    private val gate = CountDownLatch(1)

    private val logger = LoggerFactory.getLogger(NntpStreamingService::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach
    fun setUp() {
        appender.start()
        logger.addAppender(appender)
    }

    @AfterEach
    fun tearDown() {
        logger.detachAppender(appender)
        gate.countDown()
        sessions.cancel()
        servers.forEach { it.close() }
    }

    private val segmentSize = 16 * 1024
    private fun bytesOf(name: String) = ByteArray(segmentSize) { (it * 7 + name.hashCode()).toByte() }

    /** Answers `BODY <name@test>` with [bytesOf] name, but never the BODYs [held] picks by the order read (from 0). */
    private fun server(held: (Int) -> Boolean): FakeNntpServer {
        val read = AtomicInteger()
        return FakeNntpServer(body = { id ->
            if (held(read.getAndIncrement())) gate.await(60, TimeUnit.SECONDS)
            YencEncoder.encodeSinglePart(bytesOf(id.removePrefix("<").substringBefore('@')), "data.bin").data
        }).also { servers += it }
    }

    private fun service(server: FakeNntpServer, streaming: StreamingConfig, connections: Int = 2) =
        service(listOf(server), streaming, connections)

    private fun service(servers: List<FakeNntpServer>, streaming: StreamingConfig, connections: Int = 2) =
        NntpStreamingService(
            servers.map { NntpConfig("127.0.0.1", it.port, "", "", useTls = false, maxConnections = connections) },
            streaming,
        ).also { runBlocking { it.connect() } }

    private val retrying = StreamingConfig(segmentFetchTimeoutMs = TIMEOUT_MS, segmentTimeoutRetries = 1)

    private fun warnings() = appender.list.toList().filter { it.level == Level.WARN }.map { it.formattedMessage }

    @Test
    fun `a segment whose fetch times out on one connection comes from another within the fetch's time`() = runBlocking<Unit> {
        val registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        try {
            // The first BODY is never answered, as on a connection that died in use; the rest are.
            val server = server(held = { it == 0 })
            service(server, retrying).use { service ->
                val started = TimeSource.Monotonic.markNow()
                assertContentEquals(bytesOf("a"), service.fetchSegment("a@test"))
                val took = started.elapsedNow()

                assertTrue(took < 1_200.milliseconds, "took $took")
                assertEquals(listOf("<a@test>", "<a@test>"), server.bodiesRead)
                assertEquals(2, server.connections.get())
                // The timeout tried again shows as one that failed would, in the log and the count.
                assertEquals(
                    listOf("Segment <a@test> timed out after ${TIMEOUT_MS}ms on pool[0] (127.0.0.1:${server.port}), trying another connection"),
                    warnings(),
                )
                assertEquals(1.0, registry.counter("nzb.segments.body_timeouts", "pool.name", "127.0.0.1:${server.port}").count())
            }
        } finally {
            Metrics.removeRegistry(registry)
        }
    }

    @Test
    fun `by default a segment fails at its first timeout`() = runBlocking<Unit> {
        val server = server(held = { it == 0 })
        service(server, StreamingConfig(segmentFetchTimeoutMs = TIMEOUT_MS)).use { service ->
            val failure = assertFailsWith<SegmentTimeoutException> { service.fetchSegment("a@test") }

            assertEquals("Segment <a@test> timed out after ${TIMEOUT_MS}ms on pool[0] (127.0.0.1:${server.port})", failure.message)
            assertEquals(listOf("<a@test>"), server.bodiesRead)
        }
    }

    @Test
    fun `a retry that times out too ends within the fetch's own time`() = runBlocking<Unit> {
        val server = server(held = { true })
        service(server, retrying).use { service ->
            val started = TimeSource.Monotonic.markNow()
            val failure = assertFailsWith<SegmentTimeoutException> { service.fetchSegment("a@test") }
            val took = started.elapsedNow()

            // A fetch has twice the timeout on a pool in all, its retry included.
            assertTrue(took >= (2 * TIMEOUT_MS).milliseconds && took < (2 * TIMEOUT_MS + 1_000).milliseconds, "failed after $took")
            assertEquals(
                "Segment <a@test> timed out after ${TIMEOUT_MS}ms and again on another connection on pool[0] (127.0.0.1:${server.port})",
                failure.message,
            )
            assertEquals(2, server.bodiesRead.size)
        }
    }

    @Test
    fun `a wait for a connection that runs out isn't tried again`() = runBlocking<Unit> {
        service(server(held = { false }), retrying, connections = 1).use { service ->
            // The pool's one connection is kept from it until the test ends.
            val holding = CompletableDeferred<Unit>()
            sessions.launch { service.withClient { holding.complete(Unit); CompletableDeferred<Unit>().await() } }
            withTimeout(10_000) { holding.await() }

            val started = TimeSource.Monotonic.markNow()
            val failure = assertFailsWith<SegmentTimeoutException> { service.fetchSegment("a@test") }
            val took = started.elapsedNow()

            // Waiting again would have run to the fetch's whole time, twice the timeout.
            assertTrue(took >= TIMEOUT_MS.milliseconds && took < (2 * TIMEOUT_MS - 100).milliseconds, "failed after $took")
            assertTrue(failure.message!!.contains("got no connection within ${TIMEOUT_MS}ms"), failure.message)
        }
    }

    /**
     * With several pools the retry stays on the pool it timed out on, and the next is asked only once that has failed
     * too: a connection that died in use on the first then costs one timeout and no fetch from a backup, which may be
     * paid by the gigabyte or lack the article.
     */
    @Test
    fun `with two pools a connection that died in use is tried again on its own pool, not the next`() = runBlocking<Unit> {
        val first = server(held = { it == 0 })
        val second = server(held = { false })
        service(listOf(first, second), retrying).use { service ->
            val started = TimeSource.Monotonic.markNow()
            assertContentEquals(bytesOf("a"), service.fetchSegment("a@test"))
            val took = started.elapsedNow()

            assertTrue(took < 1_200.milliseconds, "took $took")
            assertEquals(listOf("<a@test>", "<a@test>"), first.bodiesRead)
            assertEquals(emptyList(), second.bodiesRead)
        }
    }

    /** The price of the above: a pool whose every fetch stalls holds a segment twice the timeout before the next. */
    @Test
    fun `with two pools a pool whose fetches all stall is left for the next once its retry timed out too`() = runBlocking<Unit> {
        val first = server(held = { true })
        val second = server(held = { false })
        service(listOf(first, second), retrying).use { service ->
            val started = TimeSource.Monotonic.markNow()
            assertContentEquals(bytesOf("a"), service.fetchSegment("a@test"))
            val took = started.elapsedNow()

            assertTrue(took >= (2 * TIMEOUT_MS).milliseconds && took < (2 * TIMEOUT_MS + 1_000).milliseconds, "took $took")
            assertEquals(2, first.bodiesRead.size)
            assertEquals(listOf("<a@test>"), second.bodiesRead)
        }
    }

    private companion object {
        const val TIMEOUT_MS = 500L
    }
}
