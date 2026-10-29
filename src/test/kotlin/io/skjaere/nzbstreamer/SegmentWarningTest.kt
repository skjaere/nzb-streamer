package io.skjaere.nzbstreamer

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.skjaere.nntp.NntpConnectionLostException
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/**
 * What the log says at WARN, which a log without DEBUG lines keeps, of a segment no pool served. A fetch that got no
 * connection said it timed out (only its exception said otherwise), and a pool that failed otherwise (its connections
 * dropped, a protocol error) went unlogged when no pool was left to try: with one provider, always.
 */
class SegmentWarningTest {
    private val servers = mutableListOf<FakeNntpServer>()

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
        servers.forEach { it.close() }
    }

    private val article = YencEncoder.encodeSinglePart(ByteArray(16 * 1024) { it.toByte() }, "data.bin").data

    private fun server(
        body: (String) -> ByteArray? = { article },
        dropAfter: (String) -> Int? = { null },
        login: (String, String) -> Boolean = { _, _ -> true },
    ) = FakeNntpServer(body = body, dropAfter = dropAfter, login = login).also { servers += it }

    private fun service(
        vararg providers: FakeNntpServer,
        user: String = "",
        connections: Int = 1,
        config: StreamingConfig = StreamingConfig(segmentFetchTimeoutMs = TIMEOUT_MS),
    ) = NntpStreamingService(
        providers.map { NntpConfig("127.0.0.1", it.port, user, user, useTls = false, maxConnections = connections) },
        config,
    ).also { runBlocking { it.connect() } }

    private fun warnings() = appender.list.toList().filter { it.level == Level.WARN }.map { it.formattedMessage }

    @Test
    fun `a fetch that got no connection says so, on every pool`() = runBlocking<Unit> {
        // Logins never answered: each pool's connect waits on, and the fetch gets no connection.
        val first = server(login = { _, _ -> gate.await(60, TimeUnit.SECONDS); true })
        val second = server(login = { _, _ -> gate.await(60, TimeUnit.SECONDS); true })
        service(first, second, user = "user").use { service ->
            assertFailsWith<SegmentTimeoutException> { service.fetchSegment("a@test") }

            assertEquals(
                listOf(
                    "Segment <a@test> got no connection within ${TIMEOUT_MS}ms on pool[0] (127.0.0.1:${first.port}), trying pool[1]",
                    "Segment <a@test> got no connection within ${TIMEOUT_MS}ms on all 2 pool(s)",
                ),
                warnings(),
            )
        }
    }

    @Test
    fun `a fetch that timed out on a connection still says it timed out`() = runBlocking<Unit> {
        val server = server(body = { gate.await(60, TimeUnit.SECONDS); article })
        service(server).use { service ->
            assertFailsWith<SegmentTimeoutException> { service.fetchSegment("a@test") }

            assertEquals(listOf("Segment <a@test> timed out after ${TIMEOUT_MS}ms on all 1 pool(s)"), warnings())
        }
    }

    @Test
    fun `a pool that failed is named when no pool is left to try`() = runBlocking<Unit> {
        // Each connection drops partway through the article, the pool's own retry's too.
        val server = server(dropAfter = { 100 })
        service(server).use { service ->
            assertFailsWith<NntpConnectionLostException> { service.fetchSegment("a@test") }

            val lines = warnings()
            assertEquals(1, lines.size, "$lines")
            // Then the connection's own words.
            val expected = "Segment <a@test> NNTP error on pool[0] (127.0.0.1:${server.port}), no pool left to try — NntpConnectionLostException: "
            assertTrue(lines.single().startsWith(expected), lines.single())
        }
    }

    /**
     * A fetch whose connection dropped is fetched again by the pool on another, inside the same try. When no other came
     * in time, the fetch had had a connection once, and said it timed out on one: a dead or slow connection, where the
     * cause was getting none (the account at its limit, a login that never finished).
     */
    @Test
    fun `a fetch whose connection dropped and got no other says so`() = runBlocking<Unit> {
        // The first connection drops partway through the article, and the login of the one the pool opens in its place
        // is never answered.
        val logins = AtomicInteger()
        val server = server(
            dropAfter = { 100 },
            login = { _, _ -> if (logins.getAndIncrement() > 0) gate.await(60, TimeUnit.SECONDS); true },
        )
        service(server, user = "user").use { service ->
            val failure = assertFailsWith<SegmentTimeoutException> { service.fetchSegment("a@test") }

            assertEquals(
                listOf("Segment <a@test> lost its connection and got no other in time on all 1 pool(s)"),
                warnings(),
            )
            assertEquals(
                "Segment <a@test> lost its connection and got no other in time on pool[0] (127.0.0.1:${server.port})",
                failure.message,
            )
        }
    }

    @Test
    fun `a retry whose connection dropped and got no other says so`() = runBlocking<Unit> {
        // The first BODY is never answered; the retry's connection drops partway through the article, and the login of
        // the one the pool opens in its place is never answered.
        val bodies = AtomicInteger()
        val logins = AtomicInteger()
        val server = server(
            body = { if (bodies.incrementAndGet() == 1) gate.await(60, TimeUnit.SECONDS); article },
            dropAfter = { if (bodies.get() == 2) 100 else null },
            login = { _, _ -> if (logins.getAndIncrement() > 1) gate.await(60, TimeUnit.SECONDS); true },
        )
        val retrying = StreamingConfig(segmentFetchTimeoutMs = TIMEOUT_MS, segmentTimeoutRetries = 1)
        service(server, user = "user", connections = 3, config = retrying).use { service ->
            assertFailsWith<SegmentTimeoutException> { service.fetchSegment("a@test") }

            assertEquals(
                listOf(
                    "Segment <a@test> timed out after ${TIMEOUT_MS}ms on pool[0] (127.0.0.1:${server.port}), trying another connection",
                    "Segment <a@test> timed out after ${TIMEOUT_MS}ms, then lost the next connection and got no other in time on all 1 pool(s)",
                ),
                warnings(),
            )
        }
    }

    private companion object {
        const val TIMEOUT_MS = 500L
    }
}
