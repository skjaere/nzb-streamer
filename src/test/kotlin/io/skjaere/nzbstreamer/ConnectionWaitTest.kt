package io.skjaere.nzbstreamer

import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.readFully
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * The segment fetch timeout covered the wait for a connection too. With every connection fetching,
 * a segment queued behind them had what the others left of it: a stream fetching as many at once as
 * the pool has connections timed out whole waves of segments on a slow link, and a stream started
 * as another one's cancelled fetches were read off their connections (a seek) timed out its first.
 */
class ConnectionWaitTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> })

    // Holds back the answers a test wants never to come; opened in tearDown.
    private val gate = CountDownLatch(1)

    @AfterEach
    fun tearDown() {
        gate.countDown()
        sessions.cancel()
        servers.forEach { it.close() }
    }

    private val segmentSize = 16 * 1024
    private fun bytesOf(id: String) = ByteArray(segmentSize) { (it * 7 + id.hashCode()).toByte() }

    /** Answers `BODY <name@test>` with [bytesOf] name, after [answerAfterMs] says how long; [held] ids are answered once [gate] opens. */
    private fun server(answerAfterMs: (String) -> Long = { 0 }, held: (String) -> Boolean = { false }) = FakeNntpServer(body = { id ->
        val name = id.removePrefix("<").substringBefore('@')
        if (held(name)) gate.await(60, TimeUnit.SECONDS)
        Thread.sleep(answerAfterMs(name))
        YencEncoder.encodeSinglePart(bytesOf(name), "$name.bin").data
    }).also { servers += it }

    private fun service(server: FakeNntpServer, connections: Int, timeoutMs: Long = 5_000) = NntpStreamingService(
        listOf(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = connections)),
        StreamingConfig(segmentFetchTimeoutMs = timeoutMs),
    ).also { runBlocking { it.connect() } }

    private suspend fun FakeNntpServer.awaitBodies(vararg names: String) = withTimeout(10_000) {
        while (!bodiesRead.containsAll(names.map { "<$it@test>" })) delay(10)
    }

    @Test
    fun `a segment that waits for a busy pool's connection gets the whole timeout once it has one`() = runBlocking<Unit> {
        // The one connection is fetching for 4 s; the next segment then takes 2 s: 6 s in all.
        val server = server(answerAfterMs = { name -> if (name == "busy") 4_000 else 2_000 })
        service(server, connections = 1).use { service ->
            val busy = async(Dispatchers.IO) { service.fetchSegment("busy@test") }
            server.awaitBodies("busy")
            assertContentEquals(bytesOf("next"), service.fetchSegment("next@test"))
            assertContentEquals(bytesOf("busy"), busy.await())
        }
    }

    @Test
    fun `a wait for a connection that never comes still fails within the timeout`() = runBlocking<Unit> {
        val timeoutMs = 1_000L
        service(server(), connections = 1, timeoutMs).use { service ->
            // The pool's one connection is kept from it until the test ends.
            val holding = CompletableDeferred<Unit>()
            val holder = sessions.launch { service.withClient { holding.complete(Unit); CompletableDeferred<Unit>().await() } }
            withTimeout(10_000) { holding.await() }
            val started = TimeSource.Monotonic.markNow()
            assertFailsWith<SegmentTimeoutException> { service.fetchSegment("next@test") }
            val waited = started.elapsedNow()
            // The pool's own wait for a connection is 30 s.
            assertTrue(waited >= timeoutMs.milliseconds && waited < (3 * timeoutMs).milliseconds, "failed after $waited")
            holder.cancel()
        }
    }

    private fun file(vararg names: String) = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = names.mapIndexed { i, name -> NzbSegment(segmentSize.toLong(), i + 1, "$name@test") },
        yencHeaders = YencHeaders(line = 128, size = names.size.toLong() * segmentSize, name = "data.bin", partEnd = segmentSize.toLong()),
    )

    /** Starts streaming [file] from a session coroutine, as an Android app does, fetching as many at once as the pool has connections. */
    private suspend fun start(service: NntpStreamingService, file: NzbFile): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(service.launchStreamSegments(SegmentQueueService.createFileQueue(file, 0, 0L), concurrency = 2, readAheadSegments = 6, name = "test"))
        }
        return writer.await()
    }

    @Test
    fun `a stream started while a cancelled stream's fetches are read off gets its first segment`() = runBlocking<Unit> {
        // The old stream's answers never come: each of its connections is read for 3 s after it is
        // cancelled, then dropped and opened again. The new stream's first segment then takes 3 s.
        val server = server(answerAfterMs = { name -> if (name == "new0") 3_000 else 0 }, held = { it.startsWith("old") })
        service(server, connections = 2).use { service ->
            val old = start(service, file("old0", "old1"))
            server.awaitBodies("old0", "old1")
            // A seek: the player stops reading the old stream and opens a new one at once.
            old.job.cancel()
            val new = start(service, file("new0", "new1"))
            val first = ByteArray(segmentSize)
            withTimeout(20_000) { new.channel.readFully(first) }
            assertContentEquals(bytesOf("new0"), first)
        }
    }
}
