package io.skjaere.nzbstreamer

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readFully
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.PoolTuning
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import java.io.ByteArrayOutputStream
import java.io.IOException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * StreamingConfig's pool tuning reaches every pool. Without it each pool kept the NNTP client's own timings: no check
 * before an idle connection was lent, and a DATE a minute. So a stream that rested while a NAT, a VPN or the provider
 * dropped its idle connections fetched on them when it read on, and failed at the segment timeout.
 */
class PoolTuningTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> })

    @AfterEach
    fun tearDown() {
        sessions.cancel()
        servers.forEach { it.close() }
    }

    private val segmentSize = 256 * 1024
    private val segments = List(32) { i -> ByteArray(segmentSize) { (it * 7 + i).toByte() } }
    private val encoded = segments.map { YencEncoder.encodeSinglePart(it, "data.bin").data }

    /** Answers `BODY <s<i>@test>` with segment i, its connections dying after [idleAfterMs] idle. */
    private fun server(idleAfterMs: Long) = FakeNntpServer(
        body = { id -> encoded[id.removePrefix("<s").substringBefore('@').toInt()] },
        idleAfterMs = idleAfterMs,
    ).also { servers += it }

    private fun config(server: FakeNntpServer, connections: Int) =
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = connections)

    private val file = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = segments.indices.map { NzbSegment(segmentSize.toLong(), it + 1, "s$it@test") },
        yencHeaders = YencHeaders(line = 128, size = segments.size.toLong() * segmentSize, name = "data.bin", partEnd = segmentSize.toLong()),
    )

    /** Starts streaming [file] from a session coroutine, as the app does. */
    private suspend fun start(service: NntpStreamingService): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(service.launchStreamSegments(SegmentQueueService.createFileQueue(file, 0, 0L), concurrency = 3, readAheadSegments = 3, name = "test"))
        }
        return writer.await()
    }

    /** Everything readable from the channel, and why it ended: its close cause (null at a plain end). */
    private suspend fun ByteReadChannel.readToEnd(): Pair<ByteArray, Throwable?> {
        val out = ByteArrayOutputStream()
        val buffer = ByteArray(64 * 1024)
        while (true) {
            val n = try {
                readAvailable(buffer)
            } catch (e: IOException) {
                return out.toByteArray() to e
            }
            if (n == -1) return out.toByteArray() to closedCause
            out.write(buffer, 0, n)
        }
    }

    @Test
    fun `a stream that rests past the provider's silence reads on, its dead connections checked and replaced`() = runBlocking<Unit> {
        val server = server(idleAfterMs = 300)
        // The connection wait (the fetch timeout) is shorter than the pool's own 2 s check: the check only fits with the
        // tuning's 300 ms.
        val streaming = StreamingConfig(
            segmentFetchTimeoutMs = 1_500,
            poolTuning = PoolTuning(validateAfterIdleMs = 100, validateTimeoutMs = 300),
        )
        NntpStreamingService(listOf(config(server, connections = 4)), streaming).use { service ->
            service.connect()
            val channel = start(service).channel
            val head = ByteArray(2 shl 20)
            withTimeout(20_000) { channel.readFully(head) }
            // The writer fills the channel and its read-ahead at once, and then its connections sit idle past the
            // silence: what it fetches once the reader reads on goes to them first.
            delay(600)
            val opened = server.connections.get()

            val (rest, cause) = withTimeout(20_000) { channel.readToEnd() }

            assertNull(cause, "the stream failed: $cause")
            assertContentEquals(segments.reduce { a, b -> a + b }, head + rest)
            assertTrue(server.silenced.get() > 0, "no fetch after the rest met a dead connection")
            assertTrue(server.connections.get() > opened, "the dead connections weren't replaced")
        }
    }

    @Test
    fun `the keepalive's pace and timeout reach a pool added later`() = runBlocking<Unit> {
        val server = server(idleAfterMs = 100)
        val streaming = StreamingConfig(
            segmentFetchTimeoutMs = 1_000,
            poolTuning = PoolTuning(keepaliveIntervalMs = 300, keepaliveTimeoutMs = 100),
        )
        NntpStreamingService(emptyList(), streaming).use { service ->
            service.connect()
            service.addPool(config(server, connections = 1))
            assertContentEquals(segments[0], service.fetchSegment("s0@test"))
            // The pool's one connection dies 100 ms after that answer. The keepalive's DATE finds it dead within two
            // ticks and drops it 100 ms later; with the client's own 10 s it would keep it from the next fetch that long.
            delay(1_200)

            val started = TimeSource.Monotonic.markNow()
            assertContentEquals(segments[1], service.fetchSegment("s1@test"))
            val took = started.elapsedNow()

            assertTrue(took < 1_000.milliseconds, "took $took")
            assertEquals(1, server.silenced.get(), "the keepalive's DATE met the dead connection")
            assertEquals(2, server.connections.get())
        }
    }
}
