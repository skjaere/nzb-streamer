package io.skjaere.nzbstreamer

import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.toByteArray
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * A stream's read-ahead in bytes ([StreamingConfig.readAheadBytes]). Counted in segments only, a
 * release posted in 3.84 MB parts held twelve of them ahead of its reader, 46 MB, beside the
 * player's own buffer: on a TV box's 192 MB heap the app ran out of memory.
 */
class ReadAheadBytesTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> })

    @AfterEach
    fun tearDown() {
        sessions.cancel()
        servers.forEach { it.close() }
    }

    // More than the 1 MB a Ktor channel takes before its writer waits: one segment unread fills it.
    private val segmentSize = 1_500_000
    private val segments = (0 until 12).map { i -> ByteArray(segmentSize) { (it * 7 + i).toByte() } }
    private val whole = segments.reduce { a, b -> a + b }
    private val asked = ConcurrentLinkedQueue<Int>()

    private val file = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = segments.indices.map { NzbSegment(segments[it].size.toLong(), it + 1, "seg$it@test") },
        yencHeaders = YencHeaders(line = 128, size = whole.size.toLong(), name = "data.bin", partEnd = segmentSize.toLong()),
    )

    private fun service(config: StreamingConfig = StreamingConfig()): NntpStreamingService {
        val server = FakeNntpServer(body = { id ->
            val i = id.removePrefix("<seg").substringBefore('@').toInt()
            asked += i
            YencEncoder.encodeSinglePart(segments[i], "data.bin").data
        }).also { servers += it }
        return NntpStreamingService(listOf(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)), config)
            .also { runBlocking { it.connect() } }
    }

    /** Starts streaming every segment from a session coroutine, as the app does; the settings are the service's. */
    private suspend fun start(service: NntpStreamingService): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(service.launchStreamSegments(SegmentQueueService.createFileQueue(file, 0, 0L), concurrency = 4, readAheadSegments = 6, name = "test"))
        }
        return writer.await()
    }

    /** How many segments were asked for once no more have been for 300 ms (at most 10 s). */
    private suspend fun settledAsks(): Int {
        var last = -1
        repeat(100) {
            val now = asked.size
            if (now == last) return now
            last = now
            delay(300)
        }
        return asked.size
    }

    @Test
    fun `a stream holds no more segments than its byte budget allows`() = runBlocking {
        val service = service(StreamingConfig(readAheadBytes = 3L * segmentSize))
        val writer = start(service)
        // Nothing read yet: the writer waits on the first segment, two more downloaded behind it.
        assertEquals(3, settledAsks(), "segments asked for before anything was read")
        assertContentEquals(whole, writer.channel.toByteArray())
        assertEquals(segments.indices.toList(), asked.sorted())
        service.close()
    }

    @Test
    fun `a budget smaller than a segment still streams, a segment at a time`() = runBlocking {
        val service = service(StreamingConfig(readAheadBytes = segmentSize / 2L))
        val writer = start(service)
        assertEquals(1, settledAsks(), "segments asked for before anything was read")
        assertContentEquals(whole, writer.channel.toByteArray())
        service.close()
    }

    @Test
    fun `without a byte budget the read-ahead is counted in segments`() = runBlocking {
        val service = service()
        val writer = start(service)
        // The one being written, the six read ahead and the one waiting to join them.
        assertEquals(8, settledAsks(), "segments asked for before anything was read")
        assertContentEquals(whole, writer.channel.toByteArray())
        service.close()
    }

    @Test
    fun `the byte budget can be changed for the streams opened next`() {
        val service = service(StreamingConfig(readAheadBytes = 5_000_000))
        assertEquals(5_000_000, service.getReadAheadBytes())
        service.setReadAheadBytes(null)
        assertEquals(null, service.getReadAheadBytes())
        service.close()
    }
}
