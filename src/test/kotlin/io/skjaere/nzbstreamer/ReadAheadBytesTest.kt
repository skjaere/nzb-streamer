package io.skjaere.nzbstreamer

import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.readFully
import io.ktor.utils.io.toByteArray
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * A stream's read-ahead in bytes ([StreamingConfig.readAheadBytes]). Counted in segments only, a
 * release posted in 3.84 MB parts held twelve of them ahead of its reader, 46 MB, beside the
 * player's own buffer: on a TV box's 192 MB heap the app ran out of memory. These streams start
 * their whole read-ahead at once (no [StreamingConfig.initialReadAheadSegments]), so it can be
 * counted before anything is read; ReadAheadStartTest has it bound a stream that started small.
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
    // A whole number of the 64 KiB units the budget is counted in (each segment rounded up to them),
    // so a budget of three segments holds three.
    private val segmentSize = 23 * 64 * 1024
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
        return NntpStreamingService(listOf(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)), config.copy(initialReadAheadSegments = null))
            .also { runBlocking { it.connect() } }
    }

    /** Starts streaming [queue] (every segment) from a session coroutine, as the app does; the settings are the service's. */
    private suspend fun start(service: NntpStreamingService, queue: Flow<SegmentQueueItem> = SegmentQueueService.createFileQueue(file, 0, 0L)): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(service.launchStreamSegments(queue, concurrency = 4, readAheadSegments = 6, name = "test"))
        }
        return writer.await()
    }

    /**
     * How many segments were asked for once [expected] have been (waiting 10 s at most) and 500 ms
     * more have passed, time for one more to be asked. Waiting until two counts agreed took none asked
     * yet for all of them, when the first connect was slow.
     */
    private suspend fun asksAfter(expected: Int): Int {
        withTimeoutOrNull(10_000) { while (asked.size < expected) delay(10) }
        delay(500)
        return asked.size
    }

    @Test
    fun `a stream holds no more segments than its byte budget allows`() = runBlocking {
        service(StreamingConfig(readAheadBytes = 3L * segmentSize)).use { service ->
            val writer = start(service)
            // Nothing read yet: the writer waits on the first segment, two more downloaded behind it.
            assertEquals(3, asksAfter(3), "segments asked for before anything was read")
            assertContentEquals(whole, writer.channel.toByteArray())
            assertEquals(segments.indices.toList(), asked.sorted())
        }
    }

    @Test
    fun `a budget smaller than a segment still streams, a segment at a time`() = runBlocking {
        service(StreamingConfig(readAheadBytes = segmentSize / 2L)).use { service ->
            val writer = start(service)
            assertEquals(1, asksAfter(1), "segments asked for before anything was read")
            assertContentEquals(whole, writer.channel.toByteArray())
        }
    }

    /**
     * An NZB that lists fewer parts than the file was posted in, when their sizes can't be told,
     * gives its last one the rest of the file, gigabytes. The budget was counted in segments of the
     * largest, so a stream that included it (the player's first open covers the whole file) held one.
     */
    @Test
    fun `one segment sized past the budget doesn't cut the others' read-ahead`() = runBlocking {
        // The yEnc total says twelve parts, the size a hundred times theirs.
        val short = file.copy(yencHeaders = file.yencHeaders!!.copy(size = 100L * whole.size, total = segments.size))
        val queue = SegmentQueueService.createFileQueue(short, 0, 0L)
        assertTrue(queue.toList().last().segmentDecodedSize > 50L * whole.size, "the last part is given the rest of the file")
        service(StreamingConfig(readAheadBytes = 3L * segmentSize)).use { service ->
            val writer = start(service, queue)
            assertEquals(3, asksAfter(3), "segments asked for before anything was read")
            assertContentEquals(whole, writer.channel.toByteArray())
        }
    }

    @Test
    fun `a segment larger than the budget is held alone, and those after it read ahead again`() = runBlocking {
        val items = SegmentQueueService.createFileQueue(file, 0, 0L).toList()
        val large = items.first().let { it.copy(segmentDecodedSize = 30L * segmentSize, readEnd = 30L * segmentSize) }
        service(StreamingConfig(readAheadBytes = 3L * segmentSize)).use { service ->
            val writer = start(service, (listOf(large) + items.drop(1)).asFlow())
            assertEquals(1, asksAfter(1), "segments asked for before anything was read")
            val first = ByteArray(segmentSize)
            writer.channel.readFully(first)
            assertContentEquals(segments[0], first)
            assertEquals(4, asksAfter(4), "segments asked for once the large one was read")
            assertContentEquals(segments.drop(1).reduce { a, b -> a + b }, writer.channel.toByteArray())
        }
    }

    @Test
    fun `without a byte budget the read-ahead is counted in segments`() = runBlocking {
        service().use { service ->
            val writer = start(service)
            // The one being written, the six read ahead and the one waiting to join them.
            assertEquals(8, asksAfter(8), "segments asked for before anything was read")
            assertContentEquals(whole, writer.channel.toByteArray())
        }
    }

    @Test
    fun `the byte budget can be changed for the streams opened next`() {
        service(StreamingConfig(readAheadBytes = 5_000_000)).use { service ->
            assertEquals(5_000_000, service.getReadAheadBytes())
            service.setReadAheadBytes(null)
            assertEquals(null, service.getReadAheadBytes())
        }
    }
}
