package io.skjaere.nzbstreamer

import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.cancel
import io.ktor.utils.io.readFully
import io.ktor.utils.io.toByteArray
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.writesUntilFirstTaken
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * A stream's first segments ([StreamingConfig.initialReadAheadSegments]): the first two download
 * until its reader has taken the first one's bytes, then the whole read-ahead. A player's first
 * opens and the track probe read a few KB and close or seek: each queued the whole read-ahead, a
 * dozen 3.84 MB segments of a 4K remux, and a probe allocated 68-284 MB in under a second on an
 * emulator. A Plex scan through debridav opens many streams that read a few KB each.
 */
class ReadAheadStartTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> })
    private val held = CountDownLatch(1)

    @AfterEach
    fun tearDown() {
        held.countDown()
        sessions.cancel()
        servers.forEach { it.close() }
    }

    // More than the 1 MiB a Ktor channel takes before its writer waits, as the 3.84 MB parts of a
    // large release are; a whole number of the 64 KiB units a byte budget is counted in.
    private val segmentSize = 23 * 64 * 1024
    private val segments = (0 until 12).map { i -> ByteArray(segmentSize) { (it * 7 + i).toByte() } }
    private val whole = segments.reduce { a, b -> a + b }
    private val asked = ConcurrentLinkedQueue<Int>()

    private val file = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = segments.indices.map { NzbSegment(segments[it].size.toLong(), it + 1, "seg$it@test") },
        yencHeaders = YencHeaders(line = 128, size = whole.size.toLong(), name = "data.bin", partEnd = segmentSize.toLong()),
    )

    /** A service on a server of four connections that holds back the segments [hold] names until the test ends. */
    private fun service(config: StreamingConfig = StreamingConfig(), hold: (Int) -> Boolean = { false }): NntpStreamingService {
        val server = FakeNntpServer(body = { id ->
            val i = id.removePrefix("<seg").substringBefore('@').toInt()
            asked += i
            if (hold(i)) held.await()
            YencEncoder.encodeSinglePart(segments[i], "data.bin").data
        }).also { servers += it }
        return NntpStreamingService(listOf(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)), config)
            .also { runBlocking { it.connect() } }
    }

    /** Starts streaming [queue] from a session coroutine, as the app does: four at once, six read ahead. */
    private suspend fun start(service: NntpStreamingService, queue: Flow<SegmentQueueItem> = SegmentQueueService.createFileQueue(file, 0, 0L)): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(service.launchStreamSegments(queue, concurrency = 4, readAheadSegments = 6, name = "test"))
        }
        return writer.await()
    }

    /** How many segments were asked for once [expected] have been (waiting 10 s at most) and 500 ms more have passed. */
    private suspend fun asksAfter(expected: Int): Int {
        withTimeoutOrNull(10_000) { while (asked.size < expected) delay(10) }
        delay(500)
        return asked.size
    }

    @Test
    fun `a reader that reads a few KB and stops fetches the first two segments only`() = runBlocking<Unit> {
        service().use { service ->
            val writer = start(service)
            writer.channel.readFully(ByteArray(4096))
            assertEquals(2, asksAfter(2), "segments asked for by a reader that read 4 KB")
            writer.channel.cancel()
        }
    }

    @Test
    fun `a reader that reads past the first segment gets the whole read-ahead`() = runBlocking<Unit> {
        service().use { service ->
            val writer = start(service)
            writer.channel.readFully(ByteArray(segmentSize + 4096))
            // The second taken, the third being written, the six read ahead and the one waiting to join them.
            assertEquals(10, asksAfter(10), "segments asked for once the first was read")
            assertContentEquals(whole.copyOfRange(segmentSize + 4096, whole.size), writer.channel.toByteArray())
        }
    }

    /** A seek: the stream at the old position closed, one at the new opened. */
    @Test
    fun `a stream opened further on starts with two segments again`() = runBlocking<Unit> {
        service().use { service ->
            val first = start(service)
            first.channel.readFully(ByteArray(4096))
            asksAfter(2)
            first.channel.cancel()
            first.job.cancelAndJoin()
            asked.clear()
            val from = 6L * segmentSize
            val second = start(service, SegmentQueueService.createRangeQueue(NzbDocument(listOf(file)), from, whole.size - from))
            val read = ByteArray(4096)
            second.channel.readFully(read)
            assertContentEquals(whole.copyOfRange(from.toInt(), from.toInt() + read.size), read)
            assertEquals(2, asksAfter(2), "segments asked for by the second stream")
            assertEquals(listOf(6, 7), asked.sorted())
            second.channel.cancel()
        }
    }

    /** Segments from the third on wait at the server: those asked for meanwhile are the ones downloading at once. */
    @Test
    fun `once it reads on a stream downloads as many at once as its concurrency allows and no more`() = runBlocking<Unit> {
        service(hold = { it >= 2 }).use { service ->
            val writer = start(service)
            writer.channel.readFully(ByteArray(segmentSize + 4096))
            assertEquals(6, asksAfter(6), "the first two, then four at once")
            held.countDown()
            assertContentEquals(whole.copyOfRange(segmentSize + 4096, whole.size), writer.channel.toByteArray())
        }
    }

    /**
     * A write waits only while the channel holds 1 MiB its reader hasn't taken, so the writer of
     * smaller segments can't tell from the second one that the reader took the first: it starts
     * as many as make 2 MiB and a segment past the first, here five of 768 KiB.
     */
    @Test
    fun `a stream of segments under 1 MiB starts five of them, and the rest once its reader reads on`() = runBlocking<Unit> {
        val small = 12 * 64 * 1024
        val parts = (0 until 12).map { i -> ByteArray(small) { (it * 5 + i).toByte() } }
        val all = parts.reduce { a, b -> a + b }
        val smallFile = file.copy(
            segments = parts.indices.map { NzbSegment(small.toLong(), it + 1, "seg$it@test") },
            yencHeaders = YencHeaders(line = 128, size = all.size.toLong(), name = "data.bin", partEnd = small.toLong()),
        )
        val server = FakeNntpServer(body = { id ->
            val i = id.removePrefix("<seg").substringBefore('@').toInt()
            asked += i
            YencEncoder.encodeSinglePart(parts[i], "data.bin").data
        }).also { servers += it }
        NntpStreamingService(listOf(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4))).use { service ->
            service.connect()
            val writer = start(service, SegmentQueueService.createFileQueue(smallFile, 0, 0L))
            val read = ByteArray(5 * small + 4096)
            writer.channel.readFully(read, 0, 4096)
            assertEquals(5, asksAfter(5), "segments asked for by a reader that read 4 KB")
            // Read on past the five: then the whole read-ahead.
            writer.channel.readFully(read, 4096, read.size)
            assertEquals(12, asksAfter(12), "segments asked for once the reader read on")
            assertContentEquals(all, read + writer.channel.toByteArray())
        }
    }

    /** As the writer counts: the reader surely took the first write's bytes once these many writes returned. */
    @Test
    fun `the writer knows the reader took the first segment from the second of 1 MiB or more`() {
        val big = 3_840_000L
        assertEquals(2, writesUntilFirstTaken(List(12) { big }.asSequence()))
        // A stream that starts in a segment's last MiB: the first write tells nothing.
        assertEquals(3, writesUntilFirstTaken((listOf(100L) + List(12) { big }).asSequence()))
        // Segments under 1 MiB: 2 MiB and a segment past the first.
        assertEquals(5, writesUntilFirstTaken(List(12) { 768_000L }.asSequence()))
        assertEquals(23, writesUntilFirstTaken(List(60) { 100_000L }.asSequence()))
        // Writes that never show it: the stream starts them all.
        assertEquals(null, writesUntilFirstTaken(listOf(big, 500_000L).asSequence()))
        assertEquals(null, writesUntilFirstTaken(emptySequence()))
    }

    @Test
    fun `the first segments can be changed for the streams opened next`() = runBlocking<Unit> {
        service(StreamingConfig(initialReadAheadSegments = 3)).use { service ->
            assertEquals(3, service.getInitialReadAheadSegments())
            service.setInitialReadAheadSegments(null)
            assertEquals(null, service.getInitialReadAheadSegments())
            // The whole read-ahead at once: the one being written, the six read ahead and the one waiting to join them.
            val writer = start(service)
            assertEquals(8, asksAfter(8), "segments asked for before anything was read")
            writer.channel.cancel()
        }
    }

    @Test
    fun `a streaming config refuses no first segments`() {
        listOf(0, -1).forEach { segments ->
            assertFailsWith<IllegalArgumentException>("$segments segments") { StreamingConfig(initialReadAheadSegments = segments) }
        }
        assertEquals(StreamingConfig.DEFAULT_INITIAL_READ_AHEAD_SEGMENTS, StreamingConfig().initialReadAheadSegments)
    }

    @Test
    fun `once it reads on a stream holds no more than its byte budget allows`() = runBlocking<Unit> {
        service(StreamingConfig(readAheadBytes = 3L * segmentSize)).use { service ->
            val writer = start(service)
            writer.channel.readFully(ByteArray(segmentSize + 4096))
            // The second taken, then three held: the third being written and two downloaded behind it.
            assertEquals(5, asksAfter(5), "segments asked for once the first was read")
            assertContentEquals(whole.copyOfRange(segmentSize + 4096, whole.size), writer.channel.toByteArray())
        }
    }
}
