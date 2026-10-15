package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals

/**
 * A queue item's read range is a Long: an item sized as several GiB (a file's last listed segment,
 * or a caller's own) must be cut from the bytes the segment really holds, not from its range
 * narrowed to an Int, which wraps past 2 GiB.
 */
class LargeReadOffsetTest {
    private val data = ByteArray(16 * 1024) { (it * 7).toByte() }
    private val server = FakeNntpServer(body = { YencEncoder.encodeSinglePart(data, "data.bin").data })
    private val service = NntpStreamingService(
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 2)
    ).also { runBlocking { it.connect() } }

    @AfterEach
    fun tearDown() {
        service.close()
        server.close()
    }

    private fun item(index: Int, readStart: Long, readEnd: Long) = SegmentQueueItem(
        segment = NzbSegment(data.size.toLong(), index + 1, "seg$index@test"),
        fileIndex = 0,
        segmentIndex = index,
        segmentDecodedSize = FOUR_GIB + 100,
        globalOffset = index * (FOUR_GIB + 100),
        readStart = readStart,
        readEnd = readEnd,
    )

    private fun stream(vararg items: SegmentQueueItem): ByteArray = runBlocking {
        var bytes = ByteArray(0)
        service.streamSegments(flowOf(*items), name = "test") { bytes = it.toByteArray() }
        bytes
    }

    @Test
    fun `a read range ending past 4 GiB keeps the whole segment`() {
        // (4 GiB + 100).toInt() is 100: only the segment's first 100 bytes were written.
        assertContentEquals(data, stream(item(0, readStart = 0, readEnd = FOUR_GIB + 100)))
    }

    @Test
    fun `a read range starting past 2 GiB takes nothing from a shorter segment`() {
        // (2 GiB).toInt() is negative: the write was asked to start before the segment.
        val past = item(1, readStart = TWO_GIB, readEnd = TWO_GIB + 10)
        assertContentEquals(data, stream(item(0, readStart = 0, readEnd = data.size.toLong()), past))
    }

    private companion object {
        const val TWO_GIB = 1L shl 31
        const val FOUR_GIB = 1L shl 32
    }
}
