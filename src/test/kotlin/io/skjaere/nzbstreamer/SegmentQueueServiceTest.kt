package io.skjaere.nzbstreamer

import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SegmentQueueServiceTest {

    @Test
    fun `createQueue produces correct items for single file`() = runTest {
        val file = createTestFile(
            fileSize = 3000,
            partEnd = 1000,
            segmentCount = 3
        )
        val nzb = NzbDocument(listOf(file))

        val items = SegmentQueueService.createQueue(nzb).toList()

        assertEquals(3, items.size)
        assertEquals(0L, items[0].globalOffset)
        assertEquals(1000L, items[0].segmentDecodedSize)
        assertEquals(1000L, items[1].globalOffset)
        assertEquals(1000L, items[1].segmentDecodedSize)
        assertEquals(2000L, items[2].globalOffset)
        assertEquals(1000L, items[2].segmentDecodedSize)
    }

    @Test
    fun `createQueue handles last segment smaller than partSize`() = runTest {
        val file = createTestFile(
            fileSize = 2500,
            partEnd = 1000,
            segmentCount = 3
        )
        val nzb = NzbDocument(listOf(file))

        val items = SegmentQueueService.createQueue(nzb).toList()

        assertEquals(3, items.size)
        assertEquals(1000L, items[0].segmentDecodedSize)
        assertEquals(1000L, items[1].segmentDecodedSize)
        assertEquals(500L, items[2].segmentDecodedSize)
    }

    @Test
    fun `createRangeQueue returns correct partial segments`() = runTest {
        val file = createTestFile(
            fileSize = 3000,
            partEnd = 1000,
            segmentCount = 3
        )
        val nzb = NzbDocument(listOf(file))

        // Range: [500, 2500) - spans all three segments partially
        val items = SegmentQueueService.createRangeQueue(nzb, 500, 2000).toList()

        assertEquals(3, items.size)
        // First segment: [0, 1000), range [500, 2500) -> readStart=500, readEnd=1000
        assertEquals(500L, items[0].readStart)
        assertEquals(1000L, items[0].readEnd)
        // Second segment: [1000, 2000), range [500, 2500) -> full
        assertEquals(0L, items[1].readStart)
        assertEquals(1000L, items[1].readEnd)
        // Third segment: [2000, 3000), range [500, 2500) -> readStart=0, readEnd=500
        assertEquals(0L, items[2].readStart)
        assertEquals(500L, items[2].readEnd)
    }

    @Test
    fun `createQueue for multi-file NZB has correct global offsets`() = runTest {
        val file1 = createTestFile(fileSize = 2000, partEnd = 1000, segmentCount = 2)
        val file2 = createTestFile(fileSize = 1000, partEnd = 1000, segmentCount = 1)
        val nzb = NzbDocument(listOf(file1, file2))

        val items = SegmentQueueService.createQueue(nzb).toList()

        assertEquals(3, items.size)
        assertEquals(0L, items[0].globalOffset)
        assertEquals(1000L, items[1].globalOffset)
        assertEquals(2000L, items[2].globalOffset) // file2 starts after file1
    }

    private fun createTestFile(fileSize: Long, partEnd: Long, segmentCount: Int): NzbFile {
        val segments = (1..segmentCount).map { i ->
            NzbSegment(bytes = 1500, number = i, articleId = "seg$i@test")
        }
        return NzbFile(
            poster = "test",
            date = 0,
            subject = "test",
            groups = listOf("test"),
            segments = segments,
            yencHeaders = YencHeaders(
                line = 128,
                size = fileSize,
                name = "test.rar",
                part = 1,
                total = segmentCount,
                partBegin = 1,
                partEnd = partEnd
            )
        )
    }
}
