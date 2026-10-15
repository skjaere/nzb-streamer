package io.skjaere.nzbstreamer

import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentGap
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

    @Test
    fun `parts after one a file's NZB doesn't list keep their place`() = runTest {
        // Posted in 6 parts of 1000 bytes (the last 500); the NZB lists no article for part 3.
        val file = createTestFile(fileSize = 5500, partEnd = 1000, segmentCount = 6, unlisted = setOf(3))

        val listed = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList().filter { it.segment.articleId.isNotEmpty() }

        assertEquals(listOf(0L, 1000L, 3000L, 4000L, 5000L), listed.map { it.globalOffset })
        assertEquals(listOf(1000L, 1000L, 1000L, 1000L, 500L), listed.map { it.segmentDecodedSize })
    }

    @Test
    fun `parts a file's NZB doesn't list are a gap in their place`() = runTest {
        val file = createTestFile(fileSize = 5500, partEnd = 1000, segmentCount = 6, unlisted = setOf(3))

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        val gap = items.single { it.gap != null }
        assertEquals(SegmentGap("test.rar", 3..3, 6), gap.gap)
        assertEquals(listOf(2000L, 1000L, 0L, 1000L), listOf(gap.globalOffset, gap.segmentDecodedSize, gap.readStart, gap.readEnd))
        assertEquals("", gap.segment.articleId)
        assertEquals(items.sortedBy { it.globalOffset }, items)
        assertEquals("File 'test.rar' lacks part 3 of 6: its NZB lists no article for it", gap.gap?.toException()?.message)
    }

    @Test
    fun `the parts after the last a file's NZB lists are one gap to the end of the file`() = runTest {
        val file = createTestFile(fileSize = 5500, partEnd = 1000, segmentCount = 6, unlisted = setOf(5, 6))

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        assertEquals(listOf(0L, 1000L, 2000L, 3000L, 4000L), items.map { it.globalOffset })
        assertEquals(SegmentGap("test.rar", 5..6, 6), items.last().gap)
        assertEquals(1500L, items.last().segmentDecodedSize)
        assertEquals(5500L, SegmentQueueService.segmentLayout(NzbDocument(listOf(file))).last().let { it.globalOffset + it.segmentDecodedSize })
    }

    @Test
    fun `a range read stops at the gap it reaches`() = runTest {
        val file = createTestFile(fileSize = 5500, partEnd = 1000, segmentCount = 6, unlisted = setOf(3))

        val items = SegmentQueueService.createRangeQueue(NzbDocument(listOf(file)), 1500, 1000).toList()

        assertEquals(listOf("seg2@test", ""), items.map { it.segment.articleId })
        assertEquals(listOf(0L, 500L), items.map { it.readStart to it.readEnd }.last().toList())
    }

    @Test
    fun `a file whose NZB lacks part 1 starts with a gap, its first article placed by its =ypart begin`() = runTest {
        // The first listed article (enriched) is part 2: =ypart begin=1001 end=2000.
        val file = createTestFile(fileSize = 5500, partEnd = 2000, segmentCount = 6, unlisted = setOf(1), partBegin = 1001)

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        assertEquals(SegmentGap("test.rar", 1..1, 6), items.first().gap)
        assertEquals(listOf(0L, 1000L, 2000L, 3000L, 4000L, 5000L), items.map { it.globalOffset })
        assertEquals(500L, items.last().segmentDecodedSize)
    }

    @Test
    fun `numbers that aren't the parts' put the listed articles in order, the gap after them`() = runTest {
        // Two articles numbered 2: where part 3's would go can't be told.
        val file = createTestFile(fileSize = 5500, partEnd = 1000, segmentCount = 6, unlisted = setOf(3))
            .let { it.copy(segments = it.segments.map { s -> if (s.number == 4) s.copy(number = 2) else s }) }

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        assertEquals(listOf(0L, 1000L, 2000L, 3000L, 4000L, 5000L), items.map { it.globalOffset })
        assertEquals(SegmentGap("test.rar", 6..6, 6), items.last().gap)
        assertEquals(500L, items.last().segmentDecodedSize)
    }

    @Test
    fun `the last part a file's NZB lists doesn't claim the parts after it`() = runTest {
        val file = createTestFile(fileSize = 5500, partEnd = 1000, segmentCount = 6, unlisted = setOf(5, 6))

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        assertEquals(1000L, items.single { it.segment.articleId == "seg4@test" }.segmentDecodedSize)
    }

    @Test
    fun `without a yEnc total, the parts are counted from the file's size`() = runTest {
        val file = createTestFile(fileSize = 5500, partEnd = 1000, segmentCount = 6, unlisted = setOf(2), total = null)

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        assertEquals(2000L, items.single { it.segment.articleId == "seg3@test" }.globalOffset)
    }

    @Test
    fun `parts bigger than the first, by their byte counts, are not taken for parts missing`() = runTest {
        // No yEnc total, and parts 2-4 hold twice what part 1 does: the size in parts of the first's
        // overstates the count, so nothing says a part is missing.
        val file = createTestFile(fileSize = 7000, partEnd = 1000, segmentCount = 4, total = null, bytes = { if (it == 1) 1050 else 2100 })

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        assertEquals(listOf(0L, 1000L, 2000L, 3000L), items.map { it.globalOffset })
        assertEquals(listOf(1000L, 1000L, 1000L, 4000L), items.map { it.segmentDecodedSize })
        assertEquals(emptyList(), items.mapNotNull { it.gap })
    }

    @Test
    fun `a yEnc total the NZB lists every part of is not taken for parts missing`() = runTest {
        // Posted in 4 parts, the later ones bigger than part 1: the total says all 4 are listed.
        val file = createTestFile(fileSize = 7000, partEnd = 1000, segmentCount = 4)

        val items = SegmentQueueService.createQueue(NzbDocument(listOf(file))).toList()

        assertEquals(listOf(0L, 1000L, 2000L, 3000L), items.map { it.globalOffset })
        assertEquals(4000L, items.last().segmentDecodedSize)
        assertEquals(emptyList(), items.mapNotNull { it.gap })
    }

    /**
     * A file of [fileSize] posted in [segmentCount] parts (yEnc [total], the part count by default),
     * whose NZB lists no article for the parts in [unlisted]. The first listed one's =ypart is
     * [partBegin]-[partEnd].
     */
    @Suppress("LongParameterList")
    private fun createTestFile(
        fileSize: Long,
        partEnd: Long,
        segmentCount: Int,
        unlisted: Set<Int> = emptySet(),
        total: Int? = segmentCount,
        bytes: (Int) -> Long = { 1500 },
        partBegin: Long = 1,
    ): NzbFile {
        val segments = (1..segmentCount).filterNot { it in unlisted }.map { i ->
            NzbSegment(bytes = bytes(i), number = i, articleId = "seg$i@test")
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
                total = total,
                partBegin = partBegin,
                partEnd = partEnd
            )
        )
    }
}
