package io.skjaere.nzbstreamer

import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService.Companion.adjustSplitsForRange
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService.Companion.computeVolumeOffsets
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService.Companion.computeVolumeSizes
import io.skjaere.nzbstreamer.stream.FileResolveResult
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.StreamableFile
import io.skjaere.nzbstreamer.stream.toSplits
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class ArchiveStreamingServiceTest {

    private val dummyConfig = NntpConfig(
        host = "localhost", port = 119, username = "", password = "",
        useTls = false, concurrency = 1
    )

    private val service = ArchiveStreamingService(
        streamingService = NntpStreamingService(dummyConfig)
    )

    // -- Helper builders --

    private fun nzbFile(name: String, size: Long): NzbFile = NzbFile(
        poster = "test", date = 0, subject = name,
        groups = listOf("test"),
        segments = listOf(NzbSegment(bytes = size, number = 1, articleId = "$name@test")),
        yencHeaders = YencHeaders(line = 128, size = size, name = name)
    )

    private fun archiveNzb(vararg volumeSizes: Long): NzbDocument =
        NzbDocument(volumeSizes.mapIndexed { i, sz -> nzbFile("vol$i.rar", sz) })

    private fun rarEntry(
        path: String,
        uncompressedSize: Long,
        dataPosition: Long = 100,
        volumeIndex: Int = 0,
        compressionMethod: Int = 0,
        isDirectory: Boolean = false,
        splitParts: List<SplitInfo> = emptyList(),
    ) = RarFileEntry(
        path = path,
        uncompressedSize = uncompressedSize,
        compressedSize = uncompressedSize,
        headerPosition = 0,
        dataPosition = dataPosition,
        isDirectory = isDirectory,
        volumeIndex = volumeIndex,
        compressionMethod = compressionMethod,
        splitParts = splitParts
    )

    private fun sevenZipEntry(
        path: String,
        size: Long,
        dataOffset: Long = 200,
        isDirectory: Boolean = false,
        method: String? = "Copy"
    ) = SevenZipFileEntry(
        path = path,
        size = size,
        packedSize = size,
        dataOffset = dataOffset,
        isDirectory = isDirectory,
        method = method
    )

    // ======================================================================
    // computeVolumeOffsets / computeVolumeSizes
    // ======================================================================

    @Nested
    inner class VolumeComputations {
        @Test
        fun `computeVolumeSizes returns sizes from yencHeaders`() {
            val nzb = archiveNzb(1000, 2000, 3000)
            assertEquals(listOf(1000L, 2000L, 3000L), computeVolumeSizes(nzb))
        }

        @Test
        fun `computeVolumeOffsets returns cumulative offsets`() {
            val nzb = archiveNzb(1000, 2000, 3000)
            assertEquals(listOf(0L, 1000L, 3000L), computeVolumeOffsets(nzb))
        }

        @Test
        fun `single volume has offset zero`() {
            val nzb = archiveNzb(5000)
            assertEquals(listOf(0L), computeVolumeOffsets(nzb))
            assertEquals(listOf(5000L), computeVolumeSizes(nzb))
        }
    }

    // ======================================================================
    // adjustSplitsForRange
    // ======================================================================

    @Nested
    inner class AdjustSplitsForRange {

        @Test
        fun `full range returns original splits`() {
            val splits = listOf(
                SplitInfo(0, 0, 1000),
                SplitInfo(1, 1000, 1000)
            )
            val result = adjustSplitsForRange(splits, 0, 2000)
            assertEquals(splits, result)
        }

        @Test
        fun `range within single split trims correctly`() {
            val splits = listOf(SplitInfo(0, 0, 1000))
            val result = adjustSplitsForRange(splits, 200, 500)
            assertEquals(1, result.size)
            assertEquals(SplitInfo(0, 200, 500), result[0])
        }

        @Test
        fun `range spanning two splits trims both ends`() {
            val splits = listOf(
                SplitInfo(0, 0, 1000),
                SplitInfo(1, 1000, 1000)
            )
            val result = adjustSplitsForRange(splits, 800, 400)
            assertEquals(2, result.size)
            // First split: offset 800 within first, 200 bytes remain
            assertEquals(SplitInfo(0, 800, 200), result[0])
            // Second split: starts at 0, needs 200 bytes
            assertEquals(SplitInfo(1, 1000, 200), result[1])
        }

        @Test
        fun `range skips splits before range start`() {
            val splits = listOf(
                SplitInfo(0, 0, 1000),
                SplitInfo(1, 1000, 1000),
                SplitInfo(2, 2000, 1000)
            )
            val result = adjustSplitsForRange(splits, 1500, 1000)
            assertEquals(2, result.size)
            assertEquals(SplitInfo(1, 1500, 500), result[0])
            assertEquals(SplitInfo(2, 2000, 500), result[1])
        }

        @Test
        fun `range skips splits after range end`() {
            val splits = listOf(
                SplitInfo(0, 0, 1000),
                SplitInfo(1, 1000, 1000),
                SplitInfo(2, 2000, 1000)
            )
            val result = adjustSplitsForRange(splits, 0, 500)
            assertEquals(1, result.size)
            assertEquals(SplitInfo(0, 0, 500), result[0])
        }

        @Test
        fun `empty splits returns empty`() {
            assertEquals(emptyList(), adjustSplitsForRange(emptyList(), 0, 100))
        }

        @Test
        fun `range entirely past all splits returns empty`() {
            val splits = listOf(SplitInfo(0, 0, 100))
            assertEquals(emptyList(), adjustSplitsForRange(splits, 200, 100))
        }
    }

    // ======================================================================
    // resolveFile
    // ======================================================================

    @Nested
    inner class ResolveFile {

        @Test
        fun `returns NotFound for missing path`() {
            val entries = listOf(rarEntry("file.txt", 1000))
            val nzb = archiveNzb(5000)
            assertIs<FileResolveResult.NotFound>(service.resolveFile(entries, nzb, "missing.txt"))
        }

        @Test
        fun `returns IsDirectory for directory entry`() {
            val entries = listOf(rarEntry("folder", 0, isDirectory = true))
            val nzb = archiveNzb(5000)
            assertIs<FileResolveResult.IsDirectory>(service.resolveFile(entries, nzb, "folder"))
        }

        @Test
        fun `returns Compressed for compressed RAR entry`() {
            val entries = listOf(rarEntry("file.txt", 1000, compressionMethod = 3))
            val nzb = archiveNzb(5000)
            val result = service.resolveFile(entries, nzb, "file.txt")
            assertIs<FileResolveResult.Compressed>(result)
        }

        @Test
        fun `returns Compressed for compressed 7z entry`() {
            val entries = listOf(sevenZipEntry("file.txt", 1000, method = "LZMA2"))
            val nzb = archiveNzb(5000)
            val result = service.resolveFile(entries, nzb, "file.txt")
            assertIs<FileResolveResult.Compressed>(result)
        }

        @Test
        fun `returns Ok for uncompressed RAR entry`() {
            val entries = listOf(rarEntry("file.txt", 500, dataPosition = 100, volumeIndex = 0))
            val nzb = archiveNzb(5000)
            val result = service.resolveFile(entries, nzb, "file.txt")
            assertIs<FileResolveResult.Ok>(result)
            assertEquals(500, result.totalSize)
            assertEquals(1, result.splits.size)
            assertEquals(SplitInfo(0, 100, 500), result.splits[0])
        }

        @Test
        fun `returns Ok for uncompressed 7z entry with Copy method`() {
            val entries = listOf(sevenZipEntry("file.bin", 2048, dataOffset = 512, method = "Copy"))
            val nzb = archiveNzb(10000)
            val result = service.resolveFile(entries, nzb, "file.bin")
            assertIs<FileResolveResult.Ok>(result)
            assertEquals(2048, result.totalSize)
            assertEquals(listOf(SplitInfo(0, 512, 2048)), result.splits)
        }

        @Test
        fun `returns Ok for 7z entry with null method`() {
            val entries = listOf(sevenZipEntry("file.bin", 2048, dataOffset = 512, method = null))
            val nzb = archiveNzb(10000)
            val result = service.resolveFile(entries, nzb, "file.bin")
            assertIs<FileResolveResult.Ok>(result)
        }

        @Test
        fun `returns Ok with split parts for split RAR entry`() {
            val splitParts = listOf(
                SplitInfo(0, 100, 3000),
                SplitInfo(1, 50, 2000)
            )
            val entries = listOf(rarEntry("large.bin", 5000, splitParts = splitParts))
            val nzb = archiveNzb(4000, 4000)
            val result = service.resolveFile(entries, nzb, "large.bin")
            assertIs<FileResolveResult.Ok>(result)
            assertEquals(5000, result.totalSize)
            assertEquals(splitParts, result.splits)
        }

        @Test
        fun `returns IsDirectory for 7z directory`() {
            val entries = listOf(sevenZipEntry("folder/", 0, isDirectory = true))
            val nzb = archiveNzb(5000)
            assertIs<FileResolveResult.IsDirectory>(service.resolveFile(entries, nzb, "folder/"))
        }
    }

    // ======================================================================
    // resolveStreamableFile / resolveStreamableFiles
    // ======================================================================

    @Nested
    inner class ResolveStreamableFile {

        @Test
        fun `returns null for directory RAR entry`() {
            val entry = rarEntry("folder", 0, isDirectory = true)
            assertNull(service.resolveStreamableFile(entry, archiveNzb(5000)))
        }

        @Test
        fun `returns null for compressed RAR entry`() {
            val entry = rarEntry("file.txt", 1000, compressionMethod = 3)
            assertNull(service.resolveStreamableFile(entry, archiveNzb(5000)))
        }

        @Test
        fun `returns null for directory 7z entry`() {
            val entry = sevenZipEntry("folder/", 0, isDirectory = true)
            assertNull(service.resolveStreamableFile(entry, archiveNzb(5000)))
        }

        @Test
        fun `returns null for compressed 7z entry`() {
            val entry = sevenZipEntry("file.txt", 1000, method = "LZMA2")
            assertNull(service.resolveStreamableFile(entry, archiveNzb(5000)))
        }

        @Test
        fun `resolves non-split RAR entry`() {
            val entry = rarEntry("file.bin", 5000, dataPosition = 200, volumeIndex = 1)
            val nzb = archiveNzb(3000, 8000)
            val result = service.resolveStreamableFile(entry, nzb)!!
            assertEquals("file.bin", result.path)
            assertEquals(5000, result.totalSize)
            assertEquals(1, result.startVolumeIndex)
            assertEquals(200, result.startOffsetInVolume)
            assertEquals(0, result.continuationHeaderSize)
            assertEquals(0, result.endOfArchiveSize)
        }

        @Test
        fun `resolves split RAR entry`() {
            // Two volumes of 10000 bytes each.
            // Split starts at volume 0, absolute offset 500 (= local offset 500 in vol0)
            // Second split in volume 1, absolute offset 10100 (= local offset 100 in vol1)
            val splitParts = listOf(
                SplitInfo(volumeIndex = 0, dataStartPosition = 500, dataSize = 9000),
                SplitInfo(volumeIndex = 1, dataStartPosition = 10100, dataSize = 1000)
            )
            val entry = rarEntry("large.bin", 10000, splitParts = splitParts)
            val nzb = archiveNzb(10000, 10000)

            val result = service.resolveStreamableFile(entry, nzb)!!
            assertEquals("large.bin", result.path)
            assertEquals(10000, result.totalSize)
            assertEquals(0, result.startVolumeIndex)
            assertEquals(500, result.startOffsetInVolume) // 500 - volumeOffset[0](0) = 500
            assertEquals(100, result.continuationHeaderSize) // 10100 - volumeOffset[1](10000) = 100
            assertEquals(500, result.endOfArchiveSize) // volumeSize[0](10000) - 500 - 9000 = 500
        }

        @Test
        fun `resolves 7z entry`() {
            val entry = sevenZipEntry("video.mkv", 50000, dataOffset = 1024, method = "Copy")
            val nzb = archiveNzb(100000)
            val result = service.resolveStreamableFile(entry, nzb)!!
            assertEquals("video.mkv", result.path)
            assertEquals(50000, result.totalSize)
            assertEquals(0, result.startVolumeIndex)
            assertEquals(1024, result.startOffsetInVolume)
            assertEquals(0, result.continuationHeaderSize)
            assertEquals(0, result.endOfArchiveSize)
        }

        @Test
        fun `resolveStreamableFiles filters out directories and compressed`() {
            val entries = listOf(
                rarEntry("dir", 0, isDirectory = true),
                rarEntry("compressed.bin", 1000, compressionMethod = 2),
                rarEntry("good.bin", 500, dataPosition = 100),
                sevenZipEntry("also-good.bin", 800, dataOffset = 200)
            )
            val nzb = archiveNzb(10000)
            val result = service.resolveStreamableFiles(entries, nzb)
            assertEquals(2, result.size)
            assertEquals("good.bin", result[0].path)
            assertEquals("also-good.bin", result[1].path)
        }
    }

    // ======================================================================
    // StreamableFile.toSplits
    // ======================================================================

    @Nested
    inner class ToSplits {

        @Test
        fun `non-split file produces single split`() {
            val nzb = archiveNzb(10000)
            val file = StreamableFile(
                path = "file.bin",
                totalSize = 5000,
                startVolumeIndex = 0,
                startOffsetInVolume = 200,
                continuationHeaderSize = 0,
                endOfArchiveSize = 0
            )
            val splits = file.toSplits(nzb)
            assertEquals(1, splits.size)
            // dataStartPosition = volumeOffset[0](0) + startOffsetInVolume(200) = 200
            // available = 10000 - 200 - 0 = 9800, dataSize = min(5000, 9800) = 5000
            assertEquals(SplitInfo(0, 200, 5000), splits[0])
        }

        @Test
        fun `split file spanning multiple volumes`() {
            // 3 volumes of 1000 each
            val nzb = archiveNzb(1000, 1000, 1000)
            val file = StreamableFile(
                path = "large.bin",
                totalSize = 2500,
                startVolumeIndex = 0,
                startOffsetInVolume = 100,
                continuationHeaderSize = 50,
                endOfArchiveSize = 100
            )
            val splits = file.toSplits(nzb)

            // Vol 0: header=100 (startOffset), available = 1000-100-100 = 800
            //        dataStart = 0 + 100 = 100, dataSize = min(2500, 800) = 800
            assertEquals(SplitInfo(0, 100, 800), splits[0])

            // Vol 1: header=50 (continuation), available = 1000-50-100 = 850
            //        dataStart = 1000 + 50 = 1050, dataSize = min(1700, 850) = 850
            assertEquals(SplitInfo(1, 1050, 850), splits[1])

            // Vol 2: header=50 (continuation), available = 1000-50-100 = 850
            //        dataStart = 2000 + 50 = 2050, dataSize = min(850, 850) = 850
            assertEquals(SplitInfo(2, 2050, 850), splits[2])

            assertEquals(3, splits.size)
            assertEquals(2500, splits.sumOf { it.dataSize })
        }

        @Test
        fun `file starting at non-zero volume`() {
            val nzb = archiveNzb(1000, 2000, 3000)
            val file = StreamableFile(
                path = "file.bin",
                totalSize = 1500,
                startVolumeIndex = 1,
                startOffsetInVolume = 300,
                continuationHeaderSize = 0,
                endOfArchiveSize = 0
            )
            val splits = file.toSplits(nzb)
            // Vol 1: offset=1000, header=300, available=2000-300-0=1700
            //        dataStart=1000+300=1300, dataSize=min(1500,1700)=1500
            assertEquals(1, splits.size)
            assertEquals(SplitInfo(1, 1300, 1500), splits[0])
        }
    }
}
