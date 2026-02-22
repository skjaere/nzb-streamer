package io.skjaere.nzbstreamer

import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.ArchiveService
import io.skjaere.compressionutils.ListFilesResult
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.compressionutils.TranslatedFileEntry
import io.skjaere.compressionutils.VolumeMetaData
import io.skjaere.compressionutils.generation.ArchiveFile
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ArchiveVolume
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.compressionutils.validation.ConcatenatedFileSeekableInputStream
import io.skjaere.nzbstreamer.seekable.NestedSeekableInputStream
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.ByteArrayOutputStream
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.zip.CRC32
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Tests nested archive support using the real double-archive example at lmdoublerar/:
 * 11 split 7z volumes (Copy method) containing 52 old-style RAR volumes (.rar + .r00-.r50)
 * plus proof.jpg, sample.mkv, .nfo, and .sfv.
 */
class NestedArchiveTest {

    companion object {
        private val archiveDir = File("/home/william/IdeaProjects/nzb-streamer-utils/lmdoublerar")
        private lateinit var outerVolumes: List<File>
        private lateinit var outerVolumeMetadata: List<VolumeMetaData>
        private lateinit var outerEntries: List<ArchiveFileEntry>

        @JvmStatic
        @BeforeAll
        fun setup() {
            assumeTrue(archiveDir.exists(), "Test archive not available at ${archiveDir.absolutePath}")

            outerVolumes = archiveDir.listFiles()!!
                .filter { it.name.matches(Regex(""".*\.7z\.\d{3}$""")) }
                .sortedBy { it.name }

            outerVolumeMetadata = outerVolumes.map {
                VolumeMetaData(filename = it.name, size = it.length())
            }

            outerEntries = runBlocking {
                ConcatenatedFileSeekableInputStream(outerVolumes).use { stream ->
                    val result = ArchiveService.listFiles(stream, outerVolumeMetadata)
                    assertIs<ListFilesResult.Success>(result)
                    result.entries
                }
            }
        }
    }

    @Test
    fun `outer 7z contains expected number of entries`() {
        val files = outerEntries.filter { !it.isDirectory }
        assertTrue(files.size >= 56, "Expected at least 56 non-directory entries, got ${files.size}")
    }

    @Test
    fun `detects inner archive files`() {
        val innerArchiveFiles = outerEntries.filter { entry ->
            !entry.isDirectory && ArchiveService.fileHasKnownExtension(entry.path)
        }
        assertEquals(52, innerArchiveFiles.size, "Expected 52 inner archive files (.rar + .r00-.r50)")
    }

    @Test
    fun `nested archive detection heuristic triggers`() {
        val innerArchiveFiles = outerEntries.filter { entry ->
            !entry.isDirectory && ArchiveService.fileHasKnownExtension(entry.path)
        }
        assertTrue(innerArchiveFiles.size >= 2)

        val archiveSize = innerArchiveFiles.sumOf { it.size }
        val totalSize = outerEntries.filter { !it.isDirectory }.sumOf { it.size }
        val ratio = archiveSize.toDouble() / totalSize
        assertTrue(ratio > 0.5, "Archive ratio $ratio should be > 0.5")
    }

    @Nested
    inner class NestedSeekableInputStreamTest {

        @Test
        @Timeout(value = 2, unit = TimeUnit.MINUTES)
        fun `can parse inner RAR archive via NestedSeekableInputStream`() = runBlocking {
            val innerArchiveEntries = outerEntries
                .filter { !it.isDirectory && ArchiveService.fileHasKnownExtension(it.path) }

            val innerVolumes = innerArchiveEntries.map { entry ->
                VolumeMetaData(filename = entry.path, size = entry.size)
            }
            val resolvedInnerVolumes = ArchiveService.resolveVolumes(innerVolumes)
            val orderedInnerEntries = resolvedInnerVolumes.map { vol ->
                innerArchiveEntries.first { it.path == vol.filename }
            }

            val innerVolumeSizes = resolvedInnerVolumes.map { it.size }
            val outerChunks = orderedInnerEntries.map { entry ->
                assertIs<SevenZipFileEntry>(entry)
                listOf(SplitInfo(0, entry.dataOffset, entry.size))
            }

            val volumesWithHeaders = ConcatenatedFileSeekableInputStream(outerVolumes).use { outerStream ->
                resolvedInnerVolumes.zip(outerChunks).map { (vol, chunks) ->
                    val first16kb = ByteArray(minOf(16384, vol.size).toInt())
                    outerStream.seek(chunks[0].dataStartPosition)
                    var totalRead = 0
                    while (totalRead < first16kb.size) {
                        val n = outerStream.read(first16kb, totalRead, first16kb.size - totalRead)
                        if (n <= 0) break
                        totalRead += n
                    }
                    vol.copy(first16kb = first16kb.copyOf(totalRead))
                }
            }

            val nestedStream = NestedSeekableInputStream(
                outerStream = ConcatenatedFileSeekableInputStream(outerVolumes),
                innerVolumeSizes = innerVolumeSizes,
                outerChunks = outerChunks
            )

            val innerResult = nestedStream.use { stream ->
                ArchiveService.listFiles(stream, volumesWithHeaders)
            }

            val innerEntries = assertIs<ListFilesResult.Success>(innerResult).entries
            assertTrue(innerEntries.isNotEmpty(), "Inner archive should contain at least one file")

            val files = innerEntries.filter { !it.isDirectory }
            assertTrue(files.isNotEmpty(), "Inner archive should have non-directory files")
            println("Inner archive contains ${files.size} file(s):")
            files.forEach { entry ->
                assertIs<RarFileEntry>(entry)
                println("  ${entry.path} (${entry.uncompressedSize} bytes, split=${entry.isSplit}, parts=${entry.splitParts.size})")
            }
        }
    }

    @Nested
    inner class OffsetTranslationTest {

        private val EXPECTED_MKV_CRC = 0x63c5b81eL

        @Test
        @Timeout(value = 5, unit = TimeUnit.MINUTES)
        fun `translated offsets produce correct CRC32 for inner file`() = runBlocking {
            val innerArchiveEntries = outerEntries
                .filter { !it.isDirectory && ArchiveService.fileHasKnownExtension(it.path) }

            val innerVolumes = innerArchiveEntries.map { entry ->
                VolumeMetaData(filename = entry.path, size = entry.size)
            }
            val resolvedInnerVolumes = ArchiveService.resolveVolumes(innerVolumes)
            val orderedInnerEntries = resolvedInnerVolumes.map { vol ->
                innerArchiveEntries.first { it.path == vol.filename }
            }

            val innerVolumeSizes = resolvedInnerVolumes.map { it.size }
            val outerChunks = orderedInnerEntries.map { entry ->
                assertIs<SevenZipFileEntry>(entry)
                listOf(SplitInfo(0, entry.dataOffset, entry.size))
            }

            val volumesWithHeaders = ConcatenatedFileSeekableInputStream(outerVolumes).use { outerStream ->
                resolvedInnerVolumes.zip(outerChunks).map { (vol, chunks) ->
                    val first16kb = ByteArray(minOf(16384, vol.size).toInt())
                    outerStream.seek(chunks[0].dataStartPosition)
                    var totalRead = 0
                    while (totalRead < first16kb.size) {
                        val n = outerStream.read(first16kb, totalRead, first16kb.size - totalRead)
                        if (n <= 0) break
                        totalRead += n
                    }
                    vol.copy(first16kb = first16kb.copyOf(totalRead))
                }
            }

            val nestedStream = NestedSeekableInputStream(
                outerStream = ConcatenatedFileSeekableInputStream(outerVolumes),
                innerVolumeSizes = innerVolumeSizes,
                outerChunks = outerChunks
            )
            val innerEntries = nestedStream.use { stream ->
                assertIs<ListFilesResult.Success>(ArchiveService.listFiles(stream, volumesWithHeaders)).entries
            }

            val innerCumOffsets = buildCumOffsets(innerVolumeSizes)

            val mainFile = innerEntries.filterIsInstance<RarFileEntry>()
                .filter { !it.isDirectory && it.isUncompressed }
                .maxByOrNull { it.uncompressedSize }
                ?: error("No uncompressed file found in inner archive")

            println("Testing offset translation for: ${mainFile.path} (${mainFile.uncompressedSize} bytes)")
            println("Split parts: ${mainFile.splitParts.size}")

            val translatedSplits = if (mainFile.splitParts.isNotEmpty()) {
                mainFile.splitParts.flatMap { split ->
                    translateRange(
                        split.dataStartPosition, split.dataSize,
                        innerCumOffsets, innerVolumeSizes, outerChunks
                    )
                }
            } else {
                val innerAbsPos = innerCumOffsets[mainFile.volumeIndex] + mainFile.dataPosition
                translateRange(
                    innerAbsPos, mainFile.uncompressedSize,
                    innerCumOffsets, innerVolumeSizes, outerChunks
                )
            }

            println("Translated to ${translatedSplits.size} outer splits")
            assertEquals(
                mainFile.uncompressedSize, translatedSplits.sumOf { it.dataSize },
                "Total translated data size must match file size"
            )

            val crc = CRC32()
            ConcatenatedFileSeekableInputStream(outerVolumes).use { outerStream ->
                for (split in translatedSplits) {
                    outerStream.seek(split.dataStartPosition)
                    val buffer = ByteArray(65536)
                    var remaining = split.dataSize
                    while (remaining > 0) {
                        val toRead = minOf(remaining, buffer.size.toLong()).toInt()
                        val bytesRead = outerStream.read(buffer, 0, toRead)
                        if (bytesRead <= 0) break
                        crc.update(buffer, 0, bytesRead)
                        remaining -= bytesRead
                    }
                    assertEquals(0L, remaining, "Should read all bytes in split")
                }
            }

            val actualCrc = crc.value
            println("Computed CRC32: ${"%08x".format(actualCrc)}")
            assertEquals(
                EXPECTED_MKV_CRC, actualCrc,
                "CRC32 mismatch: expected ${"%08x".format(EXPECTED_MKV_CRC)}, got ${"%08x".format(actualCrc)}"
            )
            println("CRC32 verified successfully!")
        }
    }

    @Nested
    inner class ChunkedNestedSeekableInputStreamTest {

        @Test
        fun `read across chunk boundary skips gaps`() = runBlocking {
            // Outer: [ABCD____EFGH] where ____ is a 4-byte gap
            // Volume 0: chunk at 0 size 4, chunk at 8 size 4 → inner size = 8
            val outerData = ByteArray(12)
            outerData[0] = 0x41; outerData[1] = 0x42; outerData[2] = 0x43; outerData[3] = 0x44
            outerData[8] = 0x45; outerData[9] = 0x46; outerData[10] = 0x47; outerData[11] = 0x48

            val stream = NestedSeekableInputStream(
                outerStream = ByteArraySeekableInputStream(outerData),
                innerVolumeSizes = listOf(8),
                outerChunks = listOf(listOf(SplitInfo(0, 0, 4), SplitInfo(0, 8, 4)))
            )

            val buf = ByteArray(8)
            val n1 = stream.read(buf, 0, 8)
            assertEquals(4, n1, "First read should clamp at chunk boundary")
            assertEquals(0x41, buf[0].toInt() and 0xFF)
            assertEquals(0x44, buf[3].toInt() and 0xFF)

            val n2 = stream.read(buf, 0, 8)
            assertEquals(4, n2, "Second read should return second chunk")
            assertEquals(0x45, buf[0].toInt() and 0xFF)
            assertEquals(0x48, buf[3].toInt() and 0xFF)

            assertEquals(-1, stream.read(buf, 0, 1))
        }

        @Test
        fun `clamp at chunk boundary returns partial read`() = runBlocking {
            val outerData = ByteArray(20) { it.toByte() }
            val stream = NestedSeekableInputStream(
                outerStream = ByteArraySeekableInputStream(outerData),
                innerVolumeSizes = listOf(10),
                outerChunks = listOf(listOf(SplitInfo(0, 0, 5), SplitInfo(0, 10, 5)))
            )

            val buf = ByteArray(10)
            val n = stream.read(buf, 0, 10)
            assertEquals(5, n)
            for (i in 0 until 5) assertEquals(i.toByte(), buf[i])
        }

        @Test
        fun `seek into second chunk works across gap`() = runBlocking {
            val outerData = ByteArray(100) { it.toByte() }
            val stream = NestedSeekableInputStream(
                outerStream = ByteArraySeekableInputStream(outerData),
                innerVolumeSizes = listOf(10),
                outerChunks = listOf(listOf(SplitInfo(0, 10, 5), SplitInfo(0, 50, 5)))
            )

            stream.seek(7) // 2 bytes into second chunk → outer pos 52
            val buf = ByteArray(3)
            val n = stream.read(buf, 0, 3)
            assertEquals(3, n)
            assertEquals(52.toByte(), buf[0])
            assertEquals(53.toByte(), buf[1])
            assertEquals(54.toByte(), buf[2])
        }

        @Test
        fun `single-chunk degenerate case works like old API`() = runBlocking {
            val data = ByteArray(256) { it.toByte() }
            val outerData = ByteArray(400)
            System.arraycopy(data, 0, outerData, 100, 256)

            val stream = NestedSeekableInputStream(
                outerStream = ByteArraySeekableInputStream(outerData),
                innerVolumeSizes = listOf(256),
                outerChunks = listOf(listOf(SplitInfo(0, 100, 256)))
            )

            val buf = ByteArray(256)
            var totalRead = 0
            while (totalRead < 256) {
                val n = stream.read(buf, totalRead, 256 - totalRead)
                if (n <= 0) break
                totalRead += n
            }
            assertEquals(256, totalRead)
            for (i in 0 until 256) assertEquals(i.toByte(), buf[i])
        }

        @Test
        fun `multi-volume with mixed chunk counts`() = runBlocking {
            // Volume 0: single chunk at pos 0, size 10
            // Volume 1: two chunks at pos 20 size 5 and pos 30 size 5
            val outerData = ByteArray(40)
            for (i in 0 until 10) outerData[i] = (i + 1).toByte()
            for (i in 0 until 5) outerData[20 + i] = (11 + i).toByte()
            for (i in 0 until 5) outerData[30 + i] = (16 + i).toByte()

            val stream = NestedSeekableInputStream(
                outerStream = ByteArraySeekableInputStream(outerData),
                innerVolumeSizes = listOf(10, 10),
                outerChunks = listOf(
                    listOf(SplitInfo(0, 0, 10)),
                    listOf(SplitInfo(0, 20, 5), SplitInfo(0, 30, 5))
                )
            )

            val result = ByteArray(20)
            var pos = 0
            while (pos < 20) {
                val n = stream.read(result, pos, 20 - pos)
                if (n <= 0) break
                pos += n
            }
            assertEquals(20, pos)
            for (i in 0 until 20) assertEquals((i + 1).toByte(), result[i])
        }
    }

    @Nested
    inner class GeneratedArchiveIntegrationTest {

        @Test
        @Timeout(value = 1, unit = TimeUnit.MINUTES)
        fun `RAR5 inside RAR5 - parse and verify CRC`() = runBlocking {
            val testData = ByteArray(4096)
            Random(42).nextBytes(testData)
            val expectedCrc = CRC32().also { it.update(testData) }.value

            // Inner RAR5 (3 volumes)
            val innerArchiveVolumes = ArchiveGenerator.generate(testData, 3, ContainerType.RAR5, "payload.bin")
            assertEquals(3, innerArchiveVolumes.size)

            // Outer RAR5 with small volume size to force splits
            val archiveFiles = innerArchiveVolumes.map { ArchiveFile(it.filename, it.data) }
            val maxInnerSize = archiveFiles.maxOf { it.data.size }
            val outerVolumeSize = (maxInnerSize * 0.7).toLong()
            val outerArchiveVolumes = ArchiveGenerator.generate(archiveFiles, outerVolumeSize, ContainerType.RAR5)
            assertTrue(outerArchiveVolumes.size >= 2, "Outer should be multi-volume")

            verifyNestedArchive(outerArchiveVolumes, 3, "payload.bin", expectedCrc)
        }

        @Test
        @Timeout(value = 1, unit = TimeUnit.MINUTES)
        fun `7z inside RAR5 - parse and verify CRC`() = runBlocking {
            val testData = ByteArray(4096)
            Random(123).nextBytes(testData)
            val expectedCrc = CRC32().also { it.update(testData) }.value

            // Inner 7z (3 volumes)
            val innerArchiveVolumes = ArchiveGenerator.generate(testData, 3, ContainerType.SEVENZIP, "payload.bin")
            assertEquals(3, innerArchiveVolumes.size)

            // Outer RAR5 with small volume size to force splits
            val archiveFiles = innerArchiveVolumes.map { ArchiveFile(it.filename, it.data) }
            val maxInnerSize = archiveFiles.maxOf { it.data.size }
            val outerVolumeSize = (maxInnerSize * 0.7).toLong()
            val outerArchiveVolumes = ArchiveGenerator.generate(archiveFiles, outerVolumeSize, ContainerType.RAR5)
            assertTrue(outerArchiveVolumes.size >= 2, "Outer should be multi-volume")

            verifyNestedArchive(outerArchiveVolumes, 3, "payload.bin", expectedCrc)
        }

        private suspend fun verifyNestedArchive(
            outerArchiveVolumes: List<ArchiveVolume>,
            expectedInnerVolumeCount: Int,
            expectedFileName: String,
            expectedCrc: Long
        ) {
            val outerData = ByteArrayOutputStream().also { out ->
                outerArchiveVolumes.forEach { out.write(it.data) }
            }.toByteArray()

            val outerVolumeMetadata = outerArchiveVolumes.map { vol ->
                VolumeMetaData(vol.filename, vol.data.size.toLong(),
                    first16kb = vol.data.copyOf(minOf(16384, vol.data.size)))
            }

            // Parse outer
            val outerResult = ArchiveService.listFiles(ByteArraySeekableInputStream(outerData), outerVolumeMetadata)
            val outerEntries = assertIs<ListFilesResult.Success>(outerResult).entries

            println("Outer volumes: ${outerArchiveVolumes.map { "${it.filename} (${it.data.size})" }}")
            println("All outer entries (${outerEntries.size}):")
            outerEntries.forEach { e ->
                val extra = when (e) {
                    is RarFileEntry -> "vol=${e.volumeIndex} split=${e.isSplit} parts=${e.splitParts.size} dataPos=${e.dataPosition}"
                    else -> e::class.simpleName ?: ""
                }
                println("  ${e.path} size=${e.size} dir=${e.isDirectory} known=${ArchiveService.fileHasKnownExtension(e.path)} $extra")
            }
            val innerArchiveEntries = outerEntries.filter {
                !it.isDirectory && ArchiveService.fileHasKnownExtension(it.path)
            }
            assertEquals(expectedInnerVolumeCount, innerArchiveEntries.size)

            // Resolve inner volumes
            val innerVolMeta = innerArchiveEntries.map { VolumeMetaData(it.path, it.size) }
            val resolvedInner = ArchiveService.resolveVolumes(innerVolMeta)
            val orderedInnerEntries = resolvedInner.map { vol ->
                innerArchiveEntries.first { it.path == vol.filename }
            }

            val outerVolumeOffsets = computeVolumeOffsets(outerArchiveVolumes)
            val innerVolumeSizes = resolvedInner.map { it.size }
            val outerChunks = orderedInnerEntries.map { getEntryChunks(it, outerVolumeOffsets) }

            // Read first-16KB headers
            val volumesWithHeaders = readVolumeHeaders(resolvedInner, outerChunks, outerData)

            // Parse inner archive
            val innerResult = NestedSeekableInputStream(
                outerStream = ByteArraySeekableInputStream(outerData),
                innerVolumeSizes = innerVolumeSizes,
                outerChunks = outerChunks
            ).use { stream ->
                ArchiveService.listFiles(stream, volumesWithHeaders)
            }
            val innerEntries = assertIs<ListFilesResult.Success>(innerResult).entries
            val innerFiles = innerEntries.filter { !it.isDirectory }
            assertEquals(1, innerFiles.size)
            assertEquals(expectedFileName, innerFiles[0].path)

            // Translate and verify CRC
            val innerCumOffsets = buildCumOffsets(innerVolumeSizes)
            val translatedSplits = translateInnerFile(innerFiles[0], innerCumOffsets, innerVolumeSizes, outerChunks)
            assertEquals(innerFiles[0].size, translatedSplits.sumOf { it.dataSize })

            val actualCrc = computeCrc(translatedSplits, outerData)
            assertEquals(expectedCrc, actualCrc,
                "CRC mismatch: expected ${"%08x".format(expectedCrc)}, got ${"%08x".format(actualCrc)}")
        }
    }

    // --- Shared helper functions ---

    private fun computeVolumeOffsets(volumes: List<ArchiveVolume>): List<Long> = buildList {
        var cum = 0L
        add(cum)
        for (vol in volumes) {
            cum += vol.data.size
            add(cum)
        }
    }

    private fun getEntryChunks(entry: ArchiveFileEntry, outerVolumeOffsets: List<Long>): List<SplitInfo> =
        when (entry) {
            is RarFileEntry -> {
                if (entry.splitParts.isNotEmpty()) entry.splitParts
                else listOf(SplitInfo(0, outerVolumeOffsets[entry.volumeIndex] + entry.dataPosition, entry.size))
            }
            is SevenZipFileEntry -> listOf(SplitInfo(0, entry.dataOffset, entry.size))
            is TranslatedFileEntry -> entry.splitParts
        }

    private suspend fun readVolumeHeaders(
        resolvedVolumes: List<VolumeMetaData>,
        outerChunks: List<List<SplitInfo>>,
        outerData: ByteArray
    ): List<VolumeMetaData> = resolvedVolumes.zip(outerChunks).map { (vol, chunks) ->
        val stream = ByteArraySeekableInputStream(outerData)
        val first16kb = ByteArray(minOf(16384, vol.size).toInt())
        stream.seek(chunks[0].dataStartPosition)
        var totalRead = 0
        while (totalRead < first16kb.size) {
            val n = stream.read(first16kb, totalRead, first16kb.size - totalRead)
            if (n <= 0) break
            totalRead += n
        }
        vol.copy(first16kb = if (totalRead > 0) first16kb.copyOf(totalRead) else null)
    }

    private fun buildCumOffsets(sizes: List<Long>): List<Long> = buildList {
        var cum = 0L
        add(cum)
        for (size in sizes) { cum += size; add(cum) }
    }

    private fun translateInnerFile(
        entry: ArchiveFileEntry,
        innerCumOffsets: List<Long>,
        innerVolumeSizes: List<Long>,
        outerChunks: List<List<SplitInfo>>
    ): List<SplitInfo> = when (entry) {
        is RarFileEntry -> {
            if (entry.splitParts.isNotEmpty()) {
                entry.splitParts.flatMap { split ->
                    translateRange(split.dataStartPosition, split.dataSize, innerCumOffsets, innerVolumeSizes, outerChunks)
                }
            } else {
                val innerAbsPos = innerCumOffsets[entry.volumeIndex] + entry.dataPosition
                translateRange(innerAbsPos, entry.uncompressedSize, innerCumOffsets, innerVolumeSizes, outerChunks)
            }
        }
        is SevenZipFileEntry ->
            translateRange(entry.dataOffset, entry.size, innerCumOffsets, innerVolumeSizes, outerChunks)
        is TranslatedFileEntry -> entry.splitParts
    }

    private fun translateRange(
        startPos: Long,
        size: Long,
        innerCumOffsets: List<Long>,
        innerVolumeSizes: List<Long>,
        outerChunks: List<List<SplitInfo>>
    ): List<SplitInfo> {
        val splits = mutableListOf<SplitInfo>()
        var remaining = size
        var pos = startPos
        while (remaining > 0) {
            val volIndex = findVolumeIndex(pos, innerCumOffsets)
            val localOffset = pos - innerCumOffsets[volIndex]
            val availableInVol = innerVolumeSizes[volIndex] - localOffset
            val takeFromVol = minOf(remaining, availableInVol)

            var chunkLocalPos = localOffset
            var volRemaining = takeFromVol
            for (chunk in outerChunks[volIndex]) {
                if (chunkLocalPos >= chunk.dataSize) {
                    chunkLocalPos -= chunk.dataSize
                    continue
                }
                val takeFromChunk = minOf(volRemaining, chunk.dataSize - chunkLocalPos)
                splits.add(SplitInfo(0, chunk.dataStartPosition + chunkLocalPos, takeFromChunk))
                volRemaining -= takeFromChunk
                chunkLocalPos = 0
                if (volRemaining <= 0) break
            }

            remaining -= takeFromVol
            pos += takeFromVol
        }
        return splits
    }

    private fun findVolumeIndex(pos: Long, cumOffsets: List<Long>): Int {
        var lo = 0
        var hi = cumOffsets.size - 2
        while (lo < hi) {
            val mid = (lo + hi + 1) / 2
            if (cumOffsets[mid] <= pos) lo = mid else hi = mid - 1
        }
        return lo
    }

    private suspend fun computeCrc(splits: List<SplitInfo>, outerData: ByteArray): Long {
        val crc = CRC32()
        val stream = ByteArraySeekableInputStream(outerData)
        for (split in splits) {
            stream.seek(split.dataStartPosition)
            val buffer = ByteArray(65536)
            var remaining = split.dataSize
            while (remaining > 0) {
                val toRead = minOf(remaining, buffer.size.toLong()).toInt()
                val bytesRead = stream.read(buffer, 0, toRead)
                if (bytesRead <= 0) break
                crc.update(buffer, 0, bytesRead)
                remaining -= bytesRead
            }
            assertEquals(0L, remaining, "Should read all bytes in split")
        }
        return crc.value
    }
}
