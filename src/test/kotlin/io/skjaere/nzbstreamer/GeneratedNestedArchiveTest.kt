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
import io.skjaere.nzbstreamer.seekable.NestedSeekableInputStream
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit
import java.util.zip.CRC32
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Nested archives generated in memory: an inner archive's volumes packed into a multi-volume
 * outer RAR5, parsed through [NestedSeekableInputStream], with the inner file's CRC checked at
 * the outer offsets it translates to.
 */
class GeneratedNestedArchiveTest {

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
