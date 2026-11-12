package io.skjaere.nzbstreamer

import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.ArchiveService
import io.skjaere.compressionutils.ListFilesResult
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.compressionutils.VolumeMetaData
import io.skjaere.compressionutils.validation.ConcatenatedFileSeekableInputStream
import io.skjaere.nzbstreamer.seekable.NestedSeekableInputStream
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.zip.CRC32
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Tests nested archive support using a real double archive: 11 split 7z volumes (Copy method)
 * containing 52 old-style RAR volumes (.rar + .r00-.r50) plus proof.jpg, sample.mkv, .nfo, and
 * .sfv. The folder holding the 7z volumes comes from the system property [DIR_PROPERTY] or the
 * environment variable [DIR_VARIABLE]; the tests are skipped when neither is set, or set blank. The
 * nested-archive tests that need no fixture are in [GeneratedNestedArchiveTest],
 * [ChunkedNestedSeekableInputStreamTest] and, over NNTP, [NestedArchivePrepareIntegrationTest].
 */
class NestedArchiveTest {

    companion object {
        const val DIR_PROPERTY = "nzbstreamer.doubleArchiveDir"
        const val DIR_VARIABLE = "NZB_STREAMER_DOUBLE_ARCHIVE_DIR"

        private lateinit var outerVolumes: List<File>
        private lateinit var outerVolumeMetadata: List<VolumeMetaData>
        private lateinit var outerEntries: List<ArchiveFileEntry>

        @JvmStatic
        @BeforeAll
        fun setup() {
            val dir = (System.getProperty(DIR_PROPERTY) ?: System.getenv(DIR_VARIABLE))?.takeIf { it.isNotBlank() }
            assumeTrue(dir != null, "Set $DIR_PROPERTY or $DIR_VARIABLE to the double archive's folder to run these")
            val archiveDir = File(dir!!)
            check(archiveDir.isDirectory) { "No folder at ${archiveDir.absolutePath}" }

            outerVolumes = archiveDir.listFiles()!!
                .filter { it.name.matches(Regex(""".*\.7z\.\d{3}$""")) }
                .sortedBy { it.name }
            check(outerVolumes.isNotEmpty()) { "No 7z volumes (name.7z.001, ...) in ${archiveDir.absolutePath}" }

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
}
