package io.skjaere.nzbstreamer.metadata

import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.ArchiveService
import io.skjaere.compressionutils.ListFilesResult
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.compressionutils.TranslatedFileEntry
import io.skjaere.compressionutils.VolumeMetaData
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.seekable.NestedSeekableInputStream
import io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import org.slf4j.LoggerFactory

class NestedArchiveMetadataService(
    private val streamingService: NntpStreamingService,
    private val forwardThresholdBytes: Long
) {
    private val logger = LoggerFactory.getLogger(NestedArchiveMetadataService::class.java)

    /**
     * Heuristic: outer entries contain multiple files with archive extensions
     * whose total size is a significant portion (>50%) of all non-directory entries.
     */
    fun looksLikeNestedArchive(
        archiveEntries: List<ArchiveFileEntry>,
        allEntries: List<ArchiveFileEntry>
    ): Boolean {
        if (archiveEntries.size < 2) return false
        val archiveSize = archiveEntries.sumOf { it.size }
        val totalSize = allEntries.filter { !it.isDirectory }.sumOf { it.size }
        return totalSize > 0 && archiveSize.toDouble() / totalSize > 0.5
    }

    suspend fun extractNestedMetadata(
        outerEntries: List<ArchiveFileEntry>,
        innerArchiveEntries: List<ArchiveFileEntry>,
        orderedArchiveNzb: NzbDocument,
        orderedVolumes: List<VolumeMetaData>,
        obfuscated: Boolean
    ): ExtractedMetadata {
        val (resolvedInnerVolumes, innerVolumeSizes, outerDataOffsets) =
            resolveInnerVolumes(innerArchiveEntries, orderedArchiveNzb)

        val innerRawEntries = parseInnerArchive(
            resolvedInnerVolumes, innerVolumeSizes, outerDataOffsets, orderedArchiveNzb
        ) ?: run {
            logger.info("Inner archive format not supported; falling back to outer entries")
            return ExtractedMetadata.Archive(
                response = NzbMetadataResponse(
                    volumes = orderedVolumes.map { it.filename },
                    obfuscated = obfuscated,
                    entries = outerEntries.map { it.toResponse() }
                ),
                orderedArchiveNzb = orderedArchiveNzb,
                entries = outerEntries
            )
        }

        return buildNestedResult(
            innerRawEntries, innerVolumeSizes, outerDataOffsets,
            outerEntries, orderedArchiveNzb, orderedVolumes, obfuscated
        )
    }

    private data class ResolvedInnerVolumes(
        val volumes: List<VolumeMetaData>,
        val sizes: List<Long>,
        val outerDataOffsets: List<Long>
    )

    private fun resolveInnerVolumes(
        innerArchiveEntries: List<ArchiveFileEntry>,
        orderedArchiveNzb: NzbDocument
    ): ResolvedInnerVolumes {
        val innerVolumes = innerArchiveEntries.map { entry ->
            VolumeMetaData(filename = entry.path, size = entry.size)
        }
        val resolvedInnerVolumes = ArchiveService.resolveVolumes(innerVolumes)
        val orderedInnerArchiveEntries = resolvedInnerVolumes.map { vol ->
            innerArchiveEntries.first { it.path == vol.filename }
        }
        val outerVolumeOffsets = ArchiveStreamingService.computeVolumeOffsets(orderedArchiveNzb)
        return ResolvedInnerVolumes(
            volumes = resolvedInnerVolumes,
            sizes = resolvedInnerVolumes.map { it.size },
            outerDataOffsets = orderedInnerArchiveEntries.map { getEntryDataOffset(it, outerVolumeOffsets) }
        )
    }

    /**
     * Reads first-16KB headers for each inner volume and parses the inner archive.
     * Returns null if the inner archive format is unsupported.
     */
    private suspend fun parseInnerArchive(
        resolvedInnerVolumes: List<VolumeMetaData>,
        innerVolumeSizes: List<Long>,
        outerDataOffsets: List<Long>,
        orderedArchiveNzb: NzbDocument
    ): List<ArchiveFileEntry>? {
        val volumesWithHeaders = NntpSeekableInputStream(
            orderedArchiveNzb, streamingService, forwardThresholdBytes
        ).use { stream ->
            resolvedInnerVolumes.zip(outerDataOffsets).map { (vol, outerOffset) ->
                val first16kb = ByteArray(minOf(16384, vol.size).toInt())
                stream.seek(outerOffset)
                var totalRead = 0
                while (totalRead < first16kb.size) {
                    val n = stream.read(first16kb, totalRead, first16kb.size - totalRead)
                    if (n <= 0) break
                    totalRead += n
                }
                vol.copy(first16kb = if (totalRead > 0) first16kb.copyOf(totalRead) else null)
            }
        }

        val innerListResult = NestedSeekableInputStream(
            outerStream = NntpSeekableInputStream(orderedArchiveNzb, streamingService, forwardThresholdBytes),
            innerVolumeSizes = innerVolumeSizes,
            outerDataOffsets = outerDataOffsets
        ).use { stream ->
            ArchiveService.listFiles(stream, volumesWithHeaders)
        }

        return when (innerListResult) {
            is ListFilesResult.Success -> innerListResult.entries
            is ListFilesResult.UnsupportedFormat -> null
        }
    }

    private fun buildNestedResult(
        innerRawEntries: List<ArchiveFileEntry>,
        innerVolumeSizes: List<Long>,
        outerDataOffsets: List<Long>,
        outerEntries: List<ArchiveFileEntry>,
        orderedArchiveNzb: NzbDocument,
        orderedVolumes: List<VolumeMetaData>,
        obfuscated: Boolean
    ): ExtractedMetadata.NestedArchive {
        val innerCumOffsets = buildList {
            var cum = 0L
            add(cum)
            for (size in innerVolumeSizes) {
                cum += size
                add(cum)
            }
        }

        val translatedEntries = translateInnerEntries(
            innerRawEntries, innerCumOffsets, innerVolumeSizes, outerDataOffsets
        )

        val allResponseEntries = translatedEntries.map { it.toResponse() } +
                outerEntries.filter { entry ->
                    !entry.isDirectory && !ArchiveService.fileHasKnownExtension(entry.path)
                }.map { it.toResponse() }

        return ExtractedMetadata.NestedArchive(
            response = NzbMetadataResponse(
                volumes = orderedVolumes.map { it.filename },
                obfuscated = obfuscated,
                entries = allResponseEntries
            ),
            orderedArchiveNzb = orderedArchiveNzb,
            innerEntries = translatedEntries,
            outerEntries = outerEntries
        )
    }

    /**
     * Gets the absolute data offset of an entry in the outer concatenated stream.
     */
    private fun getEntryDataOffset(
        entry: ArchiveFileEntry,
        outerVolumeOffsets: List<Long>
    ): Long = when (entry) {
        is RarFileEntry -> {
            if (entry.splitParts.isNotEmpty()) {
                entry.splitParts[0].dataStartPosition
            } else {
                outerVolumeOffsets[entry.volumeIndex] + entry.dataPosition
            }
        }
        is SevenZipFileEntry -> entry.dataOffset
        is TranslatedFileEntry -> entry.splitParts[0].dataStartPosition
    }

    /**
     * Translates inner archive entries so their offsets point into the outer stream.
     */
    private fun translateInnerEntries(
        innerEntries: List<ArchiveFileEntry>,
        innerCumOffsets: List<Long>,
        innerVolumeSizes: List<Long>,
        outerDataOffsets: List<Long>
    ): List<ArchiveFileEntry> = innerEntries.map { entry ->
        when (entry) {
            is RarFileEntry -> {
                val translatedSplits = if (entry.splitParts.isNotEmpty()) {
                    entry.splitParts.flatMap { split ->
                        translateRange(split.dataStartPosition, split.dataSize, innerCumOffsets, innerVolumeSizes, outerDataOffsets)
                    }
                } else {
                    val innerAbsPos = innerCumOffsets[entry.volumeIndex] + entry.dataPosition
                    translateRange(innerAbsPos, entry.uncompressedSize, innerCumOffsets, innerVolumeSizes, outerDataOffsets)
                }
                TranslatedFileEntry(
                    path = entry.path,
                    size = entry.uncompressedSize,
                    isDirectory = entry.isDirectory,
                    splitParts = translatedSplits,
                    crc32 = entry.crc32
                )
            }
            is SevenZipFileEntry -> {
                val translatedSplits = translateRange(
                    entry.dataOffset, entry.size, innerCumOffsets, innerVolumeSizes, outerDataOffsets
                )
                TranslatedFileEntry(
                    path = entry.path,
                    size = entry.size,
                    isDirectory = entry.isDirectory,
                    splitParts = translatedSplits,
                    crc32 = entry.crc32
                )
            }
            is TranslatedFileEntry -> entry // already translated
        }
    }

    /**
     * Translates a contiguous byte range in the inner concatenated stream
     * into one or more [SplitInfo] entries with outer stream positions.
     * Handles ranges that cross inner volume boundaries.
     */
    internal fun translateRange(
        startPos: Long,
        size: Long,
        innerCumOffsets: List<Long>,
        innerVolumeSizes: List<Long>,
        outerDataOffsets: List<Long>
    ): List<SplitInfo> {
        val splits = mutableListOf<SplitInfo>()
        var remaining = size
        var pos = startPos

        while (remaining > 0) {
            val volIndex = findVolumeIndex(pos, innerCumOffsets)
            val localOffset = pos - innerCumOffsets[volIndex]
            val availableInVol = innerVolumeSizes[volIndex] - localOffset
            val chunkSize = minOf(remaining, availableInVol)

            splits.add(
                SplitInfo(
                    volumeIndex = 0, // volume index is meaningless for translated splits
                    dataStartPosition = outerDataOffsets[volIndex] + localOffset,
                    dataSize = chunkSize
                )
            )

            remaining -= chunkSize
            pos += chunkSize
        }

        return splits
    }

    private fun findVolumeIndex(pos: Long, cumOffsets: List<Long>): Int {
        var lo = 0
        var hi = cumOffsets.size - 2 // last valid volume index
        while (lo < hi) {
            val mid = (lo + hi + 1) / 2
            if (cumOffsets[mid] <= pos) lo = mid else hi = mid - 1
        }
        return lo
    }
}
