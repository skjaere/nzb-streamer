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
        val (resolvedInnerVolumes, innerVolumeSizes, outerChunks) =
            resolveInnerVolumes(innerArchiveEntries, orderedArchiveNzb)

        val innerRawEntries = parseInnerArchive(
            resolvedInnerVolumes, innerVolumeSizes, outerChunks, orderedArchiveNzb
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
            innerRawEntries, innerVolumeSizes, outerChunks,
            outerEntries, orderedArchiveNzb, orderedVolumes, obfuscated
        )
    }

    private data class ResolvedInnerVolumes(
        val volumes: List<VolumeMetaData>,
        val sizes: List<Long>,
        val outerChunks: List<List<SplitInfo>>
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
            outerChunks = orderedInnerArchiveEntries.map { getEntryChunks(it, outerVolumeOffsets) }
        )
    }

    /**
     * Reads first-16KB headers for each inner volume and parses the inner archive.
     * Returns null if the inner archive format is unsupported.
     */
    private suspend fun parseInnerArchive(
        resolvedInnerVolumes: List<VolumeMetaData>,
        innerVolumeSizes: List<Long>,
        outerChunks: List<List<SplitInfo>>,
        orderedArchiveNzb: NzbDocument
    ): List<ArchiveFileEntry>? {
        val volumesWithHeaders = NntpSeekableInputStream(
            orderedArchiveNzb, streamingService, forwardThresholdBytes
        ).use { stream ->
            resolvedInnerVolumes.zip(outerChunks).map { (vol, chunks) ->
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
        }

        val innerListResult = NestedSeekableInputStream(
            outerStream = NntpSeekableInputStream(orderedArchiveNzb, streamingService, forwardThresholdBytes),
            innerVolumeSizes = innerVolumeSizes,
            outerChunks = outerChunks
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
        outerChunks: List<List<SplitInfo>>,
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
            innerRawEntries, innerCumOffsets, innerVolumeSizes, outerChunks
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
     * Gets the chunks describing where an entry's data lives in the outer concatenated stream.
     */
    private fun getEntryChunks(
        entry: ArchiveFileEntry,
        outerVolumeOffsets: List<Long>
    ): List<SplitInfo> = when (entry) {
        is RarFileEntry -> {
            if (entry.splitParts.isNotEmpty()) {
                entry.splitParts
            } else {
                listOf(SplitInfo(0, outerVolumeOffsets[entry.volumeIndex] + entry.dataPosition, entry.size))
            }
        }

        is SevenZipFileEntry -> listOf(SplitInfo(0, entry.dataOffset, entry.size))
        is TranslatedFileEntry -> entry.splitParts
    }

    /**
     * Translates inner archive entries so their offsets point into the outer stream.
     */
    private fun translateInnerEntries(
        innerEntries: List<ArchiveFileEntry>,
        innerCumOffsets: List<Long>,
        innerVolumeSizes: List<Long>,
        outerChunks: List<List<SplitInfo>>
    ): List<ArchiveFileEntry> = innerEntries.map { entry ->
        when (entry) {
            is RarFileEntry -> {
                val translatedSplits = if (entry.splitParts.isNotEmpty()) {
                    entry.splitParts.flatMap { split ->
                        translateRange(
                            split.dataStartPosition,
                            split.dataSize,
                            innerCumOffsets,
                            innerVolumeSizes,
                            outerChunks
                        )
                    }
                } else {
                    val innerAbsPos = innerCumOffsets[entry.volumeIndex] + entry.dataPosition
                    translateRange(innerAbsPos, entry.uncompressedSize, innerCumOffsets, innerVolumeSizes, outerChunks)
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
                    entry.dataOffset, entry.size, innerCumOffsets, innerVolumeSizes, outerChunks
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
     * Handles ranges that cross inner volume boundaries, and walks each
     * volume's chunks to handle non-contiguous outer data (e.g., RAR splits).
     */
    internal fun translateRange(
        startPos: Long,
        size: Long,
        innerCumOffsets: List<Long>,
        innerVolumeSizes: List<Long>,
        outerChunks: List<List<SplitInfo>>
    ): List<SplitInfo> {
        if (size <= 0) return emptyList()
        val totalInnerSize = innerCumOffsets.last()
        if (startPos >= totalInnerSize) return emptyList()

        // Clamp to what actually exists in the inner stream — prevents
        // infinite loop when size > remaining data (e.g. uncompressedSize
        // passed for a compressed entry).
        val effectiveSize = minOf(size, totalInnerSize - startPos)
        val splits = mutableListOf<SplitInfo>()
        var remaining = effectiveSize
        var pos = startPos
        var volIndex = findVolumeIndex(pos, innerCumOffsets)

        while (remaining > 0) {
            // Advance volume index forward (pos only increases)
            while (volIndex < innerVolumeSizes.size - 1 && pos >= innerCumOffsets[volIndex + 1]) {
                volIndex++
            }

            val localOffset = pos - innerCumOffsets[volIndex]
            val availableInVol = innerVolumeSizes[volIndex] - localOffset
            val takeFromVol = minOf(remaining, availableInVol)

            // Walk chunks within this volume to map localOffset → outer positions
            var chunkLocalPos = localOffset
            var volRemaining = takeFromVol
            for (chunk in outerChunks[volIndex]) {
                if (volRemaining <= 0) break
                if (chunkLocalPos >= chunk.dataSize) {
                    chunkLocalPos -= chunk.dataSize
                    continue
                }
                val takeFromChunk = minOf(volRemaining, chunk.dataSize - chunkLocalPos)
                splits.add(
                    SplitInfo(
                        volumeIndex = 0,
                        dataStartPosition = chunk.dataStartPosition + chunkLocalPos,
                        dataSize = takeFromChunk
                    )
                )
                volRemaining -= takeFromChunk
                chunkLocalPos = 0 // subsequent chunks start from beginning
            }

            remaining -= takeFromVol
            pos += takeFromVol
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
