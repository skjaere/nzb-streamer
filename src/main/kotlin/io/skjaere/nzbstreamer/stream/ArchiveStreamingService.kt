package io.skjaere.nzbstreamer.stream

import io.ktor.utils.io.*
import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.compressionutils.TranslatedFileEntry
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory

sealed interface FileResolveResult {
    data class Ok(val splits: List<SplitInfo>, val totalSize: Long) : FileResolveResult
    data object NotFound : FileResolveResult
    data object IsDirectory : FileResolveResult
    data class Compressed(val description: String) : FileResolveResult
}

class ArchiveStreamingService(
    private val streamingService: NntpStreamingService
) {
    private val logger = LoggerFactory.getLogger(ArchiveStreamingService::class.java)

    fun resolveFile(
        entries: List<ArchiveFileEntry>,
        archiveNzb: NzbDocument,
        path: String
    ): FileResolveResult {
        val entry = entries.firstOrNull { it.path == path }
            ?: return FileResolveResult.NotFound

        if (entry.isDirectory) return FileResolveResult.IsDirectory

        return resolveExistingFileEntry(entry, archiveNzb)
    }

    private fun resolveExistingFileEntry(entry: ArchiveFileEntry, archiveNzb: NzbDocument): FileResolveResult {
        when (entry) {
            is RarFileEntry -> if (!entry.isUncompressed) {
                return FileResolveResult.Compressed("File is compressed (RAR method=${entry.compressionMethod})")
            }
            is SevenZipFileEntry -> if (entry.method != null && entry.method != "Copy") {
                return FileResolveResult.Compressed("File is compressed (7z method=${entry.method})")
            }
            is TranslatedFileEntry -> { /* always uncompressed — already validated during translation */ }
        }

        val splits = getSplitsForEntry(entry, archiveNzb)

        return FileResolveResult.Ok(splits, entry.size)
    }

    private fun getSplitsForEntry(
        entry: ArchiveFileEntry,
        archiveNzb: NzbDocument
    ): List<SplitInfo> = when (entry) {
        is RarFileEntry -> {
            entry.splitParts.ifEmpty {
                val volumeOffsets = computeVolumeOffsets(archiveNzb)
                listOf(
                    SplitInfo(
                        volumeIndex = entry.volumeIndex,
                        dataStartPosition = volumeOffsets[entry.volumeIndex] + entry.dataPosition,
                        dataSize = entry.uncompressedSize
                    )
                )
            }
        }

        is SevenZipFileEntry -> listOf(SplitInfo(0, entry.dataOffset, entry.size))

        is TranslatedFileEntry -> entry.splitParts
    }

    suspend fun streamFile(
        archiveNzb: NzbDocument,
        splits: List<SplitInfo>,
        range: LongRange? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val effectiveSplits = if (range != null) {
            adjustSplitsForRange(splits, range.first, range.last - range.first + 1)
        } else {
            splits
        }

        logger.info(
            "Streaming {} splits (range={})",
            effectiveSplits.size,
            range?.let { "${it.first}-${it.last}" } ?: "full"
        )

        val combinedQueue = flow {
            for (split in effectiveSplits) {
                SegmentQueueService.createRangeQueue(
                    archiveNzb, split.dataStartPosition, split.dataSize
                ).collect { emit(it) }
            }
        }
        streamingService.streamSegments(combinedQueue, consume = consume)
    }

    fun launchStreamFile(
        archiveNzb: NzbDocument,
        splits: List<SplitInfo>,
        range: LongRange? = null
    ): WriterJob {
        val effectiveSplits = if (range != null) {
            adjustSplitsForRange(splits, range.first, range.last - range.first + 1)
        } else {
            splits
        }

        val combinedQueue = flow {
            for (split in effectiveSplits) {
                SegmentQueueService.createRangeQueue(
                    archiveNzb, split.dataStartPosition, split.dataSize
                ).collect { emit(it) }
            }
        }
        return streamingService.launchStreamSegments(combinedQueue)
    }

    companion object {
        private fun splitOverlapsRange(
            fileOffset: Long,
            split: SplitInfo,
            rangeStart: Long,
            rangeEnd: Long
        ): Boolean = fileOffset + split.dataSize > rangeStart && fileOffset < rangeEnd

        internal fun adjustSplitsForRange(
            splits: List<SplitInfo>,
            rangeStart: Long,
            rangeLength: Long
        ): List<SplitInfo> {
            val rangeEnd = rangeStart + rangeLength

            return splits.runningFold(0L) { offset, split -> offset + split.dataSize }
                .zip(splits)
                .filter { (fileOffset, split) -> splitOverlapsRange(fileOffset, split, rangeStart, rangeEnd) }
                .map { (fileOffset, split) ->
                    val trimStart = maxOf(0L, rangeStart - fileOffset)
                    val trimEnd = minOf(split.dataSize, rangeEnd - fileOffset)
                    SplitInfo(
                        volumeIndex = split.volumeIndex,
                        dataStartPosition = split.dataStartPosition + trimStart,
                        dataSize = trimEnd - trimStart
                    )
                }
        }

        internal fun computeVolumeOffsets(archiveNzb: NzbDocument): List<Long> {
            val offsets = mutableListOf<Long>()
            var cumOffset = 0L
            for (file in archiveNzb.files) {
                offsets.add(cumOffset)
                cumOffset += file.yencHeaders!!.size
            }
            return offsets
        }

        internal fun computeVolumeSizes(archiveNzb: NzbDocument): List<Long> {
            return archiveNzb.files.map { it.yencHeaders!!.size }
        }
    }

    fun resolveStreamableFile(
        entry: ArchiveFileEntry,
        archiveNzb: NzbDocument
    ): StreamableFile? {
        return when (entry) {
            is RarFileEntry -> resolveRarFile(entry, archiveNzb)
            is SevenZipFileEntry -> resolveSevenZipFile(entry)
            is TranslatedFileEntry -> null // StreamableFile cannot represent pre-computed nested splits
        }
    }

    private fun resolveSevenZipFile(
        entry: SevenZipFileEntry
    ): StreamableFile? {
        if (entry.isDirectory) return null
        if (entry.method != null && entry.method != "Copy") return null

        return StreamableFile(
            path = entry.path,
            totalSize = entry.size,
            startVolumeIndex = 0,
            startOffsetInVolume = entry.dataOffset,
            continuationHeaderSize = 0,
            endOfArchiveSize = 0
        )
    }

    private fun resolveRarFile(
        entry: RarFileEntry,
        archiveNzb: NzbDocument
    ): StreamableFile? {
        if (entry.isDirectory) return null
        if (!entry.isUncompressed) return null

        return if (entry.splitParts.isEmpty()) {
            // Non-split RAR file — dataPosition is already the local offset within the volume
            StreamableFile(
                path = entry.path,
                totalSize = entry.uncompressedSize,
                startVolumeIndex = entry.volumeIndex,
                startOffsetInVolume = entry.dataPosition,
                continuationHeaderSize = 0,
                endOfArchiveSize = 0
            )
        } else {
            // Split RAR file - derive overhead values from parsed split parts
            val volumeOffsets = computeVolumeOffsets(archiveNzb)
            val volumeSizes = computeVolumeSizes(archiveNzb)
            val startVolumeIndex = entry.splitParts[0].volumeIndex
            val startOffsetInVolume = entry.splitParts[0].dataStartPosition - volumeOffsets[startVolumeIndex]

            val continuationHeaderSize = if (entry.splitParts.size > 1) {
                entry.splitParts[1].dataStartPosition - volumeOffsets[entry.splitParts[1].volumeIndex]
            } else {
                0L
            }

            val endOfArchiveSize = volumeSizes[startVolumeIndex] -
                    startOffsetInVolume - entry.splitParts[0].dataSize

            StreamableFile(
                path = entry.path,
                totalSize = entry.uncompressedSize,
                startVolumeIndex = startVolumeIndex,
                startOffsetInVolume = startOffsetInVolume,
                continuationHeaderSize = continuationHeaderSize,
                endOfArchiveSize = endOfArchiveSize
            )
        }
    }

    fun resolveStreamableFiles(
        entries: List<ArchiveFileEntry>,
        archiveNzb: NzbDocument
    ): List<StreamableFile> {
        return entries.mapNotNull { resolveStreamableFile(it, archiveNzb) }
    }

    suspend fun streamFile(
        archiveNzb: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val splits = file.toSplits(archiveNzb)
        streamFile(archiveNzb, splits, range, consume)
    }

    fun launchStreamFile(
        archiveNzb: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null
    ): WriterJob {
        val splits = file.toSplits(archiveNzb)
        return launchStreamFile(archiveNzb, splits, range)
    }
}
