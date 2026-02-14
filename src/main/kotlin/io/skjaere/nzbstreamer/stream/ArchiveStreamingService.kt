package io.skjaere.nzbstreamer.stream

import io.ktor.utils.io.*
import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.SplitInfo
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
        val entry = entries.firstOrNull { entry ->
            when (entry) {
                is RarFileEntry -> entry.path == path
                is SevenZipFileEntry -> entry.path == path
            }
        } ?: return FileResolveResult.NotFound

        val isDirectory = when (entry) {
            is RarFileEntry -> entry.isDirectory
            is SevenZipFileEntry -> entry.isDirectory
        }
        if (isDirectory) return FileResolveResult.IsDirectory

        when (entry) {
            is RarFileEntry -> if (!entry.isUncompressed) {
                return FileResolveResult.Compressed("File is compressed (RAR method=${entry.compressionMethod})")
            }
            is SevenZipFileEntry -> if (entry.method != null && entry.method != "Copy") {
                return FileResolveResult.Compressed("File is compressed (7z method=${entry.method})")
            }
        }

        val splits = when (entry) {
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
        }

        val totalSize = when (entry) {
            is RarFileEntry -> entry.uncompressedSize
            is SevenZipFileEntry -> entry.size
        }

        return FileResolveResult.Ok(splits, totalSize)
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

    companion object {
        internal fun adjustSplitsForRange(
            splits: List<SplitInfo>,
            rangeStart: Long,
            rangeLength: Long
        ): List<SplitInfo> {
            val rangeEnd = rangeStart + rangeLength
            val result = mutableListOf<SplitInfo>()
            var fileOffset = 0L

            for (split in splits) {
                val splitEnd = fileOffset + split.dataSize

                if (splitEnd <= rangeStart || fileOffset >= rangeEnd) {
                    fileOffset = splitEnd
                    continue
                }

                val trimStart = maxOf(0L, rangeStart - fileOffset)
                val trimEnd = minOf(split.dataSize, rangeEnd - fileOffset)

                result.add(
                    SplitInfo(
                        volumeIndex = split.volumeIndex,
                        dataStartPosition = split.dataStartPosition + trimStart,
                        dataSize = trimEnd - trimStart
                    )
                )

                fileOffset = splitEnd
            }

            return result
        }

        private fun computeVolumeOffsets(archiveNzb: NzbDocument): List<Long> {
            val offsets = mutableListOf<Long>()
            var cumOffset = 0L
            for (file in archiveNzb.files) {
                offsets.add(cumOffset)
                cumOffset += file.yencHeaders!!.size
            }
            return offsets
        }
    }
}
