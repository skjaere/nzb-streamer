package io.skjaere.nzbstreamer.stream

import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nzbstreamer.nzb.NzbDocument

data class StreamableFile(
    val path: String,
    val totalSize: Long,
    val startVolumeIndex: Int,
    val startOffsetInVolume: Long,
    val continuationHeaderSize: Long,  // 0 for non-split / 7z
    val endOfArchiveSize: Long         // 0 for non-split / 7z
)

fun StreamableFile.toSplits(archiveNzb: NzbDocument): List<SplitInfo> {

    val volumeOffsets = ArchiveStreamingService.computeVolumeOffsets(archiveNzb)
    val volumeSizes = ArchiveStreamingService.computeVolumeSizes(archiveNzb)
    val parts = mutableListOf<SplitInfo>()
    var remaining = totalSize
    var cumulativeOffset = if (startVolumeIndex < volumeOffsets.size) volumeOffsets[startVolumeIndex] else 0L

    for (volIdx in startVolumeIndex until volumeSizes.size) {
        if (remaining <= 0) break

        val volumeSize = volumeSizes[volIdx]
        val headerOverhead = if (volIdx == startVolumeIndex) startOffsetInVolume else continuationHeaderSize
        val available = volumeSize - headerOverhead - endOfArchiveSize
        val dataSize = minOf(remaining, available)

        parts.add(
            SplitInfo(
                volumeIndex = volIdx,
                dataStartPosition = cumulativeOffset + headerOverhead,
                dataSize = dataSize
            )
        )

        remaining -= dataSize
        cumulativeOffset += volumeSize
    }

    return parts
}
