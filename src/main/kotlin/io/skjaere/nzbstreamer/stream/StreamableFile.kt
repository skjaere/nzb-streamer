package io.skjaere.nzbstreamer.stream

import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nzbstreamer.nzb.NzbDocument

data class NamedSplits(
    val splits: List<SplitInfo>,
    val totalSize: Long,
    val name: String = "unknown"
)

/**
 * A file [startOffsetInVolume] bytes into volume [startVolumeIndex] of its archive. The offset may
 * lie past that volume's end: a 7z entry's is into the volumes laid end to end from volume 0, and
 * the file then starts in the volume the offset falls in ([toSplits]).
 */
data class StreamableFile(
    val path: String,
    val totalSize: Long,
    val startVolumeIndex: Int,
    val startOffsetInVolume: Long,
    val continuationHeaderSize: Long,  // 0 for non-split / 7z
    val endOfArchiveSize: Long,        // 0 for non-split / 7z
    val preComputedSplits: List<SplitInfo>? = null  // used when inference can't reproduce correct splits
)

fun StreamableFile.toSplits(archiveNzb: NzbDocument): List<SplitInfo> {
    if (preComputedSplits != null) return preComputedSplits

    val volumeOffsets = ArchiveStreamingService.computeVolumeOffsets(archiveNzb)
    val volumeSizes = ArchiveStreamingService.computeVolumeSizes(archiveNzb)
    // An offset past its volume's end (a 7z entry's) starts the file in a later volume.
    val (startVolume, startOffset) = generateSequence(startVolumeIndex to startOffsetInVolume) { (volume, offset) ->
        if (volume < volumeSizes.lastIndex && offset >= volumeSizes[volume]) volume + 1 to offset - volumeSizes[volume] else null
    }.last()
    val parts = mutableListOf<SplitInfo>()
    var remaining = totalSize
    var cumulativeOffset = if (startVolume < volumeOffsets.size) volumeOffsets[startVolume] else 0L

    for (volIdx in startVolume until volumeSizes.size) {
        if (remaining <= 0) break

        val volumeSize = volumeSizes[volIdx]
        val headerOverhead = if (volIdx == startVolume) startOffset else continuationHeaderSize
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
