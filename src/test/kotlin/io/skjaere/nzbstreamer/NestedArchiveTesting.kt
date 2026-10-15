package io.skjaere.nzbstreamer

import io.skjaere.compressionutils.SplitInfo

// Offset translation shared by the nested-archive tests: where a range of the inner volumes, laid
// end to end, lies in the outer stream.

internal fun buildCumOffsets(sizes: List<Long>): List<Long> = buildList {
    var cum = 0L
    add(cum)
    for (size in sizes) { cum += size; add(cum) }
}

internal fun translateRange(
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
