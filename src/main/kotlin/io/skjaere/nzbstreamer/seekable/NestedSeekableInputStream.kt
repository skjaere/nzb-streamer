package io.skjaere.nzbstreamer.seekable

import io.skjaere.compressionutils.SeekableInputStream
import io.skjaere.compressionutils.SplitInfo

/**
 * A [SeekableInputStream] that maps positions in an "inner concatenated stream"
 * (inner archive volumes laid out sequentially) to positions in an outer stream.
 *
 * This enables reading nested archives (e.g., RAR volumes inside a 7z or RAR archive)
 * by translating inner byte positions to outer byte positions.
 * Both archives must be uncompressed (7z Copy / RAR store) for this to work.
 *
 * Each inner volume's data may consist of multiple non-contiguous chunks in the outer
 * stream (e.g., when a RAR entry is split across outer RAR volumes). Each chunk is
 * described by a [SplitInfo] with an absolute position and size in the outer stream.
 *
 * @param outerStream The underlying seekable stream (e.g., NntpSeekableInputStream over the outer archive)
 * @param innerVolumeSizes Size of each inner volume file in bytes
 * @param outerChunks For each inner volume, the list of chunks describing where its data
 *                    lives in the outer stream. For contiguous data (e.g., 7z outer),
 *                    this is a single-element list per volume.
 */
class NestedSeekableInputStream(
    private val outerStream: SeekableInputStream,
    private val innerVolumeSizes: List<Long>,
    private val outerChunks: List<List<SplitInfo>>
) : SeekableInputStream {

    private val innerCumOffsets: List<Long> = buildList {
        var cum = 0L
        add(cum)
        for (size in innerVolumeSizes) {
            cum += size
            add(cum)
        }
    }

    private val totalSize: Long = innerCumOffsets.last()
    private var position: Long = 0

    override suspend fun seek(position: Long) {
        this.position = position
        val (outerPos, _) = findChunkPosition(findVolumeIndex(position), position - innerCumOffsets[findVolumeIndex(position)])
        outerStream.seek(outerPos)
    }

    override suspend fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (position >= totalSize) return -1
        if (length == 0) return 0

        val volIndex = findVolumeIndex(position)
        val localOffset = position - innerCumOffsets[volIndex]
        val (expectedOuterPos, remainingInChunk) = findChunkPosition(volIndex, localOffset)
        val remainingInTotal = totalSize - position
        val clampedLen = minOf(length.toLong(), remainingInChunk, remainingInTotal).toInt()

        if (outerStream.position() != expectedOuterPos) {
            outerStream.seek(expectedOuterPos)
        }

        val bytesRead = outerStream.read(buffer, offset, clampedLen)
        if (bytesRead > 0) {
            position += bytesRead
        }
        return bytesRead
    }

    override suspend fun read(): Int {
        if (position >= totalSize) return -1
        val buf = ByteArray(1)
        val n = read(buf, 0, 1)
        return if (n == 1) buf[0].toInt() and 0xFF else -1
    }

    override fun position(): Long = position

    override fun size(): Long = totalSize

    override fun close() {
        outerStream.close()
    }

    /**
     * Given a volume index and a local offset within that volume, finds which chunk
     * contains the offset and returns the absolute outer stream position plus the
     * number of bytes remaining in that chunk.
     */
    private fun findChunkPosition(volIndex: Int, localOffset: Long): Pair<Long, Long> {
        var remaining = localOffset
        for (chunk in outerChunks[volIndex]) {
            if (remaining < chunk.dataSize) {
                return Pair(chunk.dataStartPosition + remaining, chunk.dataSize - remaining)
            }
            remaining -= chunk.dataSize
        }
        // Should not reach here if localOffset is within volume bounds
        val lastChunk = outerChunks[volIndex].last()
        return Pair(lastChunk.dataStartPosition + lastChunk.dataSize, 0)
    }

    private fun findVolumeIndex(innerPos: Long): Int {
        var lo = 0
        var hi = innerVolumeSizes.size - 1
        while (lo < hi) {
            val mid = (lo + hi + 1) / 2
            if (innerCumOffsets[mid] <= innerPos) lo = mid else hi = mid - 1
        }
        return lo
    }
}
