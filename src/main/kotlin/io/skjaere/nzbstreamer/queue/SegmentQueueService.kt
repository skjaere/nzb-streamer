package io.skjaere.nzbstreamer.queue

import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

object SegmentQueueService {

    fun createQueue(nzb: NzbDocument): Flow<SegmentQueueItem> = flow {
        var globalOffset = 0L
        for ((fileIndex, file) in nzb.files.withIndex()) {
            val items = createFileQueueList(file, fileIndex, globalOffset)
            for (item in items) {
                emit(item)
            }
            globalOffset += getFileDecodedSize(file)
        }
    }

    fun createFileQueue(file: NzbFile, fileIndex: Int, fileGlobalOffset: Long): Flow<SegmentQueueItem> = flow {
        val items = createFileQueueList(file, fileIndex, fileGlobalOffset)
        for (item in items) {
            emit(item)
        }
    }

    fun createRangeQueue(nzb: NzbDocument, startOffset: Long, limit: Long): Flow<SegmentQueueItem> = flow {
        val rangeEnd = startOffset + limit
        var globalOffset = 0L

        for ((fileIndex, file) in nzb.files.withIndex()) {
            val fileSize = getFileDecodedSize(file)
            val fileEnd = globalOffset + fileSize

            if (fileEnd <= startOffset || globalOffset >= rangeEnd) {
                globalOffset = fileEnd
                continue
            }

            val items = createFileQueueList(file, fileIndex, globalOffset)
            for (item in items) {
                val segStart = item.globalOffset
                val segEnd = segStart + item.segmentDecodedSize

                if (segEnd <= startOffset || segStart >= rangeEnd) continue

                val readStart = maxOf(0L, startOffset - segStart)
                val readEnd = minOf(item.segmentDecodedSize, rangeEnd - segStart)
                emit(item.copy(readStart = readStart, readEnd = readEnd))
            }

            globalOffset = fileEnd
        }
    }

    /** Every segment of [nzb] with its decoded size and position in the concatenated files. */
    fun segmentLayout(nzb: NzbDocument): List<SegmentQueueItem> {
        var globalOffset = 0L
        return nzb.files.flatMapIndexed { fileIndex, file ->
            createFileQueueList(file, fileIndex, globalOffset).also { globalOffset += getFileDecodedSize(file) }
        }
    }

    fun getTotalDecodedSize(nzb: NzbDocument): Long {
        return nzb.files.sumOf { getFileDecodedSize(it) }
    }

    private fun getFileDecodedSize(file: NzbFile): Long {
        return file.yencHeaders?.size
            ?: error("File not enriched: yencHeaders is null")
    }

    private fun getPartSize(file: NzbFile): Long {
        val headers = file.yencHeaders
            ?: error("File not enriched: yencHeaders is null")
        // For single-part articles, partEnd is null — the part size equals the file size
        val end = headers.partEnd ?: return headers.size
        // Part 1 begins at 1, so its end is its size. A first listed article that is a later part
        // (the NZB lacks part 1) holds the bytes from its begin.
        val begin = headers.partBegin ?: 1
        return if (begin > 1) end - begin + 1 else end
    }

    private fun createFileQueueList(
        file: NzbFile,
        fileIndex: Int,
        fileGlobalOffset: Long
    ): List<SegmentQueueItem> {
        val fileSize = getFileDecodedSize(file)
        val partSize = getPartSize(file)
        val posted = postedParts(file, fileSize, partSize)
            ?: return listedParts(file, fileIndex, fileGlobalOffset, fileSize, partSize)

        // Each listed article goes where its part is, by its number when the numbers are the
        // parts' (and the first agrees with its =ypart begin), else in order from the start.
        val numbers = file.segments.map { it.number }
        val begin = file.yencHeaders?.partBegin
        val byNumber = numbers.all { it in 1..posted } && numbers.toSet().size == numbers.size &&
            (begin == null || (numbers.first() - 1) * partSize + 1 == begin)
        val listedAt = if (byNumber) numbers.map { it - 1 } else file.segments.indices.toList()
        fun item(part: Int, size: Long, segment: NzbSegment, segmentIndex: Int, gap: SegmentGap? = null) = SegmentQueueItem(
            segment = segment,
            fileIndex = fileIndex,
            segmentIndex = segmentIndex,
            segmentDecodedSize = size,
            globalOffset = fileGlobalOffset + part * partSize,
            readStart = 0,
            readEnd = size,
            gap = gap,
        )
        val listed = listedAt.zip(file.segments).mapIndexed { segmentIndex, (part, segment) ->
            item(part, minOf(partSize, fileSize - part * partSize), segment, segmentIndex)
        }
        val name = file.yencHeaders?.name ?: file.subject
        val gaps = (listOf(-1) + listedAt.sorted() + posted).zipWithNext()
            .filter { (before, after) -> after - before > 1 }
            .map { (before, after) ->
                val parts = before + 1 until after
                val size = minOf(after * partSize, fileSize) - parts.first * partSize
                val gap = SegmentGap(name, parts.first + 1..parts.last + 1, posted)
                item(parts.first, size, NzbSegment(0, gap.parts.first, ""), -1, gap)
            }
        return (listed + gaps).sortedBy { it.globalOffset }
    }

    /**
     * How many parts [file] was posted in, when its NZB lists fewer: as many as [fileSize] takes in
     * parts of [partSize] (its first part's). Null when the NZB lists that many, and when the parts
     * can't be told to be that size: the yEnc total says otherwise, or, without one, an article's
     * byte count says it is bigger than the first.
     */
    private fun postedParts(file: NzbFile, fileSize: Long, partSize: Long): Int? {
        if (partSize <= 0 || file.segments.isEmpty()) return null
        val parts = (fileSize + partSize - 1) / partSize
        if (file.segments.size >= parts || parts > Int.MAX_VALUE) return null
        val total = file.yencHeaders?.total
        // Articles of a part size differ by a few bytes of yEnc escapes and headers.
        val first = file.segments.first().bytes
        val sameSize = if (total != null) total.toLong() == parts else file.segments.all { it.bytes <= first + first / 10 }
        return parts.toInt().takeIf { sameSize }
    }

    /**
     * Every listed article of [file] in order, each a part of [partSize], and the last one the rest
     * of the file: right when the NZB lists every part, and the best guess when the parts' sizes
     * can't be told ([postedParts]).
     */
    private fun listedParts(
        file: NzbFile,
        fileIndex: Int,
        fileGlobalOffset: Long,
        fileSize: Long,
        partSize: Long,
    ): List<SegmentQueueItem> {
        return file.segments.mapIndexed { segmentIndex, segment ->
            // Last segment may be larger OR smaller than partSize
            val decodedSize = if (segmentIndex < file.segments.size - 1) {
                partSize
            } else {
                fileSize - segmentIndex.toLong() * partSize
            }
            val globalOffset = fileGlobalOffset + segmentIndex.toLong() * partSize
            SegmentQueueItem(
                segment = segment,
                fileIndex = fileIndex,
                segmentIndex = segmentIndex,
                segmentDecodedSize = decodedSize,
                globalOffset = globalOffset,
                readStart = 0,
                readEnd = decodedSize
            )
        }
    }
}
