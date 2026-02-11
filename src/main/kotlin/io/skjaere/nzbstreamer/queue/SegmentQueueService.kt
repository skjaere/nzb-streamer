package io.skjaere.nzbstreamer.queue

import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
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

    fun getTotalDecodedSize(nzb: NzbDocument): Long {
        return nzb.files.sumOf { getFileDecodedSize(it) }
    }

    private fun getFileDecodedSize(file: NzbFile): Long {
        return file.yencHeaders?.size
            ?: throw IllegalStateException("File not enriched: yencHeaders is null")
    }

    private fun getPartSize(file: NzbFile): Long {
        val headers = file.yencHeaders
            ?: throw IllegalStateException("File not enriched: yencHeaders is null")
        // For single-part articles, partEnd is null — the part size equals the file size
        return headers.partEnd ?: headers.size
    }

    private fun createFileQueueList(
        file: NzbFile,
        fileIndex: Int,
        fileGlobalOffset: Long
    ): List<SegmentQueueItem> {
        val fileSize = getFileDecodedSize(file)
        val partSize = getPartSize(file)

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
