package io.skjaere.nzbstreamer.queue

import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.nzb.printable

/**
 * A stretch of a file's decoded bytes: [segment]'s article, or, when [gap] is set, parts of the
 * file its NZB lists no article for. A gap's [segment] is a placeholder with no article id (its
 * number is the gap's first part) and its [segmentIndex] is -1; reading it fails with
 * [SegmentGap.toException].
 */
data class SegmentQueueItem @JvmOverloads constructor(
    val segment: NzbSegment,
    val fileIndex: Int,
    val segmentIndex: Int,
    val segmentDecodedSize: Long,
    val globalOffset: Long,
    val readStart: Long,
    val readEnd: Long,
    val gap: SegmentGap? = null,
)

/**
 * Parts [parts] (numbered from 1) of the [totalParts] that [fileName] was posted in, which its NZB
 * lists no article for.
 */
data class SegmentGap(val fileName: String, val parts: IntRange, val totalParts: Int) {
    /** What reading the gap fails with: no provider can have an article the NZB doesn't name. */
    fun toException(): ArticleNotFoundException {
        val which = if (parts.first == parts.last) "part ${parts.first}" else "parts ${parts.first}-${parts.last}"
        val them = if (parts.first == parts.last) "it" else "them"
        return ArticleNotFoundException("File '${fileName.printable()}' lacks $which of $totalParts: its NZB lists no article for $them")
    }
}
