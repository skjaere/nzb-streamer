package io.skjaere.nzbstreamer.queue

import io.skjaere.nzbstreamer.nzb.NzbSegment

data class SegmentQueueItem(
    val segment: NzbSegment,
    val fileIndex: Int,
    val segmentIndex: Int,
    val segmentDecodedSize: Long,
    val globalOffset: Long,
    val readStart: Long,
    val readEnd: Long
)
