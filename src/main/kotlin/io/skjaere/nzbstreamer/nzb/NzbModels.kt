package io.skjaere.nzbstreamer.nzb

import io.skjaere.nntp.YencHeaders

data class NzbDocument(val files: List<NzbFile>)

data class NzbFile(
    val poster: String,
    val date: Long,
    val subject: String,
    val groups: List<String>,
    val segments: List<NzbSegment>,
    var yencHeaders: YencHeaders? = null,
    var first16kb: ByteArray? = null,
    var par2Data: ByteArray? = null
)

data class NzbSegment(
    val bytes: Long,
    val number: Int,
    val articleId: String
)
