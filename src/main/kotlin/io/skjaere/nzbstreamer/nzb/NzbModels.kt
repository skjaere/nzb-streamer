package io.skjaere.nzbstreamer.nzb

import io.skjaere.nntp.YencHeaders

/**
 * @property password Optional archive password parsed from the NZB XML's
 *   `<head><meta type="password">…</meta></head>` element. NULL for the common
 *   case of unencrypted NZBs. Threaded through to [io.skjaere.compressionutils.ArchiveService.listFiles]
 *   so the RAR parser can decrypt encrypted archives (Phase 3 of encryption support).
 */
data class NzbDocument(
    val files: List<NzbFile>,
    val password: String? = null,
)

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
