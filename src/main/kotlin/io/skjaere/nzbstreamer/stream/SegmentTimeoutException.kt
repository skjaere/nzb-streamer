package io.skjaere.nzbstreamer.stream

import io.skjaere.nntp.NntpException

/**
 * No pool delivered the segment [articleId] within the segment fetch timeout ([timeoutMs], see
 * [io.skjaere.nzbstreamer.config.StreamingConfig.segmentFetchTimeoutMs]). The library's own
 * timeout, so not a [kotlinx.coroutines.CancellationException]: a caller can tell it from its own
 * cancellation, and a stream's reader gets it as the channel's cause. The article may still
 * exist; unlike [io.skjaere.nntp.ArticleNotFoundException], this says nothing about that. [cause]
 * is the timeout that fired on the first pool that timed out.
 */
class SegmentTimeoutException(
    val articleId: String,
    val timeoutMs: Long,
    message: String,
    cause: Throwable? = null,
) : NntpException(message, cause = cause)
