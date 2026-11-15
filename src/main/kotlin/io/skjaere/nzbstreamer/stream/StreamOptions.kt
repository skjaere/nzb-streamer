package io.skjaere.nzbstreamer.stream

import io.skjaere.nzbstreamer.config.MissingArticlePolicy

/**
 * How one stream fetches, where it differs from the streamer's other streams: a download saving a
 * file beside a player's stream, say. Given to the overloads that take it ([io.skjaere.nzbstreamer.NzbStreamer.launchStreamFile],
 * [ArchiveStreamingService.launchStreamFile], [NntpStreamingService.launchStreamSegments],
 * [NntpStreamingService.streamSegments], [io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream]); the
 * overloads without it stream with `StreamOptions()`, which is every stream as it was. A null field
 * is the streamer's own setting ([io.skjaere.nzbstreamer.config.StreamingConfig]), read when the
 * stream opens.
 *
 * @property priority what the stream's fetches ask the pools for a connection at: a pool serves the
 *   higher number first, and asks of one number in turn. [NntpPriority.STREAMING] by default; prepare
 *   asks at [NntpPriority.PREPARE] and the checks at [NntpPriority.HEALTH_CHECK], so a stream at
 *   [BACKGROUND_PRIORITY] waits for every one of them. A stream below [NntpPriority.STREAMING] also
 *   waits for each connection for as long as it takes: its waits have no limit of their own, and
 *   never count against a pool's circuit breaker or its timeouts. Its fetch on a connection has the
 *   segment fetch timeout as any, and its fetch on one pool twice that in all, counting only its
 *   time on connections. It never shares a fetch of an article with another stream, so a play never
 *   waits on one of its fetches, nor it on a play's.
 * @property cache whether the stream stores what it fetches in the segment cache ([CacheUse.READ_WRITE],
 *   as every stream did) or only reads it ([CacheUse.READ_ONLY]).
 * @property missingArticles what the stream does with a segment no pool can give; null: the
 *   streamer's ([io.skjaere.nzbstreamer.config.StreamingConfig.missingArticles]).
 * @property concurrency how many segments the stream downloads at once; null: the streamer's.
 * @property readAheadSegments how many downloaded segments the stream holds ahead of its reader; null: the streamer's.
 * @property readAheadBytes the most the stream holds in segments ahead of its reader
 *   ([io.skjaere.nzbstreamer.config.StreamingConfig.readAheadBytes]); null: the streamer's. So a
 *   stream can't ask for no limit in bytes where the streamer has one.
 */
data class StreamOptions(
    val priority: Int = NntpPriority.STREAMING.value,
    val cache: CacheUse = CacheUse.READ_WRITE,
    val missingArticles: MissingArticlePolicy? = null,
    val concurrency: Int? = null,
    val readAheadSegments: Int? = null,
    val readAheadBytes: Long? = null,
) {
    init {
        // As the streamer's own setters.
        require(concurrency == null || concurrency > 0) { "concurrency must be > 0, got $concurrency" }
        require(readAheadSegments == null || readAheadSegments > 0) { "readAheadSegments must be > 0, got $readAheadSegments" }
        require(readAheadBytes == null || readAheadBytes > 0) { "readAheadBytes must be > 0, got $readAheadBytes" }
    }

    companion object {
        /**
         * A priority below everything the library asks a pool at: under [NntpPriority.HEALTH_CHECK]'s 0, which the
         * library's own checks ask at too, so a stream at it waits for every play's stream, prepare and check.
         */
        const val BACKGROUND_PRIORITY = -1
    }
}

/** What a stream does with the segment cache ([StreamOptions.cache]). */
enum class CacheUse {
    /** Reads what the cache holds, and stores what it fetches: every stream as it was. */
    READ_WRITE,

    /**
     * Reads what the cache holds, and stores nothing it fetches: for a stream whose bytes go elsewhere (a file saved
     * whole), which would push out what plays keep there.
     */
    READ_ONLY,
}
