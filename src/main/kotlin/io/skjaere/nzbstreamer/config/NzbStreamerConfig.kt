package io.skjaere.nzbstreamer.config

import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import java.nio.file.Path

data class NntpConfig(
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val useTls: Boolean,
    val maxConnections: Int = 8,
    val priority: Int = 0,
    /**
     * With [useTls], check that the server's certificate names [host] and send [host] as the
     * server name (SNI). Turn off only for a server reached by an address its certificate
     * doesn't list; the certificate must still be trusted.
     */
    val verifyHostname: Boolean = true
) {
    /** Every value but the password, so a config logged, or put in an exception's message, never carries it. */
    override fun toString() =
        "NntpConfig(host=$host, port=$port, username=$username, useTls=$useTls, maxConnections=$maxConnections, " +
            "priority=$priority, verifyHostname=$verifyHostname)"
}

data class StreamingConfig(
    val concurrency: Int = 4,
    val verificationConcurrency: Int = concurrency,
    val readAheadSegments: Int = concurrency * 3,
    /**
     * Per-segment body fetch deadline. Caps how long a single article fetch can suspend
     * before the call gives up and falls through to the next pool. Without this, a TCP
     * connection that was silently dropped by an intermediary (NAT eviction, firewall idle
     * timeout) would only fail on the OS-level keepalive timeout, which can be hours away —
     * during which the in-flight streamSegments coroutineScope cannot unwind and its
     * activeStreams counter stays stuck.
     *
     * It runs from when the pool hands the fetch a connection: a fetch queued behind the
     * others for one isn't given less. The wait for a connection has as long again of its own,
     * and a fetch on one pool, its retry on another connection included, twice this at most.
     */
    val segmentFetchTimeoutMs: Long = 5_000L,
    /**
     * Per-pool circuit breaker: number of *consecutive* failures (timeouts, IO errors,
     * protocol/connection NntpExceptions — anything that wasn't a clean server 430, or a
     * damaged copy of an article, see [crcMismatch])
     * before the circuit opens and that pool is skipped for [circuitBreakerCooldownMs].
     * A single success resets the counter. The breaker is inert when only one pool is
     * configured — there'd be nowhere to fall back to and skipping it would just turn
     * every request into a hard failure.
     */
    val circuitBreakerFailureThreshold: Int = 5,
    /**
     * How long to skip a pool after its circuit opens. After this window elapses the
     * circuit resets to closed and the next request tries the pool again; if it fails
     * the threshold count starts over and the breaker can re-open.
     */
    val circuitBreakerCooldownMs: Long = 60_000L,
    /**
     * The most a stream holds of segments it has downloaded, or is downloading, and hasn't written
     * to its channel yet: their decoded sizes, each rounded up to a whole 64 KiB, total at most
     * this, except that one larger than this is held alone. With segments this large it downloads
     * fewer at once and reads fewer ahead than [concurrency] and [readAheadSegments] allow (always
     * one at least). Null: no limit in bytes, only those counts. An app on a small heap needs it:
     * twelve segments read ahead of a release posted in 3.84 MB parts are 46 MB.
     *
     * The stream's channel isn't counted. A segment counts until its write returns, and meanwhile
     * its bytes are in its array and copied into the channel. The write returns once less than
     * 1 MiB waits in the channel, and the reader's side takes all that waits at once, so it may
     * still hold the segment written last while the next one's copy waits. So with segments of
     * 1 MiB or more a stream's segments take up to this plus two segments, and 1 MiB more if its
     * reader waits for more than a byte at a time (it may then take more with under 1 MiB unread).
     * Smaller ones are written on until 1 MiB waits, so each side may hold under 1 MiB beside a
     * segment: with any sizes, up to this plus two segments and 3 MiB (under 5 MiB more with
     * segments under 1 MiB). Nor are each download's socket and decoder buffers counted, or the
     * segment cache.
     */
    val readAheadBytes: Long? = null,
    /**
     * What becomes of a segment (streamed, read at random, or a first article prepare reads) that no
     * pool has a good copy of, only copies whose yEnc CRC isn't the one their =yend line gives: used
     * ([CrcMismatchPolicy.TOLERATE]) or failed ([CrcMismatchPolicy.STRICT]). A copy like that is a
     * damaged copy on its provider, not the provider failing: the next pool is tried for a good one
     * either way, and no circuit breaker hears of it.
     */
    val crcMismatch: CrcMismatchPolicy = CrcMismatchPolicy.TOLERATE,
    /**
     * The segments a stream downloads, its first ones, until its reader has taken the first one's
     * bytes (more where the writer needs them to tell, below): then the rest of its read-ahead
     * starts, as [concurrency], [readAheadSegments] and [readAheadBytes] allow. A player's first
     * open and a probe of the file read a few KB before they close or seek, and a media server's
     * scan opens many streams that do: each started the whole read-ahead, a dozen segments of
     * 3.84 MB for a 4K remux. With two the first comes as soon as before, and the second is there
     * when the reader gets to it while the rest start. Null: the whole read-ahead at once.
     *
     * A file's stream starts with them only from the file's first byte, where those reads are. One
     * opened further on (a seek, a resume, an index at the file's end) starts its whole read-ahead at
     * once: its reader reads on, and a player waits for seconds of it before it plays, several
     * segments of a high-bitrate release, all but two of which would come a segment's download later.
     * The streams of segments a seekable stream reads through (archive headers, an encrypted file:
     * [io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream]) start with them wherever they start.
     *
     * The writer learns that its reader took bytes only from its writes waiting, as a write waits
     * while the channel holds 1 MiB the reader hasn't taken. So a stream of segments of 1 MiB or
     * more widens when the second one's write returns; one of smaller segments starts as many more
     * as make 2 MiB and a segment past the first (five of 750 KB) and widens once the last of them
     * is written; one that starts in a segment's last MiB starts one more.
     */
    val initialReadAheadSegments: Int? = DEFAULT_INITIAL_READ_AHEAD_SEGMENTS,
) {
    init {
        // As NntpStreamingService.setReadAheadBytes: no bytes would stream a segment at a time.
        require(readAheadBytes == null || readAheadBytes > 0) { "readAheadBytes must be > 0, got $readAheadBytes" }
        require(initialReadAheadSegments == null || initialReadAheadSegments > 0) {
            "initialReadAheadSegments must be > 0, got $initialReadAheadSegments"
        }
    }

    companion object {
        /** See [initialReadAheadSegments]. */
        const val DEFAULT_INITIAL_READ_AHEAD_SEGMENTS = 2
    }
}

/** What a segment gets that no pool has a good copy of, only damaged ones ([StreamingConfig.crcMismatch]). */
enum class CrcMismatchPolicy {
    /**
     * The first damaged copy's bytes are used, with a warning naming the segment and the pools, and
     * counted (nzb.segments.crc_mismatch_used): a few wrong bytes in a video play through, where a
     * failed segment ends the stream. They are never cached, so a later play tries again.
     */
    TOLERATE,

    /** The segment fails with the mismatch ([io.skjaere.nntp.YencCrcMismatchException]), as with a timeout or a dropped connection. */
    STRICT,
}

data class PrepareConfig(
    val verifySegments: Boolean = false,
    /**
     * Max concurrent per-file operations during NZB enrichment (fetching the first
     * segment of each file to populate yenc headers + first-16KB, plus the par2
     * download fan-out). One large multi-volume release expands to ~100 wire-level
     * NZB files; without this bound, a parallel awaitAll over `nzb.files` opens
     * 100+ priority=PREPARE NNTP connections at once, exceeding any reasonable
     * pool size and provider account session limit.
     */
    val enrichmentConcurrency: Int = NzbEnrichmentService.DEFAULT_ENRICHMENT_CONCURRENCY,
)

/**
 * Configuration for the on-disk segment cache. Each downloaded NNTP segment is stored
 * as one file under [cacheDir], keyed by SHA-256 of the article-id; an `index.json`
 * sidecar in [cacheDir] tracks per-segment size + lastAccess for LRU eviction and
 * warm restart recovery.
 */
data class SegmentCacheConfig(
    val cacheDir: Path,
    val maxBytes: Long,
    val evictThresholdRatio: Double = 0.9
)
