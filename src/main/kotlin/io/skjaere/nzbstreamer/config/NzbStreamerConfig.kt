package io.skjaere.nzbstreamer.config

import java.nio.file.Path

data class NntpConfig(
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val useTls: Boolean,
    val maxConnections: Int = 8,
    val priority: Int = 0
)

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
     */
    val segmentFetchTimeoutMs: Long = 20_000L
)

data class PrepareConfig(
    val verifySegments: Boolean = false
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
