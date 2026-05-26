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
    val segmentFetchTimeoutMs: Long = 5_000L,
    /**
     * Per-pool circuit breaker: number of *consecutive* failures (timeouts, IO errors,
     * protocol/connection NntpExceptions — anything that wasn't a clean server 430)
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
)

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
