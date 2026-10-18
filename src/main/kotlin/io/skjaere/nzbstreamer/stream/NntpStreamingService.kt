package io.skjaere.nzbstreamer.stream

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.ktor.network.selector.SelectorManager
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.cancel
import io.ktor.utils.io.writeFully
import io.ktor.utils.io.writer
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpClientPool
import io.skjaere.nntp.NntpException
import io.skjaere.nntp.StatResult
import io.skjaere.nntp.YencArticle
import io.skjaere.nntp.YencCrcMismatchException
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.CrcMismatchPolicy
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.nzb.printable
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import java.io.IOException
import java.time.Duration
import kotlinx.coroutines.CompletableDeferred
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration.Companion.milliseconds
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

/** Smallest STAT batch worth a pipeline of its own. */
private const val STAT_CHUNK_MIN = 32

/** What a stream's read-ahead in bytes ([StreamingConfig.readAheadBytes]) is counted in. */
private const val HELD_UNIT_BYTES = 64L * 1024

/** What a Ktor channel holds before its writer waits for the reader (Ktor's CHANNEL_MAX_SIZE). */
private const val CHANNEL_HOLDS = 1L shl 20

/**
 * How many of a stream's first writes, of [sizes], it takes before the writer knows its reader has
 * taken the first one's bytes, if the reader reads on; null if they never show it. The channel
 * shows it only by making writes wait: a write returns once less than [CHANNEL_HOLDS] waits in it,
 * and the reader takes all that waits whenever it has read all it took before (if it waits for a
 * byte at a time). So a write of [CHANNEL_HOLDS] or more returns only once the reader has read all
 * but what waited before it, and once any write returns the reader holds less than twice that and
 * the largest write unread.
 */
internal fun writesUntilFirstTaken(sizes: Sequence<Long>): Int? {
    var first = 0L
    var written = 0L
    var largest = 0L
    // Once the last write returned: the most that may wait in the channel, and the least the reader has read.
    var waiting = 0L
    var read = 0L
    sizes.forEachIndexed { i, size ->
        if (i == 0) first = size
        if (size >= CHANNEL_HOLDS) {
            read = maxOf(read, written - waiting)
            waiting = 0
        } else {
            waiting = minOf(waiting + size, CHANNEL_HOLDS - 1)
        }
        written += size
        largest = maxOf(largest, size)
        read = maxOf(read, written - waiting - (CHANNEL_HOLDS - 1 + largest))
        if (i > 0 && read >= first) return i + 1
    }
    return null
}

/** A STAT answer that settles an article on a pool: a 223, or a 430. Any other is the pool failing. */
private val StatResult.settles: Boolean
    get() = this is StatResult.Found || (this is StatResult.NotFound && code == 430)

/** An article's yEnc headers as the segment cache keeps them, beside its bytes ([NntpStreamingService.cacheArticle]). */
@Serializable
private data class CachedYencHeaders(
    val line: Int,
    val size: Long,
    val name: String,
    val part: Int? = null,
    val total: Int? = null,
    val partBegin: Long? = null,
    val partEnd: Long? = null,
) {
    constructor(headers: YencHeaders) :
        this(headers.line, headers.size, headers.name, headers.part, headers.total, headers.partBegin, headers.partEnd)

    fun toYencHeaders() = YencHeaders(line, size, name, part, total, partBegin, partEnd)
}

/**
 * The segment cache's key for [articleId]'s yEnc headers: no article's own, as an article id never
 * holds a NUL (an NZB is XML, which can't).
 */
private fun headersKey(articleId: String) = "$articleId\u0000yenc-headers"

/**
 * An article fetched whole ([NntpStreamingService.fetchArticle]), and whether it may be cached: not
 * a damaged copy used for want of a good one.
 */
internal class FetchedArticle(val article: YencArticle, val storable: Boolean)

/** [close] closes [segmentCache] unless [closeSegmentCache] is false (a cache the caller keeps). */
class NntpStreamingService(
    initialConfigs: List<NntpConfig>,
    private val streamingConfig: StreamingConfig = StreamingConfig(),
    private val segmentCache: SegmentCache? = null,
    private val closeSegmentCache: Boolean = true,
) : Closeable {
    constructor(config: NntpConfig) : this(listOf(config))

    /**
     * [name] is the pool's identity: it keys its circuit breaker and tags its metrics (see
     * [nameFor]), so two accounts on one server don't share either.
     */
    private data class PoolEntry(val config: NntpConfig, val pool: NntpClientPool, val name: String)

    // The pools' connects (a blocking DNS lookup), keepalives, QUITs and drains run here, and the
    // shared SelectorManager's loop (it blocks in select) on the same dispatcher: IO, not Default,
    // whose few workers (one per core) are for CPU-bound work, which may hold them all.
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val logger = LoggerFactory.getLogger(NntpStreamingService::class.java)
    private val pools = mutableListOf<PoolEntry>()
    private lateinit var selectorManager: SelectorManager
    private val poolLock = ReentrantReadWriteLock()

    // Serializes addPool and removePool. A pool being removed keeps its name until it has closed,
    // as its close removes the metrics under that name; poolLock is held only to change the list.
    private val poolChangeLock = ReentrantLock()

    /**
     * Per-pool circuit breaker. Each pool gets a CircuitBreaker named after the pool ([PoolEntry.name]),
     * sharing the same config (count-based sliding window, threshold + cooldown
     * from [StreamingConfig]). Failures that count: timeouts, IOExceptions, and
     * NntpException family — anything we can't blame on a clean server 430.
     * ArticleNotFoundException is *ignored* (server cleanly said the article
     * doesn't exist — that's not the pool's fault), and so is YencCrcMismatchException
     * (the pool's copy of that article is damaged, the pool is fine). The breaker is inert when
     * only one pool is configured, see [executeWithCircuitBreaker].
     *
     * [currentCircuitBreakerConfig] is mutable so the live override path in
     * debridav can re-tune thresholds without a restart. Each [circuitBreakerFor]
     * call hands out the breaker for that pool using whatever config the registry
     * currently holds for that name; [setCircuitBreakerConfig] swaps the config
     * and removes existing breakers so the next acquisition re-creates them with
     * the new thresholds.
     */
    @Volatile
    private var currentCircuitBreakerFailureThreshold: Int = streamingConfig.circuitBreakerFailureThreshold

    @Volatile
    private var currentCircuitBreakerCooldownMs: Long = streamingConfig.circuitBreakerCooldownMs

    private fun buildCircuitBreakerConfig(failureThreshold: Int, cooldownMs: Long): CircuitBreakerConfig =
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(failureThreshold * 2)
            .minimumNumberOfCalls(failureThreshold)
            .failureRateThreshold(50f)
            .waitDurationInOpenState(Duration.ofMillis(cooldownMs))
            .permittedNumberOfCallsInHalfOpenState(1)
            .automaticTransitionFromOpenToHalfOpenEnabled(true)
            .recordExceptions(
                NntpException::class.java,
                IOException::class.java,
                TimeoutCancellationException::class.java,
            )
            .ignoreExceptions(ArticleNotFoundException::class.java, YencCrcMismatchException::class.java)
            .build()

    private val circuitBreakerRegistry: CircuitBreakerRegistry =
        CircuitBreakerRegistry.of(
            buildCircuitBreakerConfig(
                streamingConfig.circuitBreakerFailureThreshold,
                streamingConfig.circuitBreakerCooldownMs,
            )
        ).also { reg ->
            // Surface state changes (closed → open, open → half-open, etc.) in logs.
            reg.eventPublisher.onEntryAdded { event ->
                event.addedEntry.eventPublisher.onStateTransition { transition ->
                    logger.warn(
                        "Circuit breaker {} → {} for pool {}",
                        transition.stateTransition.fromState,
                        transition.stateTransition.toState,
                        transition.circuitBreakerName,
                    )
                }
            }
            // Wire breaker state/calls into micrometer so they appear in Grafana
            // alongside the existing pool gauges. We reference the global registry
            // directly here because the `registry` field is declared further down
            // in the class and isn't initialized yet at this point in construction.
            TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(reg).bindTo(Metrics.globalRegistry)
        }

    private fun circuitBreakerFor(entry: PoolEntry): CircuitBreaker =
        circuitBreakerRegistry.circuitBreaker(
            entry.name,
            buildCircuitBreakerConfig(currentCircuitBreakerFailureThreshold, currentCircuitBreakerCooldownMs),
        )

    /**
     * Replace the breaker thresholds at runtime. Each existing per-pool breaker is
     * removed from the registry; the next [circuitBreakerFor] call recreates it with
     * the new config. Drops the per-breaker rolling window state — acceptable for a
     * tuning knob since the new window fills within a handful of subsequent calls.
     * In-flight calls already past the permission-acquisition point keep their old
     * config for the rest of their flight.
     */
    fun setCircuitBreakerConfig(failureThreshold: Int, cooldownMs: Long) {
        require(failureThreshold > 0) { "failureThreshold must be > 0, got $failureThreshold" }
        require(cooldownMs > 0) { "cooldownMs must be > 0, got $cooldownMs" }
        currentCircuitBreakerFailureThreshold = failureThreshold
        currentCircuitBreakerCooldownMs = cooldownMs
        circuitBreakerRegistry.allCircuitBreakers.forEach { breaker ->
            circuitBreakerRegistry.remove(breaker.name)
        }
        logger.info(
            "Updated circuit breaker config: failureThreshold={}, cooldownMs={}",
            failureThreshold, cooldownMs,
        )
    }

    fun getCircuitBreakerFailureThreshold(): Int = currentCircuitBreakerFailureThreshold

    fun getCircuitBreakerCooldownMs(): Long = currentCircuitBreakerCooldownMs

    // Per-segment body fetch deadline. Read fresh from this volatile on each
    // withTimeout wrap so live overrides apply to the next fetch without
    // disturbing already-in-flight ones (those keep their original deadline).
    @Volatile
    private var currentSegmentFetchTimeoutMs: Long = streamingConfig.segmentFetchTimeoutMs

    fun setSegmentFetchTimeoutMs(value: Long) {
        require(value > 0) { "segmentFetchTimeoutMs must be > 0, got $value" }
        currentSegmentFetchTimeoutMs = value
        logger.info("Updated segment fetch timeout: {}ms", value)
    }

    fun getSegmentFetchTimeoutMs(): Long = currentSegmentFetchTimeoutMs

    private val initialConfigs = initialConfigs.toList()

    // Mutable streaming knobs. Read fresh on each new streamSegments call (via the
    // default-arg pattern below), so live updates apply to the next stream that opens
    // without disturbing in-flight ones — those keep the Semaphore/produce capacity
    // they were launched with. Volatile is enough: the field is read once into a
    // local at call entry, no compound check-then-act.
    @Volatile
    private var currentConcurrency: Int = streamingConfig.concurrency

    @Volatile
    private var currentReadAheadSegments: Int = streamingConfig.readAheadSegments

    @Volatile
    private var currentReadAheadBytes: Long? = streamingConfig.readAheadBytes

    @Volatile
    private var currentInitialReadAheadSegments: Int? = streamingConfig.initialReadAheadSegments

    private val registry = Metrics.globalRegistry
    private val segmentsDownloaded = registry.counter("nzb.segments.downloaded")
    private val segmentsBytes = registry.counter("nzb.segments.bytes")
    private val segmentDownloadTimer = Timer.builder("nzb.segments.download.duration")
        .description("End-to-end time per segment fetch: status line + yenc body transfer + any multi-pool fallback retries")
        .publishPercentileHistogram()
        .register(registry)
    private val segmentsFailed = registry.counter("nzb.segments.failed")
    private val segmentsFallback = registry.counter("nzb.segments.fallback")
    // Segments (and prepare's first articles) no pool had a good copy of, used damaged (CrcMismatchPolicy.TOLERATE).
    private val segmentsCrcMismatchUsed = registry.counter("nzb.segments.crc_mismatch_used")
    private val segmentCacheHits = registry.counter("nzb.segments.cache.hits")
    private val segmentCacheMisses = registry.counter("nzb.segments.cache.misses")

    // Cache lookup time on the hot path (one row per call regardless of hit/miss).
    // Distinguishes "cache.get is slow due to disk contention" from "miss → NNTP
    // is slow" — both look the same in the existing segmentDownloadTimer otherwise.
    private val segmentCacheGetTimer = Timer.builder("nzb.segments.cache.get.duration")
        .description("Per-call cache.get() time (returns null on miss). Disk-bound when the index has the entry.")
        .publishPercentileHistogram()
        .register(registry)
    // Time for the full miss-path: singleflight wait + NNTP fetch + disk store.
    // Sustained gap between this and nntp.body.duration p99 implies time burned
    // in singleflight contention or disk-write rather than wire transfer.
    private val segmentCacheGetOrFetchTimer = Timer.builder("nzb.segments.cache.getorfetch.duration")
        .description("getOrFetch() wall time on cache miss: singleflight wait + fetch lambda + store.")
        .publishPercentileHistogram()
        .register(registry)

    // Lookup-by-pool of the same Timer that NntpConnection uses internally for its
    // body-duration recordings. Same name + same `pool.name` tag → Micrometer reuses
    // the underlying meter, so timeouts we record here appear in the existing
    // "BODY transfer duration by pool" panel without any dashboard changes.
    //
    // The connection-side timer only stops on markBodyConsumed, which never fires
    // when our withTimeout cancels the bodyYenc Flow mid-stream — those attempts
    // disappear from the latency percentiles entirely. Recording the configured
    // timeout value here closes that gap: p99 of `nntp_body_duration_seconds`
    // will reflect the slow tail that the connection-side timer ignores.
    private val bodyDurationByPool = ConcurrentHashMap<String, Timer>()
    private fun bodyDurationTimerFor(poolName: String): Timer =
        bodyDurationByPool.computeIfAbsent(poolName) {
            Timer.builder("nntp.body.duration")
                .description("Full BODY round-trip: command write to article terminator consumed off the wire")
                .publishPercentileHistogram()
                .tag("pool.name", it)
                .register(registry)
        }

    // Time from launchStreamSegments entry to the first SegmentQueueItem being
    // emitted from the queue Flow. For RAW streams this is near-instant (just
    // walking NzbFile.segments). For archive streams it captures the cost of
    // walking RAR/7z metadata to locate the first relevant article.
    private val firstArticleResolvedTimer = Timer.builder("nzb.stream.first_article_resolved.duration")
        .description("Time from stream-segments launch to first article ID emitted from the queue")
        .publishPercentileHistogram()
        .register(registry)
    private val activeStreams = AtomicLong(0).also { registry.gauge("nzb.streams.active", it) }

    // Per-stream bitrate: one MultiGauge with rows backed by the state
    // objects in streamBitrateStates. A single viewer session typically spawns
    // many overlapping launchStreamSegments calls (one per range request from
    // the media player), so we reference-count active users of a given name
    // to keep the gauge row alive until the last one releases it.
    private data class RefCountedState(val state: BitrateState, var refCount: Int)

    private val streamBitrateStates = ConcurrentHashMap<String, RefCountedState>()
    private val streamBitrateGauge = MultiGauge.builder("nzb.streams.bitrate").register(registry)

    private fun refreshBitrateGauge() {
        val names = streamBitrateStates.keys.toList()
        streamBitrateGauge.register(
            names.map { name ->
                MultiGauge.Row.of(Tags.of("name", name)) {
                    streamBitrateStates[name]?.state?.sampleBitrate() ?: 0.0
                }
            },
            true,
        )
    }

    private class BitrateState {
        private val totalBytes = AtomicLong(0)
        @Volatile private var lastSampleBytes = 0L
        @Volatile private var lastSampleNanos = System.nanoTime()

        fun addBytes(n: Long) {
            totalBytes.addAndGet(n)
        }

        /** Bytes/sec averaged between the last read and now. Resets each read. */
        fun sampleBitrate(): Double {
            val now = System.nanoTime()
            val bytes = totalBytes.get()
            val deltaBytes = bytes - lastSampleBytes
            val deltaNanos = now - lastSampleNanos
            lastSampleBytes = bytes
            lastSampleNanos = now
            return if (deltaNanos <= 0) 0.0 else deltaBytes * 1_000_000_000.0 / deltaNanos
        }
    }

    private fun acquireBitrateState(name: String): BitrateState {
        var newlyCreated = false
        val entry = streamBitrateStates.compute(name) { _, existing ->
            if (existing == null) {
                newlyCreated = true
                RefCountedState(BitrateState(), 1)
            } else {
                existing.copy(refCount = existing.refCount + 1)
            }
        }!!
        if (newlyCreated) refreshBitrateGauge()
        return entry.state
    }

    private fun releaseBitrateState(name: String) {
        var removed = false
        streamBitrateStates.compute(name) { _, existing ->
            when {
                existing == null -> null
                existing.refCount <= 1 -> {
                    removed = true
                    null
                }
                else -> existing.copy(refCount = existing.refCount - 1)
            }
        }
        if (removed) refreshBitrateGauge()
    }

    suspend fun connect() {
        selectorManager = SelectorManager(Dispatchers.IO)
        initialConfigs.forEachIndexed { index, config ->
            pools.add(createPool(config))
            logger.info(
                "NNTP pool[{}] initialized: {} maxConnections={}",
                index, pools.last().name, config.maxConnections
            )
        }
    }

    /**
     * A name for a new pool on [config]'s server, unique among [pools]: `host:port`, or
     * `host:port#2`, `#3`, ... for further pools on the same server (a second account, or one
     * provider listed twice). A pool keeps its name; one that is removed frees it.
     */
    private fun nameFor(config: NntpConfig): String {
        val server = "${config.host}:${config.port}"
        val taken = pools.map { it.name }.toSet()
        return generateSequence(1) { it + 1 }.map { if (it == 1) server else "$server#$it" }.first { it !in taken }
    }

    private fun createPool(config: NntpConfig): PoolEntry {
        val name = nameFor(config)
        return PoolEntry(config, newPool(config, name), name)
    }

    private fun newPool(config: NntpConfig, name: String): NntpClientPool {
        return NntpClientPool(
            host = config.host,
            port = config.port,
            selectorManager = selectorManager,
            useTls = config.useTls,
            username = config.username.ifEmpty { null },
            password = config.password.ifEmpty { null },
            maxConnections = config.maxConnections,
            scope = scope,
            verifyHostname = config.verifyHostname,
            name = name
        )
    }

    /**
     * Adds a pool for [config]. Another account on a server that already has a pool gets a pool
     * of its own; the same config twice is refused, as [removePool] tells pools apart by config.
     */
    fun addPool(config: NntpConfig) {
        poolChangeLock.withLock {
            poolLock.write {
                require(pools.none { it.config == config }) {
                    "Pool already exists for this config of ${config.host}:${config.port}"
                }
                val entry = createPool(config)
                pools.add(entry)
                logger.info(
                    "NNTP pool added at runtime: {} maxConnections={} (total pools: {})",
                    entry.name, config.maxConnections, pools.size
                )
            }
        }
    }

    /**
     * Closes and removes the pool for [config], and its circuit breaker: a pool added later under
     * the same name starts with a closed one. The pool leaves the list before it closes, so the
     * other pools serve on while it does; this returns once it has closed.
     */
    fun removePool(config: NntpConfig) {
        poolChangeLock.withLock {
            val (entry, remaining) = poolLock.write {
                val index = pools.indexOfFirst { it.config == config }
                require(index >= 0) { "No pool found for ${config.host}:${config.port}" }
                pools.removeAt(index) to pools.size
            }
            // Closing waits for the pool's connections (QUIT), so not under poolLock.
            entry.pool.close()
            circuitBreakerRegistry.remove(entry.name)
            logger.info(
                "NNTP pool removed at runtime: {} (total pools: {})",
                entry.name, remaining
            )
        }
    }

    fun getPoolConfigs(): List<NntpConfig> {
        return poolLock.read { pools.map { it.config } }
    }

    fun setStreamingConcurrency(value: Int) {
        require(value > 0) { "concurrency must be > 0, got $value" }
        currentConcurrency = value
    }

    fun setReadAheadSegments(value: Int) {
        require(value > 0) { "readAheadSegments must be > 0, got $value" }
        currentReadAheadSegments = value
    }

    /** See [StreamingConfig.readAheadBytes]; null removes the limit. Applies to streams opened from now on. */
    fun setReadAheadBytes(value: Long?) {
        require(value == null || value > 0) { "readAheadBytes must be > 0, got $value" }
        currentReadAheadBytes = value
    }

    /** See [StreamingConfig.initialReadAheadSegments]; null starts the whole read-ahead at once. Applies to streams opened from now on. */
    fun setInitialReadAheadSegments(value: Int?) {
        require(value == null || value > 0) { "initialReadAheadSegments must be > 0, got $value" }
        currentInitialReadAheadSegments = value
    }

    fun getStreamingConcurrency(): Int = currentConcurrency

    fun getReadAheadSegments(): Int = currentReadAheadSegments

    fun getReadAheadBytes(): Long? = currentReadAheadBytes

    fun getInitialReadAheadSegments(): Int? = currentInitialReadAheadSegments

    /**
     * Tries [block] on each pool in order. Falls back to the next pool on conditions
     * that are recoverable by re-issuing against a different upstream:
     *
     *  - [ArticleNotFoundException] (server said 430 — try another provider)
     *  - [NntpProtocolException] (wire-state corruption on the pool's connection,
     *    e.g. yenc body bytes leaking into a status line; the pool's catch-all has
     *    already marked that connection dead, but the request itself is fine and
     *    should be retried elsewhere)
     *  - [NntpConnectionException] / [IOException] (transient network failure to
     *    this pool — exhausted by the inner pool's `.retry()` budget already)
     *  - [NntpAuthenticationException] (this pool's login was rejected: an expired
     *    account, or too many connections on it; the pool throws it at once, and
     *    another provider's account may well serve the call)
     *  - [YencCrcMismatchException] (this pool's copy of the article is damaged; another
     *    provider's may not be. Like a 430, not counted against the pool's breaker)
     *
     * Not caught (propagated directly):
     *  - [kotlinx.coroutines.CancellationException] — structured-concurrency
     *    cancellation, not our exception to swallow.
     *
     * Throws the last fallback-triggering exception if every pool fails, except that a 430
     * never replaces another failure: a pool whose connection dropped may have the article.
     */
    @Suppress("TooGenericExceptionCaught")
    private suspend fun <T> withFallback(
        logPrefix: String,
        block: suspend (entry: PoolEntry) -> T
    ): T {
        val snapshot = poolLock.read { pools.toList() }
        if (snapshot.isEmpty()) {
            error("$logPrefix: no NNTP pools configured")
        }
        // Breakers protect *some* healthy pool from being starved by a sick one. When
        // every pool's breaker is open simultaneously (e.g. a brief outage that tripped
        // both), denying every call just amplifies the failure — falling through with
        // breakers disabled for this attempt at least surfaces the real upstream error
        // (timeout, IO failure) instead of an opaque "no permits anywhere" miss.
        val effectiveBreakerActive = snapshot.size > 1 &&
            !snapshot.all { circuitBreakerFor(it).state == CircuitBreaker.State.OPEN }
        var lastException: Throwable? = null
        fun record(e: Throwable) {
            if (e is ArticleNotFoundException && lastException.let { it != null && it !is ArticleNotFoundException }) return
            lastException = e
        }
        var anyAttempted = false
        val result = snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            val breaker = if (effectiveBreakerActive) circuitBreakerFor(entry) else null
            if (breaker != null && !breaker.tryAcquirePermission()) {
                logger.debug(
                    "{} skipping pool[{}] ({}:{}) — circuit breaker is {}",
                    logPrefix, index, entry.config.host, entry.config.port, breaker.state
                )
                segmentsFallback.increment()
                return@firstNotNullOfOrNull null
            }
            anyAttempted = true
            tryOnePool(entry, breaker, index, snapshot.size, logPrefix, block, ::record)
        }
        if (result != null) return result
        if (lastException != null) throw lastException!!
        // We get here when `effectiveBreakerActive` was true at preflight (so the breakers
        // gated permission acquisition) but every pool refused at iteration time. Two ways
        // that happens:
        //   1. Race: a breaker we observed as not-OPEN transitioned to OPEN between the
        //      preflight check and our tryAcquirePermission call. Possible whenever
        //      concurrent traffic is driving probe transitions.
        //   2. HALF_OPEN permit exhaustion: with permittedNumberOfCallsInHalfOpenState(1),
        //      a single concurrent caller holds the lone probe permit on each breaker, and
        //      every other call refuses. Compounded by any path that leaks a permit (e.g.
        //      the CancellationException catch added below for TimeoutCancellationException),
        //      because the stuck permit never frees and the breaker becomes permanently
        //      unable to admit any caller while still reporting HALF_OPEN to the preflight
        //      check.
        //
        // Either way, throwing IllegalStateException isn't useful — fail the same way we'd
        // fail when every pool is OPEN at preflight: drop the breaker gate and try the call
        // anyway. If the upstream is actually healthy, we get our result; if not, we
        // surface the real error from `block`.
        logger.warn(
            "{} every pool refused the breaker permit (likely HALF_OPEN with permit taken) — " +
                "retrying with breakers disabled to avoid stalling on a probe race",
            logPrefix
        )
        return snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            tryOnePool(entry, breaker = null, index, snapshot.size, logPrefix, block, ::record)
        } ?: throw (lastException ?: IllegalStateException(
            "$logPrefix: retry-with-breakers-disabled also produced no result — anyAttempted=$anyAttempted"
        ))
    }

    @Suppress("TooGenericExceptionCaught", "LongParameterList")
    private suspend fun <T> tryOnePool(
        entry: PoolEntry,
        breaker: CircuitBreaker?,
        index: Int,
        totalPools: Int,
        logPrefix: String,
        block: suspend (entry: PoolEntry) -> T,
        recordException: (Throwable) -> Unit,
    ): T? {
        val callStart = System.nanoTime()
        return try {
            val result = block(entry)
            // Pass Unit rather than `result`: T is unbounded (could be nullable),
            // and resilience4j only inspects the result when a recordResultPredicate
            // is configured (we don't). Avoids the Java Object/T platform-type warning.
            breaker?.onResult(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                Unit,
            )
            result
        } catch (e: ArticleNotFoundException) {
            // Clean 430 — the pool is healthy, the article just isn't there.
            breaker?.releasePermission()
            recordException(e)
            if (index < totalPools - 1) {
                logger.debug(
                    "{} not found on pool[{}] ({}:{}) — falling back to pool[{}]",
                    logPrefix, index, entry.config.host, entry.config.port, index + 1,
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: YencCrcMismatchException) {
            // A damaged copy of the article, read to its end: the pool is healthy, its copy isn't.
            // Like a clean 430, nothing the breaker records; another pool may have a good copy.
            breaker?.releasePermission()
            recordException(e)
            if (index < totalPools - 1) {
                logger.debug(
                    "{} CRC mismatch on pool[{}] ({}:{}) — falling back to pool[{}]: {}",
                    logPrefix, index, entry.config.host, entry.config.port, index + 1, e.message,
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: io.skjaere.nntp.NntpException) {
            // A rejected login (NntpAuthenticationException) lands here too: it is this pool's
            // account that fails, and it counts against this pool's breaker like any other failure.
            breaker?.onError(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                e,
            )
            recordException(e)
            if (index < totalPools - 1) {
                logger.debug(
                    "{} failed on pool[{}] ({}:{}) with {} — falling back to pool[{}]: {}",
                    logPrefix, index, entry.config.host, entry.config.port,
                    e::class.simpleName, index + 1, e.message,
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: java.io.IOException) {
            breaker?.onError(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                e,
            )
            recordException(e)
            if (index < totalPools - 1) {
                logger.debug(
                    "{} I/O failure on pool[{}] ({}:{}) — falling back to pool[{}]: {}",
                    logPrefix, index, entry.config.host, entry.config.port,
                    index + 1, e.message,
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: kotlinx.coroutines.CancellationException) {
            // Coroutine cancellation must rethrow to preserve structured concurrency.
            // But the breaker permit must be released first — otherwise a HALF_OPEN
            // probe that gets cancelled (e.g. by withTimeout firing) leaks its
            // single permit and the breaker becomes permanently unable to admit any
            // probe, manifesting as the "every pool refused" failure above.
            //
            // For TimeoutCancellationException specifically, count it as a failure
            // (it's in `recordExceptions` for the breaker config). Other cancellations
            // (parent scope cancel, structured-concurrency tear-down) just release
            // the permit without staining the failure-rate metric.
            if (e is kotlinx.coroutines.TimeoutCancellationException) {
                breaker?.onError(
                    System.nanoTime() - callStart,
                    java.util.concurrent.TimeUnit.NANOSECONDS,
                    e,
                )
            } else {
                breaker?.releasePermission()
            }
            throw e
        } catch (e: Throwable) {
            // Anything else (the pool closed under us, an error from the caller's block) says
            // nothing the breaker records, but the permit must go back all the same.
            breaker?.releasePermission()
            throw e
        }
    }

    suspend fun <T> withClient(
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        block: suspend (io.skjaere.nntp.NntpClient) -> T
    ): T = withFallback("Article") { entry -> entry.pool.withClient(priority.value, block) }

    /**
     * [articleId]'s article whole, its yEnc headers and decoded bytes, from the first pool that has
     * it, at [priority]: prepare reads a file's name and size from its first article's. A damaged
     * copy (its CRC isn't its =yend line's) is tried on the next pool, and with no good copy on any
     * it is used or is the failure, as [StreamingConfig.crcMismatch] says, as for a segment.
     */
    internal suspend fun fetchArticle(articleId: String, priority: NntpPriority): FetchedArticle {
        val shown = articleId.printable()
        // Pools whose copy of it is damaged, in the order tried, with the mismatch.
        val mismatched = mutableListOf<Pair<String, YencCrcMismatchException>>()
        return try {
            val article = withFallback("Article <$shown>") { entry ->
                try {
                    entry.pool.withClient(priority.value) { it.bodyYencBytes("<$articleId>") }
                } catch (e: YencCrcMismatchException) {
                    mismatched += entry.name to e
                    throw e
                }
            }
            FetchedArticle(article, storable = true)
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            FetchedArticle(tolerated("Article <$shown>", mismatched) ?: throw e, storable = false)
        }
    }

    /**
     * The copy to use of an article no pool has a good copy of, as [StreamingConfig.crcMismatch]
     * says: the first of [mismatched] (each a pool's name and its copy's mismatch) under TOLERATE,
     * with a warning naming [what] and the pools, counted; null under STRICT, the mismatch then
     * being the failure.
     */
    private fun tolerated(what: String, mismatched: List<Pair<String, YencCrcMismatchException>>): YencArticle? {
        if (streamingConfig.crcMismatch != CrcMismatchPolicy.TOLERATE) return null
        val (pool, first) = mismatched.firstOrNull { it.second.article != null } ?: return null
        logger.warn(
            "{}: no pool has a copy whose CRC matches (damaged on {}); using the copy from {}, not cached: {}",
            what, mismatched.joinToString { it.first }, pool, first.message,
        )
        segmentsCrcMismatchUsed.increment()
        return first.article
    }

    suspend fun statAcrossPools(articleId: String): StatResult {
        val snapshot = poolLock.read { pools.toList() }
        // Weight the starting-pool pick by maxConnections so verification load spreads
        // proportionally to per-pool capacity. The default linear iteration always sends
        // STATs to pool[0] first; with a 100-conn primary + 50-conn fill, that caps health
        // checks at 100 in-flight (pool[1] only gets traffic when pool[0] returns 430).
        // Weighted-random pick distributes the first try ~67/33, so both pools saturate.
        // Body streaming via fetchSegmentFromNntp is unaffected — it still prefers pool[0].
        val ordered = orderForStat(snapshot)
        return ordered.withIndex().firstNotNullOfOrNull { (index, entry) ->
            val result = entry.pool.withClient(NntpPriority.HEALTH_CHECK.value) { it.stat(articleId) }
            when (result) {
                is StatResult.Found -> result
                is StatResult.NotFound -> {
                    if (index < ordered.size - 1) {
                        logger.debug(
                            "STAT {} not found on pool[{}] ({}:{}), trying next pool",
                            articleId.printable(), index, entry.config.host, entry.config.port,
                        )
                    }
                    null
                }
            }
        } ?: StatResult.NotFound(430, "Not found on any pool")
    }

    /**
     * STATs many articles with pipelining ([io.skjaere.nntp.NntpClient.statPipelined]) and
     * returns, per article (in order), whether any pool has it. Each pool's articles are split into
     * batches run on up to [concurrency] of its connections at once (never more than the pool's
     * maxConnections); an article a pool lacks is asked of the next pool straight away, while that
     * pool is still answering the others.
     *
     * [onProgress] gets (answered, total) as articles get their final answer: found on a pool, or
     * missing on every one. A pool that fails other than with a 430 passes the articles it hasn't
     * answered on to the next pool, as [statUntilMissing] does; when no pool can answer for an
     * article, this throws the first pool's failure for it.
     */
    suspend fun statManyAcrossPools(
        articleIds: List<String>,
        concurrency: Int,
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        onProgress: ((answered: Int, total: Int) -> Unit)? = null,
    ): List<Boolean> {
        val check = statPipelinedAcrossPools(articleIds, concurrency, priority, onProgress, stopAtMissing = false)
        check.failure?.let { throw it }
        val missing = check.missing.toSet()
        return articleIds.indices.map { it !in missing }
    }

    /**
     * Like [statManyAcrossPools], but stops at the first article that no pool has, for
     * availability checks where one missing article settles the answer. The STATs still owed are
     * cancelled, so the pool drains those connections (or discards them if the server is slow to
     * answer), and the ones not sent yet never go out.
     *
     * An article is missing only when every pool answered 430 for it. A pool that fails other than
     * with a 430 (its login refused, unreachable, a connection that drops or times out, an error
     * answer, the pool closed) is given up on for this check: the articles it hasn't answered go on
     * to the next pool at once. A pool whose circuit breaker is open is skipped (unless every
     * pool's is, as in streaming). When no pool can answer for an article, the check stops there
     * and [StatCheck.failure] says why.
     */
    suspend fun statUntilMissing(
        articleIds: List<String>,
        concurrency: Int,
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        onProgress: ((answered: Int, total: Int) -> Unit)? = null,
    ): StatCheck = statPipelinedAcrossPools(articleIds, concurrency, priority, onProgress, stopAtMissing = true)

    private suspend fun statPipelinedAcrossPools(
        articleIds: List<String>,
        concurrency: Int,
        priority: NntpPriority,
        onProgress: ((answered: Int, total: Int) -> Unit)?,
        stopAtMissing: Boolean,
    ): StatCheck {
        val total = articleIds.size
        val snapshot = poolLock.read { pools.toList() }
        if (total == 0 || snapshot.isEmpty()) {
            // With no pool, nothing is there.
            val missing = articleIds.indices.toList().let { if (stopAtMissing) it.take(1) else it }
            return StatCheck(missing, missing.size, total)
        }
        val missing = java.util.concurrent.ConcurrentLinkedQueue<Int>()
        val unanswered = java.util.concurrent.ConcurrentLinkedQueue<Int>()
        val failure = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        val answered = java.util.concurrent.atomic.AtomicInteger()
        val passedOn = java.util.concurrent.atomic.AtomicIntegerArray(snapshot.size)
        val timer = StatCheckTimer(snapshot)
        val skipped = breakerSkips(snapshot)
        // asks[p]: the articles to ask pool p for. Those it lacks or can't answer go on to
        // asks[p + 1] as soon as it says so, and asks[p + 1] closes once pool p is done.
        val asks = snapshot.map { Channel<StatAsk>(Channel.UNLIMITED) }
        articleIds.indices.forEach { asks[0].trySend(StatAsk(it, failure = null)) }
        asks[0].close()
        try {
            coroutineScope {
                launch {
                    val check = coroutineContext.job
                    // Pool p gave no answer for [ask]: [cause] says why, null for a 430.
                    fun passOn(p: Int, ask: StatAsk, cause: Throwable?) {
                        val failed = ask.failure ?: cause
                        when {
                            p < snapshot.lastIndex -> {
                                passedOn.incrementAndGet(p)
                                asks[p + 1].trySend(StatAsk(ask.index, failed))
                            }
                            failed == null -> {
                                missing += ask.index
                                val done = answered.incrementAndGet()
                                onProgress?.invoke(done, total)
                                if (stopAtMissing) check.cancel()
                            }
                            else -> {
                                // The check's answer can only be a failure now.
                                unanswered += ask.index
                                failure.compareAndSet(null, failed)
                                check.cancel()
                            }
                        }
                    }
                    snapshot.forEachIndexed { p, entry ->
                        launch {
                            val skip = skipped[p]
                            if (skip != null) {
                                for (ask in asks[p]) passOn(p, ask, skip)
                            } else {
                                askPool(
                                    entry, p, asks[p], articleIds, concurrency, priority, timer,
                                    onFailed = { ask, cause -> passOn(p, ask, cause) },
                                ) { ask, result ->
                                    if (result is StatResult.Found) {
                                        val done = answered.incrementAndGet()
                                        onProgress?.invoke(done, total)
                                    } else {
                                        passOn(p, ask, cause = null)
                                    }
                                }
                            }
                            asks.getOrNull(p + 1)?.close()
                            if (passedOn[p] > 0) {
                                logger.debug(
                                    "{} of {} articles not answered by pool[{}] ({}:{}), asked of the next pool",
                                    passedOn[p], total, p, entry.config.host, entry.config.port,
                                )
                            }
                        }
                    }
                }
            }
        } finally {
            timer.log(
                total, answered.get(), missing.size, unanswered.size,
                stopped = (stopAtMissing && missing.isNotEmpty()) || unanswered.isNotEmpty(),
            )
        }
        return StatCheck(missing.toList(), answered.get(), total, unanswered.toList(), failure.get())
    }

    /**
     * An article on its way through the pools of a [statPipelinedAcrossPools] check: [index] into
     * the ids, and the first failure of a pool it went through (null while every pool asked so
     * far answered 430).
     */
    private class StatAsk(val index: Int, val failure: Throwable?)

    /**
     * Per pool, why a STAT check skips it: its circuit breaker is open. Null for a pool to ask.
     * As in streaming, the breakers only count with more than one pool, and not while every
     * pool's is open: then there is nothing healthy to protect.
     */
    private fun breakerSkips(snapshot: List<PoolEntry>): List<Throwable?> {
        val breakers = snapshot.map(::circuitBreakerFor)
        val open = breakers.map { it.state == CircuitBreaker.State.OPEN || it.state == CircuitBreaker.State.FORCED_OPEN }
        if (snapshot.size < 2 || open.all { it }) return snapshot.map { null }
        return breakers.zip(open) { breaker, isOpen ->
            if (isOpen) CallNotPermittedException.createCallNotPermittedException(breaker) else null
        }
    }

    /**
     * Asks [entry]'s pool for every article [asks] brings, in pipelined batches on up to
     * [concurrency] of its connections, until [asks] is closed. [onAnswer] gets each 223 or 430.
     *
     * A batch that fails other than with a 430 (after the pool's own retry) gives the pool up for
     * this check: the batches still running are cancelled, and every article the pool hasn't
     * answered, those still to come included, goes to [onFailed] with the failure. So does a batch
     * that ends with articles unanswered while the check goes on, whatever ended it.
     */
    @Suppress("LongParameterList")
    private suspend fun askPool(
        entry: PoolEntry,
        poolIndex: Int,
        asks: ReceiveChannel<StatAsk>,
        articleIds: List<String>,
        concurrency: Int,
        priority: NntpPriority,
        timer: StatCheckTimer,
        onFailed: (ask: StatAsk, cause: Throwable) -> Unit,
        onAnswer: (ask: StatAsk, result: StatResult) -> Unit,
    ) = coroutineScope {
        val connections = minOf(concurrency, entry.config.maxConnections).coerceAtLeast(1)
        // Enough batches to use the connections, each big enough to keep a pipeline full.
        val batchSize = maxOf(STAT_CHUNK_MIN, (articleIds.size + connections - 1) / connections)
        val failure = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        val running = ConcurrentHashMap.newKeySet<Job>()
        fun failed(e: Throwable) {
            if (!failure.compareAndSet(null, e)) return
            logger.warn(
                "STAT check: pool[{}] ({}:{}) failed, asking the next pool for the articles it hasn't answered: {}",
                poolIndex, entry.config.host, entry.config.port, e.toString(),
            )
            running.forEach { it.cancel() }
        }
        // One batch on its own job, which the pool failing cancels; the articles it leaves
        // unanswered then go to onFailed.
        suspend fun ask(batch: List<StatAsk>) {
            // Set for each article a 223 or 430 settles; read once the batch is done.
            val settled = BooleanArray(batch.size)
            // Set when onAnswer (the caller's progress callback) throws: the check's failure,
            // not the pool's.
            var answerFailed = false
            // The CancellationException that ended the batch, if one did.
            var cancelled: CancellationException? = null
            val job = launch {
                try {
                    statBatch(entry, poolIndex, batch.map { articleIds[it.index] }, priority, timer) { i, result ->
                        // Any other answer is the pool failing: statPipelined throws it once the
                        // batch is done, unless the connection fails first and the pool's retry
                        // asks that article again.
                        if (result.settles) {
                            settled[i] = true
                            try {
                                onAnswer(batch[i], result)
                            } catch (e: Throwable) {
                                answerFailed = true
                                throw e
                            }
                        }
                    }
                } catch (e: CancellationException) {
                    cancelled = e
                    throw e
                } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                    if (answerFailed) throw e
                    failed(e)
                }
            }
            running += job
            if (failure.get() != null) job.cancel()
            job.join()
            running -= job
            val unsettled = batch.filterIndexed { i, _ -> !settled[i] }
            if (unsettled.isEmpty()) return
            if (failure.get() == null) {
                // The check stopping (or failing) ends its batches too, and leaves nothing to ask.
                ensureActive()
                // Here neither the check nor the pool ended the batch (a stray CancellationException,
                // say), yet no answer settled these: count that as the pool failing for them.
                failed(
                    IllegalStateException(
                        "A STAT batch on pool[$poolIndex] (${entry.config.host}:${entry.config.port}) ended " +
                            "with ${unsettled.size} of ${batch.size} articles unanswered",
                        cancelled,
                    )
                )
            }
            val cause = checkNotNull(failure.get())
            unsettled.forEach { onFailed(it, cause) }
        }
        repeat(connections) {
            launch {
                for (first in asks) {
                    val batch = mutableListOf(first)
                    while (batch.size < batchSize) batch += asks.tryReceive().getOrNull() ?: break
                    val cause = failure.get()
                    if (cause == null) ask(batch) else batch.forEach { onFailed(it, cause) }
                }
            }
        }
    }

    /**
     * STATs [ids] pipelined on one connection of [entry]'s pool, passing each answer to
     * [onResult] (an index into [ids]). Stops once the calling coroutine is cancelled, at the
     * next answer even when the rest are already buffered, or right after the answer that
     * cancelled it (the 430 that ends a check), and sends no more STATs: the pool then drains
     * what's still owed.
     * When the pool's own retry runs the batch again on a new connection, only the articles still
     * without a 223 or 430 are asked: one answered with an error (which statPipelined would have
     * thrown at the end) is asked again, so [onResult] gets its error and then the retry's answer.
     */
    @Suppress("LongParameterList")
    private suspend fun statBatch(
        entry: PoolEntry,
        poolIndex: Int,
        ids: List<String>,
        priority: NntpPriority,
        timer: StatCheckTimer,
        onResult: (index: Int, result: StatResult) -> Unit,
    ) {
        val context = currentCoroutineContext()
        val asked = System.nanoTime()
        var connected = 0L
        var firstAnswer = 0L
        var answers = 0
        var connection = -1L
        val done = BooleanArray(ids.size)
        try {
            entry.pool.withClient(priority.value) { client ->
                connected = System.nanoTime()
                connection = client.connection.id
                timer.waitedForConnection(connected - asked)
                val asking = ids.indices.filterNot { done[it] }
                client.statPipelined(asking.map { "<${ids[it]}>" }) { i, result ->
                    context.ensureActive()
                    if (answers++ == 0) firstAnswer = System.nanoTime()
                    timer.answered()
                    if (result.settles) done[asking[i]] = true
                    onResult(asking[i], result)
                    // An answer that ends the check cancels this coroutine: stop before
                    // statPipelined tops up its window with STATs nobody wants.
                    context.ensureActive()
                }
            }
        } finally {
            val ms = { from: Long, to: Long -> if (from == 0L || to == 0L) -1L else (to - from) / 1_000_000 }
            val end = System.nanoTime()
            logger.debug(
                "STAT batch on pool[{}] conn={}: {} of {} answered; waited {} ms for the connection, " +
                    "first answer after {} ms, {} ms in all{}",
                poolIndex, connection, answers, ids.size, ms(asked, connected), ms(connected, firstAnswer),
                ms(asked, end), if (context.isActive) "" else " (stopped)",
            )
        }
    }

    /** Where a [statPipelinedAcrossPools] call's time goes, for its log line. */
    private inner class StatCheckTimer(private val snapshot: List<PoolEntry>) {
        private val start = System.nanoTime()
        private val openedBefore = snapshot.map { it.pool.connectionsOpened }
        private val firstAnswerNs = AtomicLong(0)
        private val longestWaitNs = AtomicLong(0)
        private val answers = java.util.concurrent.atomic.AtomicInteger()

        fun waitedForConnection(ns: Long) {
            longestWaitNs.accumulateAndGet(ns, ::maxOf)
        }

        fun answered() {
            answers.incrementAndGet()
            firstAnswerNs.compareAndSet(0, System.nanoTime() - start)
        }

        fun log(total: Int, answered: Int, missing: Int, unanswered: Int, stopped: Boolean) {
            val ms = (System.nanoTime() - start) / 1_000_000
            val opened = snapshot.zip(openedBefore) { entry, before -> entry.pool.connectionsOpened - before }
            logger.info(
                "STAT check of {} articles on {} pool(s){}: {} answered, {} missing, {} unanswerable in {} ms; " +
                    "first answer after {} ms, longest wait for a connection {} ms, {} answers/s, " +
                    "connections opened per pool {}",
                total, snapshot.size,
                when {
                    !stopped -> ""
                    unanswered > 0 -> " stopped at an article no pool could answer"
                    else -> " stopped at a missing article"
                },
                answered, missing, unanswered, ms,
                if (firstAnswerNs.get() == 0L) -1 else firstAnswerNs.get() / 1_000_000,
                longestWaitNs.get() / 1_000_000,
                if (ms == 0L) answers.get() else answers.get() * 1000L / ms,
                opened,
            )
        }
    }

    private fun orderForStat(snapshot: List<PoolEntry>): List<PoolEntry> {
        if (snapshot.size <= 1) return snapshot
        val totalWeight = snapshot.sumOf { it.config.maxConnections }
        if (totalWeight <= 0) return snapshot
        var roll = java.util.concurrent.ThreadLocalRandom.current().nextInt(totalWeight)
        var firstIndex = snapshot.lastIndex
        for ((i, entry) in snapshot.withIndex()) {
            roll -= entry.config.maxConnections
            if (roll < 0) {
                firstIndex = i
                break
            }
        }
        if (firstIndex == 0) return snapshot
        return listOf(snapshot[firstIndex]) + snapshot.filterIndexed { i, _ -> i != firstIndex }
    }

    /**
     * Streams segments concurrently and passes the resulting [ByteReadChannel] to [consume].
     * Uses structured concurrency via [coroutineScope] — a segment download's failure is thrown
     * from here once [consume] returns or fails (the channel closes with it when the writer gets
     * to that segment), and cancellation of the caller cancels all in-flight downloads automatically.
     * A [consume] that has what it wants may return, closing the channel ([ByteReadChannel.cancel])
     * or not: the writer stops, and nothing that fails after that is thrown from here.
     *
     * A segment that no pool delivers within the segment fetch timeout fails like any other, with a
     * [SegmentTimeoutException]; parts of a file its NZB lists no article for
     * ([SegmentQueueItem.gap]) fail with an [io.skjaere.nntp.ArticleNotFoundException]. One that no
     * pool has a good copy of, only damaged ones, is streamed damaged or fails with the
     * [YencCrcMismatchException], as [StreamingConfig.crcMismatch] says.
     */
    suspend fun streamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int = currentConcurrency,
        readAheadSegments: Int = currentReadAheadSegments,
        name: String = "unknown",
        readAheadBytes: Long? = currentReadAheadBytes,
        initialReadAheadSegments: Int? = currentInitialReadAheadSegments,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val bitrate = acquireBitrateState(name)
        activeStreams.incrementAndGet()
        val failure = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        try {
            val early = coroutineScope {
                val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, readAheadBytes, initialReadAheadSegments, bitrate, name) {
                    failure.compareAndSet(null, it)
                }
                try {
                    consume(writerJob.channel)
                    // What failed before consume returned: the early end it may have taken for the
                    // end (the writer records a failure before closing the channel with it). What
                    // fails from here on, it didn't wait for. Cancelling the writer doesn't stop a
                    // failure already on its way to it, so the channel is closed first: the writer
                    // then finds it closed and records nothing.
                    failure.get().also { writerJob.channel.cancel() }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // Most likely the channel closing with the download's failure: throw that.
                    throw failure.get() ?: e
                } finally {
                    writerJob.job.cancel()
                }
            }
            // consume may have taken the early end of the data for its end.
            early?.let { throw it }
        } finally {
            activeStreams.decrementAndGet()
            releaseBitrateState(name)
        }
    }

    /**
     * Starts streaming the segments of [queue] and returns at once with the [WriterJob] whose
     * channel delivers them.
     *
     * The writer runs as a child of the calling coroutine's job: cancelling that job (or
     * [WriterJob.job], or the channel) stops it, and that job doesn't complete until the writer
     * has. A failure (an article no provider has or the NZB doesn't list, a timeout) never fails
     * the caller's job: the writer writes everything in front of the failing segment, then closes
     * the channel with the failure as its cause ([ByteReadChannel.closedCause]). Ktor drops the
     * bytes the reader hasn't taken yet when a channel closes with a cause.
     */
    suspend fun launchStreamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int = currentConcurrency,
        readAheadSegments: Int = currentReadAheadSegments,
        name: String = "unknown",
        readAheadBytes: Long? = currentReadAheadBytes,
        initialReadAheadSegments: Int? = currentInitialReadAheadSegments,
    ): WriterJob {
        val bitrate = acquireBitrateState(name)
        val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, readAheadBytes, initialReadAheadSegments, bitrate, name)
        writerJob.job.invokeOnCompletion { releaseBitrateState(name) }
        return writerJob
    }

    /**
     * [onFailure] gets the writer's failure (not its cancellation) before the channel closes with it,
     * unless the reader has closed the channel first: what fails then (a write to the closed
     * channel, a download nobody reads any more) is not the stream failing.
     */
    @Suppress("LongParameterList")
    private suspend fun launchStreamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int,
        readAheadSegments: Int,
        readAheadBytes: Long?,
        initialReadAheadSegments: Int?,
        bitrate: BitrateState,
        name: String,
        onFailure: (Throwable) -> Unit = {},
    ): WriterJob {
        val callerScope = CoroutineScope(currentCoroutineContext())
        return callerScope.writer(autoFlush = false) {
            // Once the channel is closed (by the reader, or below by a failed download that has
            // been reported already) nothing more is reported.
            fun failed(e: Throwable) {
                if (e !is CancellationException && !channel.isClosedForWrite) onFailure(e)
            }
            try {
                // A scoped coroutine: a failure in here is thrown to the writer, which closes the
                // channel with it, instead of failing the caller's job.
                coroutineScope {
                    // Phase timestamps for the FIRST item only. Logged at INFO when the first
                    // byte hits the output channel, so each stream startup leaves exactly one
                    // structured trace line. Lets us localize stall causes without a thread dump:
                    //   t0           = writer entry
                    //   tResolved    = first item emitted by the queue Flow
                    //   tToList      = queue.toList() finished (entire item list realized)
                    //   tFirstAcq    = first downloadSemaphore.acquire() returned
                    //   tFirstAsync  = first async{} launched
                    //   tFirstSend   = first send(item to deferred) returned
                    //   tFirstRecv   = first consumeEach iteration received its pair
                    //   tFirstAwait  = first deferred.await() returned (download complete)
                    //   tFirstWrite  = first channel.writeFully() returned
                    val t0 = System.nanoTime()
                    var tResolved = 0L
                    var tFirstAcq = 0L
                    var tFirstAsync = 0L
                    var tFirstSend = 0L
                    var tFirstRecv = 0L
                    var tFirstAwait = 0L
                    var firstArticleId: String? = null

                    val resolveSample = Timer.start(registry)
                    var firstResolved = false
                    val items = queue.onEach {
                        if (!firstResolved) {
                            resolveSample.stop(firstArticleResolvedTimer)
                            tResolved = System.nanoTime()
                            firstResolved = true
                        }
                    }.toList()
                    val tToList = System.nanoTime()
                    if (items.isEmpty()) return@coroutineScope

                    val downloadSemaphore = Semaphore(concurrency)
                    // Segments downloaded or downloading that the channel hasn't taken yet, counted at
                    // their decoded size in HELD_UNIT_BYTES (each rounded up), as many as readAheadBytes
                    // holds: a segment takes its units before its download starts and gives them back
                    // once its bytes are written. One larger than the budget takes all of it, so it is
                    // held alone, and a large one doesn't cut what the others hold. Only this producer
                    // takes units, so taking several one at a time can't deadlock. What the channel
                    // holds isn't counted: see StreamingConfig.readAheadBytes.
                    val heldUnits = readAheadBytes?.let { budget -> (budget / HELD_UNIT_BYTES).coerceIn(1, Int.MAX_VALUE.toLong()).toInt() }
                    val held = heldUnits?.let { Semaphore(it) }
                    fun unitsOf(item: SegmentQueueItem): Int =
                        ((item.segmentDecodedSize - 1) / HELD_UNIT_BYTES + 1).coerceIn(1, heldUnits?.toLong() ?: 1).toInt()

                    // The stream starts only its first startCount segments (StreamingConfig.initialReadAheadSegments)
                    // until its reader has taken the first one's bytes, which the firstTaken'th write
                    // returning shows; then the rest of its read-ahead.
                    val firstTaken = initialReadAheadSegments?.let { writesUntilFirstTaken(items.asSequence().map { item -> item.readEnd - item.readStart }) }
                    val startCount = if (firstTaken != null && initialReadAheadSegments != null) maxOf(firstTaken, initialReadAheadSegments) else items.size
                    val widened = CompletableDeferred<Unit>()
                    var handed = 0

                    // The read-ahead downloads are children of a supervisor: one that fails (an
                    // article no provider has) cancels neither the others nor the writer. Its
                    // failure is thrown when the writer gets to it and awaits it, after everything
                    // in front of it has been written.
                    supervisorScope {
                        @OptIn(ExperimentalCoroutinesApi::class)
                        produce(capacity = readAheadSegments) {
                            items.forEachIndexed { index, item ->
                                // Parts the NZB lists no article for: the writer fails there, after
                                // everything in front of them, so nothing past them is fetched.
                                item.gap?.let { gap ->
                                    val unlisted: Deferred<ByteArray> = CompletableDeferred<ByteArray>().apply { completeExceptionally(gap.toException()) }
                                    send(item to unlisted)
                                    return@produce
                                }
                                if (index == startCount) widened.await()
                                held?.let { units -> repeat(unitsOf(item)) { units.acquire() } }
                                downloadSemaphore.acquire()
                                if (index == 0) {
                                    tFirstAcq = System.nanoTime()
                                    firstArticleId = item.segment.articleId
                                }
                                val deferred = this@supervisorScope.async {
                                    try {
                                        downloadSegment(item.segment.articleId)
                                    } finally {
                                        downloadSemaphore.release()
                                    }
                                }
                                if (index == 0) tFirstAsync = System.nanoTime()
                                send(item to deferred)
                                if (index == 0) tFirstSend = System.nanoTime()
                            }
                        }.consumeEach { (item, deferred) ->
                            if (tFirstRecv == 0L) tFirstRecv = System.nanoTime()
                            val data = try {
                                deferred.await()
                            } catch (e: Throwable) {
                                // Close the channel with it at once: the downloads still in flight
                                // are cancelled and cleaned up before it leaves this scope.
                                failed(e)
                                channel.cancel(e)
                                throw e
                            }
                            if (tFirstAwait == 0L) tFirstAwait = System.nanoTime()

                            // Clamped as Longs: an item sized past 2 GiB would wrap as an Int.
                            val start = minOf(item.readStart, data.size.toLong()).toInt()
                            val end = minOf(item.readEnd, data.size.toLong()).toInt()
                            if (end > start) {
                                val written = (end - start).toLong()
                                channel.writeFully(data, start, end)
                                // Hand each segment to the reader as it's written, not once 1 MB
                                // has piled up: what a failure further on would otherwise drop.
                                channel.flush()
                                bitrate.addBytes(written)
                                // One-shot startup trace on first successful write. Subsequent writes
                                // are sustained-throughput and the bitrate gauge already covers them.
                                if (tFirstAwait != 0L && firstArticleId != null) {
                                    val tFirstWrite = System.nanoTime()
                                    val ms = { ns: Long -> if (ns == 0L) -1L else (ns - t0) / 1_000_000 }
                                    logger.info(
                                        "Stream startup trace [{}] articleId={}: " +
                                            "resolved={}ms toList={}ms acquire={}ms async={}ms " +
                                            "send={}ms recv={}ms await={}ms write={}ms " +
                                            "(items={}, concurrency={}, readAhead={}, readAheadBytes={}, start={})",
                                        name, firstArticleId?.printable(),
                                        ms(tResolved), ms(tToList), ms(tFirstAcq), ms(tFirstAsync),
                                        ms(tFirstSend), ms(tFirstRecv), ms(tFirstAwait), ms(tFirstWrite),
                                        items.size, concurrency, readAheadSegments, readAheadBytes ?: "unlimited",
                                        startCount,
                                    )
                                    firstArticleId = null
                                }
                            }
                            held?.let { units -> repeat(unitsOf(item)) { units.release() } }
                            if (++handed == firstTaken) widened.complete(Unit)
                        }
                    }
                }
            } catch (e: Throwable) {
                failed(e)
                throw e
            }
        }
    }

    /**
     * One decoded segment, through the segment cache and provider fallback like streaming.
     * For random access (archive headers), where streaming's read-ahead would be wasted.
     *
     * @throws SegmentTimeoutException when no pool delivers it and one timed out (the segment
     *   fetch timeout), even if the others said 430: the one that timed out may have it.
     * @throws YencCrcMismatchException when every pool that has it has a damaged copy, under
     *   [CrcMismatchPolicy.STRICT]; under TOLERATE, the default, the first such copy is returned.
     */
    suspend fun fetchSegment(articleId: String): ByteArray = downloadSegment(articleId)

    /**
     * An article's yEnc headers and decoded bytes from the segment cache, put there by
     * [cacheArticle]; null without a cache, or if either isn't in it.
     */
    suspend fun cachedArticle(articleId: String): Pair<YencHeaders, ByteArray>? {
        val cache = segmentCache ?: return null
        val headers = cache.get(headersKey(articleId))
            ?.let { bytes -> runCatching { Json.decodeFromString<CachedYencHeaders>(bytes.decodeToString()) }.getOrNull() }
            ?: return null
        return cache.get(articleId)?.let { headers.toYencHeaders() to it }
    }

    /**
     * Puts an article fetched with its yEnc headers (prepare reads a file's name and size from its
     * first article's) in the segment cache, if there is one: its bytes as streaming's are, and its
     * headers beside them, so a release prepared again (a replay, a resume) fetches it no more.
     */
    suspend fun cacheArticle(articleId: String, headers: YencHeaders, article: ByteArray) {
        val cache = segmentCache ?: return
        cache.getOrFetch(articleId) { article }
        cache.getOrFetch(headersKey(articleId)) { Json.encodeToString(CachedYencHeaders(headers)).encodeToByteArray() }
    }

    private suspend fun downloadSegment(articleId: String): ByteArray {
        if (segmentCache != null) {
            val getSample = Timer.start(registry)
            val cached = segmentCache.get(articleId)
            getSample.stop(segmentCacheGetTimer)
            if (cached != null) {
                segmentCacheHits.increment()
                return cached
            }
            segmentCacheMisses.increment()
            // Singleflight in SegmentCache dedupes concurrent fetches for the same article.
            val getOrFetchSample = Timer.start(registry)
            try {
                return segmentCache.getOrFetchStorable(articleId) { fetchSegmentFromNntp(articleId) }
            } finally {
                getOrFetchSample.stop(segmentCacheGetOrFetchTimer)
            }
        }
        return fetchSegmentFromNntp(articleId).bytes
    }

    /** [articleId]'s bytes, from the first pool with a good copy; or, with none, a damaged one not to be cached ([tolerated]). */
    private suspend fun fetchSegmentFromNntp(articleId: String): SegmentCache.Fetched {
        val sample = Timer.start(registry)
        // The id as logged and reported: an NZB can put a CR/LF in it (it's never sent then).
        val shown = articleId.printable()
        // Snapshot the timeout once so the warning log's "timed out after Nms" matches
        // the actual deadline used by withTimeout — a runtime override mid-fetch
        // shouldn't desync the log from the cancellation event.
        val timeoutMs = currentSegmentFetchTimeoutMs
        val snapshot = poolLock.read { pools.toList() }
        // The circuit breaker is only meaningful when there's somewhere to fall back
        // to. With a single pool, opening it would just convert every request into
        // a CallNotPermittedException — worse than the timeout/error we'd otherwise
        // surface. With multiple pools, an open breaker lets us skip a sick upstream
        // for `circuitBreakerCooldownMs` without trying it at all — *unless* every
        // pool's breaker is OPEN at once (transient outage tripping all of them).
        // In that case the breaker has nothing healthy to protect, so disable gating
        // for this call and let the real upstream error surface.
        val breakerActive = snapshot.size > 1 &&
            !snapshot.all { circuitBreakerFor(it).state == CircuitBreaker.State.OPEN }
        // Track timeout failures across pools. ArticleNotFoundException means the server
        // gave a definitive 430; a timeout means we don't actually know whether the
        // article is missing or just slow. If ANY pool timed out, surface as a
        // SegmentTimeoutException rather than ArticleNotFoundException — otherwise
        // a transient slow-NNTP incident gets misclassified as "article permanently
        // missing", which downstream (NzbImportService, NzbFileResource) treats as
        // DMCA/retention drop and blocklists the NZB or skips it silently. The same goes
        // for a pool that failed any other way (its connection dropped mid-article, a
        // protocol or decoding error): surface that failure if no pool timed out. A pool whose
        // copy is damaged (a CRC mismatch) did answer: its mismatch is surfaced only if no pool
        // failed otherwise, when no damaged copy is used instead (StreamingConfig.crcMismatch).
        var firstTimeout: SegmentTimeoutException? = null
        var firstError: Exception? = null
        // Pools whose copy is damaged, in the order tried, with the mismatch.
        val mismatched = mutableListOf<Pair<String, YencCrcMismatchException>>()
        var anyAttempted = false
        suspend fun fetchFromPools(useBreakers: Boolean): ByteArray? = snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            val breaker = if (useBreakers) circuitBreakerFor(entry) else null
            if (breaker != null && !breaker.tryAcquirePermission()) {
                logger.debug(
                    "Segment <{}> skipping pool[{}] ({}:{}) — circuit breaker is {}",
                    shown, index, entry.config.host, entry.config.port, breaker.state
                )
                segmentsFallback.increment()
                return@firstNotNullOfOrNull null
            }
            anyAttempted = true
            fetchFromPool(articleId, entry, index, snapshot.size, breaker, timeoutMs) { e ->
                when {
                    e is YencCrcMismatchException -> mismatched += entry.name to e
                    e is SegmentTimeoutException -> if (firstTimeout == null) firstTimeout = e
                    firstError == null -> firstError = e
                }
            }
        }
        // Every pool refused a permit (a probe race, or each HALF_OPEN breaker's one permit
        // taken): as in withFallback, try anyway rather than call the article missing.
        val data = fetchFromPools(breakerActive) ?: if (breakerActive && !anyAttempted) {
            logger.warn(
                "Segment <{}>: every pool refused the breaker permit (likely HALF_OPEN with permit taken) — " +
                    "retrying with breakers disabled",
                shown,
            )
            fetchFromPools(useBreakers = false)
        } else {
            null
        }

        sample.stop(segmentDownloadTimer)
        // No pool had a good copy: a damaged one may do instead, not to be cached.
        val fetched = data?.let { SegmentCache.Fetched(it, storable = true) }
            ?: tolerated("Segment <$shown>", mismatched)?.let { SegmentCache.Fetched(it.data, storable = false) }
        if (fetched != null) {
            segmentsDownloaded.increment()
            segmentsBytes.increment(fetched.bytes.size.toDouble())
            return fetched
        }
        segmentsFailed.increment()
        firstTimeout?.let { throw it }
        firstError?.let { throw it }
        mismatched.firstOrNull()?.let { (_, mismatch) -> throw mismatch }
        throw ArticleNotFoundException("Article <$shown> not found on any pool")
    }

    /**
     * One attempt at [articleId] on [entry]'s pool, reporting to [breaker] (if any) how it went.
     * Returns null to move on to the next pool; a failure other than a clean 430 (a timeout, as a
     * [SegmentTimeoutException], a connection that dropped, a damaged copy) also goes to [onFailure].
     */
    @Suppress("LongParameterList", "LongMethod")
    private suspend fun fetchFromPool(
        articleId: String,
        entry: PoolEntry,
        index: Int,
        poolCount: Int,
        breaker: CircuitBreaker?,
        timeoutMs: Long,
        onFailure: (Exception) -> Unit,
    ): ByteArray? {
        val callStart = System.nanoTime()
        val shown = articleId.printable()
        // Set once the breaker has heard how the call went. Any other way out (cancelled as the
        // reader seeks or stops, the pool closed, an error no arm below expects) hands the permit
        // back in the finally: a HALF_OPEN breaker has one, and if it's kept, the breaker stays
        // half-open with none and this pool is skipped until the process restarts.
        var settled = false
        // Completed once the pool has handed a connection over: a timeout before that was the wait for one.
        val connected = CompletableDeferred<Unit>()
        return try {
            // Read inside withClient, not through pool.bodyYenc: there a connection that drops
            // mid-article fails only the Body channel's reader, outside withClient, and the
            // segment fails. Read here, the failure is withClient's, and the pool fetches the
            // article again on another connection (its command retry). Nothing is handed on
            // before the whole segment is read, so no byte is delivered twice. Read whole, it
            // is decoded straight into one array, not through a channel and copied out of it.
            val payload = withConnectionWithin(entry.pool, timeoutMs, connected) { client ->
                client.bodyYencBytes("<$articleId>").data
            }
            breaker?.onResult(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                payload,
            )
            settled = true
            if (index > 0) {
                logger.debug("Segment <{}> served by fallback pool[{}]", shown, index)
            }
            payload
        } catch (e: ArticleNotFoundException) {
            // Clean 430 — server told us the article is gone. That's not a pool
            // health signal, so the finally releases the permit without recording it.
            if (index < poolCount - 1) {
                logger.debug(
                    "Segment <{}> not found on pool[{}] ({}:{}), trying pool[{}]",
                    shown, index, entry.config.host, entry.config.port, index + 1
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: YencCrcMismatchException) {
            // A damaged copy, read to its end: the pool answered, its copy of the article is bad.
            // Not a pool health signal either, so the finally releases the permit without recording it.
            onFailure(e)
            if (index < poolCount - 1) {
                logger.warn(
                    "Segment <{}> CRC mismatch on pool[{}] ({}:{}), trying pool[{}]: {}",
                    shown, index, entry.config.host, entry.config.port, index + 1, e.message
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: TimeoutCancellationException) {
            // This attempt's own timeout only: a caller's timeout (or other cancellation) has
            // cancelled this coroutine too, and goes on up as the caller's.
            currentCoroutineContext().ensureActive()
            val poolName = entry.name
            val what = if (connected.isCompleted) "timed out after ${timeoutMs}ms" else "got no connection within ${timeoutMs}ms"
            val timeout = SegmentTimeoutException(articleId, timeoutMs, "Segment <$shown> $what on pool[$index] ($poolName)", e)
            breaker?.onError(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                timeout,
            )
            settled = true
            onFailure(timeout)
            // Count every per-pool timeout, regardless of whether a later pool
            // succeeded. Useful as its own rate panel even with the body-duration
            // fix below — easier to alert on "rate(timeouts) > N/min" than on a
            // percentile drift.
            registry.counter("nzb.segments.body_timeouts", "pool.name", poolName).increment()
            // Also record the *timeout duration* into the same nntp.body.duration
            // histogram NntpConnection writes successes to. The connection-side
            // timer is never stopped on cancellation, so the upper tail of slow
            // upstreams used to be invisible in percentile dashboards. Now a 9s
            // timeout shows up at 9s in the histogram — the BODY transfer duration
            // panel's p99 finally reflects the actual user-facing tail.
            bodyDurationTimerFor(poolName)
                .record(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
            if (index < poolCount - 1) {
                logger.warn(
                    "Segment <{}> timed out after {}ms on pool[{}] ({}:{}), trying pool[{}]",
                    shown, timeoutMs,
                    index, entry.config.host, entry.config.port, index + 1
                )
                segmentsFallback.increment()
            } else {
                logger.warn(
                    "Segment <{}> timed out after {}ms on all {} pool(s)",
                    shown, timeoutMs, poolCount
                )
            }
            null
        } catch (e: NntpException) {
            breaker?.onError(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                e,
            )
            settled = true
            onFailure(e)
            if (index < poolCount - 1) {
                logger.warn(
                    "Segment <{}> NNTP error on pool[{}] ({}:{}) — {}: {}",
                    shown, index, entry.config.host, entry.config.port,
                    e::class.simpleName, e.message
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: IOException) {
            breaker?.onError(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                e,
            )
            settled = true
            onFailure(e)
            if (index < poolCount - 1) {
                logger.warn(
                    "Segment <{}> I/O error on pool[{}] ({}:{}) — {}: {}",
                    shown, index, entry.config.host, entry.config.port,
                    e::class.simpleName, e.message
                )
                segmentsFallback.increment()
            }
            null
        } finally {
            if (!settled) breaker?.releasePermission()
        }
    }

    /**
     * [fetch] on one of [pool]'s connections, given [timeoutMs] from when the pool hands one over.
     * The wait for it (every connection fetching, or still reading off what a cancelled fetch left
     * on the wire, or connecting) doesn't use that time up, and has [timeoutMs] of its own. The
     * whole call, the pool's fetch again on another connection after one dropped included, has
     * twice that. Any of them running out throws a [TimeoutCancellationException]; [connected] is
     * completed once the pool has handed a connection over.
     */
    private suspend fun <T> withConnectionWithin(
        pool: NntpClientPool,
        timeoutMs: Long,
        connected: CompletableDeferred<Unit>,
        fetch: suspend (io.skjaere.nntp.NntpClient) -> T,
    ): T = withTimeout(timeoutMs.milliseconds * 2) {
        coroutineScope {
            val call = async {
                pool.withClient(NntpPriority.STREAMING.value) { client ->
                    connected.complete(Unit)
                    withTimeout(timeoutMs.milliseconds) { fetch(client) }
                }
            }
            // A call that fails before it has a connection fails this scope, which ends the wait.
            withTimeout(timeoutMs.milliseconds) { connected.await() }
            call.await()
        }
    }

    override fun close() {
        poolLock.read {
            pools.forEach { it.pool.close() }
        }
        if (closeSegmentCache) segmentCache?.close()
        // Close the ktor SelectorManager that all pool sockets share. Must happen after the
        // pools have torn their sockets down so we don't yank the selector out from under
        // an in-flight QUIT round-trip.
        if (::selectorManager.isInitialized) {
            runCatching { selectorManager.close() }
        }
        scope.cancel()
    }

}
