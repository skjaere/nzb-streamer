package io.skjaere.nzbstreamer.stream

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.ktor.network.selector.SelectorManager
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.toByteArray
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
import io.skjaere.nntp.YencEvent
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import java.io.IOException
import java.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration.Companion.milliseconds
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/** Smallest STAT batch worth a pipeline of its own. */
private const val STAT_CHUNK_MIN = 32

class NntpStreamingService(
    initialConfigs: List<NntpConfig>,
    private val streamingConfig: StreamingConfig = StreamingConfig(),
    private val segmentCache: SegmentCache? = null
) : Closeable {
    constructor(config: NntpConfig) : this(listOf(config))

    private data class PoolEntry(val config: NntpConfig, val pool: NntpClientPool)

    private val scope = CoroutineScope(SupervisorJob())
    private val logger = LoggerFactory.getLogger(NntpStreamingService::class.java)
    private val pools = mutableListOf<PoolEntry>()
    private lateinit var selectorManager: SelectorManager
    private val poolLock = ReentrantReadWriteLock()

    /**
     * Per-pool circuit breaker. Each pool gets a CircuitBreaker named `host:port`,
     * sharing the same config (count-based sliding window, threshold + cooldown
     * from [StreamingConfig]). Failures that count: timeouts, IOExceptions, and
     * NntpException family — anything we can't blame on a clean server 430.
     * ArticleNotFoundException is *ignored* (server cleanly said the article
     * doesn't exist — that's not the pool's fault). The breaker is inert when
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
            .ignoreExceptions(ArticleNotFoundException::class.java)
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
            "${entry.config.host}:${entry.config.port}",
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

    private val registry = Metrics.globalRegistry
    private val segmentsDownloaded = registry.counter("nzb.segments.downloaded")
    private val segmentsBytes = registry.counter("nzb.segments.bytes")
    private val segmentDownloadTimer = Timer.builder("nzb.segments.download.duration")
        .description("End-to-end time per segment fetch: status line + yenc body transfer + any multi-pool fallback retries")
        .publishPercentileHistogram()
        .register(registry)
    private val segmentsFailed = registry.counter("nzb.segments.failed")
    private val segmentsFallback = registry.counter("nzb.segments.fallback")
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
        selectorManager = SelectorManager(EmptyCoroutineContext)
        initialConfigs.forEachIndexed { index, config ->
            pools.add(PoolEntry(config, createPool(config)))
            logger.info(
                "NNTP pool[{}] initialized: {}:{} maxConnections={}",
                index, config.host, config.port, config.maxConnections
            )
        }
    }

    private fun createPool(config: NntpConfig): NntpClientPool {
        return NntpClientPool(
            host = config.host,
            port = config.port,
            selectorManager = selectorManager,
            useTls = config.useTls,
            username = config.username.ifEmpty { null },
            password = config.password.ifEmpty { null },
            maxConnections = config.maxConnections,
            scope = scope,
            verifyHostname = config.verifyHostname
        )
    }

    fun addPool(config: NntpConfig) {
        poolLock.write {
            require(pools.none { it.config.host == config.host && it.config.port == config.port }) {
                "Pool already exists for ${config.host}:${config.port}"
            }
            pools.add(PoolEntry(config, createPool(config)))
            logger.info(
                "NNTP pool added at runtime: {}:{} maxConnections={} (total pools: {})",
                config.host, config.port, config.maxConnections, pools.size
            )
        }
    }

    fun removePool(config: NntpConfig) {
        poolLock.write {
            val index = pools.indexOfFirst { it.config == config }
            require(index >= 0) { "No pool found for ${config.host}:${config.port}" }
            val entry = pools.removeAt(index)
            entry.pool.close()
            logger.info(
                "NNTP pool removed at runtime: {}:{} (total pools: {})",
                config.host, config.port, pools.size
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

    fun getStreamingConcurrency(): Int = currentConcurrency

    fun getReadAheadSegments(): Int = currentReadAheadSegments

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
     *
     * Not caught (propagated directly):
     *  - [NntpAuthenticationException] — credentials problem, every pool would
     *    likely fail the same way; fail loudly so the operator notices.
     *  - [kotlinx.coroutines.CancellationException] — structured-concurrency
     *    cancellation, not our exception to swallow.
     *
     * Throws the last fallback-triggering exception if every pool fails, except that a 430
     * never replaces another failure: a pool whose connection dropped may have the article.
     */
    @Suppress("TooGenericExceptionCaught")
    private suspend fun <T> withFallback(
        logPrefix: String,
        block: suspend (pool: NntpClientPool) -> T
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
        block: suspend (pool: NntpClientPool) -> T,
        recordException: (Throwable) -> Unit,
    ): T? {
        val callStart = System.nanoTime()
        return try {
            val result = block(entry.pool)
            // Pass Unit rather than `result`: T is unbounded (could be nullable),
            // and resilience4j only inspects the result when a recordResultPredicate
            // is configured (we don't). Avoids the Java Object/T platform-type warning.
            breaker?.onResult(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                Unit,
            )
            result
        } catch (e: io.skjaere.nntp.NntpAuthenticationException) {
            // Credentials issue — every pool would fail the same way. Release
            // the permit without recording, then rethrow so the operator sees it.
            breaker?.releasePermission()
            throw e
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
        } catch (e: io.skjaere.nntp.NntpException) {
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
    ): T = withFallback("Article") { pool -> pool.withClient(priority.value, block) }

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
                            articleId, index, entry.config.host, entry.config.port,
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
     * missing on the last one.
     */
    suspend fun statManyAcrossPools(
        articleIds: List<String>,
        concurrency: Int,
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        onProgress: ((answered: Int, total: Int) -> Unit)? = null,
    ): List<Boolean> {
        val missing = statPipelinedAcrossPools(articleIds, concurrency, priority, onProgress, stopAtMissing = false)
            .missing.toSet()
        return articleIds.indices.map { it !in missing }
    }

    /**
     * Like [statManyAcrossPools], but stops at the first article that no pool has, for
     * availability checks where one missing article settles the answer. The STATs still owed are
     * cancelled, so the pool drains those connections (or discards them if the server is slow to
     * answer), and the ones not sent yet never go out.
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
        val answered = java.util.concurrent.atomic.AtomicInteger()
        val passedOn = java.util.concurrent.atomic.AtomicIntegerArray(snapshot.size)
        val timer = StatCheckTimer(snapshot)
        // asks[p]: the articles to ask pool p for. Those it lacks go on to asks[p + 1] as soon as
        // it says so, and asks[p + 1] closes once pool p has answered everything.
        val asks = snapshot.map { Channel<Int>(Channel.UNLIMITED) }
        articleIds.indices.forEach { asks[0].trySend(it) }
        asks[0].close()
        try {
            coroutineScope {
                launch {
                    val check = coroutineContext.job
                    snapshot.forEachIndexed { p, entry ->
                        launch {
                            askPool(entry, p, asks[p], articleIds, concurrency, priority, timer) { index, result ->
                                when {
                                    result is StatResult.Found -> {
                                        val done = answered.incrementAndGet()
                                        onProgress?.invoke(done, total)
                                    }
                                    // Not a 430: statPipelined throws for it once its batch is done.
                                    result is StatResult.NotFound && result.code != 430 -> Unit
                                    p < snapshot.lastIndex -> {
                                        passedOn.incrementAndGet(p)
                                        asks[p + 1].trySend(index)
                                    }
                                    else -> {
                                        missing += index
                                        val done = answered.incrementAndGet()
                                        onProgress?.invoke(done, total)
                                        if (stopAtMissing) check.cancel()
                                    }
                                }
                            }
                            asks.getOrNull(p + 1)?.close()
                            if (passedOn[p] > 0) {
                                logger.debug(
                                    "{} of {} articles not on pool[{}] ({}:{}), asked of the next pool",
                                    passedOn[p], total, p, entry.config.host, entry.config.port,
                                )
                            }
                        }
                    }
                }
            }
        } finally {
            timer.log(total, answered.get(), missing.size, stopped = stopAtMissing && missing.isNotEmpty())
        }
        return StatCheck(missing.toList(), answered.get(), total)
    }

    /**
     * Asks [entry]'s pool for every article [asks] brings (indices into [articleIds]), in
     * pipelined batches on up to [concurrency] of its connections, until [asks] is closed.
     */
    @Suppress("LongParameterList")
    private suspend fun askPool(
        entry: PoolEntry,
        poolIndex: Int,
        asks: ReceiveChannel<Int>,
        articleIds: List<String>,
        concurrency: Int,
        priority: NntpPriority,
        timer: StatCheckTimer,
        onAnswer: (index: Int, result: StatResult) -> Unit,
    ) = coroutineScope {
        val connections = minOf(concurrency, entry.config.maxConnections).coerceAtLeast(1)
        // Enough batches to use the connections, each big enough to keep a pipeline full.
        val batchSize = maxOf(STAT_CHUNK_MIN, (articleIds.size + connections - 1) / connections)
        repeat(connections) {
            launch {
                for (first in asks) {
                    val batch = mutableListOf(first)
                    while (batch.size < batchSize) batch += asks.tryReceive().getOrNull() ?: break
                    statBatch(entry, poolIndex, batch.map { articleIds[it] }, priority, timer) { i, result ->
                        onAnswer(batch[i], result)
                    }
                }
            }
        }
    }

    /**
     * STATs [ids] pipelined on one connection of [entry]'s pool, passing each answer to
     * [onResult] (an index into [ids]). Stops at the next answer once the calling coroutine is
     * cancelled, even when the rest are already buffered: the pool then drains what's still owed.
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
        try {
            entry.pool.withClient(priority.value) { client ->
                connected = System.nanoTime()
                connection = client.connection.id
                timer.waitedForConnection(connected - asked)
                client.statPipelined(ids.map { "<$it>" }) { i, result ->
                    context.ensureActive()
                    if (answers++ == 0) firstAnswer = System.nanoTime()
                    timer.answered()
                    onResult(i, result)
                }
            }
        } finally {
            val ms = { from: Long, to: Long -> if (from == 0L || to == 0L) -1L else (to - from) / 1_000_000 }
            val done = System.nanoTime()
            logger.debug(
                "STAT batch on pool[{}] conn={}: {} of {} answered; waited {} ms for the connection, " +
                    "first answer after {} ms, {} ms in all{}",
                poolIndex, connection, answers, ids.size, ms(asked, connected), ms(connected, firstAnswer),
                ms(asked, done), if (context.isActive) "" else " (stopped)",
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

        fun log(total: Int, answered: Int, missing: Int, stopped: Boolean) {
            val ms = (System.nanoTime() - start) / 1_000_000
            val opened = snapshot.zip(openedBefore) { entry, before -> entry.pool.connectionsOpened - before }
            logger.info(
                "STAT check of {} articles on {} pool(s){}: {} answered, {} missing in {} ms; " +
                    "first answer after {} ms, longest wait for a connection {} ms, {} answers/s, " +
                    "connections opened per pool {}",
                total, snapshot.size, if (stopped) " stopped at a missing article" else "",
                answered, missing, ms,
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
     * A [consume] that has what it wants may close the channel ([ByteReadChannel.cancel]) and
     * return: the writer stops, and nothing that fails after that is thrown from here.
     *
     * A segment that times out on every pool is not thrown from here yet: it closes the channel
     * with a [CancellationException], which reads throw as such and [ByteReadChannel.isClosedForRead]
     * takes for the end of the data.
     */
    suspend fun streamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int = currentConcurrency,
        readAheadSegments: Int = currentReadAheadSegments,
        name: String = "unknown",
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val bitrate = acquireBitrateState(name)
        activeStreams.incrementAndGet()
        val failure = java.util.concurrent.atomic.AtomicReference<Throwable?>()
        try {
            coroutineScope {
                val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, bitrate, name) {
                    failure.compareAndSet(null, it)
                }
                try {
                    consume(writerJob.channel)
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
            failure.get()?.let { throw it }
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
     * has. A failure (an article no provider has, a timeout) never fails the caller's job: the
     * writer writes everything in front of the failing segment, then closes the channel with the
     * failure as its cause ([ByteReadChannel.closedCause]). Ktor drops the bytes the reader hasn't
     * taken yet when a channel closes with a cause.
     */
    suspend fun launchStreamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int = currentConcurrency,
        readAheadSegments: Int = currentReadAheadSegments,
        name: String = "unknown"
    ): WriterJob {
        val bitrate = acquireBitrateState(name)
        val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, bitrate, name)
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

                    // The read-ahead downloads are children of a supervisor: one that fails (an
                    // article no provider has) cancels neither the others nor the writer. Its
                    // failure is thrown when the writer gets to it and awaits it, after everything
                    // in front of it has been written.
                    supervisorScope {
                        @OptIn(ExperimentalCoroutinesApi::class)
                        produce(capacity = readAheadSegments) {
                            items.forEachIndexed { index, item ->
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

                            val start = minOf(item.readStart.toInt(), data.size)
                            val end = minOf(item.readEnd.toInt(), data.size)
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
                                            "(items={}, concurrency={}, readAhead={})",
                                        name, firstArticleId,
                                        ms(tResolved), ms(tToList), ms(tFirstAcq), ms(tFirstAsync),
                                        ms(tFirstSend), ms(tFirstRecv), ms(tFirstAwait), ms(tFirstWrite),
                                        items.size, concurrency, readAheadSegments,
                                    )
                                    firstArticleId = null
                                }
                            }
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
     */
    suspend fun fetchSegment(articleId: String): ByteArray = downloadSegment(articleId)

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
                return segmentCache.getOrFetch(articleId) { fetchSegmentFromNntp(articleId) }
            } finally {
                getOrFetchSample.stop(segmentCacheGetOrFetchTimer)
            }
        }
        return fetchSegmentFromNntp(articleId)
    }

    private suspend fun fetchSegmentFromNntp(articleId: String): ByteArray {
        val sample = Timer.start(registry)
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
        // TimeoutCancellationException rather than ArticleNotFoundException — otherwise
        // a transient slow-NNTP incident gets misclassified as "article permanently
        // missing", which downstream (NzbImportService, NzbFileResource) treats as
        // DMCA/retention drop and blocklists the NZB or skips it silently. The same goes
        // for a pool that failed any other way (its connection dropped mid-article, a
        // protocol or decoding error): surface that failure if no pool timed out.
        var firstTimeout: TimeoutCancellationException? = null
        var firstError: Exception? = null
        var anyAttempted = false
        suspend fun fetchFromPools(useBreakers: Boolean): ByteArray? = snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            val breaker = if (useBreakers) circuitBreakerFor(entry) else null
            if (breaker != null && !breaker.tryAcquirePermission()) {
                logger.debug(
                    "Segment <{}> skipping pool[{}] ({}:{}) — circuit breaker is {}",
                    articleId, index, entry.config.host, entry.config.port, breaker.state
                )
                segmentsFallback.increment()
                return@firstNotNullOfOrNull null
            }
            anyAttempted = true
            fetchFromPool(articleId, entry, index, snapshot.size, breaker, timeoutMs) { e ->
                when {
                    e is TimeoutCancellationException -> if (firstTimeout == null) firstTimeout = e
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
                articleId,
            )
            fetchFromPools(useBreakers = false)
        } else {
            null
        }

        sample.stop(segmentDownloadTimer)
        if (data != null) {
            segmentsDownloaded.increment()
            segmentsBytes.increment(data.size.toDouble())
            return data
        }
        segmentsFailed.increment()
        firstTimeout?.let { throw it }
        firstError?.let { throw it }
        throw ArticleNotFoundException("Article <$articleId> not found on any pool")
    }

    /**
     * One attempt at [articleId] on [entry]'s pool, reporting to [breaker] (if any) how it went.
     * Returns null to move on to the next pool; a failure other than a clean 430 (a timeout, a
     * connection that dropped) also goes to [onFailure].
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
        // Set once the breaker has heard how the call went. Any other way out (cancelled as the
        // reader seeks or stops, the pool closed, an error no arm below expects) hands the permit
        // back in the finally: a HALF_OPEN breaker has one, and if it's kept, the breaker stays
        // half-open with none and this pool is skipped until the process restarts.
        var settled = false
        return try {
            var result: ByteArray? = null
            withTimeout(timeoutMs.milliseconds) {
                // Read inside withClient, not through pool.bodyYenc: there a connection that drops
                // mid-article fails only the Body channel's reader, outside withClient, and the
                // segment fails. Read here, the failure is withClient's, and the pool fetches the
                // article again on another connection (its command retry). Nothing is handed on
                // before the whole segment is read, so no byte is delivered twice.
                entry.pool.withClient(NntpPriority.STREAMING.value) { client ->
                    client.bodyYenc("<$articleId>").collect { event ->
                        if (event is YencEvent.Body) {
                            result = event.data.toByteArray()
                        }
                    }
                }
            }
            val payload = result
            if (payload != null) {
                breaker?.onResult(
                    System.nanoTime() - callStart,
                    java.util.concurrent.TimeUnit.NANOSECONDS,
                    payload,
                )
                settled = true
                if (index > 0) {
                    logger.debug("Segment <{}> served by fallback pool[{}]", articleId, index)
                }
            }
            payload
        } catch (e: ArticleNotFoundException) {
            // Clean 430 — server told us the article is gone. That's not a pool
            // health signal, so the finally releases the permit without recording it.
            if (index < poolCount - 1) {
                logger.debug(
                    "Segment <{}> not found on pool[{}] ({}:{}), trying pool[{}]",
                    articleId, index, entry.config.host, entry.config.port, index + 1
                )
                segmentsFallback.increment()
            }
            null
        } catch (e: TimeoutCancellationException) {
            val poolName = "${entry.config.host}:${entry.config.port}"
            breaker?.onError(
                System.nanoTime() - callStart,
                java.util.concurrent.TimeUnit.NANOSECONDS,
                e,
            )
            settled = true
            onFailure(e)
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
                    articleId, timeoutMs,
                    index, entry.config.host, entry.config.port, index + 1
                )
                segmentsFallback.increment()
            } else {
                logger.warn(
                    "Segment <{}> timed out after {}ms on all {} pool(s)",
                    articleId, timeoutMs, poolCount
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
                    articleId, index, entry.config.host, entry.config.port,
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
                    articleId, index, entry.config.host, entry.config.port,
                    e::class.simpleName, e.message
                )
                segmentsFallback.increment()
            }
            null
        } finally {
            if (!settled) breaker?.releasePermission()
        }
    }

    override fun close() {
        poolLock.read {
            pools.forEach { it.pool.close() }
        }
        segmentCache?.close()
        // Close the ktor SelectorManager that all pool sockets share. Must happen after the
        // pools have torn their sockets down so we don't yank the selector out from under
        // an in-flight QUIT round-trip.
        if (::selectorManager.isInitialized) {
            runCatching { selectorManager.close() }
        }
        scope.cancel()
    }

}
