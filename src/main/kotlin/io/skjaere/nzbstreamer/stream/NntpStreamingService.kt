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
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
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
            scope = scope
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
     * Throws the last fallback-triggering exception if every pool fails.
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
        return snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            val breaker = if (effectiveBreakerActive) circuitBreakerFor(entry) else null
            if (breaker != null && !breaker.tryAcquirePermission()) {
                logger.debug(
                    "{} skipping pool[{}] ({}:{}) — circuit breaker is {}",
                    logPrefix, index, entry.config.host, entry.config.port, breaker.state
                )
                segmentsFallback.increment()
                return@firstNotNullOfOrNull null
            }
            val callStart = System.nanoTime()
            try {
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
                lastException = e
                if (index < snapshot.size - 1) {
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
                lastException = e
                if (index < snapshot.size - 1) {
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
                lastException = e
                if (index < snapshot.size - 1) {
                    logger.debug(
                        "{} I/O failure on pool[{}] ({}:{}) — falling back to pool[{}]: {}",
                        logPrefix, index, entry.config.host, entry.config.port,
                        index + 1, e.message,
                    )
                    segmentsFallback.increment()
                }
                null
            }
        } ?: throw (lastException ?: error(
            "$logPrefix: every pool refused the call but none threw — this branch is " +
                "only reachable if all circuit breakers transitioned to OPEN between the " +
                "preflight check and the per-pool tryAcquirePermission. Retry."
        ))
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
     * Uses structured concurrency via [coroutineScope] — exceptions from segment downloads
     * propagate directly to the caller, and cancellation of the caller cancels all in-flight
     * downloads automatically.
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
        try {
            coroutineScope {
                val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, bitrate, name)
                try {
                    consume(writerJob.channel)
                } finally {
                    writerJob.job.cancel()
                }
            }
        } finally {
            activeStreams.decrementAndGet()
            releaseBitrateState(name)
        }
    }

    /**
     * Launches segment streaming as a child of the caller's coroutine scope, returning a [WriterJob]
     * whose channel can be read independently. The caller manages the job's lifecycle.
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

    private suspend fun launchStreamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int,
        readAheadSegments: Int,
        bitrate: BitrateState,
        name: String,
    ): WriterJob {
        val callerScope = CoroutineScope(currentCoroutineContext())
        return callerScope.writer(autoFlush = false) {
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
            if (items.isEmpty()) return@writer

            val downloadSemaphore = Semaphore(concurrency)

            @OptIn(ExperimentalCoroutinesApi::class)
            produce(capacity = readAheadSegments) {
                items.forEachIndexed { index, item ->
                    downloadSemaphore.acquire()
                    if (index == 0) {
                        tFirstAcq = System.nanoTime()
                        firstArticleId = item.segment.articleId
                    }
                    val deferred = async {
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
                val data = deferred.await()
                if (tFirstAwait == 0L) tFirstAwait = System.nanoTime()

                val start = minOf(item.readStart.toInt(), data.size)
                val end = minOf(item.readEnd.toInt(), data.size)
                if (end > start) {
                    val written = (end - start).toLong()
                    channel.writeFully(data, start, end)
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
        // DMCA/retention drop and blocklists the NZB or skips it silently.
        var firstTimeout: TimeoutCancellationException? = null
        val data = snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            val breaker = if (breakerActive) circuitBreakerFor(entry) else null
            if (breaker != null && !breaker.tryAcquirePermission()) {
                logger.debug(
                    "Segment <{}> skipping pool[{}] ({}:{}) — circuit breaker is {}",
                    articleId, index, entry.config.host, entry.config.port, breaker.state
                )
                segmentsFallback.increment()
                return@firstNotNullOfOrNull null
            }
            val callStart = System.nanoTime()
            try {
                var result: ByteArray? = null
                withTimeout(timeoutMs.milliseconds) {
                    entry.pool.bodyYenc("<$articleId>", NntpPriority.STREAMING.value).collect { event ->
                        if (event is YencEvent.Body) {
                            result = event.data.toByteArray()
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
                    if (index > 0) {
                        logger.debug("Segment <{}> served by fallback pool[{}]", articleId, index)
                    }
                }
                payload
            } catch (e: ArticleNotFoundException) {
                // Clean 430 — server told us the article is gone. That's not a pool
                // health signal, so release the permit without recording it.
                breaker?.releasePermission()
                if (index < snapshot.size - 1) {
                    logger.debug(
                        "Segment <{}> not found on pool[{}] ({}:{}), trying pool[{}]",
                        articleId, index, entry.config.host, entry.config.port, index + 1
                    )
                    segmentsFallback.increment()
                }
                null
            } catch (e: TimeoutCancellationException) {
                breaker?.onError(
                    System.nanoTime() - callStart,
                    java.util.concurrent.TimeUnit.NANOSECONDS,
                    e,
                )
                if (firstTimeout == null) firstTimeout = e
                // Count every per-pool timeout, regardless of whether a later pool
                // succeeded. The TTFB / body-duration histograms drop timed-out
                // commands entirely (the timer is never stopped on cancellation),
                // so without this counter slow upstreams that hit the segment
                // timeout are invisible in latency percentiles.
                registry.counter(
                    "nzb.segments.body_timeouts",
                    "pool.name", "${entry.config.host}:${entry.config.port}"
                ).increment()
                if (index < snapshot.size - 1) {
                    logger.warn(
                        "Segment <{}> timed out after {}ms on pool[{}] ({}:{}), trying pool[{}]",
                        articleId, timeoutMs,
                        index, entry.config.host, entry.config.port, index + 1
                    )
                    segmentsFallback.increment()
                } else {
                    logger.warn(
                        "Segment <{}> timed out after {}ms on all {} pool(s)",
                        articleId, timeoutMs, snapshot.size
                    )
                }
                null
            } catch (e: NntpException) {
                breaker?.onError(
                    System.nanoTime() - callStart,
                    java.util.concurrent.TimeUnit.NANOSECONDS,
                    e,
                )
                if (index < snapshot.size - 1) {
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
                if (index < snapshot.size - 1) {
                    logger.warn(
                        "Segment <{}> I/O error on pool[{}] ({}:{}) — {}: {}",
                        articleId, index, entry.config.host, entry.config.port,
                        e::class.simpleName, e.message
                    )
                    segmentsFallback.increment()
                }
                null
            }
        }

        sample.stop(segmentDownloadTimer)
        if (data != null) {
            segmentsDownloaded.increment()
            segmentsBytes.increment(data.size.toDouble())
            return data
        }
        segmentsFailed.increment()
        firstTimeout?.let { throw it }
        throw ArticleNotFoundException("Article <$articleId> not found on any pool")
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
