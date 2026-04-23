package io.skjaere.nzbstreamer.stream

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
import io.skjaere.nntp.StatResult
import io.skjaere.nntp.YencEvent
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class NntpStreamingService(
    initialConfigs: List<NntpConfig>,
    private val streamingConfig: StreamingConfig = StreamingConfig()
) : Closeable {
    constructor(config: NntpConfig) : this(listOf(config))

    private data class PoolEntry(val config: NntpConfig, val pool: NntpClientPool)

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val logger = LoggerFactory.getLogger(NntpStreamingService::class.java)
    private val pools = mutableListOf<PoolEntry>()
    private lateinit var selectorManager: SelectorManager
    private val poolLock = ReentrantReadWriteLock()

    private val initialConfigs = initialConfigs.toList()

    private val registry = Metrics.globalRegistry
    private val segmentsDownloaded = registry.counter("nzb.segments.downloaded")
    private val segmentsBytes = registry.counter("nzb.segments.bytes")
    private val segmentDownloadTimer = registry.timer("nzb.segments.download.duration")
    private val segmentsFailed = registry.counter("nzb.segments.failed")
    private val segmentsFallback = registry.counter("nzb.segments.fallback")
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

    /**
     * Tries [block] on each pool in order. On [ArticleNotFoundException], falls back to the next pool.
     * Throws the last exception if all pools fail.
     */
    private suspend fun <T> withFallback(
        logPrefix: String,
        block: suspend (pool: NntpClientPool) -> T
    ): T {
        val snapshot = poolLock.read { pools.toList() }
        var lastException: ArticleNotFoundException? = null
        return snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            try {
                block(entry.pool)
            } catch (e: ArticleNotFoundException) {
                lastException = e
                if (index < snapshot.size - 1) {
                    logger.debug(
                        "{} not found on pool[{}] ({}:{}), trying pool[{}]",
                        logPrefix, index, entry.config.host, entry.config.port, index + 1
                    )
                    segmentsFallback.increment()
                }
                null
            }
        } ?: throw lastException!!
    }

    suspend fun <T> withClient(
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        block: suspend (io.skjaere.nntp.NntpClient) -> T
    ): T = withFallback("Article") { pool -> pool.withClient(priority.value, block) }

    suspend fun statAcrossPools(articleId: String): StatResult {
        val snapshot = poolLock.read { pools.toList() }
        return snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            val result = entry.pool.withClient(NntpPriority.HEALTH_CHECK.value) { it.stat(articleId) }
            when (result) {
                is StatResult.Found -> result
                is StatResult.NotFound -> {
                    if (index < snapshot.size - 1) {
                        logger.debug(
                            "STAT {} not found on pool[{}] ({}:{}), trying pool[{}]",
                            articleId, index, entry.config.host, entry.config.port, index + 1
                        )
                    }
                    null
                }
            }
        } ?: StatResult.NotFound(430, "Not found on any pool")
    }

    /**
     * Streams segments concurrently and passes the resulting [ByteReadChannel] to [consume].
     * Uses structured concurrency via [coroutineScope] — exceptions from segment downloads
     * propagate directly to the caller, and cancellation of the caller cancels all in-flight
     * downloads automatically.
     */
    suspend fun streamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int = streamingConfig.concurrency,
        readAheadSegments: Int = streamingConfig.readAheadSegments,
        name: String = "unknown",
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val bitrate = acquireBitrateState(name)
        activeStreams.incrementAndGet()
        try {
            coroutineScope {
                val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, bitrate)
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
        concurrency: Int = streamingConfig.concurrency,
        readAheadSegments: Int = streamingConfig.readAheadSegments,
        name: String = "unknown"
    ): WriterJob {
        val bitrate = acquireBitrateState(name)
        val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, bitrate)
        writerJob.job.invokeOnCompletion { releaseBitrateState(name) }
        return writerJob
    }

    private suspend fun launchStreamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int,
        readAheadSegments: Int,
        bitrate: BitrateState
    ): WriterJob {
        val callerScope = CoroutineScope(currentCoroutineContext())
        return callerScope.writer(autoFlush = false) {
            val items = queue.toList()
            if (items.isEmpty()) return@writer

            val downloadSemaphore = Semaphore(concurrency)

            @OptIn(ExperimentalCoroutinesApi::class)
            produce(capacity = readAheadSegments) {
                items.forEach { item ->
                    downloadSemaphore.acquire()
                    val deferred = async {
                        try {
                            downloadSegment(item.segment.articleId)
                        } finally {
                            downloadSemaphore.release()
                        }
                    }
                    send(item to deferred)
                }
            }.consumeEach { (item, deferred) ->
                val data = deferred.await()

                val start = minOf(item.readStart.toInt(), data.size)
                val end = minOf(item.readEnd.toInt(), data.size)
                if (end > start) {
                    val written = (end - start).toLong()
                    channel.writeFully(data, start, end)
                    bitrate.addBytes(written)
                }
            }
        }
    }

    private suspend fun downloadSegment(articleId: String): ByteArray {
        val sample = Timer.start(registry)
        val snapshot = poolLock.read { pools.toList() }
        val data = snapshot.withIndex().firstNotNullOfOrNull { (index, entry) ->
            try {
                var result: ByteArray? = null
                entry.pool.bodyYenc("<$articleId>", NntpPriority.STREAMING.value).collect { event ->
                    if (event is YencEvent.Body) {
                        result = event.data.toByteArray()
                    }
                }
                result?.also {
                    if (index > 0) {
                        logger.debug("Segment <{}> served by fallback pool[{}]", articleId, index)
                    }
                }
            } catch (e: ArticleNotFoundException) {
                if (index < snapshot.size - 1) {
                    logger.debug(
                        "Segment <{}> not found on pool[{}] ({}:{}), trying pool[{}]",
                        articleId, index, entry.config.host, entry.config.port, index + 1
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
        throw ArticleNotFoundException("Article <$articleId> not found on any pool")
    }

    override fun close() {
        poolLock.read {
            pools.forEach { it.pool.close() }
        }
        scope.cancel()
    }

}
