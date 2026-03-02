package io.skjaere.nzbstreamer.stream

import io.ktor.network.selector.SelectorManager
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.toByteArray
import io.ktor.utils.io.writeFully
import io.ktor.utils.io.writer
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
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

class NntpStreamingService(
    private val configs: List<NntpConfig>,
    private val streamingConfig: StreamingConfig = StreamingConfig()
) : Closeable {
    constructor(config: NntpConfig) : this(listOf(config))

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val logger = LoggerFactory.getLogger(NntpStreamingService::class.java)
    private lateinit var pools: List<NntpClientPool>

    private val registry = Metrics.globalRegistry
    private val segmentsDownloaded = registry.counter("nzb.segments.downloaded")
    private val segmentsBytes = registry.counter("nzb.segments.bytes")
    private val segmentDownloadTimer = registry.timer("nzb.segments.download.duration")
    private val segmentsFailed = registry.counter("nzb.segments.failed")
    private val segmentsFallback = registry.counter("nzb.segments.fallback")
    private val activeStreams = AtomicLong(0).also { registry.gauge("nzb.streams.active", it) }
    private val streamCounters = ConcurrentHashMap<String, Counter>()

    suspend fun connect() {
        val selectorManager = SelectorManager(Dispatchers.IO)
        pools = configs.mapIndexed { index, config ->
            NntpClientPool(
                host = config.host,
                port = config.port,
                selectorManager = selectorManager,
                useTls = config.useTls,
                username = config.username.ifEmpty { null },
                password = config.password.ifEmpty { null },
                maxConnections = config.maxConnections,
                scope = scope
            ).also {
                logger.info(
                    "NNTP pool[{}] initialized: {}:{} maxConnections={}",
                    index, config.host, config.port, config.maxConnections
                )
            }
        }
    }

    /**
     * Tries [block] on each pool in order. On [ArticleNotFoundException], falls back to the next pool.
     * Throws the last exception if all pools fail.
     */
    private suspend fun <T> withFallback(
        logPrefix: String,
        block: suspend (pool: NntpClientPool) -> T
    ): T {
        var lastException: ArticleNotFoundException? = null
        return pools.withIndex().firstNotNullOfOrNull { (index, pool) ->
            try {
                block(pool)
            } catch (e: ArticleNotFoundException) {
                lastException = e
                if (index < pools.size - 1) {
                    logger.debug(
                        "{} not found on pool[{}] ({}:{}), trying pool[{}]",
                        logPrefix, index, configs[index].host, configs[index].port, index + 1
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
        return pools.withIndex().firstNotNullOfOrNull { (index, pool) ->
            val result = pool.withClient(NntpPriority.HEALTH_CHECK.value) { it.stat(articleId) }
            when (result) {
                is StatResult.Found -> result
                is StatResult.NotFound -> {
                    if (index < pools.size - 1) {
                        logger.debug(
                            "STAT {} not found on pool[{}] ({}:{}), trying pool[{}]",
                            articleId, index, configs[index].host, configs[index].port, index + 1
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
        val counter = streamCounters.computeIfAbsent(name) {
            registry.counter("nzb.streams.bytes", "name", name)
        }
        activeStreams.incrementAndGet()
        try {
            coroutineScope {
                val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments, counter)
                try {
                    consume(writerJob.channel)
                } finally {
                    writerJob.job.cancel()
                }
            }
        } finally {
            activeStreams.decrementAndGet()
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
        val counter = streamCounters.computeIfAbsent(name) {
            registry.counter("nzb.streams.bytes", "name", name)
        }
        return launchStreamSegments(queue, concurrency, readAheadSegments, counter)
    }

    private suspend fun launchStreamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int,
        readAheadSegments: Int,
        counter: Counter
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
                    channel.writeFully(data, start, end)
                    counter.increment((end - start).toDouble())
                }
            }
        }
    }

    private suspend fun downloadSegment(articleId: String): ByteArray {
        val sample = Timer.start(registry)
        val data = pools.withIndex().firstNotNullOfOrNull { (index, pool) ->
            try {
                var result: ByteArray? = null
                pool.bodyYenc("<$articleId>", NntpPriority.STREAMING.value).collect { event ->
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
                if (index < pools.size - 1) {
                    logger.debug(
                        "Segment <{}> not found on pool[{}] ({}:{}), trying pool[{}]",
                        articleId, index, configs[index].host, configs[index].port, index + 1
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
        if (::pools.isInitialized) {
            pools.forEach { it.close() }
        }
        scope.cancel()
    }
}
