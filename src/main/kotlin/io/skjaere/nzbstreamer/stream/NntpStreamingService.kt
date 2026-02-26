package io.skjaere.nzbstreamer.stream

import io.ktor.network.selector.SelectorManager
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.toByteArray
import io.ktor.utils.io.writeFully
import io.ktor.utils.io.writer
import io.skjaere.nntp.NntpClient
import io.skjaere.nntp.NntpClientPool
import io.skjaere.nntp.YencEvent
import io.skjaere.nzbstreamer.config.NntpConfig
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

class NntpStreamingService(
    private val config: NntpConfig
) : Closeable {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val logger = LoggerFactory.getLogger(NntpStreamingService::class.java)
    private lateinit var pool: NntpClientPool

    suspend fun connect() {
        val selectorManager = SelectorManager(Dispatchers.IO)
        pool = NntpClientPool(
            host = config.host,
            port = config.port,
            selectorManager = selectorManager,
            useTls = config.useTls,
            username = config.username.ifEmpty { null },
            password = config.password.ifEmpty { null },
            maxConnections = config.maxConnections,
            scope = scope
        )
        pool.connect()
        logger.info(
            "NNTP connection pool initialized with {} connections, per-stream concurrency={}",
            config.maxConnections, config.concurrency
        )
    }

    suspend fun <T> withClient(
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        block: suspend (NntpClient) -> T
    ): T {
        return pool.withClient(priority.value, block)
    }

    /**
     * Streams segments concurrently and passes the resulting [ByteReadChannel] to [consume].
     * Uses structured concurrency via [coroutineScope] — exceptions from segment downloads
     * propagate directly to the caller, and cancellation of the caller cancels all in-flight
     * downloads automatically.
     */
    suspend fun streamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int = config.concurrency,
        readAheadSegments: Int = config.readAheadSegments,
        consume: suspend (ByteReadChannel) -> Unit
    ) = coroutineScope {
        val writerJob = launchStreamSegments(queue, concurrency, readAheadSegments)
        try {
            consume(writerJob.channel)
        } finally {
            writerJob.job.cancel()
        }
    }

    /**
     * Launches segment streaming as a child of the caller's coroutine scope, returning a [WriterJob]
     * whose channel can be read independently. The caller manages the job's lifecycle.
     */
    suspend fun launchStreamSegments(
        queue: Flow<SegmentQueueItem>,
        concurrency: Int = config.concurrency,
        readAheadSegments: Int = config.readAheadSegments
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
                }
            }
        }
    }

    private suspend fun downloadSegment(articleId: String): ByteArray {
        var result: ByteArray? = null
        pool.bodyYenc("<$articleId>", NntpPriority.STREAMING.value).collect { event ->
            if (event is YencEvent.Body) {
                result = event.data.toByteArray()
            }
        }
        return result ?: throw IllegalStateException("No body received for <$articleId>")
    }

    override fun close() {
        pool.close()
        scope.cancel()
    }
}
