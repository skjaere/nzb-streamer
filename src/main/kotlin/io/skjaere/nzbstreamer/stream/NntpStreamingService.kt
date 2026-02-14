package io.skjaere.nzbstreamer.stream

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.nntp.NntpClient
import io.skjaere.nntp.NntpClientPool
import io.skjaere.nntp.YencEvent
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import kotlinx.coroutines.*
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
            maxConnections = config.concurrency,
            scope = scope
        )
        pool.connect()
        logger.info("NNTP connection pool initialized with {} connections", config.concurrency)
    }

    suspend fun <T> withClient(block: suspend (NntpClient) -> T): T {
        return pool.withClient(block)
    }

    /**
     * Streams segments concurrently and passes the resulting [ByteReadChannel] to [consume].
     * The writer coroutine's lifecycle is tied to the calling coroutine via [coroutineScope] —
     * if the caller is cancelled, all in-flight downloads are cancelled automatically.
     */
    suspend fun streamSegments(
        queue: Flow<SegmentQueueItem>,
        readAheadSegments: Int = config.readAheadSegments,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val writerJob = launchSegmentWriter(queue, readAheadSegments)
        try {
            consume(writerJob.channel)
        } finally {
            writerJob.job.cancel()
        }
    }

    private fun launchSegmentWriter(
        queue: Flow<SegmentQueueItem>,
        readAheadSegments: Int
    ): WriterJob {
        return scope.writer(autoFlush = false) {
            val items = queue.toList()
            if (items.isEmpty()) return@writer

            val deferreds = Array(items.size) { CompletableDeferred<ByteArray>() }
            val semaphore = Semaphore(readAheadSegments)

            val downloadJob = launch {
                for ((i, item) in items.withIndex()) {
                    semaphore.acquire()
                    launch {
                        try {
                            val data = downloadSegment(item.segment.articleId)
                            deferreds[i].complete(data)
                        } catch (e: Exception) {
                            deferreds[i].completeExceptionally(e)
                        }
                    }
                }
            }

            try {
                for ((index, item) in items.withIndex()) {
                    val data = deferreds[index].await()

                    val start = minOf(item.readStart.toInt(), data.size)
                    val end = minOf(item.readEnd.toInt(), data.size)
                    if (end > start) {
                        channel.writeFully(data, start, end)
                    }
                    channel.flush()

                    semaphore.release()
                }
            } finally {
                downloadJob.cancel()
            }
        }
    }

    private suspend fun downloadSegment(articleId: String): ByteArray {
        var result: ByteArray? = null
        pool.bodyYenc("<$articleId>").collect { event ->
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
