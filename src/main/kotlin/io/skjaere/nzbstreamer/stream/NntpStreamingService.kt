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
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val writerJob = launchSegmentWriter(queue)
        try {
            consume(writerJob.channel)
        } finally {
            writerJob.job.cancel()
        }
    }

    private fun launchSegmentWriter(queue: Flow<SegmentQueueItem>): WriterJob {
        return scope.writer(autoFlush = false) {
            val items = queue.toList()
            if (items.isEmpty()) return@writer

            val windowSize = config.concurrency
            val deferreds = HashMap<Int, CompletableDeferred<ByteArray>>()
            val jobs = HashMap<Int, Job>()

            fun submitDownload(index: Int) {
                val item = items[index]
                val deferred = CompletableDeferred<ByteArray>()
                deferreds[index] = deferred
                jobs[index] = launch {
                    try {
                        val data = downloadSegment(item.segment.articleId)
                        deferred.complete(data)
                    } catch (e: Exception) {
                        deferred.completeExceptionally(e)
                    }
                }
            }

            var nextToSubmit = minOf(windowSize, items.size)
            for (i in 0 until nextToSubmit) {
                submitDownload(i)
            }

            try {
                for ((index, item) in items.withIndex()) {
                    val data = deferreds.remove(index)!!.await()
                    jobs.remove(index)

                    val start = minOf(item.readStart.toInt(), data.size)
                    val end = minOf(item.readEnd.toInt(), data.size)
                    if (end > start) {
                        channel.writeFully(data, start, end)
                    }
                    channel.flush()

                    if (nextToSubmit < items.size) {
                        submitDownload(nextToSubmit)
                        nextToSubmit++
                    }
                }
            } catch (e: Exception) {
                jobs.values.forEach { it.cancel() }
                throw e
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
