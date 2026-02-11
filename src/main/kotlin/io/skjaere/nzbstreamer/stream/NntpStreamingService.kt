package io.skjaere.nzbstreamer.stream

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.nntp.NntpClient
import io.skjaere.nntp.NntpClientPool
import io.skjaere.nntp.YencEvent
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import java.io.Closeable

class NntpStreamingService(
    private val config: NntpConfig,
    private val scope: CoroutineScope
) : Closeable {
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
     * Returns a Pair of (ByteReadChannel, Job). The Job is the writer coroutine;
     * cancel it to immediately stop all in-flight downloads and free pool connections.
     */
    fun streamSegments(queue: Flow<SegmentQueueItem>): Pair<ByteReadChannel, Job> {
        val writerJob = scope.writer(autoFlush = false) {
            val items = queue.toList()
            if (items.isEmpty()) return@writer

            // Use a sliding window of pre-fetched downloads to avoid starving
            // the connection pool. At most `windowSize` downloads run concurrently.
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

            // Pre-fill the window
            var nextToSubmit = minOf(windowSize, items.size)
            for (i in 0 until nextToSubmit) {
                submitDownload(i)
            }

            // Write results in order, sliding the window forward
            try {
                for ((index, item) in items.withIndex()) {
                    val data = deferreds.remove(index)!!.await()
                    jobs.remove(index)

                    // Ktor 3.x writeFully uses (ByteArray, startIndex, endIndex), not (offset, length)
                    val start = minOf(item.readStart.toInt(), data.size)
                    val end = minOf(item.readEnd.toInt(), data.size)
                    if (end > start) {
                        channel.writeFully(data, start, end)
                    }
                    channel.flush()

                    // Slide window: submit next download
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
        return Pair(writerJob.channel, writerJob.job)
    }

    fun streamFirstSegment(articleId: String): Pair<YencHeaders, ByteReadChannel> {
        val headersDeferred = CompletableDeferred<YencHeaders>()
        val channel = scope.writer(autoFlush = false) {
            pool.bodyYenc("<$articleId>").collect { event ->
                when (event) {
                    is YencEvent.Headers -> headersDeferred.complete(event.yencHeaders)
                    is YencEvent.Body -> {
                        val data = event.data.toByteArray()
                        channel.writeFully(data)
                        channel.flush()
                    }
                }
            }
        }.channel
        return Pair(runBlocking { headersDeferred.await() }, channel)
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
    }
}
