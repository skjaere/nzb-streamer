package io.skjaere.nzbstreamer.seekable

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.discard
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readByte
import io.micrometer.core.instrument.Metrics
import io.skjaere.compressionutils.SeekableInputStream
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.io.EOFException
import org.slf4j.LoggerFactory

private const val FORWARD_THRESHOLD_BYTES = 102400L

/**
 * The decoded bytes of [nzb] as one seekable stream, downloaded from the current position on as
 * they're read. A read starts that download as a child of the reading coroutine: [close], or a seek
 * that reopens it, ends it, and until then the reading coroutine's job doesn't complete. [close]
 * doesn't wait for the download to stop; the reading coroutine's job completes once it has. A
 * download that fails makes [read] throw why. Downloads never go past [end] (the whole of [nzb] by
 * default): reads there end the stream.
 */
class NntpSeekableInputStream @JvmOverloads constructor(
    private val nzb: NzbDocument,
    private val streamingService: NntpStreamingService,
    end: Long = Long.MAX_VALUE,
) : SeekableInputStream {
    private val logger = LoggerFactory.getLogger(NntpSeekableInputStream::class.java)
    private val forwardThresholdBytes: Long = FORWARD_THRESHOLD_BYTES
    private val totalSize: Long = SegmentQueueService.getTotalDecodedSize(nzb)
    private val end: Long = minOf(end, totalSize)
    private var currentPosition: Long = 0
    private var currentJob: Job? = null
    private var currentChannel: ByteReadChannel? = null

    private val skippedBytes = Metrics.globalRegistry.counter("nzb.stream.skipped_bytes")

    override suspend fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (currentPosition >= totalSize) return -1
        ensureChannel()
        val channel = currentChannel ?: return -1
        val bytesRead = channel.readAvailable(buffer, offset, length)
        if (bytesRead > 0) {
            currentPosition += bytesRead
        } else if (bytesRead == -1) {
            // The download stopped early: say why rather than end the stream.
            channel.closedCause?.let { throw it }
        }
        return bytesRead
    }

    override suspend fun read(): Int {
        if (currentPosition >= totalSize) return -1
        ensureChannel()
        val channel = currentChannel ?: return -1
        return try {
            val b = channel.readByte().toInt() and 0xFF
            currentPosition++
            b
        } catch (_: EOFException) {
            channel.closedCause?.let { throw it }
            -1
        }
    }

    override suspend fun seek(position: Long) {
        if (position == currentPosition) return

        val forward = position - currentPosition
        if (forward in 1..forwardThresholdBytes && currentChannel != null) {
            logger.debug("Forward skip {} bytes from {} to {}", forward, currentPosition, position)
            currentChannel!!.discard(forward)
            skippedBytes.increment(forward.toDouble())
            currentPosition = position
        } else {
            logger.debug("Seek from {} to {} (reopening stream)", currentPosition, position)
            closeStream()
            currentPosition = position
        }
    }

    override fun position(): Long = currentPosition

    override fun size(): Long = totalSize

    // Cancels without joining: the download runs in the reading coroutine's context, so blocking
    // here until it stops deadlocks a reader with one thread (Android's main thread), and the
    // reading coroutine's job waits for its child anyway.
    override fun close() {
        val job = currentJob
        currentJob = null
        currentChannel = null
        job?.cancel()
    }

    private suspend fun ensureChannel() {
        if (currentChannel == null) {
            openChannel(currentPosition)
        }
    }

    private suspend fun openChannel(position: Long) {
        val remaining = end - position
        if (remaining <= 0) return

        val queue = SegmentQueueService.createRangeQueue(nzb, position, remaining)
        val channelReady = CompletableDeferred<ByteReadChannel>()

        // A child of the reading coroutine, so it's cancelled with it and its failures go to no
        // exception handler: they reach the reader through the channel.
        currentJob = CoroutineScope(currentCoroutineContext()).launch {
            streamingService.streamSegments(queue, name = "seek") { channel ->
                channelReady.complete(channel)
                suspendCancellableCoroutine<Unit> { }
            }
        }
        currentChannel = channelReady.await()
    }

    private suspend fun closeStream() {
        val job = currentJob
        currentJob = null
        currentChannel = null
        job?.cancelAndJoin()
    }
}
