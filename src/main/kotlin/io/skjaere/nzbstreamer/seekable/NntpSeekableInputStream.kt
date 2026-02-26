package io.skjaere.nzbstreamer.seekable

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.discard
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readByte
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
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.io.EOFException
import org.slf4j.LoggerFactory

class NntpSeekableInputStream(
    private val nzb: NzbDocument,
    private val streamingService: NntpStreamingService,
    private val forwardThresholdBytes: Long = 102400L
) : SeekableInputStream {
    private val logger = LoggerFactory.getLogger(NntpSeekableInputStream::class.java)
    private val totalSize: Long = SegmentQueueService.getTotalDecodedSize(nzb)
    private var currentPosition: Long = 0
    private var currentJob: Job? = null
    private var currentChannel: ByteReadChannel? = null

    override suspend fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (currentPosition >= totalSize) return -1
        ensureChannel()
        val channel = currentChannel ?: return -1
        val bytesRead = channel.readAvailable(buffer, offset, length)
        if (bytesRead > 0) {
            currentPosition += bytesRead
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
            -1
        }
    }

    override suspend fun seek(position: Long) {
        if (position == currentPosition) return

        val forward = position - currentPosition
        if (forward in 1..forwardThresholdBytes && currentChannel != null) {
            logger.debug("Forward skip {} bytes from {} to {}", forward, currentPosition, position)
            currentChannel!!.discard(forward)
            currentPosition = position
        } else {
            logger.debug("Seek from {} to {} (reopening stream)", currentPosition, position)
            closeStream()
            currentPosition = position
        }
    }

    override fun position(): Long = currentPosition

    override fun size(): Long = totalSize

    override fun close() { // TODO: close() should be suspendable
        val job = currentJob
        currentJob = null
        currentChannel = null
        if (job != null) {
            runBlocking { job.cancelAndJoin() }
        }
    }

    private suspend fun ensureChannel() {
        if (currentChannel == null) {
            openChannel(currentPosition)
        }
    }

    private suspend fun openChannel(position: Long) {
        val remaining = totalSize - position
        if (remaining <= 0) return

        val queue = SegmentQueueService.createRangeQueue(nzb, position, remaining)
        val channelReady = CompletableDeferred<ByteReadChannel>()

        val scope = CoroutineScope(currentCoroutineContext() + Job())
        currentJob = scope.launch {
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
