package io.skjaere.nzbstreamer.seekable

import io.ktor.utils.io.*
import io.ktor.utils.io.jvm.javaio.*
import io.skjaere.compressionutils.SeekableInputStream
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.io.InputStream

class NntpSeekableInputStream(
    private val nzb: NzbDocument,
    private val streamingService: NntpStreamingService,
    private val forwardThresholdBytes: Long = 102400L
) : SeekableInputStream {
    private val logger = LoggerFactory.getLogger(NntpSeekableInputStream::class.java)
    private val totalSize: Long = SegmentQueueService.getTotalDecodedSize(nzb)
    private var currentPosition: Long = 0
    private var currentChannel: ByteReadChannel? = null
    private var currentWriterJob: Job? = null
    private var currentStream: InputStream? = null

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (currentPosition >= totalSize) return -1
        ensureStream()
        val stream = currentStream ?: return -1
        val bytesRead = stream.read(buffer, offset, length)
        if (bytesRead > 0) {
            currentPosition += bytesRead
        }
        return bytesRead
    }

    override fun read(): Int {
        if (currentPosition >= totalSize) return -1
        ensureStream()
        val stream = currentStream ?: return -1
        val b = stream.read()
        if (b >= 0) {
            currentPosition++
        }
        return b
    }

    override fun seek(position: Long) {
        if (position == currentPosition) return

        val forward = position - currentPosition
        if (forward in 1..forwardThresholdBytes && currentStream != null) {
            // Forward seek within threshold: skip bytes
            logger.debug("Forward skip {} bytes from {} to {}", forward, currentPosition, position)
            skipBytes(forward)
        } else {
            // Backward or large forward: reopen stream from position
            logger.debug("Seek from {} to {} (reopening stream)", currentPosition, position)
            closeStream()
            currentPosition = position
        }
    }

    override fun position(): Long = currentPosition

    override fun size(): Long = totalSize

    override fun close() {
        closeStream()
    }

    private fun ensureStream() {
        if (currentStream == null) {
            openStream(currentPosition)
        }
    }

    private fun openStream(position: Long) {
        val remaining = totalSize - position
        if (remaining <= 0) return

        val queue = SegmentQueueService.createRangeQueue(nzb, position, remaining)
        val (channel, job) = streamingService.streamSegments(queue)
        currentChannel = channel
        currentWriterJob = job
        currentStream = channel.toInputStream()
    }

    private fun skipBytes(count: Long) {
        val buf = ByteArray(minOf(count, 8192).toInt())
        var remaining = count
        while (remaining > 0) {
            val toRead = minOf(remaining.toInt(), buf.size)
            val bytesRead = currentStream!!.read(buf, 0, toRead)
            if (bytesRead < 0) break
            remaining -= bytesRead
            currentPosition += bytesRead
        }
    }

    private fun closeStream() {
        val job = currentWriterJob
        currentWriterJob = null
        currentChannel?.cancel()
        currentChannel = null
        currentStream?.close()
        currentStream = null
        if (job != null) {
            runBlocking { job.cancelAndJoin() }
        }
    }
}
