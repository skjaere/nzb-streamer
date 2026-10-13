package io.skjaere.nzbstreamer.seekable

import io.skjaere.compressionutils.SeekableInputStream
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import kotlin.math.abs

/**
 * A [SeekableInputStream] for reading archive headers: it fetches exactly the segments that are
 * read, keeps the last few, and never streams ahead. [NntpSeekableInputStream] opens a streaming
 * pipeline with read-ahead on every seek, which is right for playback but wastes several
 * segments per jump when a parser hops from header to header.
 *
 * Header walks over scene RAR volumes jump by a constant stride (every volume holds the same
 * amount of data). When two consecutive forward seeks have the same length, the segments at
 * the next few strides are fetched in parallel before the parser asks for them. Every guess
 * costs a whole article (~750 KB), so the look-ahead starts at [INITIAL_DEPTH], doubles while
 * the pattern holds (up to [prefetchDepth]) and drops back when it breaks.
 *
 * The few KB around every read are also kept after their segment leaves the cache, so a
 * second pass over the same headers (nested archives read inner volume starts twice) costs
 * nothing.
 */
class SegmentRandomAccessStream(
    private val layout: List<SegmentQueueItem>,
    private val fetch: suspend (articleId: String) -> ByteArray,
    private val prefetchDepth: Int = 32,
    private val maxCached: Int = 64,
    parallelFetches: Int = 8,
) : SeekableInputStream {

    constructor(nzb: NzbDocument, streamingService: NntpStreamingService) :
        this(SegmentQueueService.segmentLayout(nzb), streamingService::fetchSegment)

    private val logger = LoggerFactory.getLogger(SegmentRandomAccessStream::class.java)
    private val totalSize = layout.lastOrNull()?.let { it.globalOffset + it.segmentDecodedSize } ?: 0L
    private val permits = Semaphore(parallelFetches)
    private var scope: CoroutineScope? = null
    private var position = 0L
    private var lastSeekTarget: Long? = null
    private var lastStride: Long? = null
    private var depth = minOf(INITIAL_DEPTH, prefetchDepth)

    /** Small slices of segments that were read, by global position (see class doc). */
    private val retained = java.util.TreeMap<Long, ByteArray>()
    private var retainedBytes = 0L

    /** Segment index → its bytes (fetched or in flight), oldest first. */
    private val cache = object : LinkedHashMap<Int, Deferred<ByteArray>>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Int, Deferred<ByteArray>>) = size > maxCached
    }

    var segmentsFetched: Int = 0
        private set
    private var retainedHits = 0L

    override suspend fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        if (position >= totalSize) return -1
        // Distances are compared as Long: a jump past 2 GiB must not wrap into a slice or segment.
        retained.floorEntry(position)?.let { (start, slice) ->
            if (position - start < slice.size) {
                val within = (position - start).toInt()
                val n = minOf(length, slice.size - within)
                System.arraycopy(slice, within, buffer, offset, n)
                position += n
                retainedHits += n
                return n
            }
        }
        val index = indexOf(position)
        val item = layout[index]
        val bytes = segment(index).await()
        if (position - item.globalOffset >= bytes.size) return -1
        val within = (position - item.globalOffset).toInt()
        val n = minOf(length, bytes.size - within)
        retain(item.globalOffset, bytes, within)
        System.arraycopy(bytes, within, buffer, offset, n)
        position += n
        return n
    }

    /** Keeps [RETAIN_BYTES] from [within] (headers are small; a read rarely needs more). */
    private fun retain(segmentStart: Long, bytes: ByteArray, within: Int) {
        val start = segmentStart + within
        if (retained.floorEntry(start)?.let { (s, b) -> start < s + b.size } == true) return
        val slice = bytes.copyOfRange(within, minOf(bytes.size, within + RETAIN_BYTES))
        retained[start] = slice
        retainedBytes += slice.size
        while (retainedBytes > MAX_RETAINED_BYTES) {
            retained.pollFirstEntry()?.let { retainedBytes -= it.value.size } ?: break
        }
    }

    override suspend fun read(): Int {
        val one = ByteArray(1)
        return if (read(one, 0, 1) == 1) one[0].toInt() and 0xFF else -1
    }

    override suspend fun seek(position: Long) {
        val previous = lastSeekTarget
        val stride = previous?.let { position - it }
        // Two equal forward jumps, each beyond one segment: assume the pattern continues.
        val patternHolds = stride != null && stride > segmentSizeAt(position) && lastStride?.let { abs(it - stride) <= tolerance(stride) } == true
        if (patternHolds) {
            prefetch((1..depth).map { position + stride!! * it })
            depth = minOf(depth * 2, prefetchDepth)
        } else {
            depth = minOf(INITIAL_DEPTH, prefetchDepth)
        }
        lastStride = stride
        lastSeekTarget = position
        this.position = position
    }

    /** Starts fetching the segments holding [positions] (and their successors when a read may straddle). */
    suspend fun prefetch(positions: Collection<Long>) {
        positions.filter { it in 0 until totalSize }.map(::indexOf).distinct().forEach { segment(it) }
    }

    override fun position(): Long = position

    override fun size(): Long = totalSize

    override fun close() {
        logger.debug("Random access over {} segments: fetched {} (read {} bytes from retained slices)", layout.size, segmentsFetched, retainedHits)
        scope?.cancel()
        scope = null
        cache.clear()
        retained.clear()
        retainedBytes = 0
    }

    private suspend fun segment(index: Int): Deferred<ByteArray> {
        cache[index]?.let { return it }
        val s = scope ?: CoroutineScope(currentCoroutineContext().minusKey(Job) + SupervisorJob()).also { scope = it }
        val articleId = layout[index].segment.articleId
        val deferred = s.async(start = CoroutineStart.DEFAULT) {
            permits.withPermit { fetch(articleId) }
        }
        segmentsFetched++
        cache[index] = deferred
        return deferred
    }

    private fun segmentSizeAt(position: Long): Long = layout.getOrNull(indexOf(position.coerceIn(0, maxOf(totalSize - 1, 0))))?.segmentDecodedSize ?: 0

    private fun indexOf(position: Long): Int {
        var lo = 0
        var hi = layout.size - 1
        while (lo < hi) {
            val mid = (lo + hi + 1) ushr 1
            if (layout[mid].globalOffset <= position) lo = mid else hi = mid - 1
        }
        return lo
    }

    /** Header sizes vary by a few bytes between volumes; unrelated jumps differ by far more. */
    private fun tolerance(stride: Long): Long = minOf(64L * 1024, stride / 100)

    private companion object {
        const val INITIAL_DEPTH = 4
        const val RETAIN_BYTES = 32 * 1024
        const val MAX_RETAINED_BYTES = 16L * 1024 * 1024
    }
}
