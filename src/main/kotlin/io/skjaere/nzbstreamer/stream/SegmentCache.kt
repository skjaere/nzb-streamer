package io.skjaere.nzbstreamer.stream

import io.micrometer.core.instrument.Metrics
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.getLastModifiedTime
import kotlin.io.path.readBytes
import kotlin.io.path.readText
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText

/**
 * Persistent segment cache backed by per-segment files plus an `index.json` sidecar.
 *
 * The sidecar records each entry's size and last-access timestamp, so the cache survives
 * restarts and supports global LRU eviction without scanning every file's stat info on
 * the hot path. Last access is wall-clock time in milliseconds, which goes on across a reboot
 * (System.nanoTime starts again from boot), each stamp later than every one before it.
 * Concurrent fetches for the same article-id are deduplicated via a single-flight map keyed
 * by article-id.
 *
 * The cache is best-effort: failures to read/write the disk degrade silently to a
 * passthrough to the [fetch] lambda. Bytes returned to callers are always correct
 * (a corrupt cache entry is detected via size mismatch and re-fetched).
 */
class SegmentCache(private val config: SegmentCacheConfig) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(SegmentCache::class.java)

    /**
     * [lastAccessMillis] is [UNSTAMPED] in an index written by a version that stamped entries with
     * System.nanoTime (as lastAccessNanos), meaningless after a reboot: such an entry is stamped
     * with its file's modification time when loaded.
     */
    @Serializable
    private data class Entry(val articleId: String, val size: Long, var lastAccessMillis: Long = UNSTAMPED)

    @Serializable
    private data class IndexFile(val entries: List<Entry>)

    private val json = Json { ignoreUnknownKeys = true }
    private val indexLock = Mutex()
    private val index = mutableMapOf<String, Entry>()
    // ConcurrentHashMap.putIfAbsent gives us atomic single-flight registration without
    // a coarse-grained mutex around the entire claim flow. Prior implementation used a
    // global `singleflightLock` mutex whose critical section included a disk-I/O recheck
    // (get → readEntry → withContext(Dispatchers.IO) { path.readBytes() }) — that
    // serialized every concurrent fetch across the cache, observed in prod as 51-199s
    // stream-startup stalls when Plex fired its parallel range probes.
    private val inFlight = java.util.concurrent.ConcurrentHashMap<String, CompletableDeferred<ByteArray>>()
    private val totalBytes = AtomicLong(0)
    private val pendingWrites = AtomicLong(0)
    // The last stamp given: the next is the wall clock's time or one past this, whichever is
    // later, so a clock set back never stamps an access before an earlier one.
    private val lastStamp = AtomicLong(0)

    private val indexPath: Path = config.cacheDir.resolve("index.json")
    private val evictThreshold: Long = (config.maxBytes * config.evictThresholdRatio).toLong()

    init {
        require(config.maxBytes > 0) { "maxBytes must be > 0" }
        config.cacheDir.createDirectories()
        loadIndex()

        // Gauges read state on every scrape; AtomicLong satisfies Number, and the
        // index gauge reads .size unsynchronized — fine for a metric.
        val registry = Metrics.globalRegistry
        registry.gauge("nzb.segments.cache.bytes", totalBytes)
        registry.gauge("nzb.segments.cache.max.bytes", AtomicLong(config.maxBytes))
        registry.gauge("nzb.segments.cache.entries", index) { it.size.toDouble() }
    }

    private fun loadIndex() {
        if (!indexPath.exists()) return
        runCatching {
            val parsed = json.decodeFromString(IndexFile.serializer(), indexPath.readText())
            for (e in parsed.entries) {
                val path = filePathFor(e.articleId)
                if (path.exists()) {
                    if (e.lastAccessMillis == UNSTAMPED) e.lastAccessMillis = path.getLastModifiedTime().toMillis()
                    index[e.articleId] = e
                    totalBytes.addAndGet(e.size)
                } else {
                    logger.debug("Index references missing file for {}, dropping", e.articleId)
                }
            }
            lastStamp.set(index.values.maxOfOrNull { it.lastAccessMillis } ?: 0)
            logger.info("Loaded segment cache index: {} entries, {} bytes", index.size, totalBytes.get())
        }.onFailure {
            logger.warn("Failed to load segment cache index, starting empty", it)
            index.clear()
            totalBytes.set(0)
        }
    }

    private suspend fun saveIndex() {
        val snapshot = indexLock.withLock { IndexFile(index.values.map { it.copy() }) }
        withContext(Dispatchers.IO) {
            runCatching { indexPath.writeText(json.encodeToString(IndexFile.serializer(), snapshot)) }
                .onFailure { logger.warn("Failed to persist cache index", it) }
        }
    }

    /** Now, by the wall clock, or just after the last stamp if the clock is behind it. */
    private fun stamp(): Long = lastStamp.updateAndGet { maxOf(it + 1, System.currentTimeMillis()) }

    private fun filePathFor(articleId: String): Path {
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(articleId.toByteArray(Charsets.UTF_8))
        val hex = digest.joinToString("") { "%02x".format(it) }
        return config.cacheDir.resolve(hex.substring(0, 2)).resolve(hex)
    }

    /** Returns cached bytes if present + readable, else null. Updates lastAccess on hit. */
    suspend fun get(articleId: String): ByteArray? {
        val entry = indexLock.withLock { index[articleId] } ?: return null
        return readEntry(articleId, entry)
    }

    private suspend fun readEntry(articleId: String, entry: Entry): ByteArray? {
        val path = filePathFor(articleId)
        return withContext(Dispatchers.IO) {
            runCatching {
                val bytes = path.readBytes()
                if (bytes.size.toLong() != entry.size) {
                    logger.warn(
                        "Cache size mismatch for {}: expected {} got {}, dropping",
                        articleId, entry.size, bytes.size
                    )
                    invalidate(articleId)
                    null
                } else {
                    bytes
                }
            }.getOrElse { e ->
                logger.warn("Failed to read cached segment {}: {}", articleId, e.message)
                invalidate(articleId)
                null
            }
        }?.also {
            indexLock.withLock { index[articleId]?.lastAccessMillis = stamp() }
        }
    }

    private suspend fun invalidate(articleId: String) {
        indexLock.withLock {
            val removed = index.remove(articleId)
            if (removed != null) totalBytes.addAndGet(-removed.size)
        }
        runCatching { filePathFor(articleId).deleteIfExists() }
    }

    /**
     * Returns cached bytes for [articleId], invoking [fetch] on miss and caching the result.
     * Concurrent calls for the same article-id share a single fetch.
     *
     * Single-flight is implemented via `ConcurrentHashMap.putIfAbsent` on [inFlight],
     * so concurrent calls for *different* article-ids never block each other. The
     * leader-side TOCTOU recheck (a concurrent fetch may have completed and removed
     * its own inflight entry between our `get` fast-path and our `putIfAbsent`) runs
     * outside any lock — the worst that happens is one redundant disk read or, very
     * rarely, one redundant NNTP fetch, both correctness-preserving.
     *
     * A leader's failure is its followers' too, but not its cancellation: a follower whose
     * leader is cancelled fetches for itself (or follows the next leader).
     */
    suspend fun getOrFetch(articleId: String, fetch: suspend () -> ByteArray): ByteArray {
        get(articleId)?.let { return it }

        val ourDeferred = CompletableDeferred<ByteArray>()
        val existing = inFlight.putIfAbsent(articleId, ourDeferred)
        if (existing != null) {
            return follow(existing) ?: run {
                // The cancelled leader may not have let go of it yet.
                inFlight.remove(articleId, existing)
                getOrFetch(articleId, fetch)
            }
        }

        return try {
            // Leader-side recheck: a previous leader may have finished and removed
            // itself between our fast-path `get` and our `putIfAbsent`. Outside any
            // lock — other articles' fetches are unaffected by this disk read.
            val fromDisk = get(articleId)
            val bytes = if (fromDisk != null) {
                fromDisk
            } else {
                val fetched = fetch()
                store(articleId, fetched)
                fetched
            }
            ourDeferred.complete(bytes)
            bytes
        } catch (t: Throwable) {
            ourDeferred.completeExceptionally(t)
            throw t
        } finally {
            inFlight.remove(articleId, ourDeferred)
        }
    }

    /** [leader]'s bytes, or null if it was cancelled and we weren't. */
    private suspend fun follow(leader: CompletableDeferred<ByteArray>): ByteArray? = try {
        leader.await()
    } catch (_: CancellationException) {
        // Ours goes on up; the leader's isn't ours.
        currentCoroutineContext().ensureActive()
        null
    }

    private suspend fun store(articleId: String, bytes: ByteArray) {
        val path = filePathFor(articleId)
        // A file written is one the index must know, or it is never evicted: no cancellation
        // between the write and the index update.
        val newTotal = withContext(NonCancellable) {
            val ok = withContext(Dispatchers.IO) {
                runCatching {
                    path.parent.createDirectories()
                    path.writeBytes(bytes)
                    true
                }.getOrElse {
                    logger.warn("Failed to write cache file {}: {}", path, it.message)
                    false
                }
            }
            if (!ok) return@withContext null

            indexLock.withLock {
                index[articleId]?.let { totalBytes.addAndGet(-it.size) }
                index[articleId] = Entry(articleId, bytes.size.toLong(), stamp())
                totalBytes.addAndGet(bytes.size.toLong())
            }
        } ?: return
        if (newTotal > evictThreshold) evict()
        if (pendingWrites.incrementAndGet() % INDEX_FLUSH_INTERVAL == 0L) saveIndex()
    }

    private suspend fun evict() {
        val toRemove = indexLock.withLock {
            if (totalBytes.get() <= config.maxBytes) return@withLock emptyList()
            val sorted = index.values.sortedBy { it.lastAccessMillis }
            val targets = mutableListOf<Entry>()
            var t = totalBytes.get()
            for (cand in sorted) {
                if (t <= config.maxBytes) break
                targets += cand
                t -= cand.size
            }
            for (cand in targets) {
                index.remove(cand.articleId)
                totalBytes.addAndGet(-cand.size)
            }
            targets
        }
        if (toRemove.isEmpty()) return
        // Out of the index already: delete them even if cancelled, or they stay on disk for good.
        withContext(NonCancellable + Dispatchers.IO) {
            for (cand in toRemove) {
                runCatching { filePathFor(cand.articleId).deleteIfExists() }
            }
        }
        logger.debug("Evicted {} segments, totalBytes now {}", toRemove.size, totalBytes.get())
    }

    fun stats(): Stats = Stats(
        entryCount = indexSizeUnlocked(),
        totalBytes = totalBytes.get(),
        maxBytes = config.maxBytes,
    )

    private fun indexSizeUnlocked(): Int = index.size

    data class Stats(val entryCount: Int, val totalBytes: Long, val maxBytes: Long)

    override fun close() {
        kotlinx.coroutines.runBlocking { saveIndex() }
    }

    companion object {
        private const val INDEX_FLUSH_INTERVAL = 64L
        private const val UNSTAMPED = -1L
    }
}
