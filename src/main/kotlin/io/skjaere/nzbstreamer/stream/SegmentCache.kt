package io.skjaere.nzbstreamer.stream

import io.micrometer.core.instrument.Metrics
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
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
import kotlin.io.path.readBytes
import kotlin.io.path.readText
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText

/**
 * Persistent segment cache backed by per-segment files plus an `index.json` sidecar.
 *
 * The sidecar records each entry's size and last-access timestamp, so the cache survives
 * restarts and supports global LRU eviction without scanning every file's stat info on
 * the hot path. Concurrent fetches for the same article-id are deduplicated via a
 * single-flight map keyed by article-id.
 *
 * The cache is best-effort: failures to read/write the disk degrade silently to a
 * passthrough to the [fetch] lambda. Bytes returned to callers are always correct
 * (a corrupt cache entry is detected via size mismatch and re-fetched).
 */
class SegmentCache(private val config: SegmentCacheConfig) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(SegmentCache::class.java)

    @Serializable
    private data class Entry(val articleId: String, val size: Long, var lastAccessNanos: Long)

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
                if (filePathFor(e.articleId).exists()) {
                    index[e.articleId] = e
                    totalBytes.addAndGet(e.size)
                } else {
                    logger.debug("Index references missing file for {}, dropping", e.articleId)
                }
            }
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
            indexLock.withLock { index[articleId]?.lastAccessNanos = System.nanoTime() }
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
     */
    suspend fun getOrFetch(articleId: String, fetch: suspend () -> ByteArray): ByteArray {
        get(articleId)?.let { return it }

        val ourDeferred = CompletableDeferred<ByteArray>()
        val existing = inFlight.putIfAbsent(articleId, ourDeferred)
        if (existing != null) return existing.await()

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

    private suspend fun store(articleId: String, bytes: ByteArray) {
        val path = filePathFor(articleId)
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
        if (!ok) return

        val newTotal = indexLock.withLock {
            index[articleId]?.let { totalBytes.addAndGet(-it.size) }
            index[articleId] = Entry(articleId, bytes.size.toLong(), System.nanoTime())
            totalBytes.addAndGet(bytes.size.toLong())
        }
        if (newTotal > evictThreshold) evict()
        if (pendingWrites.incrementAndGet() % INDEX_FLUSH_INTERVAL == 0L) saveIndex()
    }

    private suspend fun evict() {
        val toRemove = indexLock.withLock {
            if (totalBytes.get() <= config.maxBytes) return@withLock emptyList()
            val sorted = index.values.sortedBy { it.lastAccessNanos }
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
        withContext(Dispatchers.IO) {
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
    }
}
