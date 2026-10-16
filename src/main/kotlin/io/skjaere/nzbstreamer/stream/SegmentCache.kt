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
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.moveTo
import kotlin.io.path.name
import kotlin.io.path.readAttributes
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
 * The index is saved every 64 writes, by [save] and on [close], but a process may be killed
 * without either (Android kills them so): loading reconciles it with the files on disk, so a
 * file written since the last save is taken in, not lost or left on disk uncounted. Files are
 * written whole (to a temporary file, then renamed), so a write a kill cuts short is never
 * taken for a segment. Loading walks the cache directory: construct the cache off the main
 * thread. One instance per directory: two would each take the other's files for their own.
 *
 * The cache is best-effort: failures to read/write the disk degrade silently to a
 * passthrough to the [fetch] lambda. Bytes returned to callers are always correct
 * (a corrupt cache entry is detected via size mismatch and re-fetched).
 */
class SegmentCache(private val config: SegmentCacheConfig) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(SegmentCache::class.java)

    /** A segment's file, by [name] (its article id's SHA-256, in hex): its size, and when it was last read or written. */
    @Serializable
    private data class Entry(val name: String, val size: Long, var lastAccessMillis: Long)

    /** The index as this version writes it, [version] [INDEX_VERSION]. */
    @Serializable
    private data class IndexFile(val entries: List<Entry>, val version: Int)

    /**
     * An entry of an index without a version, an older version's: keyed by article id, and stamped
     * with System.nanoTime (as lastAccessNanos), meaningless after a reboot, so not read.
     */
    @Serializable
    private data class OlderEntry(val articleId: String, val size: Long)

    @Serializable
    private data class OlderIndexFile(val entries: List<OlderEntry>)

    /** The entries an index lists, and whether it vouches for files it doesn't (one of this version). */
    private class Listed(val entries: Map<String, Entry>, val current: Boolean)

    private val json = Json { ignoreUnknownKeys = true }
    private val indexLock = Mutex()
    // Serializes saves: each writes the index's temporary file, and a later snapshot must land last.
    private val saveLock = Mutex()
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
    private val indexTempPath: Path = config.cacheDir.resolve("index.json$TEMP_SUFFIX")
    private val evictThreshold: Long = (config.maxBytes * config.evictThresholdRatio).toLong()

    init {
        require(config.maxBytes > 0) { "maxBytes must be > 0" }
        config.cacheDir.createDirectories()
        load()

        // Gauges read state on every scrape; AtomicLong satisfies Number, and the
        // index gauge reads .size unsynchronized — fine for a metric.
        val registry = Metrics.globalRegistry
        registry.gauge("nzb.segments.cache.bytes", totalBytes)
        registry.gauge("nzb.segments.cache.max.bytes", AtomicLong(config.maxBytes))
        registry.gauge("nzb.segments.cache.entries", index) { it.size.toDouble() }
    }

    /**
     * Loads the index and reconciles it with the segment files on disk. A file the index doesn't
     * list was written since it was saved: it is taken in, stamped with its modification time,
     * when the index is of this version, as this version writes files whole. Older versions wrote
     * them in place, where a kill could cut one short, so with an older index, one that can't be
     * read or none, it is deleted; the index is then saved at once, in this version. An entry
     * whose file is gone is dropped, and temporary files (writes or saves a kill cut short) are
     * deleted. A failure leaves the cache empty.
     */
    private fun load() {
        runCatching {
            indexTempPath.deleteIfExists()
            val listed = readIndex()
            var takenIn = 0
            var deleted = 0
            for ((name, attributes) in scanSegmentFiles()) {
                val modified = attributes.lastModifiedTime().toMillis()
                val known = listed.entries[name]
                val entry = when {
                    known != null -> if (known.lastAccessMillis == UNSTAMPED) known.copy(lastAccessMillis = modified) else known
                    listed.current && attributes.size() > 0 -> Entry(name, attributes.size(), modified).also { takenIn++ }
                    else -> null
                }
                if (entry == null) {
                    pathFor(name).deleteIfExists()
                    deleted++
                    continue
                }
                index[name] = entry
                totalBytes.addAndGet(entry.size)
            }
            lastStamp.set(index.values.maxOfOrNull { it.lastAccessMillis } ?: 0)
            logger.info(
                "Loaded segment cache index: {} entries, {} bytes ({} files taken in, {} deleted, " +
                    "{} entries without a file dropped)",
                index.size, totalBytes.get(), takenIn, deleted, listed.entries.keys.count { it !in index },
            )
            if (!listed.current) writeIndex(snapshot())
        }.onFailure {
            logger.warn("Failed to load segment cache index, starting empty", it)
            index.clear()
            totalBytes.set(0)
        }
    }

    /** The index on disk: none, or one that can't be read, lists nothing and vouches for nothing. */
    private fun readIndex(): Listed {
        if (!indexPath.exists()) return Listed(emptyMap(), current = false)
        return runCatching {
            val element = json.parseToJsonElement(indexPath.readText())
            if (element.jsonObject["version"]?.jsonPrimitive?.intOrNull == INDEX_VERSION) {
                Listed(json.decodeFromJsonElement<IndexFile>(element).entries.associateBy { it.name }, current = true)
            } else {
                val older = json.decodeFromJsonElement<OlderIndexFile>(element).entries
                Listed(older.map { Entry(nameFor(it.articleId), it.size, UNSTAMPED) }.associateBy { it.name }, current = false)
            }
        }.getOrElse {
            logger.warn("Failed to read segment cache index, deleting the files it doesn't vouch for", it)
            Listed(emptyMap(), current = false)
        }
    }

    /** The segment files under the cache directory, by name; temporary files are deleted on the way. */
    private fun scanSegmentFiles(): Map<String, BasicFileAttributes> =
        config.cacheDir.listDirectoryEntries()
            .filter { it.isDirectory() && SHARD_NAME.matches(it.name) }
            .flatMap { it.listDirectoryEntries() }
            .mapNotNull { path ->
                when {
                    path.name.endsWith(TEMP_SUFFIX) -> {
                        path.deleteIfExists()
                        null
                    }
                    SEGMENT_NAME.matches(path.name) && path.name.startsWith(path.parent.name) ->
                        path.name to path.readAttributes<BasicFileAttributes>()
                    else -> null
                }
            }
            .toMap()

    /**
     * Writes the index now. Only the index keeps when entries were last read (a file taken in on
     * load is stamped with its modification time): save it when the process may be killed, as
     * when an app goes to the background. Stores save it every 64 writes too.
     */
    suspend fun save() {
        saveLock.withLock {
            val snapshot = indexLock.withLock { snapshot() }
            withContext(Dispatchers.IO) { writeIndex(snapshot) }
        }
    }

    private fun snapshot() = IndexFile(index.values.map { it.copy() }, INDEX_VERSION)

    /** Replaces the index whole (written beside it, then renamed): a kill mid-save leaves the last one. */
    private fun writeIndex(snapshot: IndexFile) {
        runCatching {
            config.cacheDir.createDirectories()
            indexTempPath.writeText(json.encodeToString(IndexFile.serializer(), snapshot))
            indexTempPath.moveTo(indexPath, StandardCopyOption.ATOMIC_MOVE)
        }.onFailure { logger.warn("Failed to persist cache index", it) }
    }

    /** Now, by the wall clock, or just after the last stamp if the clock is behind it. */
    private fun stamp(): Long = lastStamp.updateAndGet { maxOf(it + 1, System.currentTimeMillis()) }

    /** The name of [articleId]'s file: its SHA-256, in hex. */
    private fun nameFor(articleId: String): String {
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(articleId.toByteArray(Charsets.UTF_8))
        return digest.joinToString("") { "%02x".format(it) }
    }

    private fun pathFor(name: String): Path = config.cacheDir.resolve(name.substring(0, 2)).resolve(name)

    /** Returns cached bytes if present + readable, else null. Updates lastAccess on hit. */
    suspend fun get(articleId: String): ByteArray? {
        val name = nameFor(articleId)
        val entry = indexLock.withLock { index[name] } ?: return null
        return readEntry(articleId, name, entry)
    }

    private suspend fun readEntry(articleId: String, name: String, entry: Entry): ByteArray? {
        val path = pathFor(name)
        return withContext(Dispatchers.IO) {
            runCatching {
                val bytes = path.readBytes()
                if (bytes.size.toLong() != entry.size) {
                    logger.warn(
                        "Cache size mismatch for {}: expected {} got {}, dropping",
                        articleId, entry.size, bytes.size
                    )
                    invalidate(name)
                    null
                } else {
                    bytes
                }
            }.getOrElse { e ->
                logger.warn("Failed to read cached segment {}: {}", articleId, e.message)
                invalidate(name)
                null
            }
        }?.also {
            indexLock.withLock { index[name]?.lastAccessMillis = stamp() }
        }
    }

    private suspend fun invalidate(name: String) {
        indexLock.withLock {
            val removed = index.remove(name)
            if (removed != null) totalBytes.addAndGet(-removed.size)
        }
        runCatching { pathFor(name).deleteIfExists() }
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
        val name = nameFor(articleId)
        val path = pathFor(name)
        // A file written is one the index must know, or it is never evicted: no cancellation
        // between the write and the index update.
        val newTotal = withContext(NonCancellable) {
            val ok = withContext(Dispatchers.IO) {
                runCatching {
                    writeWhole(path, bytes)
                    true
                }.getOrElse {
                    logger.warn("Failed to write cache file {}: {}", path, it.message)
                    false
                }
            }
            if (!ok) return@withContext null

            indexLock.withLock {
                index[name]?.let { totalBytes.addAndGet(-it.size) }
                index[name] = Entry(name, bytes.size.toLong(), stamp())
                totalBytes.addAndGet(bytes.size.toLong())
            }
        } ?: return
        if (newTotal > evictThreshold) evict()
        if (pendingWrites.incrementAndGet() % INDEX_FLUSH_INTERVAL == 0L) save()
    }

    /** Writes [bytes] to [path] whole or not at all: to a temporary file beside it, then renamed to it. */
    private fun writeWhole(path: Path, bytes: ByteArray) {
        path.parent.createDirectories()
        val temp = Files.createTempFile(path.parent, path.name, TEMP_SUFFIX)
        try {
            temp.writeBytes(bytes)
            temp.moveTo(path, StandardCopyOption.ATOMIC_MOVE)
        } finally {
            temp.deleteIfExists()
        }
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
                index.remove(cand.name)
                totalBytes.addAndGet(-cand.size)
            }
            targets
        }
        if (toRemove.isEmpty()) return
        // Out of the index already: delete them even if cancelled, or they stay on disk for good.
        withContext(NonCancellable + Dispatchers.IO) {
            for (cand in toRemove) {
                runCatching { pathFor(cand.name).deleteIfExists() }
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
        kotlinx.coroutines.runBlocking { save() }
    }

    companion object {
        private const val INDEX_FLUSH_INTERVAL = 64L
        private const val INDEX_VERSION = 1
        // An older index's entry, stamped with its file's modification time once that is read.
        private const val UNSTAMPED = -1L
        private const val TEMP_SUFFIX = ".tmp"
        private val SHARD_NAME = Regex("[0-9a-f]{2}")
        private val SEGMENT_NAME = Regex("[0-9a-f]{64}")
    }
}
