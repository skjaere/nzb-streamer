package io.skjaere.nzbstreamer.stream

import io.micrometer.core.instrument.Metrics
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.fileSize
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
 * A file is renamed into place and deleted under the index lock, so the index lists a file
 * exactly when it is on disk, a [clear] or a [resize] with reads and writes in flight included,
 * until something else deletes it ([reconcile]).
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
    // Guards the index (with totalBytes' changes), [changes] and [closed]. Never held over a
    // suspension, and over a file's rename or delete only on Dispatchers.IO.
    private val indexLock = ReentrantLock()
    // Serializes saves: each writes the index's temporary file, and a later snapshot must land last.
    private val saveLock = ReentrantLock()
    // Changes to the index (writes, reads' stamps, drops), and how many of them the index on disk
    // holds (under saveLock): a save with none since the last writes nothing.
    private var changes = 0L
    private var savedChanges = -1L
    @Volatile
    private var closed = false
    private val index = mutableMapOf<String, Entry>()
    // ConcurrentHashMap.putIfAbsent gives us atomic single-flight registration without
    // a coarse-grained mutex around the entire claim flow. Prior implementation used a
    // global `singleflightLock` mutex whose critical section included a disk-I/O recheck
    // (get → readEntry → withContext(Dispatchers.IO) { path.readBytes() }) — that
    // serialized every concurrent fetch across the cache, observed in prod as 51-199s
    // stream-startup stalls when Plex fired its parallel range probes.
    private val inFlight = java.util.concurrent.ConcurrentHashMap<String, CompletableDeferred<ByteArray>>()
    private val totalBytes = AtomicLong(0)
    private val maxBytes = AtomicLong(config.maxBytes)
    private val pendingWrites = AtomicLong(0)
    // The last stamp given: the next is the wall clock's time or one past this, whichever is
    // later, so a clock set back never stamps an access before an earlier one.
    private val lastStamp = AtomicLong(0)

    private val indexPath: Path = config.cacheDir.resolve("index.json")
    private val indexTempPath: Path = config.cacheDir.resolve("index.json$TEMP_SUFFIX")
    private val evictThreshold: Long get() = (maxBytes.get() * config.evictThresholdRatio).toLong()

    init {
        require(config.maxBytes > 0) { "maxBytes must be > 0" }
        config.cacheDir.createDirectories()
        load()

        // Gauges read state on every scrape; AtomicLong satisfies Number, and the
        // index gauge reads .size unsynchronized — fine for a metric.
        val registry = Metrics.globalRegistry
        registry.gauge("nzb.segments.cache.bytes", totalBytes)
        registry.gauge("nzb.segments.cache.max.bytes", maxBytes)
        registry.gauge("nzb.segments.cache.entries", index) { it.size.toDouble() }
    }

    /**
     * Loads the index and reconciles it with the segment files on disk. A file the index doesn't
     * list was written since it was saved: it is taken in, stamped with its modification time,
     * when the index is of this version, as this version writes files whole. Older versions wrote
     * them in place, where a kill could cut one short, so with an older index, one that can't be
     * read or none, it is deleted; the index is then saved at once, in this version. An entry
     * whose file is gone is dropped, and one whose file isn't the size it gives (cut short, or
     * written over) is dropped and its file deleted, so the usage loaded is the bytes on disk.
     * Temporary files (writes or saves a kill cut short) are deleted. A failure leaves the cache
     * empty.
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
                    known != null && known.size != attributes.size() -> null
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
            val dropped = listed.entries.keys.count { it !in index }
            logger.info(
                "Loaded segment cache index: {} entries, {} bytes ({} files taken in, {} deleted, " +
                    "{} entries dropped)",
                index.size, totalBytes.get(), takenIn, deleted, dropped,
            )
            val inStep = listed.current && takenIn == 0 && deleted == 0 && dropped == 0
            if (inStep || writeIndex(snapshot())) savedChanges = changes
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
     * when an app goes to the background. Stores save it every 64 writes too. Writes nothing if
     * the index hasn't changed since it was last written.
     */
    suspend fun save() = withContext(Dispatchers.IO) { saveNow() }

    /** [save], on the calling thread. */
    private fun saveNow() {
        saveLock.withLock {
            val (snapshot, upTo) = indexLock.withLock { snapshot() to changes }
            if (upTo == savedChanges) return
            if (writeIndex(snapshot)) savedChanges = upTo
        }
    }

    private fun snapshot() = IndexFile(index.values.map { it.copy() }, INDEX_VERSION)

    /** Replaces the index whole (written beside it, then renamed): a kill mid-save leaves the last one. Whether it did. */
    private fun writeIndex(snapshot: IndexFile): Boolean = runCatching {
        config.cacheDir.createDirectories()
        indexTempPath.writeText(json.encodeToString(IndexFile.serializer(), snapshot))
        indexTempPath.moveTo(indexPath, StandardCopyOption.ATOMIC_MOVE)
    }.onFailure { logger.warn("Failed to persist cache index", it) }.isSuccess

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
        val entry = indexLock.withLock { index[name].takeUnless { closed } } ?: return null
        return readEntry(articleId, entry)
    }

    private suspend fun readEntry(articleId: String, entry: Entry): ByteArray? = withContext(Dispatchers.IO) {
        val bytes = runCatching { pathFor(entry.name).readBytes() }
            .onFailure { logger.warn("Failed to read cached segment {}: {}", articleId, it.message) }
            .getOrNull()
        when {
            bytes == null -> null
            bytes.size.toLong() != entry.size -> {
                logger.warn("Cache size mismatch for {}: expected {} got {}, dropping", articleId, entry.size, bytes.size)
                null
            }
            else -> bytes
        }.also { read ->
            indexLock.withLock {
                // A clear may have dropped it since, and a write put the segment back: not this one's.
                if (!isListed(entry)) return@withLock
                if (read != null) {
                    entry.lastAccessMillis = stamp()
                    changes++
                } else {
                    removeLocked(entry)
                }
            }
        }
    }

    /**
     * Drops [entry] and deletes its file. Under [indexLock]: files are renamed into place under it
     * too, so one put back by a write can't be deleted between its rename and its index update.
     */
    private fun removeLocked(entry: Entry) {
        index.remove(entry.name)
        changes++
        totalBytes.addAndGet(-entry.size)
        runCatching { pathFor(entry.name).deleteIfExists() }
            .onFailure { logger.warn("Failed to delete cache file {}: {}", entry.name, it.message) }
    }

    /**
     * Whether [entry] is still the index's, under [indexLock]: not dropped (by a clear, say), nor
     * replaced by a write since, and the cache not closed (its directory may be another's now).
     */
    private fun isListed(entry: Entry): Boolean = !closed && index[entry.name] === entry

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
        if (closed) return
        // A file written is one the index must know, or it is never evicted: no cancellation
        // between the write and the index update, and the rename and the update under the lock.
        val newTotal = withContext(NonCancellable + Dispatchers.IO) {
            val temp = runCatching { writeTemp(path, bytes) }
                .onFailure { logger.warn("Failed to write cache file {}: {}", path, it.message) }
                .getOrNull() ?: return@withContext null
            val stored = try {
                indexLock.withLock {
                    // Closed meanwhile: the directory may be another instance's by now.
                    if (closed) return@withLock null
                    temp.moveTo(path, StandardCopyOption.ATOMIC_MOVE)
                    index[name]?.let { totalBytes.addAndGet(-it.size) }
                    index[name] = Entry(name, bytes.size.toLong(), stamp())
                    changes++
                    totalBytes.addAndGet(bytes.size.toLong())
                }
            } catch (e: IOException) {
                logger.warn("Failed to write cache file {}: {}", path, e.message)
                null
            }
            if (stored == null) runCatching { temp.deleteIfExists() }
            stored
        } ?: return
        if (newTotal > evictThreshold) evict()
        if (pendingWrites.incrementAndGet() % INDEX_FLUSH_INTERVAL == 0L) save()
    }

    /**
     * Writes [bytes] to a temporary file beside [path], to be renamed to it: a write a kill cuts
     * short is never taken for a segment.
     */
    private fun writeTemp(path: Path, bytes: ByteArray): Path {
        path.parent.createDirectories()
        val temp = Files.createTempFile(path.parent, path.name, TEMP_SUFFIX)
        try {
            temp.writeBytes(bytes)
        } catch (e: IOException) {
            runCatching { temp.deleteIfExists() }
            throw e
        }
        return temp
    }

    /**
     * Evicts the least recently used entries until the cache is within its limit, [DELETE_BATCH]
     * at a time under the lock, so a large eviction (the limit lowered) doesn't hold it long.
     */
    private suspend fun evict() {
        val candidates = indexLock.withLock {
            if (closed || totalBytes.get() <= maxBytes.get()) emptyList() else index.values.sortedBy { it.lastAccessMillis }
        }
        var evicted = 0
        withContext(Dispatchers.IO) {
            for (batch in candidates.chunked(DELETE_BATCH)) {
                val within = indexLock.withLock {
                    for (entry in batch) {
                        if (totalBytes.get() <= maxBytes.get()) break
                        // One a clear dropped, or a write replaced, since the candidates were picked isn't evicted.
                        if (!isListed(entry)) continue
                        removeLocked(entry)
                        evicted++
                    }
                    closed || totalBytes.get() <= maxBytes.get()
                }
                if (within) break
            }
        }
        if (evicted > 0) logger.debug("Evicted {} segments, totalBytes now {}", evicted, totalBytes.get())
    }

    /**
     * Empties the cache: drops every entry and deletes its file, then saves the index. Reads and
     * writes in flight go on: a read whose file is gone misses (getOrFetch fetches it again), and
     * a write that lands during or after the clear is kept.
     */
    suspend fun clear() {
        val entries = indexLock.withLock { index.values.toList() }
        withContext(Dispatchers.IO) {
            for (batch in entries.chunked(DELETE_BATCH)) {
                indexLock.withLock { batch.filter(::isListed).forEach(::removeLocked) }
            }
        }
        logger.info("Cleared the segment cache: {} segments", entries.size)
        save()
    }

    /**
     * Drops each entry whose file is gone or isn't the size listed (deleting the file), so the usage
     * is the bytes on disk: files can go from under a running cache, as when Android deletes an app's
     * cache files to free storage. Stats every listed file: off the main thread, before the usage
     * decides something (how much room the cache may take, say).
     */
    suspend fun reconcile() {
        val listed = indexLock.withLock { if (closed) emptyList() else index.values.toList() }
        val dropped = withContext(Dispatchers.IO) {
            // A write that put a segment back since is a new entry: isListed leaves it.
            listed.filterNot(::onDisk).chunked(DELETE_BATCH).sumOf { batch ->
                indexLock.withLock { batch.filter(::isListed).onEach(::removeLocked).size }
            }
        }
        if (dropped > 0) logger.info("Dropped {} segment cache entries whose files were gone: {} bytes cached", dropped, totalBytes.get())
    }

    /** Whether [entry]'s file is on disk, the size it lists. */
    private fun onDisk(entry: Entry): Boolean = runCatching { pathFor(entry.name).fileSize() == entry.size }.getOrDefault(false)

    /** Sets the limit to [maxBytes]; a cache over it evicts its least recently used entries at once. */
    suspend fun resize(maxBytes: Long) {
        require(maxBytes > 0) { "maxBytes must be > 0" }
        this.maxBytes.set(maxBytes)
        evict()
    }

    /** The cache's usage now. Takes no lock, so it is cheap anywhere (the main thread included). */
    fun stats(): Stats = Stats(
        entryCount = indexSizeUnlocked(),
        totalBytes = totalBytes.get(),
        maxBytes = maxBytes.get(),
    )

    private fun indexSizeUnlocked(): Int = index.size

    data class Stats(val entryCount: Int, val totalBytes: Long, val maxBytes: Long)

    /**
     * Saves the index if it changed since it was last saved, on the calling thread (call [save]
     * first, or close off the main thread), and closes the cache: from then on it serves and
     * stores nothing (get misses, getOrFetch only fetches) and leaves its directory alone, so
     * another instance can take the directory over. Closing again does nothing.
     */
    override fun close() {
        indexLock.withLock {
            if (closed) return
            closed = true
        }
        saveNow()
    }

    companion object {
        private const val INDEX_FLUSH_INTERVAL = 64L
        private const val INDEX_VERSION = 1
        // An older index's entry, stamped with its file's modification time once that is read.
        private const val UNSTAMPED = -1L
        private const val TEMP_SUFFIX = ".tmp"
        // Files deleted under one hold of the index lock, by an eviction or a clear.
        private const val DELETE_BATCH = 64
        private val SHARD_NAME = Regex("[0-9a-f]{2}")
        private val SEGMENT_NAME = Regex("[0-9a-f]{64}")
    }
}
