package io.skjaere.nzbstreamer.stream

import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.isRegularFile
import kotlin.io.path.name
import kotlin.io.path.createDirectories
import kotlin.io.path.readBytes
import kotlin.io.path.readText
import kotlin.io.path.setLastModifiedTime
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText
import kotlin.random.Random
import kotlin.streams.asSequence
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class SegmentCacheTest {

    private fun cfg(dir: Path, maxBytes: Long = 1_000_000L) =
        SegmentCacheConfig(cacheDir = dir, maxBytes = maxBytes, evictThresholdRatio = 0.9)

    private fun SegmentCache.pathOf(articleId: String): Path {
        fun call(method: String, arg: String): Any = SegmentCache::class.java.getDeclaredMethod(method, String::class.java)
            .apply { isAccessible = true }.invoke(this, arg)
        return call("pathFor", call("nameFor", articleId) as String) as Path
    }

    /** The cache's segment files: everything under [dir] but the index. */
    private fun segmentFiles(dir: Path): List<Path> = Files.walk(dir).use { paths ->
        paths.asSequence().filter { it.isRegularFile() && it.name != "index.json" }.toList()
    }

    @Test
    fun `miss invokes fetch, hit does not`(@TempDir tmp: Path) = runBlocking {
        val cache = SegmentCache(cfg(tmp))
        val calls = AtomicInteger()
        val payload = ByteArray(1024) { it.toByte() }

        val first = cache.getOrFetch("a@b") { calls.incrementAndGet(); payload }
        val second = cache.getOrFetch("a@b") { calls.incrementAndGet(); ByteArray(0) }

        assertContentEquals(payload, first)
        assertContentEquals(payload, second)
        assertEquals(1, calls.get())
    }

    @Test
    fun `singleflight dedupes concurrent miss for same id`(@TempDir tmp: Path) = runBlocking {
        val cache = SegmentCache(cfg(tmp))
        val calls = AtomicInteger()
        val payload = ByteArray(2048) { (it % 7).toByte() }

        val results = coroutineScope {
            (1..16).map {
                async {
                    cache.getOrFetch("dup@id") {
                        calls.incrementAndGet()
                        delay(20)
                        payload
                    }
                }
            }.awaitAll()
        }
        results.forEach { assertContentEquals(payload, it) }
        assertEquals(1, calls.get(), "fetch must run exactly once across concurrent callers")
    }

    @Test
    fun `a follower whose leader is cancelled fetches for itself`(@TempDir tmp: Path) = runBlocking<Unit> {
        val cache = SegmentCache(cfg(tmp))
        val payload = ByteArray(1024) { it.toByte() }
        // Undispatched, each runs until it waits: the leader in its fetch, the follower on the leader.
        val leader = launch(start = CoroutineStart.UNDISPATCHED) {
            cache.getOrFetch("s@id") { awaitCancellation() }
        }
        val followerFetches = AtomicInteger()
        val follower = async(start = CoroutineStart.UNDISPATCHED) {
            cache.getOrFetch("s@id") { followerFetches.incrementAndGet(); payload }
        }
        // The reader that started the fetch seeks away, say.
        leader.cancelAndJoin()

        val got = runCatching { follower.await() }
            .getOrElse { fail("the follower failed with its leader's cancellation: $it") }
        assertContentEquals(payload, got)
        assertEquals(1, followerFetches.get())
    }

    /**
     * The leader is cancelled while it writes the segment's file, a store that would go over
     * capacity: the file it wrote, and the ones that store evicts, are either in the index or gone.
     * A file the index doesn't know is never evicted.
     */
    @Test
    fun `a store cancelled mid-write leaves no file the index doesn't know`(@TempDir tmp: Path) = runBlocking<Unit> {
        val cache = SegmentCache(cfg(tmp, maxBytes = 4 * 1024))
        val payload = ByteArray(1024) { 0x5A }
        // At capacity: the next store evicts one.
        repeat(4) { i -> cache.getOrFetch("old-$i@id") { payload } }

        // The leader runs on a thread of its own, which is held once its fetch returns: it can't go
        // on from the write until it has been cancelled.
        val executor = Executors.newSingleThreadExecutor { Thread(it, "leader").apply { isDaemon = true } }
        val hold = CountDownLatch(1)
        try {
            val leader = launch(executor.asCoroutineDispatcher()) {
                cache.getOrFetch("new@id") {
                    executor.execute { hold.await() }
                    payload
                }
            }
            val written = cache.pathOf("new@id")
            withTimeout(10_000) {
                while (!written.exists() || written.fileSize() < payload.size) delay(5)
            }
            leader.cancel()
            hold.countDown()
            leader.join()
        } finally {
            hold.countDown()
            executor.shutdown()
        }

        val files = segmentFiles(tmp)
        val stats = cache.stats()
        assertEquals(stats.entryCount, files.size, "files on disk vs entries in the index")
        assertEquals(stats.totalBytes, files.sumOf { Files.size(it) }, "bytes on disk vs bytes in the index")
    }

    @Test
    fun `index survives reopen and serves cached entries`(@TempDir tmp: Path) = runBlocking {
        val payload = ByteArray(512) { it.toByte() }
        SegmentCache(cfg(tmp)).use { c ->
            c.getOrFetch("warm@id") { payload }
        }
        // index.json is flushed on close; open a fresh instance and verify hit.
        val reopened = SegmentCache(cfg(tmp))
        val calls = AtomicInteger()
        val got = reopened.getOrFetch("warm@id") { calls.incrementAndGet(); ByteArray(0) }
        assertContentEquals(payload, got)
        assertEquals(0, calls.get(), "warm restart should serve from disk without re-fetching")
        assertTrue(tmp.resolve("index.json").exists(), "sidecar present")
    }

    /**
     * Last access was System.nanoTime, which counts from boot: after a reboot every new read was
     * stamped earlier than every access before it, so the entries just read were evicted first. A
     * reboot here is the index's nanoTime stamps put a day ahead (the last boot had been up a day
     * longer than this one); a wall-clock stamp is the same across a reboot.
     */
    @Test
    fun `a read after a reboot keeps its entry over older ones`(@TempDir tmp: Path) {
        runBlocking {
            val payload = ByteArray(1024) { 0x11 }
            SegmentCache(cfg(tmp, maxBytes = 3 * 1024)).use { c ->
                listOf("a@id", "b@id", "c@id").forEach { id -> c.getOrFetch(id) { payload } }
            }
            val index = tmp.resolve("index.json")
            index.writeText(
                Regex(""""lastAccessNanos"\s*:\s*(-?\d+)""").replace(index.readText()) { m ->
                    """"lastAccessNanos":${m.groupValues[1].toLong() + 86_400_000_000_000L}"""
                }
            )

            val cache = SegmentCache(cfg(tmp, maxBytes = 3 * 1024))
            assertNotNull(cache.get("a@id"))
            // Over capacity: the least recently used goes, b.
            cache.getOrFetch("d@id") { payload }

            assertNotNull(cache.get("a@id"), "a, read since the reboot, was evicted")
            assertNull(cache.get("b@id"), "b, the least recently used, is still cached")
        }
    }

    /** An index written by a version that stamped last access with System.nanoTime, from an earlier boot. */
    @Test
    fun `entries of an older index are ordered by their files' times`(@TempDir tmp: Path) {
        runBlocking {
            val ids = listOf("a@id", "b@id", "c@id")
            val paths = SegmentCache(cfg(tmp)).let { c -> ids.map { c.pathOf(it) } }
            // Written a, b, c, three hours to one hour ago; their nanoTime stamps say c, b, a.
            val now = Instant.now()
            paths.forEachIndexed { i, path ->
                path.parent.createDirectories()
                path.writeBytes(ByteArray(1024) { 0x22 })
                path.setLastModifiedTime(FileTime.from(now.minusSeconds(3_600L * (3 - i))))
            }
            val entries = ids.mapIndexed { i, id ->
                """{"articleId":"$id","size":1024,"lastAccessNanos":${3 - i}}"""
            }
            tmp.resolve("index.json").writeText("""{"entries":[${entries.joinToString(",")}]}""")

            val cache = SegmentCache(cfg(tmp, maxBytes = 3 * 1024))
            cache.getOrFetch("d@id") { ByteArray(1024) { 0x33 } }

            assertNull(cache.get("a@id"), "a, written first, is still cached")
            assertNotNull(cache.get("c@id"), "c, written last, was evicted")
        }
    }

    /** The cache's index and its files agree: each file an entry, and its bytes what the index counts. */
    private fun assertMatchesDisk(cache: SegmentCache, dir: Path) {
        val files = segmentFiles(dir)
        val stats = cache.stats()
        assertEquals(files.size, stats.entryCount, "entries in the index vs files on disk")
        assertEquals(files.sumOf { Files.size(it) }, stats.totalBytes, "bytes in the index vs bytes on disk")
    }

    /**
     * Android kills a process without closing anything. The index was saved at the 64th write;
     * the files written since were in no index, never served, counted or evicted.
     */
    @Test
    fun `a kill loses no segment written since the last save and leaks no file`(@TempDir tmp: Path) {
        runBlocking {
            val payload = ByteArray(100) { 0x44 }
            val ids = (1..70).map { "k-$it@id" }
            SegmentCache(cfg(tmp)).let { killed -> ids.forEach { id -> killed.getOrFetch(id) { payload } } }

            val cache = SegmentCache(cfg(tmp))
            assertMatchesDisk(cache, tmp)
            assertEquals(ids.size, cache.stats().entryCount)
            val fetches = AtomicInteger()
            ids.forEach { id ->
                assertContentEquals(payload, cache.getOrFetch(id) { fetches.incrementAndGet(); ByteArray(0) })
            }
            assertEquals(0, fetches.get(), "segments fetched again")
        }
    }

    /** A kill in the middle of a write leaves the write's temporary file, which is nobody's. */
    @Test
    fun `a write cut short by a kill leaves no file behind`(@TempDir tmp: Path) {
        runBlocking {
            val payload = ByteArray(512) { 0x55 }
            SegmentCache(cfg(tmp)).use { c -> c.getOrFetch("whole@id") { payload } }
            val cutShort = tmp.resolve("ab").createDirectories().resolve("ab" + "0".repeat(62) + "123.tmp")
            cutShort.writeBytes(ByteArray(100))

            val cache = SegmentCache(cfg(tmp))
            assertMatchesDisk(cache, tmp)
            assertFalse(cutShort.exists(), "the cut-short write is still on disk")
            assertContentEquals(payload, cache.get("whole@id"))
        }
    }

    /**
     * Versions that kept the index without a version wrote files in place, so a file their index
     * doesn't list may be one a kill cut short: it is deleted, not taken in, and counts for nothing.
     */
    @Test
    fun `a file an older index doesn't list is deleted`(@TempDir tmp: Path) {
        runBlocking {
            val paths = SegmentCache(cfg(tmp)).let { c -> listOf("listed@id", "unlisted@id").map { c.pathOf(it) } }
            paths.forEach { path ->
                path.parent.createDirectories()
                path.writeBytes(ByteArray(1024) { 0x66 })
            }
            tmp.resolve("index.json").writeText("""{"entries":[{"articleId":"listed@id","size":1024,"lastAccessNanos":7}]}""")

            val cache = SegmentCache(cfg(tmp))
            assertMatchesDisk(cache, tmp)
            assertEquals(1, cache.stats().entryCount)
            assertNotNull(cache.get("listed@id"))
            assertNull(cache.get("unlisted@id"))
        }
    }

    /** An index that can't be read vouches for no file: they are deleted, not left on disk uncounted. */
    @Test
    fun `an unreadable index leaves no file uncounted`(@TempDir tmp: Path) {
        runBlocking {
            val paths = SegmentCache(cfg(tmp)).let { c -> listOf("a@id", "b@id").map { c.pathOf(it) } }
            paths.forEach { path ->
                path.parent.createDirectories()
                path.writeBytes(ByteArray(1024) { 0x77 })
            }
            tmp.resolve("index.json").writeText("""{"entries":[{"artic""")

            val cache = SegmentCache(cfg(tmp))
            assertMatchesDisk(cache, tmp)
        }
    }

    /** What a save keeps that the files' times don't: which entries were read since they were written. */
    @Test
    fun `save keeps the order of reads across a kill`(@TempDir tmp: Path) {
        runBlocking {
            val payload = ByteArray(1024) { 0x12 }
            SegmentCache(cfg(tmp, maxBytes = 3 * 1024)).let { killed ->
                listOf("a@id", "b@id", "c@id").forEach { id -> killed.getOrFetch(id) { payload } }
                assertNotNull(killed.get("a@id"))
                killed.save()
            }

            val cache = SegmentCache(cfg(tmp, maxBytes = 3 * 1024))
            cache.getOrFetch("d@id") { payload }
            assertNotNull(cache.get("a@id"), "a, the last read, was evicted")
            assertNull(cache.get("b@id"), "b, the least recently used, is still cached")
        }
    }

    private fun payloadOf(id: String) = ByteArray(256) { (id.hashCode() + it).toByte() }

    /** A clear while a segment is being fetched: the cache is empty, then holds that segment once it lands. */
    @Test
    fun `clear empties the cache and keeps a write that lands after it`(@TempDir tmp: Path) {
        runBlocking {
            val cache = SegmentCache(cfg(tmp))
            val old = (1..20).map { "old-$it@id" }
            old.forEach { id -> cache.getOrFetch(id) { payloadOf(id) } }
            val fetched = CompletableDeferred<Unit>()
            val writer = async(start = CoroutineStart.UNDISPATCHED) {
                cache.getOrFetch("new@id") { fetched.await(); payloadOf("new@id") }
            }

            cache.clear()
            assertEquals(SegmentCache.Stats(0, 0, 1_000_000L), cache.stats())
            assertEquals(emptyList(), segmentFiles(tmp))
            assertNull(cache.get("old-1@id"))

            fetched.complete(Unit)
            assertContentEquals(payloadOf("new@id"), writer.await())
            assertEquals(1, cache.stats().entryCount)
            assertMatchesDisk(cache, tmp)
            val fetches = AtomicInteger()
            assertContentEquals(payloadOf("old-2@id"), cache.getOrFetch("old-2@id") { fetches.incrementAndGet(); payloadOf("old-2@id") })
            assertEquals(1, fetches.get(), "a cleared segment wasn't fetched again")
        }
    }

    /**
     * Clears while readers read segments at random, fetching those that miss, until the clear is
     * done: each read gets its segment's bytes, and the index is then as the disk is. (A file a
     * clear deletes after a write put it back, still listed, is served by no read.)
     */
    @Test
    fun `clear keeps reads and writes in flight right`(@TempDir tmp: Path) {
        runBlocking {
            val cache = SegmentCache(cfg(tmp))
            val ids = (1..300).map { "seg-$it@id" }
            repeat(20) { round ->
                val cleared = AtomicBoolean(false)
                coroutineScope {
                    val readers = (1..8).map { n ->
                        launch(Dispatchers.IO) {
                            val random = Random(round * 8 + n)
                            while (!cleared.get()) {
                                val id = ids.random(random)
                                assertContentEquals(payloadOf(id), cache.getOrFetch(id) { payloadOf(id) }, id)
                            }
                        }
                    }
                    delay(50)
                    cache.clear()
                    cleared.set(true)
                    readers.joinAll()
                }
                assertMatchesDisk(cache, tmp)
            }
        }
    }

    @Test
    fun `resize evicts down to the new limit at once and lets the cache grow back`(@TempDir tmp: Path) {
        runBlocking {
            val cache = SegmentCache(cfg(tmp, maxBytes = 4 * 1024))
            val payload = ByteArray(1024) { 0x13 }
            listOf("a@id", "b@id", "c@id", "d@id").forEach { id -> cache.getOrFetch(id) { payload } }
            assertNotNull(cache.get("a@id"))

            cache.resize(2 * 1024)
            assertEquals(SegmentCache.Stats(2, 2 * 1024, 2 * 1024), cache.stats())
            assertMatchesDisk(cache, tmp)
            assertNotNull(cache.get("a@id"), "a, the last read, was evicted")
            assertNotNull(cache.get("d@id"), "d, the last written, was evicted")

            cache.resize(8 * 1024)
            listOf("e@id", "f@id", "g@id").forEach { id -> cache.getOrFetch(id) { payload } }
            assertEquals(SegmentCache.Stats(5, 5 * 1024, 8 * 1024), cache.stats())
        }
    }

    @Test
    fun `evicts oldest entries when over capacity`(@TempDir tmp: Path) = runBlocking {
        // capacity 4 KB, one entry is 1 KB → 5 entries forces 1 eviction
        val cache = SegmentCache(cfg(tmp, maxBytes = 4 * 1024))
        val payload = ByteArray(1024) { 0xAB.toByte() }

        repeat(5) { i ->
            cache.getOrFetch("seg-$i@id") { payload }
        }

        val stats = cache.stats()
        assertTrue(stats.totalBytes <= 4 * 1024, "total ${stats.totalBytes} should be within capacity")
        assertTrue(stats.entryCount in 1..4, "entryCount=${stats.entryCount}")

        // The most recent entry must still be served from cache (no fetch).
        val calls = AtomicInteger()
        cache.getOrFetch("seg-4@id") { calls.incrementAndGet(); payload }
        assertEquals(0, calls.get())
    }

    @Test
    fun `bytes are returned even when disk write fails`(@TempDir tmp: Path) = runBlocking {
        // Make the cache dir read-only after construction to force write failures.
        val cache = SegmentCache(cfg(tmp))
        tmp.toFile().setWritable(false)
        try {
            val payload = ByteArray(64) { 1 }
            val got = cache.getOrFetch("rd@only") { payload }
            assertContentEquals(payload, got)
        } finally {
            tmp.toFile().setWritable(true)
        }
    }

    @Test
    fun `corrupt cache file is detected and re-fetched`(@TempDir tmp: Path) = runBlocking {
        val cache = SegmentCache(cfg(tmp))
        val original = ByteArray(256) { it.toByte() }
        cache.getOrFetch("c@id") { original }

        // Corrupt the on-disk file by truncating it.
        val path = cache.pathOf("c@id")
        assertNotNull(path)
        path.toFile().writeBytes(ByteArray(10))
        assertEquals(10L, path.fileSize())

        val calls = AtomicInteger()
        val replacement = ByteArray(256) { (it + 1).toByte() }
        val got = cache.getOrFetch("c@id") { calls.incrementAndGet(); replacement }
        assertContentEquals(replacement, got)
        assertEquals(1, calls.get(), "size mismatch must trigger a re-fetch")
        // After re-fetch the cache should hold the new bytes.
        assertContentEquals(replacement, path.readBytes())
    }

    @Test
    fun `get returns null for missing entry`(@TempDir tmp: Path) = runBlocking {
        val cache = SegmentCache(cfg(tmp))
        assertNull(cache.get("never@stored"))
    }
}
