package io.skjaere.nzbstreamer.stream

import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.isRegularFile
import kotlin.io.path.name
import kotlin.io.path.readBytes
import kotlin.streams.asSequence
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class SegmentCacheTest {

    private fun cfg(dir: Path, maxBytes: Long = 1_000_000L) =
        SegmentCacheConfig(cacheDir = dir, maxBytes = maxBytes, evictThresholdRatio = 0.9)

    private fun SegmentCache.pathOf(articleId: String): Path =
        SegmentCache::class.java.getDeclaredMethod("filePathFor", String::class.java)
            .apply { isAccessible = true }.invoke(this, articleId) as Path

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
