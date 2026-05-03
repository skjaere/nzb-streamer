package io.skjaere.nzbstreamer.stream

import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.readBytes
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SegmentCacheTest {

    private fun cfg(dir: Path, maxBytes: Long = 1_000_000L) =
        SegmentCacheConfig(cacheDir = dir, maxBytes = maxBytes, evictThresholdRatio = 0.9)

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
        val internals = SegmentCache::class.java.getDeclaredMethod("filePathFor", String::class.java)
        internals.isAccessible = true
        val path = internals.invoke(cache, "c@id") as Path
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
