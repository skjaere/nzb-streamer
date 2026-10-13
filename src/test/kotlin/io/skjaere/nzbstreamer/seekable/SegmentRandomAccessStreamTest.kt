package io.skjaere.nzbstreamer.seekable

import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueItem
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.Collections
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SegmentRandomAccessStreamTest {
    private val segmentSize = 1000
    private val segments = 200

    /** Byte at position p is p mod 251, so any read can be checked. */
    private val data = ByteArray(segmentSize * segments) { (it % 251).toByte() }
    private val layout = (0 until segments).map { i ->
        SegmentQueueItem(NzbSegment(bytes = segmentSize.toLong(), number = i + 1, articleId = "s$i"), 0, i, segmentSize.toLong(), i.toLong() * segmentSize, 0, segmentSize.toLong())
    }
    private val fetched: MutableList<String> = Collections.synchronizedList(mutableListOf())

    private fun stream(prefetchDepth: Int = 4) = SegmentRandomAccessStream(layout, { id ->
        fetched += id
        delay(5)
        val i = id.removePrefix("s").toInt()
        data.copyOfRange(i * segmentSize, (i + 1) * segmentSize)
    }, prefetchDepth = prefetchDepth)

    private suspend fun SegmentRandomAccessStream.readFully(n: Int): ByteArray {
        val out = ByteArray(n)
        var got = 0
        while (got < n) {
            val r = read(out, got, n - got)
            if (r <= 0) break
            got += r
        }
        return out.copyOf(got)
    }

    @Test
    fun `reads across segment boundaries and to the end`() = runBlocking<Unit> {
        stream().use { s ->
            s.seek(1990)
            assertContentEquals(data.copyOfRange(1990, 2030), s.readFully(40))
            s.seek(data.size - 5L)
            assertContentEquals(data.copyOfRange(data.size - 5, data.size), s.readFully(10))
            assertEquals(-1, s.read())
        }
    }

    @Test
    fun `only touched segments are fetched, once each`() = runBlocking<Unit> {
        stream().use { s ->
            s.seek(10_500); s.readFully(100)
            s.seek(10_700); s.readFully(100)
            s.seek(57_000); s.readFully(10)
            assertEquals(listOf("s10", "s57"), fetched.toList())
        }
    }

    @Test
    fun `a constant stride prefetches the next headers in parallel`() = runBlocking<Unit> {
        // Like a RAR header walk: header at k * 20_126, read a few bytes, jump to the next.
        stream(prefetchDepth = 4).use { s ->
            listOf(0L, 20_126L, 40_252L).forEach { pos -> s.seek(pos); s.readFully(30) }
            // After the second equal jump, the next 4 strides are requested up front.
            assertTrue(fetched.containsAll(listOf("s60", "s80", "s100", "s120")), "fetched: $fetched")
            fetched.clear()
            s.seek(60_378); s.readFully(30)
            assertTrue("s60" !in fetched, "s60 was refetched: $fetched")
        }
    }

    @Test
    fun `irregular jumps prefetch nothing`() = runBlocking<Unit> {
        stream().use { s ->
            listOf(0L, 15_000L, 90_000L, 91_500L).forEach { s.seek(it); s.readFully(20) }
            assertEquals(listOf("s0", "s15", "s90", "s91"), fetched.toList())
        }
    }

    @Test
    fun `explicit prefetch fetches all requested segments concurrently`() = runBlocking<Unit> {
        stream().use { s ->
            s.prefetch(listOf(5_000L, 9_000L, 9_500L, 150_000L))
            s.seek(9_200); s.readFully(10)
            assertEquals(setOf("s5", "s9", "s150"), fetched.toSet())
            assertEquals(3, fetched.size)
        }
    }
}
