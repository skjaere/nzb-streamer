package io.skjaere.nzbstreamer

import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentCache
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * A player that seeks closes the stream it was reading and opens another, which asks again for an
 * article the closed one was fetching. That fetch, cancelled, returns only once the NNTP pool has
 * drained its connection, which waits up to 3 s for an answer that may not come. The new stream
 * doesn't wait for it: through the segment cache, whose single flight it would join, as without one.
 */
class SeekPastCancelledFetchTest {
    // Holds back the provider's answer to the first BODY for the article; opened in tearDown at the latest.
    private val held = CountDownLatch(1)
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        held.countDown()
        servers.forEach { it.close() }
    }

    @Test
    fun `an article a closed stream was fetching fails at once for the next stream`(@TempDir tmp: Path) {
        val asked = AtomicInteger()
        // No provider has it: the first BODY is answered late, as by a slow provider, the next at once.
        val server = FakeNntpServer(body = { id ->
            if (id == "<missing@test>" && asked.getAndIncrement() == 0) held.await()
            null
        }).also { servers += it }
        val config = NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)

        SegmentCache(SegmentCacheConfig(tmp, maxBytes = 10L * 1024 * 1024)).use { cache ->
            NntpStreamingService(listOf(config), StreamingConfig(), cache, closeSegmentCache = false).use { service ->
                runBlocking {
                    service.connect()
                    val closed = launch(Dispatchers.IO) { service.fetchSegment("missing@test") }
                    withTimeout(5_000) { while (asked.get() == 0) delay(5) }
                    // The seek: the stream that asked for it first is closed, and the next asks.
                    closed.cancel()
                    val started = System.nanoTime()
                    val failure = withTimeout(10_000) { runCatching { service.fetchSegment("missing@test") }.exceptionOrNull() }
                    val tookMs = (System.nanoTime() - started) / 1_000_000
                    assertIs<ArticleNotFoundException>(failure)
                    assertTrue(tookMs < 1_000, "the next stream's fetch failed $tookMs ms after the seek")
                    held.countDown()
                    closed.join()
                }
            }
        }
    }
}
