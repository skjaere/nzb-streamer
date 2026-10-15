package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * What the streaming service must not hold up while it waits, or wait on: fetches from the other
 * pools while a pool is being removed, and its connections while Dispatchers.Default is busy.
 */
class StreamingServiceBlockingTest {
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    private val articles = mapOf(
        "<x@test>" to ByteArray(4096) { it.toByte() },
        "<y@test>" to ByteArray(4096) { (it * 7).toByte() },
    )

    private fun server(quit: () -> Unit = {}) = FakeNntpServer(
        body = { id -> articles[id]?.let { YencEncoder.encodeSinglePart(it, "data.bin").data } },
        quit = quit,
    ).also { servers += it }

    private fun config(server: FakeNntpServer) =
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 2)

    private fun service(configs: List<NntpConfig>) =
        NntpStreamingService(configs).also { runBlocking { it.connect() } }

    @Test
    fun `fetches from other pools go on while a removed pool closes`() = runBlocking<Unit> {
        val quitAsked = CountDownLatch(1)
        val answerQuit = CountDownLatch(1)
        // Closing a pool QUITs its idle connections; this provider is slow to answer.
        val closing = server(quit = { quitAsked.countDown(); answerQuit.await(10, TimeUnit.SECONDS) })
        val other = server()
        service(listOf(config(closing), config(other))).use { service ->
            // The first pool serves x, leaving it an idle connection to QUIT.
            assertContentEquals(articles["<x@test>"], service.fetchSegment("x@test"))
            val removal = thread { service.removePool(config(closing)) }
            try {
                assertTrue(quitAsked.await(10, TimeUnit.SECONDS), "removing the pool sent no QUIT")
                val started = System.nanoTime()
                assertContentEquals(articles["<y@test>"], service.fetchSegment("y@test"))
                val tookMs = (System.nanoTime() - started) / 1_000_000
                // The pool waits up to 1.5 s for its QUIT; nothing else should.
                assertTrue(tookMs < 1_000, "fetching y waited ${tookMs}ms for the removed pool to close")
                assertEquals(listOf("<y@test>"), other.bodiesRead)
            } finally {
                answerQuit.countDown()
                removal.join()
            }
            assertEquals(listOf(config(other)), service.getPoolConfigs())
        }
    }

    /**
     * Connecting (its DNS lookup blocks) and the socket selector's loop (which blocks in select)
     * are blocking I/O: on Dispatchers.Default they take its few workers (one per CPU core) from
     * CPU-bound work, and wait while that work has them.
     */
    @Test
    fun `a fetch connects and reads while every Dispatchers Default worker is busy`() = runBlocking<Unit> {
        val provider = server()
        service(listOf(config(provider))).use { service ->
            // As many as Default runs at once (one per core, at least 2), all held.
            val workers = Runtime.getRuntime().availableProcessors().coerceAtLeast(2)
            val busy = CountDownLatch(workers)
            val release = CountDownLatch(1)
            val cpuWork = CoroutineScope(Dispatchers.Default)
            repeat(workers) {
                cpuWork.launch {
                    busy.countDown()
                    release.await()
                }
            }
            try {
                assertTrue(busy.await(10, TimeUnit.SECONDS), "Default's workers never all got busy")
                val data = withTimeoutOrNull(3_000) { service.fetchSegment("x@test") }
                assertContentEquals(articles["<x@test>"], data, "the fetch got nothing in 3 s while Default was busy")
            } finally {
                release.countDown()
                cpuWork.cancel()
            }
        }
    }
}
