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
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * What the streaming service must not hold up while it waits: fetches from the other pools while
 * a pool is being removed.
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
}
