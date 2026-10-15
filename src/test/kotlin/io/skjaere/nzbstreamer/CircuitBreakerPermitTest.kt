package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * A streaming fetch hands its circuit-breaker permit back however it ends. A HALF_OPEN breaker
 * has one permit; one never returned keeps the breaker half-open with none, and the pool is
 * skipped until the process restarts.
 */
class CircuitBreakerPermitTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val gate = CountDownLatch(1)

    @AfterEach
    fun tearDown() {
        gate.countDown()
        servers.forEach { it.close() }
    }

    private val articles = mapOf(
        "<x@test>" to ByteArray(4096) { it.toByte() },
        "<y@test>" to ByteArray(4096) { (it * 7).toByte() },
    )

    private fun encoded(id: String) = articles[id]?.let { YencEncoder.encodeSinglePart(it, "data.bin").data }

    private fun service(vararg servers: FakeNntpServer) = NntpStreamingService(
        servers.map { NntpConfig("127.0.0.1", it.port, "", "", useTls = false, maxConnections = 2) }
    ).also { runBlocking { it.connect() } }

    private fun NntpStreamingService.breakerOf(server: FakeNntpServer): CircuitBreaker {
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        field.isAccessible = true
        return (field.get(this) as CircuitBreakerRegistry).circuitBreaker("127.0.0.1:${server.port}")
    }

    private fun CircuitBreaker.halfOpen() {
        transitionToOpenState()
        transitionToHalfOpenState()
    }

    @Test
    fun `a fetch cancelled mid-body returns the half-open probe permit`() = runBlocking<Unit> {
        val bodyAsked = CompletableDeferred<Unit>()
        val primary = FakeNntpServer(body = { id ->
            if (id == "<x@test>") {
                bodyAsked.complete(Unit)
                gate.await(60, TimeUnit.SECONDS)
            }
            encoded(id)
        }).also { servers += it }
        val fill = FakeNntpServer().also { servers += it }
        service(primary, fill).use { service ->
            val breaker = service.breakerOf(primary)
            breaker.halfOpen()

            // The probe fetch is mid-body when the reader seeks away (cancels it).
            val fetch = launch { service.fetchSegment("x@test") }
            withTimeout(5_000) { bodyAsked.await() }
            fetch.cancel()
            gate.countDown()
            fetch.join()

            assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state)
            // The pool is tried again, rather than skipped for want of a permit.
            assertContentEquals(articles["<y@test>"], service.fetchSegment("y@test"))
            assertTrue("<y@test>" in primary.bodiesRead, "bodies asked of the first pool: ${primary.bodiesRead}")
            assertEquals(CircuitBreaker.State.CLOSED, breaker.state)
        }
    }

    @Test
    fun `a fetch that every pool's breaker refuses is tried with the breakers disabled`() = runBlocking<Unit> {
        val primary = FakeNntpServer(body = ::encoded).also { servers += it }
        val fill = FakeNntpServer(body = ::encoded).also { servers += it }
        service(primary, fill).use { service ->
            // Both half-open, their probe permits taken by calls still in flight elsewhere.
            listOf(primary, fill).map { service.breakerOf(it) }.forEach { breaker ->
                breaker.halfOpen()
                check(breaker.tryAcquirePermission()) { "test setup: the HALF_OPEN permit" }
            }

            assertContentEquals(articles["<x@test>"], service.fetchSegment("x@test"))
        }
    }

    @Test
    fun `a withClient block failing with an unexpected exception returns the half-open probe permit`() = runBlocking<Unit> {
        val primary = FakeNntpServer(stat = { true }).also { servers += it }
        val fill = FakeNntpServer(stat = { true }).also { servers += it }
        service(primary, fill).use { service ->
            val breaker = service.breakerOf(primary)
            breaker.halfOpen()

            assertFailsWith<IllegalStateException> { service.withClient { error("not the pool's fault") } }

            // The next call is the half-open probe of the first pool, not skipped past it.
            service.withClient { it.stat("<probe@test>") }
            assertEquals(listOf("<probe@test>"), primary.statsRead)
        }
    }
}
