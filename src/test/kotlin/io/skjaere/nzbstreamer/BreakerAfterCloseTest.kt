package io.skjaere.nzbstreamer

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/**
 * A breaker that opened ends its cooldown on the breakers' own scheduler, whether its service is
 * still open or not. A closed service logs nothing of it: the warning would come long after, among
 * the lines of whatever runs then, as if about a pool still in use.
 */
class BreakerAfterCloseTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val logger = LoggerFactory.getLogger(NntpStreamingService::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @AfterEach
    fun tearDown() {
        logger.detachAppender(appender)
        servers.forEach { it.close() }
    }

    private fun NntpStreamingService.breakerOf(server: FakeNntpServer): CircuitBreaker {
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        field.isAccessible = true
        return (field.get(this) as CircuitBreakerRegistry).circuitBreaker("127.0.0.1:${server.port}")
    }

    @Test
    fun `a closed service logs nothing when an open breaker's cooldown ends`() {
        val server = FakeNntpServer().also { servers += it }
        val service = NntpStreamingService(
            listOf(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 1)),
            StreamingConfig(circuitBreakerCooldownMs = COOLDOWN_MS),
        )
        runBlocking { service.connect() }
        val breaker = service.breakerOf(server)
        // Listeners run in the order they were added: the service's first, then this one.
        val halfOpen = CountDownLatch(1)
        breaker.eventPublisher.onStateTransition { event ->
            if (event.stateTransition.toState == CircuitBreaker.State.HALF_OPEN) halfOpen.countDown()
        }
        breaker.transitionToOpenState()
        service.close()

        appender.start()
        logger.addAppender(appender)
        assertTrue(halfOpen.await(5, TimeUnit.SECONDS), "the breaker's cooldown didn't end")
        assertEquals(emptyList(), appender.list.map { it.formattedMessage })
    }

    private companion object {
        const val COOLDOWN_MS = 100L
    }
}
