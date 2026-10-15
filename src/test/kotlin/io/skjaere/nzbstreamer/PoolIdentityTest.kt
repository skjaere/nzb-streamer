package io.skjaere.nzbstreamer

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Two accounts on one server (a second account, or one provider listed twice) are two pools: each
 * has its own circuit breaker and its own metrics, and addPool takes the second one.
 */
class PoolIdentityTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val registry = SimpleMeterRegistry()

    @BeforeEach
    fun setUp() {
        Metrics.addRegistry(registry)
    }

    @AfterEach
    fun tearDown() {
        Metrics.removeRegistry(registry)
        servers.forEach { it.close() }
    }

    private val articles = mapOf(
        "<x@test>" to ByteArray(4096) { it.toByte() },
        "<y@test>" to ByteArray(4096) { (it * 7).toByte() },
    )

    private fun encoded(id: String) = articles[id]?.let { YencEncoder.encodeSinglePart(it, "data.bin").data }

    /** Serves every article; logs in only the users [accepts] takes. */
    private fun server(accepts: (String) -> Boolean = { true }) =
        FakeNntpServer(body = ::encoded, login = { user, _ -> accepts(user) }).also { servers += it }

    private fun account(server: FakeNntpServer, user: String) =
        NntpConfig("127.0.0.1", server.port, user, "secret", useTls = false, maxConnections = 2)

    private fun service(configs: List<NntpConfig>, streaming: StreamingConfig = StreamingConfig()) =
        NntpStreamingService(configs, streaming).also { runBlocking { it.connect() } }

    @Test
    fun `addPool takes a second account on the same server, but not the same config twice`() = runBlocking<Unit> {
        val shared = server()
        service(listOf(account(shared, "first"))).use { service ->
            service.addPool(account(shared, "second"))
            assertEquals(listOf("first", "second"), service.getPoolConfigs().map { it.username })
            assertFailsWith<IllegalArgumentException> { service.addPool(account(shared, "second")) }
        }
    }

    @Test
    fun `one account's failures do not open the circuit breaker of another account on the same server`() = runBlocking<Unit> {
        val shared = server(accepts = { it == "good" })
        val fill = server()
        // One failure opens a breaker.
        val streaming = StreamingConfig(circuitBreakerFailureThreshold = 1)
        service(listOf(account(shared, "bad"), account(shared, "good"), account(fill, "fill")), streaming).use { service ->
            assertContentEquals(articles["<x@test>"], service.fetchSegment("x@test"))
            // The bad account's breaker is open now, the good account's closed: it serves y too.
            assertContentEquals(articles["<y@test>"], service.fetchSegment("y@test"))
            assertEquals(listOf("<x@test>", "<y@test>"), shared.bodiesRead)
            assertEquals(emptyList(), fill.bodiesRead)
        }
    }

    @Test
    fun `a pool added in place of a removed one on the same server starts with a closed breaker`() = runBlocking<Unit> {
        val shared = server(accepts = { it == "good" })
        val fill = server()
        val streaming = StreamingConfig(circuitBreakerFailureThreshold = 1)
        val bad = account(shared, "bad")
        service(listOf(bad, account(fill, "fill")), streaming).use { service ->
            // The bad account's failure opens its breaker; the fill serves x.
            assertContentEquals(articles["<x@test>"], service.fetchSegment("x@test"))
            service.removePool(bad)
            service.addPool(account(shared, "good"))
            // Pools are asked in order: the fill goes back behind the new account.
            service.removePool(account(fill, "fill"))
            service.addPool(account(fill, "fill"))
            assertContentEquals(articles["<y@test>"], service.fetchSegment("y@test"))
            assertEquals(listOf("<y@test>"), shared.bodiesRead)
        }
    }

    @Test
    fun `two accounts on one server have their own pool metrics`() = runBlocking<Unit> {
        val shared = server(accepts = { it == "good" })
        val bad = account(shared, "bad")
        val good = account(shared, "good")
        val first = "127.0.0.1:${shared.port}"
        val second = "127.0.0.1:${shared.port}#2"
        service(listOf(bad, good)).use { service ->
            fun sizeGauges() = registry.find("nntp.pool.size").gauges()
                .mapNotNull { it.id.getTag("pool.name") }.filter { it.startsWith(first) }.toSet()
            assertEquals(setOf(first, second), sizeGauges())

            // The good account (the second pool) serves x: its BODY is timed under its own name.
            assertContentEquals(articles["<x@test>"], service.fetchSegment("x@test"))
            assertEquals(1L, registry.find("nntp.body.duration").tag("pool.name", second).timer()?.count())

            // Closing one pool removes its metrics only.
            service.removePool(good)
            assertEquals(setOf(first), sizeGauges())
        }
    }
}
