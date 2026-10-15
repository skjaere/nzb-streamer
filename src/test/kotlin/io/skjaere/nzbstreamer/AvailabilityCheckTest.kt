package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.skjaere.nntp.NntpAuthenticationException
import io.skjaere.nntp.StatResult
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * The availability check stops at the first article that every provider lacks, instead of
 * waiting for the answers to everything it asked.
 */
class AvailabilityCheckTest {
    private val servers = mutableListOf<FakeNntpServer>()

    // Holds back the answers the tests want to come late; opened in tearDown at the latest.
    private val gate = CountDownLatch(1)

    @AfterEach
    fun tearDown() {
        gate.countDown()
        servers.forEach { it.close() }
    }

    private fun server(stat: (String) -> Boolean) = FakeNntpServer(stat = stat).also { servers += it }

    /** Answers 223 once [gate] opens. */
    private fun heldBack(): Boolean {
        gate.await(60, TimeUnit.SECONDS)
        return true
    }

    private fun service(vararg servers: FakeNntpServer, connections: Int) =
        service(servers.map { config(it, connections) })

    private fun service(configs: List<NntpConfig>) = NntpStreamingService(configs).also { runBlocking { it.connect() } }

    /** Logs in as [user] when one is given. */
    private fun config(server: FakeNntpServer, connections: Int = 2, user: String = "") =
        NntpConfig("127.0.0.1", server.port, user, if (user.isEmpty()) "" else "secret", useTls = false, maxConnections = connections)

    /** A provider that has every article but refuses every login. */
    private fun refusingLogin() = config(FakeNntpServer(stat = { true }, login = { _, _ -> false }).also { servers += it }, user = "user")

    /** A provider whose host name doesn't resolve. */
    private fun unresolvable() = NntpConfig("provider.invalid", 119, "", "", useTls = false, maxConnections = 2)

    private fun NntpStreamingService.breakerOf(config: NntpConfig): CircuitBreaker {
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        field.isAccessible = true
        return (field.get(this) as CircuitBreakerRegistry).circuitBreaker("${config.host}:${config.port}")
    }

    private fun nzb(ids: List<String>) = NzbDocument(
        listOf(
            NzbFile(
                poster = "test", date = 0, subject = "release.part01.rar", groups = listOf("alt.binaries.test"),
                segments = ids.mapIndexed { i, id -> NzbSegment(bytes = 1000, number = i + 1, articleId = id) },
            )
        )
    )

    private fun ids(n: Int) = (0 until n).map { "s$it@test" }

    /**
     * Every pooled connection (up to [connections]) answers a STAT with its own answer: none was
     * returned with answers of the check still owed. Holds them all at once so each is used.
     */
    private suspend fun assertConnectionsInStep(service: NntpStreamingService, connections: Int) = coroutineScope {
        val holding = AtomicInteger()
        val allHeld = CompletableDeferred<Unit>()
        (0 until connections).map { i ->
            async {
                service.withClient { client ->
                    val id = "<probe$i@test>"
                    val result = client.stat(id)
                    if (holding.incrementAndGet() == connections) allHeld.complete(Unit)
                    allHeld.await()
                    id to result
                }
            }
        }.awaitAll().forEach { (id, result) ->
            assertEquals(id, assertIs<StatResult.Found>(result, "answer to $id: $result").messageId)
        }
    }

    @Test
    fun `one provider - the first missing article ends the check without waiting for the rest`() = runBlocking<Unit> {
        // s0 is missing; the server holds back every other answer until the gate opens.
        val provider = server { id -> id != "<s0@test>" && heldBack() }
        service(provider, connections = 2).use { service ->
            val result = withTimeout(10_000) {
                VerificationService(service, concurrency = 2).verifySegments(nzb(ids(80)))
            }
            assertIs<VerificationResult.MissingArticles>(result)
            assertTrue(result.message.contains("s0@test"), result.message)
            assertEquals(1, result.missingCount)
            assertEquals(1, result.answeredCount)
            assertEquals(80, result.totalCount)

            gate.countDown()
            assertConnectionsInStep(service, connections = 2)
        }
    }

    @Test
    fun `one provider - STATs not yet sent when the first article is missing are never sent`() = runBlocking<Unit> {
        // Answers come at once, so stopping leaves the connection drained and pooled.
        val provider = server { id -> id != "<s0@test>" }
        service(provider, connections = 1).use { service ->
            val result = VerificationService(service, concurrency = 1).verifySegments(nzb(ids(1000)))
            assertIs<VerificationResult.MissingArticles>(result)
            assertEquals(1, result.missingCount)
            assertEquals(1, result.answeredCount)
            assertEquals(1000, result.totalCount)
            // One pipelining window (64) at most went out before the 430 came back.
            assertTrue(provider.statsRead.size <= 64, "STATs sent: ${provider.statsRead.size}")

            assertConnectionsInStep(service, connections = 1)
            assertEquals(1, provider.connections.get(), "the drained connection is reused")
        }
    }

    @Test
    fun `one provider - the 430 that ends the check sends no more STATs`() = runBlocking<Unit> {
        // s31's answer comes as half the window (64) is answered: where statPipelined tops it up.
        val provider = server { id -> id != "<s31@test>" }
        service(provider, connections = 1).use { service ->
            val result = VerificationService(service, concurrency = 1).verifySegments(nzb(ids(1000)))
            assertIs<VerificationResult.MissingArticles>(result)
            assertTrue(result.message.contains("s31@test"), result.message)

            // Once the drained connection is back, the server has read every STAT it was sent.
            assertConnectionsInStep(service, connections = 1)
            assertEquals(64, provider.statsRead.count { !it.startsWith("<probe") }, "STATs sent")
        }
    }

    @Test
    fun `several providers - an article missing on the first but on the second is not missing`() = runBlocking<Unit> {
        val primary = server { id -> id != "<s3@test>" }
        val fill = server { id -> id == "<s3@test>" }
        service(primary, fill, connections = 2).use { service ->
            val result = VerificationService(service, concurrency = 2).verifySegments(nzb(ids(40)))
            assertIs<VerificationResult.Success>(result)
            assertEquals(listOf("<s3@test>"), fill.statsRead)
        }
    }

    @Test
    fun `several providers - an article missing on the first is asked of the second at once`() = runBlocking<Unit> {
        val askedOfFill = CompletableDeferred<Unit>()
        val primary = server { id -> id != "<s0@test>" && heldBack() }
        val fill = server { askedOfFill.complete(Unit); true }
        service(primary, fill, connections = 2).use { service ->
            val check = async { VerificationService(service, concurrency = 2).verifySegments(nzb(ids(80))) }
            // The first provider still owes 79 answers.
            val asked = withTimeoutOrNull(5_000) { askedOfFill.await() }
            gate.countDown()
            assertIs<VerificationResult.Success>(check.await())
            assertNotNull(asked, "s0 was asked of the second provider only after the first answered everything")
            assertEquals(listOf("<s0@test>"), fill.statsRead)
        }
    }

    @Test
    fun `several providers - the first article missing on all of them ends the check`() = runBlocking<Unit> {
        val primary = server { id -> id != "<s0@test>" && heldBack() }
        val fill = server { false }
        service(primary, fill, connections = 2).use { service ->
            val result = withTimeout(10_000) {
                VerificationService(service, concurrency = 2).verifySegments(nzb(ids(80)))
            }
            assertIs<VerificationResult.MissingArticles>(result)
            assertTrue(result.message.contains("s0@test"), result.message)
            assertEquals(1, result.missingCount)
            assertEquals(1, result.answeredCount)
            assertEquals(listOf("<s0@test>"), fill.statsRead)

            gate.countDown()
            assertConnectionsInStep(service, connections = 2)
        }
    }

    // A provider that fails other than with a 430 (login refused, unreachable, its connection
    // dropping, an error answer, closed) passes the articles it hasn't answered on to the next
    // one at once. An article is missing only when every provider answered 430; one that no
    // provider could answer makes the check a failure with the cause. Dead providers cost
    // seconds at most.

    private val deadProviderBound = 5_000L

    private suspend fun NntpStreamingService.check(articles: Int = 40) = withTimeout(deadProviderBound) {
        VerificationService(this@check, concurrency = 2).verifySegments(nzb(ids(articles)))
    }

    private fun stats(articles: Int = 40) = ids(articles).map { "<$it>" }

    @Test
    fun `a first provider that refuses the login - its articles are asked of the second`() = runBlocking<Unit> {
        val fill = server { true }
        service(listOf(refusingLogin(), config(fill))).use { service ->
            assertIs<VerificationResult.Success>(service.check())
            assertEquals(stats().toSet(), fill.statsRead.toSet())
        }
    }

    @Test
    fun `a first provider whose host does not resolve - its articles are asked of the second`() = runBlocking<Unit> {
        val fill = server { true }
        service(listOf(unresolvable(), config(fill))).use { service ->
            assertIs<VerificationResult.Success>(service.check())
            assertEquals(stats().toSet(), fill.statsRead.toSet())
        }
    }

    @Test
    fun `a dead backup while the primary lacks an article - a failure with the backup's cause, not missing articles`() = runBlocking<Unit> {
        val primary = server { it != "<s3@test>" }
        service(listOf(config(primary), refusingLogin())).use { service ->
            val result = service.check()
            assertIs<VerificationResult.Failure>(result)
            assertIs<NntpAuthenticationException>(result.cause)
            assertTrue(result.message.contains("s3@test"), result.message)
        }
    }

    @Test
    fun `an unreachable backup while the primary lacks an article - a failure, not missing articles`() = runBlocking<Unit> {
        val primary = server { it != "<s3@test>" }
        service(listOf(config(primary), unresolvable())).use { service ->
            val result = service.check()
            assertIs<VerificationResult.Failure>(result)
            assertTrue(result.message.contains("s3@test"), result.message)
        }
    }

    @Test
    fun `a dead first provider and a second that lacks an article - not missing, a failure with the first one's cause`() = runBlocking<Unit> {
        val fill = server { it != "<s3@test>" }
        service(listOf(refusingLogin(), config(fill))).use { service ->
            val result = service.check()
            assertIs<VerificationResult.Failure>(result)
            assertIs<NntpAuthenticationException>(result.cause)
            assertTrue(result.message.contains("s3@test"), result.message)
        }
    }

    @Test
    fun `every provider dead - a failure with the cause`() = runBlocking<Unit> {
        service(listOf(refusingLogin(), unresolvable())).use { service ->
            val result = service.check()
            assertIs<VerificationResult.Failure>(result)
            assertIs<NntpAuthenticationException>(result.cause)
        }
    }

    @Test
    fun `a provider whose circuit breaker is open is not asked`() = runBlocking<Unit> {
        val primary = server { true }
        val fill = server { true }
        val configs = listOf(config(primary), config(fill))
        service(configs).use { service ->
            service.breakerOf(configs[0]).transitionToOpenState()
            assertIs<VerificationResult.Success>(service.check())
            assertEquals(emptyList(), primary.statsRead)
            assertEquals(stats().toSet(), fill.statsRead.toSet())
        }
    }

    @Test
    fun `a provider whose connections drop mid-batch - the articles it did not answer are asked of the next, once`() = runBlocking<Unit> {
        // Every connection to the first provider drops at s10, the pool's retry's too.
        val primary = FakeNntpServer(stat = { true }, dropOnStat = { it == "<s10@test>" }).also { servers += it }
        val fill = server { true }
        service(listOf(config(primary, connections = 1), config(fill, connections = 1))).use { service ->
            val progress = CopyOnWriteArrayList<Int>()
            val check = withTimeout(deadProviderBound) {
                service.statUntilMissing(ids(40), concurrency = 1, onProgress = { done, _ -> progress += done })
            }
            assertEquals(emptyList(), check.missing)
            assertEquals(40, check.answered)
            assertEquals((1..40).toList(), progress.sorted())
            assertEquals(stats().drop(10).toSet(), fill.statsRead.toSet())
            assertEquals(30, fill.statsRead.size)
        }
    }

    @Test
    fun `a STAT answered with an error - that article is asked of the next provider`() = runBlocking<Unit> {
        val primary = FakeNntpServer(stat = { true }, statError = { id -> "503 Overloaded".takeIf { id == "<s5@test>" } })
            .also { servers += it }
        val fill = server { true }
        service(listOf(config(primary, connections = 1), config(fill))).use { service ->
            assertIs<VerificationResult.Success>(service.check())
            assertEquals(listOf("<s5@test>"), fill.statsRead)
        }
    }

    @Test
    fun `a provider removed mid-check - its articles are asked of the next`() = runBlocking<Unit> {
        val asked = CompletableDeferred<Unit>()
        val primary = server { asked.complete(Unit); heldBack() }
        val fill = server { true }
        val configs = listOf(config(primary), config(fill))
        service(configs).use { service ->
            val check = async { service.check() }
            withTimeout(deadProviderBound) { asked.await() }
            withContext(Dispatchers.IO) { service.removePool(configs[0]) }
            assertIs<VerificationResult.Success>(check.await())
            assertEquals(stats().toSet(), fill.statsRead.toSet())
        }
    }

    @Test
    fun `statManyAcrossPools throws the cause when no provider can answer an article`() = runBlocking<Unit> {
        service(listOf(refusingLogin())).use { service ->
            val failure = runCatching { withTimeout(deadProviderBound) { service.statManyAcrossPools(ids(10), concurrency = 2) } }
            assertIs<NntpAuthenticationException>(failure.exceptionOrNull())
        }
    }
}
