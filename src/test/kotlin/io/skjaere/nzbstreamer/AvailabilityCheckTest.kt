package io.skjaere.nzbstreamer

import io.skjaere.nntp.StatResult
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
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

    private fun service(vararg servers: FakeNntpServer, connections: Int) = NntpStreamingService(
        servers.map { NntpConfig("127.0.0.1", it.port, "", "", useTls = false, maxConnections = connections) }
    ).also { runBlocking { it.connect() } }

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
}
