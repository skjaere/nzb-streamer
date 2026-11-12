package io.skjaere.nzbstreamer

import io.skjaere.nntp.NntpProtocolException
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * A connection whose STAT got an error answer (`500 Command not recognized`, as the mock server
 * gave every STAT it had no mock for) serves the next BODY on it with that BODY's own answer.
 *
 * Seen once on a device: after STATs got 500, the next release's BODY for an
 * article the server had failed as "not found on any pool". A STAT cancelled while its answer was
 * on the way left its connection in the pool with that answer owed, so the BODY after it read
 * the STAT's 500; nzb-streamer then reported any failure as not found.
 */
class StatErrorConnectionReuseTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val services = mutableListOf<NntpStreamingService>()

    @AfterEach
    fun tearDown() {
        services.forEach { it.close() }
        servers.forEach { it.close() }
    }

    private val segments = (1..3).associate { "<seg$it@test>" to Random(it).nextBytes(20_000) }

    private fun server(statError: (String) -> String?) = FakeNntpServer(
        statError = statError,
        body = { id -> segments[id]?.let { YencEncoder.encodeSinglePart(it, "data.bin").data } },
    ).also { servers += it }

    private fun service(server: FakeNntpServer, maxConnections: Int) = NntpStreamingService(
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = maxConnections)
    ).also { runBlocking { it.connect() } }.also { services += it }

    /** Fetches every segment, [segments] at once, and checks each is its own. */
    private suspend fun fetchAll(service: NntpStreamingService) = coroutineScope {
        segments.map { (id, data) -> async { id to (service.fetchSegment(id.removeSurrounding("<", ">")) to data) } }
            .awaitAll()
            .forEach { (id, fetched) -> assertContentEquals(fetched.second, fetched.first, id) }
    }

    @Test
    fun `STATs cancelled while their 500 is on its way leave no connection out of step for the BODYs after them`() = runBlocking<Unit> {
        // The first STAT's 500 comes at once; the others' come later, while they are cancelled.
        val server = server { id ->
            if (id != "<first@test>") Thread.sleep(300)
            "500 Command not recognized"
        }
        val service = service(server, maxConnections = 3)

        // STATs at once, as an availability check sends them: the first 500 fails the check, which
        // cancels the others.
        val failure = runCatching {
            coroutineScope {
                listOf("first", "second", "third").forEach { id ->
                    launch { service.withClient { it.stat("<$id@test>") } }
                }
            }
        }.exceptionOrNull()
        assertIs<NntpProtocolException>(failure, "the check: $failure")
        assertEquals(3, server.statsRead.size, "STATs sent")

        // Then each connection in the pool serves a BODY: its own answer, not a STAT's 500.
        fetchAll(service)
        fetchAll(service)
    }

    @Test
    fun `a connection whose STAT got 500 serves the next BODY on it`() = runBlocking<Unit> {
        val server = server { "500 Command not recognized" }
        val service = service(server, maxConnections = 1)

        service.withClient { client -> assertIs<NntpProtocolException>(runCatching { client.stat("<a@test>") }.exceptionOrNull()) }
        fetchAll(service)

        assertEquals(1, server.connections.get(), "the BODYs went on the STAT's connection")
    }

    @Test
    fun `a connection whose availability check stopped with 500s owed serves the next BODY on it`() = runBlocking<Unit> {
        // The first article is missing, which ends the check; the STATs after it, answered 500,
        // are still owed then, and the pool reads them off before it hands the connection out.
        val server = server { id -> if (id == "<missing@test>") null else "500 Command not recognized" }
        val service = service(server, maxConnections = 1)

        val check = service.statUntilMissing(listOf("missing@test") + (1..40).map { "other$it@test" }, concurrency = 1)
        assertEquals(listOf(0), check.missing)
        fetchAll(service)

        assertEquals(1, server.connections.get(), "the BODYs went on the check's connection")
        assertTrue(server.statsRead.size > 1, "the check sent one STAT only")
    }
}
