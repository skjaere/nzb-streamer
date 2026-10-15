package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.skjaere.nntp.NntpAuthenticationException
import io.skjaere.nntp.StatResult
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.firstCauseOfType
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A provider that rejects the login (481, or 502 when the account has too many connections) fails
 * only its own pool's attempt: [NntpStreamingService.withClient], which prepare fetches each file's
 * first article with, goes on to the next provider, and the rejection surfaces only when no
 * provider serves the call. The pool throws a rejected login at once (it used to time out waiting
 * for a connection), so a first provider whose login is rejected must not fail the call itself.
 */
class LoginRejectedFallbackTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val articles = mutableMapOf<String, ByteArray>()
    private val files = mutableListOf<String>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    /** A provider with every article posted, and every STAT found unless [has] says otherwise. */
    private fun provider(has: (String) -> Boolean = { true }) =
        FakeNntpServer(stat = has, body = { id -> articles[id]?.takeIf { has(id) } }).also { servers += it }

    /** A provider with every article, that answers every login's AUTHINFO PASS with [reply]. */
    private fun rejecting(reply: String) =
        FakeNntpServer(stat = { true }, body = { articles[it] }, loginError = { reply }).also { servers += it }

    private fun config(server: FakeNntpServer) =
        NntpConfig("127.0.0.1", server.port, "user", "secret", useTls = false, maxConnections = 2)

    private fun service(vararg servers: FakeNntpServer) =
        NntpStreamingService(servers.map(::config)).also { runBlocking { it.connect() } }

    private fun streamer(vararg servers: FakeNntpServer) = NzbStreamer.fromConfig(servers.map(::config))

    private fun NntpStreamingService.breakerOf(server: FakeNntpServer): CircuitBreaker {
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        field.isAccessible = true
        return (field.get(this) as CircuitBreakerRegistry).circuitBreaker("127.0.0.1:${server.port}")
    }

    /** Posts [name] in articles of 32 KiB and returns its first article's id (in brackets). */
    private fun post(name: String, size: Int = 50_000): String {
        val parts = YencEncoder.encodeMultiPart(Random(name.hashCode()).nextBytes(size), name, 32 * 1024)
        val ids = parts.indices.map { "<$name-${it + 1}@test>" }
        ids.zip(parts) { id, part -> articles[id] = part.data }
        val segments = ids.withIndex().joinToString("") { (i, id) ->
            """<segment bytes="${parts[i].data.size}" number="${i + 1}">${id.removeSurrounding("<", ">")}</segment>"""
        }
        files += """<file poster="t" date="0" subject="&quot;$name&quot; yEnc (1/${parts.size})">""" +
            """<groups><group>alt.binaries.test</group></groups><segments>$segments</segments></file>"""
        return ids.first()
    }

    private fun nzb() = ("""<?xml version="1.0" encoding="UTF-8"?>""" +
        """<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""").toByteArray()

    private fun assertWithClientServedBySecond(reply: String) = runBlocking<Unit> {
        val fill = provider()
        service(rejecting(reply), fill).use { service ->
            val result = withTimeout(10_000) { service.withClient { it.stat("<a@test>") } }
            assertIs<StatResult.Found>(result)
            assertEquals(listOf("<a@test>"), fill.statsRead)
        }
    }

    private fun assertPrepareServedBySecond(reply: String) = runBlocking<Unit> {
        val first = post("Movie.2020.mkv")
        val fill = provider()
        streamer(rejecting(reply), fill).use { streamer ->
            val prepared = withTimeout(10_000) { streamer.prepare(nzb()) }
            assertIs<PrepareResult.Success>(prepared, "prepare")
            assertTrue(first in fill.bodiesRead, "the second provider wasn't asked for $first")
        }
    }

    @Test
    fun `a first provider that rejects the login - withClient is served by the second`() {
        assertWithClientServedBySecond("481 Authentication failed")
    }

    @Test
    fun `a first provider with too many connections - withClient is served by the second`() {
        assertWithClientServedBySecond("502 Too many connections")
    }

    @Test
    fun `a first provider that rejects the login - prepare is served by the second`() {
        assertPrepareServedBySecond("481 Authentication failed")
    }

    @Test
    fun `a first provider with too many connections - prepare is served by the second`() {
        assertPrepareServedBySecond("502 Too many connections")
    }

    @Test
    fun `a rejected login counts as a failure on that provider's circuit breaker`() = runBlocking<Unit> {
        val primary = rejecting("481 Authentication failed")
        service(primary, provider()).use { service ->
            withTimeout(10_000) { service.withClient { it.stat("<a@test>") } }
            assertEquals(1, service.breakerOf(primary).metrics.numberOfFailedCalls)
        }
    }

    @Test
    fun `every provider rejects the login - withClient throws the rejection`() = runBlocking<Unit> {
        service(rejecting("481 Authentication failed"), rejecting("502 Too many connections")).use { service ->
            withTimeout(10_000) {
                assertFailsWith<NntpAuthenticationException> { service.withClient { it.stat("<a@test>") } }
            }
        }
    }

    @Test
    fun `every provider rejects the login - prepare fails with the rejection`() = runBlocking<Unit> {
        post("Movie.2020.mkv")
        streamer(rejecting("481 Authentication failed"), rejecting("502 Too many connections")).use { streamer ->
            val prepared = withTimeout(10_000) { streamer.prepare(nzb()) }
            assertIs<PrepareResult.Failure>(prepared, "prepare")
            assertNotNull(prepared.cause.firstCauseOfType<NntpAuthenticationException>(), "cause: ${prepared.cause}")
        }
    }

    @Test
    fun `the first provider rejects the login and the second lacks the article - a failure, not missing articles`() =
        runBlocking<Unit> {
            val first = post("Movie.2020.mkv")
            streamer(rejecting("481 Authentication failed"), provider { it != first }).use { streamer ->
                val prepared = withTimeout(10_000) { streamer.prepare(nzb()) }
                assertIs<PrepareResult.Failure>(prepared, "prepare")
                assertNotNull(prepared.cause.firstCauseOfType<NntpAuthenticationException>(), "cause: ${prepared.cause}")
            }
        }
}
