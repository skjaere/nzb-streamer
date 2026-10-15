package io.skjaere.nzbstreamer

import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * An NZB can put a CR/LF in a segment id (written `&#13;&#10;`). Such an id is never sent (it can't
 * be a message-id), and it doesn't reach a log line or a reported message raw either, where it
 * would start a forged log line or split the message: it is escaped (`\r\n`).
 */
class SegmentIdEscapingTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val services = mutableListOf<NntpStreamingService>()

    // The log, as Logback's console appender writes it: to whatever System.out is at the time.
    private val output = ByteArrayOutputStream()
    private val console: PrintStream = System.out

    private val forgedId = "a@test\r\nINFO forged line"
    private val escaped = "a@test\\r\\nINFO forged line"

    @BeforeEach
    fun capture() {
        System.setOut(PrintStream(output, true, Charsets.UTF_8))
    }

    @AfterEach
    fun tearDown() {
        System.setOut(console)
        services.forEach { it.close() }
        servers.forEach { it.close() }
    }

    private fun String.hasLineBreak() = '\r' in this || '\n' in this

    /**
     * Log lines that mention the forged id: there are some, and each has all of it, escaped. A raw
     * CR/LF would have put "INFO forged line" on a line of its own, like a line of the log's own.
     */
    private fun assertLoggedEscaped() {
        System.out.flush()
        val lines = output.toString(Charsets.UTF_8).lines().filter { "forged line" in it }
        assertTrue(lines.isNotEmpty(), "nothing logged the id")
        assertEquals(emptyList(), lines.filterNot { escaped in it }, "log lines where the id isn't whole and escaped")
    }

    private fun assertEscaped(message: String?) {
        assertFalse(message.orEmpty().hasLineBreak(), "a raw CR/LF in: $message")
        assertTrue(escaped in message.orEmpty(), "the id isn't in: $message")
    }

    private fun config(server: FakeNntpServer, maxConnections: Int = 2) =
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = maxConnections)

    private fun server() = FakeNntpServer(stat = { true }).also { servers += it }

    private fun service(vararg configs: NntpConfig, streaming: StreamingConfig = StreamingConfig()) =
        NntpStreamingService(configs.toList(), streaming).also { runBlocking { it.connect() } }.also { services += it }

    @Test
    fun `an availability check reports and logs a segment id with a CR-LF escaped`() = runBlocking<Unit> {
        val nzb = """<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">""" +
            """<file poster="t" date="0" subject="file&#13;&#10;INFO forged line"><groups><group>a.b</group></groups><segments>""" +
            """<segment bytes="100" number="1">ok@test</segment>""" +
            """<segment bytes="100" number="2">a@test&#13;&#10;INFO forged line</segment>""" +
            """</segments></file></nzb>"""
        val server = server()

        NzbStreamer.fromConfig(config(server)).use { streamer ->
            val result = assertIs<VerificationResult.MissingArticles>(streamer.verifySegments(nzb.toByteArray()))
            assertLoggedEscaped()
            assertEscaped(result.message)
            assertEscaped(result.cause.message)
        }
        assertEquals(listOf("<ok@test>"), server.statsRead, "STATs sent")
    }

    @Test
    fun `a segment no pool has reports and logs its id with a CR-LF escaped`() = runBlocking<Unit> {
        val service = service(config(server()), config(server()))

        val failure = runCatching { service.fetchSegment(forgedId) }.exceptionOrNull()

        assertIs<ArticleNotFoundException>(failure)
        assertLoggedEscaped()
        assertEscaped(failure.message)
    }

    @Test
    fun `a segment that times out reports and logs its id with a CR-LF escaped`() = runBlocking<Unit> {
        // The pool's one connection is taken, so the fetch waits for it until its timeout.
        val service = service(config(server(), maxConnections = 1), streaming = StreamingConfig(segmentFetchTimeoutMs = 300))
        val held = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val holder = launch {
            service.withClient { held.complete(Unit); release.await() }
        }
        held.await()

        val failure = runCatching { service.fetchSegment(forgedId) }.exceptionOrNull()
        release.complete(Unit)
        holder.join()

        assertIs<SegmentTimeoutException>(failure)
        assertLoggedEscaped()
        assertEscaped(failure.message)
    }

    /** The names prepare logs for files it leaves out come from NZB subjects, which can hold one too. */
    @Test
    fun `prepare logs the subject name of a file it leaves out with a CR-LF escaped`() = runBlocking<Unit> {
        val nzb = """<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">""" +
            """<file poster="t" date="0" subject="&quot;Movie.mkv&quot; yEnc (1/1)"><groups><group>a.b</group></groups>""" +
            """<segments><segment bytes="100" number="1">movie@test</segment></segments></file>""" +
            """<file poster="t" date="0" subject="&quot;a@test&#13;&#10;INFO forged line.jpg&quot; yEnc (1/1)"><groups><group>a.b</group></groups>""" +
            """<segments><segment bytes="100" number="1">cover@test</segment></segments></file></nzb>"""
        val movie = YencEncoder.encodeSinglePart(ByteArray(10_000) { 7 }, "Movie.mkv").data
        val server = FakeNntpServer(body = { id -> movie.takeIf { id == "<movie@test>" } }).also { servers += it }

        NzbStreamer.fromConfig(config(server)).use { streamer ->
            assertIs<PrepareResult.Success>(streamer.prepare(nzb.toByteArray()))
        }
        assertLoggedEscaped()
    }
}
