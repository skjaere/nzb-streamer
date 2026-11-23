package io.skjaere.nzbstreamer.play

import io.ktor.utils.io.ClosedByteChannelException
import io.skjaere.compressionutils.ConflictingArchiveVolumesException
import io.skjaere.compressionutils.MalformedSevenZipArchiveException
import io.skjaere.compressionutils.MissingArchiveVolumeException
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nzbstreamer.enrichment.UnplacedArchiveVolumeException
import io.skjaere.nntp.NntpAuthenticationException
import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nntp.NntpConnectionLostException
import io.skjaere.nntp.NntpResponse
import io.skjaere.nntp.YencCrcMismatchException
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.IOException
import java.net.ConnectException
import java.net.SocketException
import java.security.cert.CertificateException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class UsenetErrorTest {
    @Test
    fun `article not found nested in IO wrapper maps to MissingArticle`() {
        val e = IOException("channel closed", RuntimeException("fetch", ArticleNotFoundException("<a@b> not found")))
        assertIs<UsenetError.MissingArticle>(e.toUsenetError())
    }

    @Test
    fun `specific NNTP errors map to their own kinds`() {
        assertIs<UsenetError.AuthFailed>(NntpAuthenticationException("481").toUsenetError())
        assertIs<UsenetError.ConnectionFailed>(NntpConnectionException("refused").toUsenetError())
        assertIs<UsenetError.Corrupt>(YencCrcMismatchException("crc", 1u, 2u).toUsenetError())
    }

    /**
     * nzb-streamer's own timeout (a segment no provider delivered in time) is a library exception,
     * not a cancellation; a stream's reader gets it inside the channel's close.
     */
    @Test
    fun `a segment that timed out maps to Timeout in the library's words`() {
        val fired = runBlocking { runCatching { withTimeout(1) { delay(1_000) } }.exceptionOrNull() }
        assertIs<TimeoutCancellationException>(fired)
        val words = "Segment <seg3@test> timed out after 5000ms on pool[0] (news.example.net:563)"
        val timeout = SegmentTimeoutException("seg3@test", 5_000, words, fired)
        assertEquals(UsenetError.Timeout(words), timeout.toUsenetError())
        assertEquals(UsenetError.Timeout(words), ClosedByteChannelException(timeout).toUsenetError())
    }

    /**
     * Archive volumes whose headers give one place in their set can't be put in order: the release
     * is broken, not the network. One their headers show missing is a missing article, as
     * nzb-streamer reports it.
     */
    @Test
    fun `archive volumes their headers can't order are Corrupt, and one they show missing a MissingArticle`() {
        val conflicting = ConflictingArchiveVolumesException(1, listOf("a8f7d6e5", "b91c2d3e"), "Archive volumes a8f7d6e5, b91c2d3e give the same place in their set in their headers (volume 2)")
        assertEquals(UsenetError.Corrupt(conflicting.message), conflicting.toUsenetError())
        assertEquals(UsenetError.Corrupt(conflicting.message), IOException("header walk", conflicting).toUsenetError())
        val missing = MissingArchiveVolumeException(2, listOf("a8f7d6e5"), "The archive set of a8f7d6e5 lacks its volume 3, as the headers of the 1 volumes it holds show")
        assertEquals(UsenetError.MissingArticle(missing.message), missing.toUsenetError())
    }

    /**
     * An archive volume posted under a name that doesn't give its place, which no PAR2 index names:
     * nzb-streamer can't open the release in order whichever provider serves it. The release is
     * broken as posted, not an error nothing knows.
     */
    @Test
    fun `an archive volume nothing places is Corrupt`() {
        val unplaced = UnplacedArchiveVolumeException(
            "x7f3k9q2.avi", "Archive volume 'x7f3k9q2.avi' was posted under a name that doesn't give its place in its archive, and no PAR2 index names it",
        )
        assertEquals(UsenetError.Corrupt(unplaced.message), unplaced.toUsenetError())
    }

    /**
     * A 7z set whose end header lies past the end of its volumes lacks one, or is cut short: the
     * release is broken, not the network, also as nzb-streamer's header walk wraps it.
     */
    @Test
    fun `a 7z archive whose headers lie past its end is Corrupt`() {
        val malformed = MalformedSevenZipArchiveException("The 7z archive's end header lies at bytes 200032 to 200090, past the end of its 134554 bytes")
        assertEquals(UsenetError.Corrupt(malformed.message), malformed.toUsenetError())
        assertEquals(UsenetError.Corrupt(malformed.message), IOException("header walk", malformed).toUsenetError())
    }

    @Test
    fun `a UsenetException keeps its own classification, also when wrapped`() {
        assertEquals(UsenetError.NotConfigured, UsenetException(UsenetError.NotConfigured).toUsenetError())
        val wrapped = IOException("channel closed", UsenetException(UsenetError.AuthFailed("481"), IOException("reset")))
        assertEquals(UsenetError.AuthFailed("481"), wrapped.toUsenetError())
    }

    /**
     * A login turned away while the account has all the connections it may is no rejected password:
     * most providers answer AUTHINFO with "502 Too many connections", some with 481 or 482 and words
     * that say so. Only a rejected login leaves a provider out, and it may be retried.
     */
    @Test
    fun `a login refused for too many connections is a connection failure, not a rejected login`() {
        fun refused(code: Int, text: String) = NntpAuthenticationException("AUTHINFO PASS failed: $code $text", NntpResponse(code, text))
        assertEquals(UsenetError.AuthFailed("AUTHINFO PASS failed: 481 Authentication failed"), refused(481, "Authentication failed").toUsenetError())
        assertEquals(UsenetError.AuthFailed("AUTHINFO PASS failed: 482 Authentication rejected"), refused(482, "Authentication rejected").toUsenetError())
        listOf(502 to "Too many connections", 481 to "Too many connections for this account", 482 to "Connection limit reached", 400 to "Service temporarily unavailable")
            .forEach { (code, text) ->
                assertEquals(UsenetError.ConnectionFailed("AUTHINFO PASS failed: $code $text"), refused(code, text).toUsenetError(), "$code $text")
            }
    }

    /**
     * Some providers turn a wrong password away with 502 rather than 481: a rejected login too, which
     * leaves the provider out, or every use would try it again. Only one that says it has too many
     * connections is the account being busy.
     */
    @Test
    fun `a login refused with 502 is a rejected login unless it says too many connections`() {
        fun refused(text: String) = NntpAuthenticationException("AUTHINFO PASS failed: 502 $text", NntpResponse(502, text))
        listOf("Authentication failed", "Permission denied", "Access denied").forEach { text ->
            assertEquals(UsenetError.AuthFailed("AUTHINFO PASS failed: 502 $text"), refused(text).toUsenetError(), text)
        }
        listOf("Too many connections", "Connection limit exceeded").forEach { text ->
            assertEquals(UsenetError.ConnectionFailed("AUTHINFO PASS failed: 502 $text"), refused(text).toUsenetError(), text)
        }
    }

    /** ktor-nntp-client checks that a TLS server's certificate names the host, and says what it names otherwise. */
    @Test
    fun `a certificate that isn't for the host says so in the app's words`() {
        fun refused(reason: String) = NntpConnectionException("Could not connect to 192.0.2.7:563: $reason", CertificateException(reason))
        assertEquals(
            UsenetError.ConnectionFailed("The provider's certificate isn't for 192.0.2.7 (it names news.example.net, *.example.net)"),
            refused("The server's certificate is not for 192.0.2.7; it names news.example.net, *.example.net").toUsenetError(),
        )
        assertEquals(
            UsenetError.ConnectionFailed("The provider's certificate isn't for 192.0.2.7"),
            refused("The server's certificate is not for 192.0.2.7; it names no host").toUsenetError(),
        )
        // Any other refusal, an untrusted certificate say, is the library's.
        val untrusted = "PKIX path building failed: unable to find valid certification path to requested target"
        assertEquals(UsenetError.ConnectionFailed("Could not connect to 192.0.2.7:563: $untrusted"), refused(untrusted).toUsenetError())
    }

    /**
     * A connection the server or the network ended under a command is the network's, as the raw I/O error it was before
     * ktor-nntp-client wrapped it, also inside the stream channel's close. A connect that failed is a provider out
     * of reach, also when a connection lost while logging in failed it: the pool wraps that as a failed connect.
     */
    @Test
    fun `a lost connection maps to Network, a connect that failed to ConnectionFailed`() {
        val lost = NntpConnectionLostException("Connection to news.example.net:563 failed: Connection reset", SocketException("Connection reset"))
        assertEquals(UsenetError.Network(lost.message), lost.toUsenetError())
        assertEquals(UsenetError.Network(lost.message), ClosedByteChannelException(lost).toUsenetError())
        val refused = NntpConnectionException("Could not connect to news.example.net:563: Connection refused", ConnectException("Connection refused"))
        assertEquals(UsenetError.ConnectionFailed(refused.message), refused.toUsenetError())
        val lostLoggingIn = NntpConnectionException("Connection to news.example.net:563 failed: Connection reset", lost)
        assertEquals(UsenetError.ConnectionFailed(lostLoggingIn.message), lostLoggingIn.toUsenetError())
    }

    @Test
    fun `plain IO maps to Network and anything else to Unknown`() {
        assertIs<UsenetError.Network>(IOException("reset").toUsenetError())
        assertIs<UsenetError.Unknown>(IllegalStateException("boom").toUsenetError())
    }
}
