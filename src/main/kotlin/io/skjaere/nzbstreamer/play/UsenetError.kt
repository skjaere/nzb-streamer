package io.skjaere.nzbstreamer.play

import io.skjaere.compressionutils.ArchiveVolumesException
import io.skjaere.compressionutils.MalformedRarArchiveException
import io.skjaere.compressionutils.MalformedSevenZipArchiveException
import io.skjaere.compressionutils.MissingArchiveVolumeException
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpAuthenticationException
import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nntp.NntpConnectionLostException
import io.skjaere.nntp.NntpProtocolException
import io.skjaere.nntp.YencDecodingException
import io.skjaere.nzbstreamer.enrichment.UnplacedArchiveVolumeException
import io.skjaere.nzbstreamer.stream.SegmentTimeoutException
import java.io.IOException
import java.security.cert.CertificateException

/** User-facing classification of failures coming out of the usenet libraries. */
sealed class UsenetError(open val detail: String?) {
    data class MissingArticle(override val detail: String?) : UsenetError(detail)
    data class AuthFailed(override val detail: String?) : UsenetError(detail)
    data class ConnectionFailed(override val detail: String?) : UsenetError(detail)
    data class Timeout(override val detail: String?) : UsenetError(detail)
    data class Corrupt(override val detail: String?) : UsenetError(detail)
    data class Protocol(override val detail: String?) : UsenetError(detail)
    data class Network(override val detail: String?) : UsenetError(detail)
    data object NotConfigured : UsenetError("No usenet provider configured")
    data class Unknown(override val detail: String?) : UsenetError(detail)
}

/**
 * Walks the cause chain: the libraries wrap fetch failures (stream channels close with
 * `ClosedByteChannelException(cause = …)`, enrichment wraps NNTP errors), so the most
 * specific known type anywhere in the chain wins. A [UsenetException] in the chain was
 * classified where it was thrown (NotConfigured has no cause to derive anything from), so
 * its classification comes first. nzb-streamer's own timeout is a [SegmentTimeoutException];
 * a TimeoutCancellationException is someone's cancellation, not a failure of the provider's.
 * Archive volumes whose headers show one missing are a missing article, as nzb-streamer reports
 * them; ones they can't put in order (two give one place) a broken release, and so is a volume
 * posted under a name that doesn't place it, which no PAR2 index names, and a 7z set whose headers
 * lie past the end of its volumes, as one that lacks a volume does.
 * A connection the server or the network ended under a command (an [NntpConnectionLostException]) is the network's, as
 * the raw I/O error it was before ktor-nntp-client wrapped it; the first connection failure in the chain decides,
 * so a connect that failed, also one whose connection was lost while logging in, stays a provider out of reach.
 */
fun Throwable.toUsenetError(): UsenetError {
    val chain = causeChain()
    fun <T : Throwable> find(type: Class<T>): T? = chain.firstOrNull(type::isInstance)?.let(type::cast)
    find(UsenetException::class.java)?.let { return it.error }
    find(ArticleNotFoundException::class.java)?.let { return UsenetError.MissingArticle(it.message) }
    find(MissingArchiveVolumeException::class.java)?.let { return UsenetError.MissingArticle(it.message) }
    find(NntpAuthenticationException::class.java)?.let { return if (it.rejectsLogin) UsenetError.AuthFailed(it.message) else UsenetError.ConnectionFailed(it.message) }
    chain.filterIsInstance<CertificateException>().firstNotNullOfOrNull { it.notForHost() }?.let { return UsenetError.ConnectionFailed(it) }
    find(NntpConnectionException::class.java)?.let { return if (it is NntpConnectionLostException) UsenetError.Network(it.message) else UsenetError.ConnectionFailed(it.message) }
    find(SegmentTimeoutException::class.java)?.let { return UsenetError.Timeout(it.message) }
    find(YencDecodingException::class.java)?.let { return UsenetError.Corrupt(it.message) }
    find(MalformedRarArchiveException::class.java)?.let { return UsenetError.Corrupt(it.message) }
    find(MalformedSevenZipArchiveException::class.java)?.let { return UsenetError.Corrupt(it.message) }
    find(ArchiveVolumesException::class.java)?.let { return UsenetError.Corrupt(it.message) }
    find(UnplacedArchiveVolumeException::class.java)?.let { return UsenetError.Corrupt(it.message) }
    find(NntpProtocolException::class.java)?.let { return UsenetError.Protocol(it.message) }
    find(IOException::class.java)?.let { return UsenetError.Network(it.message) }
    return UsenetError.Unknown(message ?: this::class.simpleName)
}

/**
 * This throwable and its causes, as far as 16 deep (a cause that is its own ends it): what [toUsenetError] searches, and
 * what an app searches for a cause of its own the libraries wrap (a volume missing from the NZB, a full disk).
 */
fun Throwable.causeChain(): List<Throwable> = generateSequence(this) { it.cause.takeIf { cause -> cause !== it } }.take(16).toList()

/**
 * Whether the server refused the username or password (RFC 4643's 481, or 482, or 502, with which some
 * providers turn a wrong password away), not this connection: most providers turn a login away with
 * "502 Too many connections" while the account has all the connections it may, some with 481 or 482
 * and words that say so, and 400 is the service being down. One with no answer to go by counts as refused.
 */
private val NntpAuthenticationException.rejectsLogin: Boolean
    get() = response?.code.let { it == null || it in REJECTED_LOGIN } && !TOO_MANY_CONNECTIONS.containsMatchIn(message.orEmpty())

private val REJECTED_LOGIN = setOf(481, 482, 502)

private val TOO_MANY_CONNECTIONS = Regex("""too many (connections|sessions|logins|users)|connection limit""", RegexOption.IGNORE_CASE)

/**
 * A provider turning a login away while the account has all the connections it may: it is up, and may serve in a moment.
 * For an engine that checks its providers, so a busy one is kept rather than left out as failing.
 */
val UsenetError.tooManyConnections: Boolean
    get() = this is UsenetError.ConnectionFailed && TOO_MANY_CONNECTIONS.containsMatchIn(detail.orEmpty())

/**
 * How ktor-nntp-client refuses a TLS server's certificate that doesn't name the host it was
 * reached by, e.g. an address it doesn't list where it names the provider's
 * host name. The app keeps that check on.
 */
private val CERTIFICATE_NOT_FOR = Regex("""^The server's certificate is not for (.+?); it names (.+)$""")

/** That refusal in the app's words, naming the host and what the certificate is for; null for any other. */
private fun CertificateException.notForHost(): String? = CERTIFICATE_NOT_FOR.find(message.orEmpty())?.destructured?.let { (host, names) ->
    "The provider's certificate isn't for $host" + if (names == "no host") "" else " (it names $names)"
}

/** Thrown by the engine-facing APIs so callers can recover the classification. */
class UsenetException(val error: UsenetError, cause: Throwable? = null) :
    IOException(error.detail ?: error::class.simpleName, cause)
