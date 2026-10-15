package io.skjaere.nzbstreamer

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readFully
import io.ktor.utils.io.toByteArray
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nntp.YencEvent
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.NamedSplits
import io.skjaere.nzbstreamer.stream.NntpPriority
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * A segment no provider has, in the middle of a streamed range: everything in front of it is
 * written, then the channel closes with the failure as its cause, and the job that started the
 * stream is not failed.
 */
class StreamFailureTest {
    private val servers = mutableListOf<FakeNntpServer>()

    // Hold back the answers to the segments a test wants late; opened in tearDown at the latest.
    private val gates = (0 until 6).map { CountDownLatch(1) }

    // Where the app starts its streams: a session coroutine of their own, whose failure is only logged.
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> })

    @AfterEach
    fun tearDown() {
        gates.forEach { it.countDown() }
        sessions.cancel()
        servers.forEach { it.close() }
    }

    private val segmentSize = 16 * 1024
    private val segments = (0 until 6).map { i -> ByteArray(segmentSize) { (it * 7 + i).toByte() } }
    private val missing = 3
    private val beforeGap = segments.take(missing).reduce { a, b -> a + b }

    private fun file() = NzbFile(
        poster = "test", date = 0, subject = "data.bin", groups = listOf("alt.binaries.test"),
        segments = segments.indices.map { NzbSegment(segmentSize.toLong(), it + 1, "seg$it@test") },
        yencHeaders = YencHeaders(line = 128, size = segmentSize.toLong() * segments.size, name = "data.bin", partEnd = segmentSize.toLong()),
    )

    private fun queue() = SegmentQueueService.createFileQueue(file(), 0, 0L)

    /** Serves every segment but [missing]; those [held] are answered once their gate opens. */
    private fun server(held: Set<Int> = emptySet(), asked: (Int) -> Unit = {}) = FakeNntpServer(body = { id ->
        val i = id.removePrefix("<seg").substringBefore('@').toInt()
        asked(i)
        if (i in held) gates[i].await(60, TimeUnit.SECONDS)
        if (i == missing) null else YencEncoder.encodeSinglePart(segments[i], "data.bin").data
    }).also { servers += it }

    private fun service(server: FakeNntpServer) = NntpStreamingService(
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)
    ).also { runBlocking { it.connect() } }

    /** Starts streaming every segment from a session coroutine, as the app does. */
    private suspend fun startStream(service: NntpStreamingService): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(service.launchStreamSegments(queue(), concurrency = 4, readAheadSegments = 6, name = "test"))
        }
        return writer.await()
    }

    /**
     * Everything readable from the channel, and why it ended: its close cause, thrown to a read
     * waiting for data or left in [ByteReadChannel.closedCause] (null at a plain end).
     */
    private suspend fun ByteReadChannel.readToEnd(): Pair<ByteArray, Throwable?> {
        val out = ByteArrayOutputStream()
        val buffer = ByteArray(8192)
        while (true) {
            val n = try {
                readAvailable(buffer)
            } catch (e: IOException) {
                return out.toByteArray() to e
            }
            if (n == -1) return out.toByteArray() to closedCause
            out.write(buffer, 0, n)
        }
    }

    private fun Throwable?.isMissingArticle() = generateSequence(this) { it.cause }.any { it is ArticleNotFoundException }

    private fun Throwable?.isConnectionFailure() = generateSequence(this) { it.cause }.any { it is NntpConnectionException }

    /**
     * Serves every segment, but the connection drops partway through [dropped]'s body each time
     * it is asked for; [missing] there is 430 instead.
     */
    private fun droppingServer(dropped: Set<Int>, missing: Set<Int> = emptySet()) = FakeNntpServer(
        body = { id -> id.segment().takeUnless { it in missing }?.let { YencEncoder.encodeSinglePart(segments[it], "data.bin").data } },
        dropAfter = { id -> 2_000.takeIf { id.segment() in dropped } },
    ).also { servers += it }

    private fun String.segment() = removePrefix("<seg").substringBefore('@').toInt()

    @Test
    fun `every byte before a missing article is delivered, then the read fails with its cause`() = runBlocking<Unit> {
        // The missing article's 430 comes once the reader has everything in front of it.
        val provider = server(held = setOf(missing))
        service(provider).use { service ->
            val writer = startStream(service)
            val received = ByteArray(beforeGap.size)
            val readAll = withTimeoutOrNull(5_000) { writer.channel.readFully(received) } != null
            gates[missing].countDown()
            assertTrue(readAll, "the bytes in front of the missing article never reached the reader")
            assertContentEquals(beforeGap, received)

            val (after, cause) = writer.channel.readToEnd()
            assertEquals(0, after.size)
            assertTrue(cause.isMissingArticle(), "closed with: $cause")
        }
    }

    @Test
    fun `a missing article fetched ahead does not end the stream before the segments in front of it`() = runBlocking<Unit> {
        // Segments 1 and 2 come late; the read-ahead learns segment 3 is missing long before.
        val askedFor5 = CompletableDeferred<Unit>()
        val provider = server(held = setOf(1, 2), asked = { if (it == 5) askedFor5.complete(Unit) })
        service(provider).use { service ->
            val writer = startStream(service)
            // Segment 5 is asked for once 0 and the missing 3 have freed their slots; a stream
            // that has stopped asks for nothing more.
            withTimeoutOrNull(2_000) { askedFor5.await() }

            suspend fun next(): ByteArray? = try {
                withTimeoutOrNull(5_000) { ByteArray(segmentSize).also { writer.channel.readFully(it) } }
            } catch (e: IOException) {
                null
            }
            val first = next()
            gates[1].countDown()
            val second = next()
            gates[2].countDown()
            assertContentEquals(segments[0], first, "segment 0 never reached the reader")
            assertContentEquals(segments[1], second, "segment 1 never reached the reader")

            val (rest, cause) = writer.channel.readToEnd()
            assertTrue(cause.isMissingArticle(), "closed with: $cause")
            // Ktor drops what the reader hasn't taken when a channel closes with a cause, so of
            // segment 2 (written just before the failure) only a start may have arrived.
            assertContentEquals(segments[2].copyOf(rest.size), rest)
        }
    }

    @Test
    fun `the failure reaches the reader without waiting for the downloads still in flight`() = runBlocking<Unit> {
        // Segment 4 never comes: cancelling its download waits out the pool's drain.
        val provider = server(held = setOf(4))
        service(provider).use { service ->
            val writer = startStream(service)
            val ended = withTimeoutOrNull(2_000) { writer.channel.readToEnd() }
            assertTrue(ended != null, "the failure waited for the download in flight")
            assertTrue(ended.second.isMissingArticle(), "closed with: ${ended.second}")
        }
    }

    /**
     * A connection that drops partway through an article says nothing about whether the provider
     * has it: the stream ends with the connection failure, which a reader can retry, not as a
     * missing article, which it would give up on (and a caller might blocklist the release for).
     */
    @Test
    fun `a connection that drops mid-article ends the stream with the connection failure`() = runBlocking<Unit> {
        service(droppingServer(dropped = setOf(missing))).use { service ->
            val writer = startStream(service)
            val (_, cause) = writer.channel.readToEnd()
            assertTrue(cause.isConnectionFailure() && !cause.isMissingArticle(), "closed with: $cause")
        }
    }

    @Test
    fun `an article one provider lacks and another's connection drops on is not called missing`() = runBlocking<Unit> {
        val lacking = droppingServer(dropped = emptySet(), missing = setOf(missing))
        val dropping = droppingServer(dropped = setOf(missing))
        val configs = listOf(lacking, dropping).map { NntpConfig("127.0.0.1", it.port, "", "", useTls = false, maxConnections = 4) }
        NntpStreamingService(configs).also { runBlocking { it.connect() } }.use { service ->
            val writer = startStream(service)
            val (_, cause) = writer.channel.readToEnd()
            assertTrue(cause.isConnectionFailure() && !cause.isMissingArticle(), "closed with: $cause")
        }
    }

    /** The same for a read through [NntpStreamingService.withClient] (preparing a release reads its files' first articles so). */
    @Test
    fun `a read one provider's connection drops on and another lacks is not called missing`() = runBlocking<Unit> {
        val dropping = droppingServer(dropped = setOf(missing))
        val lacking = droppingServer(dropped = emptySet(), missing = setOf(missing))
        val configs = listOf(dropping, lacking).map { NntpConfig("127.0.0.1", it.port, "", "", useTls = false, maxConnections = 4) }
        NntpStreamingService(configs).also { runBlocking { it.connect() } }.use { service ->
            val failure = runCatching {
                service.withClient(NntpPriority.PREPARE) { client ->
                    client.bodyYenc("<seg$missing@test>").collect { if (it is YencEvent.Body) it.data.toByteArray() }
                }
            }.exceptionOrNull()
            assertTrue(failure.isConnectionFailure() && !failure.isMissingArticle(), "failed with: $failure")
        }
    }

    @Test
    fun `a stream that fails does not fail the job that started it`() = runBlocking<Unit> {
        val provider = server()
        service(provider).use { service ->
            val nzb = NzbDocument(listOf(file()))
            val size = segmentSize.toLong() * segments.size
            coroutineScope {
                val sibling = launch { awaitCancellation() }
                val writer = ArchiveStreamingService(service)
                    .launchStreamFile(nzb, NamedSplits(listOf(SplitInfo(0, 0, size)), size, "data.bin"))
                val (_, cause) = writer.channel.readToEnd()
                assertTrue(cause.isMissingArticle(), "closed with: $cause")
                writer.job.join()
                assertTrue(sibling.isActive, "the caller's other work was cancelled")
                sibling.cancel()
            }
        }
    }

    @Test
    fun `streamSegments throws a missing article to its caller even when consume takes the early end for the end`() = runBlocking<Unit> {
        val provider = server()
        service(provider).use { service ->
            assertFailsWith<ArticleNotFoundException> {
                service.streamSegments(queue(), concurrency = 4, readAheadSegments = 6, name = "test") { channel ->
                    channel.readToEnd()
                }
            }
        }
    }
}
