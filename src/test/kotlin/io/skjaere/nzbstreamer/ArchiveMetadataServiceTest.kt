package io.skjaere.nzbstreamer

import io.skjaere.compressionutils.MalformedRarArchiveException
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.metadata.ArchiveMetadataService
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.RapidYenc
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class ArchiveMetadataServiceTest {

    private fun createNzb(vararg articleIds: String): NzbDocument {
        val files = articleIds.map { articleId ->
            NzbFile(
                poster = "test",
                date = 0,
                subject = "test file",
                groups = listOf("alt.binaries.test"),
                segments = listOf(NzbSegment(bytes = 1000, number = 1, articleId = articleId))
            )
        }
        return NzbDocument(files)
    }

    private fun writeYencResponse(out: OutputStream, name: String, data: ByteArray) {
        val encoded = RapidYenc.encode(data)
        val crc = RapidYenc.crc32(data)
        out.write("222 body follows\r\n".toByteArray())
        out.write("=ybegin line=128 size=${data.size} name=$name\r\n".toByteArray())
        out.write(encoded)
        if (!String(encoded, Charsets.ISO_8859_1).endsWith("\r\n")) {
            out.write("\r\n".toByteArray())
        }
        out.write("=yend size=${data.size} crc32=${crc.toString(16).padStart(8, '0')}\r\n".toByteArray())
        out.write(".\r\n".toByteArray())
        out.flush()
    }

    private fun startServer(
        handler: (BufferedReader, OutputStream) -> Unit
    ): ServerSocket {
        val serverSocket = ServerSocket(0)
        Thread {
            try {
                val client = serverSocket.accept()
                client.use { socket ->
                    val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                    val out = socket.getOutputStream()
                    out.write("200 welcome\r\n".toByteArray())
                    out.flush()
                    handler(reader, out)
                }
            } catch (_: Exception) {
            }
        }.start()
        return serverSocket
    }

    private fun createStreamingService(port: Int): NntpStreamingService {
        val config = NntpConfig(
            host = "localhost",
            port = port,
            username = "",
            password = "",
            useTls = false,
            maxConnections = 1
        )
        val service = NntpStreamingService(config)
        runBlocking { service.connect() }
        return service
    }

    /**
     * Regression guard. Production incident: a caller marked 21 NZBs as
     * downloaded with zero files behind them, because
     * `prepare()` returned `Success(Raw(emptyList()))` on the empty-files path
     * (ArchiveMetadataService.kt — the `if (enrichedNzb.files.isEmpty())` branch
     * inside the EnrichmentResult.Success arm) instead of surfacing the missing
     * articles. Default `verifySegments=false` is the prod config, so the only
     * line of defence here is enrichment correctly bubbling 430 → MissingArticles.
     *
     * Pin the wire-level contract: when the NNTP server returns "430 No Such
     * Article Found" for every BODY, prepare() MUST end up at MissingArticles —
     * never at Success, regardless of how many files are in the underlying NZB.
     */
    @Test
    fun `prepare returns MissingArticles when server responds 430 to all BODY requests`() = runBlocking<Unit> {
        val server = startServer { reader, out ->
            while (true) {
                val line = reader.readLine() ?: break
                if (line.startsWith("BODY")) {
                    out.write("430 No Such Article Found\r\n".toByteArray())
                    out.flush()
                }
            }
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val metadataService = ArchiveMetadataService(streamingService)
                val nzb = createNzb("missing@test")

                val result = metadataService.prepare(nzb)

                assertIs<PrepareResult.MissingArticles>(result)
            }
        }
    }

    @Test
    fun `prepare returns success with empty entries when archive type cannot be detected`() = runBlocking {
        // Garbage data that does not match any archive signature
        val data = ByteArray(1024) { 0x42 }
        val server = startServer { reader, out ->
            // Handle all BODY requests (enrichment + seekable stream reads)
            while (true) {
                val line = reader.readLine() ?: break
                if (line.startsWith("BODY")) {
                    writeYencResponse(out, "data.bin", data)
                }
            }
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val metadataService = ArchiveMetadataService(streamingService)
                val nzb = createNzb("article1@test")

                val result = metadataService.prepare(nzb)

                val success = assertIs<PrepareResult.Success>(result)
                assertIs<ExtractedMetadata.Raw>(success.metadata)
                assertTrue(success.metadata.response.entries.isEmpty())
                Unit
            }
        }
    }

    // prepare reports what goes wrong while it reads the archive's headers (the header walk) as a
    // result, as it does for enrichment, and lets only its caller's cancellation through.

    /** A file posted under [name] (in its subject) as articles [ids]. */
    private fun file(name: String, ids: List<String>) = NzbFile(
        poster = "test",
        date = 0,
        subject = "\"$name\" yEnc (1/${ids.size})",
        groups = listOf("alt.binaries.test"),
        segments = ids.mapIndexed { i, id -> NzbSegment(bytes = 1000, number = i + 1, articleId = id) },
    )

    /** [data] posted as [name] in articles of [partSize] bytes: article id (in brackets) → yEnc article. */
    private fun post(data: ByteArray, name: String, partSize: Int = data.size): Map<String, ByteArray> =
        YencEncoder.encodeMultiPart(data, name, partSize).withIndex()
            .associate { (i, part) -> "<$name-${i + 1}@test>" to part.data }

    private fun nzbOf(name: String, articles: Map<String, ByteArray>) =
        NzbDocument(listOf(file(name, articles.keys.map { it.removeSurrounding("<", ">") })))

    private fun service(server: FakeNntpServer) = NntpStreamingService(
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 2)
    ).also { runBlocking { it.connect() } }

    private fun Throwable.causes(): List<Throwable> = generateSequence(this) { it.cause.takeIf { c -> c !== it } }.toList()

    // A RAR5 signature, then a header whose size is smaller than its own fields.
    private val malformedRar = byteArrayOf(0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00) +
        byteArrayOf(0, 0, 0, 0, 1, 1, 0) + ByteArray(1024)

    @Test
    fun `prepare returns Failure with the cause when the archive is malformed`() = runBlocking<Unit> {
        val articles = post(malformedRar, "release.rar")
        FakeNntpServer(body = { articles[it] }).use { server ->
            service(server).use { streaming ->
                val result = ArchiveMetadataService(streaming).prepare(nzbOf("release.rar", articles))

                val failure = assertIs<PrepareResult.Failure>(result, "prepare: $result")
                assertTrue(failure.cause.causes().any { it is MalformedRarArchiveException }, "cause: ${failure.cause}")
            }
        }
    }

    @Test
    fun `prepare returns Failure with the cause when the connection drops while it reads the archive headers`() = runBlocking<Unit> {
        val articles = post(malformedRar, "release.rar")
        val bodies = AtomicInteger()
        FakeNntpServer(
            body = { articles[it] },
            // Enrichment's BODY is served whole; the header walk's (and the pool's retry) drop.
            dropAfter = { if (bodies.incrementAndGet() > 1) 20 else null },
        ).use { server ->
            service(server).use { streaming ->
                val result = ArchiveMetadataService(streaming).prepare(nzbOf("release.rar", articles))

                val failure = assertIs<PrepareResult.Failure>(result, "prepare: $result")
                assertTrue(bodies.get() > 1, "the header walk never fetched")
                assertTrue(failure.cause.causes().any { it is IOException || it is NntpConnectionException }, "cause: ${failure.cause}")
            }
        }
    }

    @Test
    fun `prepare returns Failure with the cause when the connection drops while it enriches`() = runBlocking<Unit> {
        val articles = post(malformedRar, "release.rar")
        FakeNntpServer(body = { articles[it] }, dropAfter = { 20 }).use { server ->
            service(server).use { streaming ->
                val result = ArchiveMetadataService(streaming).prepare(nzbOf("release.rar", articles))

                val failure = assertIs<PrepareResult.Failure>(result, "prepare: $result")
                assertTrue(failure.cause.causes().any { it is IOException || it is NntpConnectionException }, "cause: ${failure.cause}")
            }
        }
    }

    @Test
    fun `prepare returns MissingArticles when an article the header walk needs is missing`() = runBlocking<Unit> {
        // A 7z keeps its header at the end: in the last of its 16 KiB articles, which is missing.
        val volume = ArchiveGenerator.generate(Random(5).nextBytes(100_000), 1, ContainerType.SEVENZIP, "movie.mkv").single()
        val articles = post(volume.data, volume.filename, partSize = 16 * 1024)
        val last = articles.keys.last()
        FakeNntpServer(body = { id -> articles[id].takeUnless { id == last } }).use { server ->
            service(server).use { streaming ->
                val result = ArchiveMetadataService(streaming).prepare(nzbOf(volume.filename, articles))

                val missing = assertIs<PrepareResult.MissingArticles>(result, "prepare: $result")
                assertTrue(missing.cause.causes().any { it is ArticleNotFoundException }, "cause: ${missing.cause}")
                assertTrue(last in server.bodiesRead, "the header walk never asked for $last")
            }
        }
    }

    /**
     * A coroutine whose sibling fails is cancelled with that failure as its cancellation's cause.
     * An NNTP failure there is someone else's: prepare is cancelled and gives no result.
     */
    @Test
    fun `prepare rethrows its caller's cancellation while it reads the archive headers`() = runBlocking<Unit> {
        val articles = post(malformedRar, "release.rar")
        val walking = CompletableDeferred<Unit>()
        val answer = CountDownLatch(1)
        val bodies = AtomicInteger()
        FakeNntpServer(body = { id ->
            if (bodies.incrementAndGet() > 1) {
                walking.complete(Unit)
                answer.await(10, TimeUnit.SECONDS)
            }
            articles[id]
        }).use { server ->
            service(server).use { streaming ->
                val siblingFailure = NntpConnectionException("another provider dropped")
                var returned: PrepareResult? = null
                var thrown: Throwable? = null
                val failure = runCatching {
                    coroutineScope {
                        launch {
                            try {
                                returned = ArchiveMetadataService(streaming).prepare(nzbOf("release.rar", articles))
                            } catch (e: Throwable) {
                                thrown = e
                                throw e
                            }
                        }
                        // Fails while the header walk waits for its BODY, then lets the server answer.
                        launch {
                            walking.await()
                            answer.countDown()
                            throw siblingFailure
                        }
                    }
                }.exceptionOrNull()
                assertSame(siblingFailure, failure)
                assertNull(returned, "a cancelled prepare returned ${returned?.let { it::class.simpleName }}")
                assertIs<CancellationException>(thrown)
            }
        }
    }
}
