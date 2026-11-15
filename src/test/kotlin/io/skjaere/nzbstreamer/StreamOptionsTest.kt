package io.skjaere.nzbstreamer

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.toByteArray
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.skjaere.compressionutils.Rar5Crypto
import io.skjaere.compressionutils.SplitEncryptionInfo
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nntp.NntpConnectionLostException
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.MissingArticlePolicy
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.CacheUse
import io.skjaere.nzbstreamer.stream.Gap
import io.skjaere.nzbstreamer.stream.NamedSplits
import io.skjaere.nzbstreamer.stream.NntpPriority
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentCache
import io.skjaere.nzbstreamer.stream.StreamOptions
import io.skjaere.nzbstreamer.stream.StreamOptions.Companion.BACKGROUND_PRIORITY
import io.skjaere.nzbstreamer.stream.StreamableFile
import io.skjaere.nzbstreamer.stream.Tries
import io.skjaere.nzbstreamer.stream.withConnectionWithin
import io.skjaere.yenc.YencEncoder
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Options for one stream ([StreamOptions]): a download saving a file beside a play fetches below every play step,
 * reads the segment cache without filling it, has its own missing-article policy, concurrency and read-ahead, and
 * waits for a connection as long as it takes without counting against a provider. Without them, or with
 * `StreamOptions()`, a stream fetches as every stream always has.
 */
class StreamOptionsTest {
    private val servers = mutableListOf<FakeNntpServer>()

    // Where an Android app starts its streams.
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> })

    // Holds back the answers a test wants to come later; opened in tearDown at the latest.
    private val gate = CountDownLatch(1)

    // The pools' gauges and timers say what waits for their connections, and at what priority each was lent.
    private val registry = SimpleMeterRegistry()

    /** Every command the servers read, in order: "BODY <id>" or "STAT <id>". */
    private val commands = CopyOnWriteArrayList<String>()

    @BeforeEach
    fun setUp() {
        Metrics.addRegistry(registry)
    }

    @AfterEach
    fun tearDown() {
        gate.countDown()
        sessions.cancel()
        servers.forEach { it.close() }
        Metrics.removeRegistry(registry)
    }

    private val segmentSize = 16 * 1024

    private fun bytesOf(name: String, size: Int = segmentSize) = ByteArray(size) { (it * 7 + name.hashCode()).toByte() }

    private fun contentOf(vararg names: String, size: Int = segmentSize) = names.map { bytesOf(it, size) }.reduce(ByteArray::plus)

    private fun nameOf(id: String) = id.removePrefix("<").substringBefore('@')

    /**
     * Serves `<name@test>` as [bytesOf] name, [size] bytes, and finds it to a STAT, but answers 430 for the names
     * [lacks] picks; [answer] runs before each BODY's answer and may block.
     */
    private fun server(size: Int = segmentSize, lacks: (String) -> Boolean = { false }, answer: (String) -> Unit = {}) = FakeNntpServer(
        stat = { id ->
            commands += "STAT $id"
            !lacks(nameOf(id))
        },
        body = { id ->
            commands += "BODY $id"
            val name = nameOf(id)
            answer(name)
            if (lacks(name)) null else YencEncoder.encodeSinglePart(bytesOf(name, size), "Harbor.Lights.mkv").data
        },
    ).also { servers += it }

    private fun config(server: FakeNntpServer, connections: Int) =
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = connections)

    private fun streamer(
        vararg servers: FakeNntpServer,
        connections: Int = 4,
        streaming: StreamingConfig = StreamingConfig(),
        cache: SegmentCache? = null,
    ) = NzbStreamer.fromConfig(servers.map { config(it, connections) }, streaming, segmentCache = cache, closeSegmentCache = false)

    private fun service(vararg servers: FakeNntpServer, connections: Int = 4, streaming: StreamingConfig = StreamingConfig()) =
        NntpStreamingService(servers.map { config(it, connections) }, streaming).also { runBlocking { it.connect() } }

    /** A file posted in one article of [size] bytes per name. */
    private fun nzb(vararg names: String, size: Int = segmentSize) = NzbDocument(
        listOf(
            NzbFile(
                poster = "test", date = 0, subject = "Harbor.Lights.mkv", groups = listOf("alt.binaries.test"),
                segments = names.mapIndexed { i, name -> NzbSegment(size.toLong(), i + 1, "$name@test") },
                yencHeaders = YencHeaders(line = 128, size = names.size.toLong() * size, name = "Harbor.Lights.mkv", partEnd = size.toLong()),
            )
        )
    )

    private fun NzbDocument.file() = StreamableFile("Harbor.Lights.mkv", files.single().yencHeaders!!.size, 0, 0, 0, 0)

    private fun names(prefix: String, count: Int) = Array(count) { "$prefix$it" }

    /**
     * Starts streaming [nzb]'s file from a session coroutine, as an Android app does: with [options], or through the
     * overload without them when null.
     */
    private suspend fun NzbStreamer.start(nzb: NzbDocument, options: StreamOptions? = null, onGap: ((Gap) -> Unit)? = null): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(
                if (options == null) {
                    launchStreamFile(nzb, nzb.file(), onGap = onGap)
                } else {
                    launchStreamFile(nzb, nzb.file(), options = options, onGap = onGap)
                }
            )
        }
        return withTimeout(10_000) { writer.await() }
    }

    /** Everything readable, and why it ended: the close cause a read threw or left behind. */
    private suspend fun ByteReadChannel.readToEnd(): Pair<ByteArray, Throwable?> = withTimeout(20_000) { readAll() }

    private suspend fun ByteReadChannel.readAll(): Pair<ByteArray, Throwable?> {
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

    private fun Throwable?.chain() = generateSequence(this) { it.cause }.toList()

    private fun Throwable?.isMissing(name: String) = chain().any { it is ArticleNotFoundException && "<$name@test>" in it.message.orEmpty() }

    private fun poolOf(server: FakeNntpServer) = "127.0.0.1:${server.port}"

    /** Fetches waiting for one of [server]'s pool's connections, at any priority. */
    private fun waiting(server: FakeNntpServer) =
        registry.find("nntp.pool.waiters").tag("pool.name", poolOf(server)).gauges().sumOf { it.value() }.toInt()

    private suspend fun awaitWaiting(server: FakeNntpServer, count: Int) = withTimeout(10_000) {
        while (waiting(server) < count) delay(10)
    }

    /** The priorities [server]'s pool lent its connections at. */
    private fun prioritiesLent(server: FakeNntpServer) =
        registry.find("nntp.pool.acquire").tag("pool.name", poolOf(server)).timers()
            .filter { it.count() > 0 }
            .map { it.id.getTag("priority")!!.toInt() }
            .toSet()

    private suspend fun awaitBodies(server: FakeNntpServer, vararg names: String) = withTimeout(10_000) {
        while (!server.bodiesRead.containsAll(names.map { "<$it@test>" })) delay(10)
    }

    /**
     * How many BODYs whose names start with [prefix] the servers have read once [expected] have (waiting 10 s at most)
     * and 300 ms more have passed, time for one more to be asked.
     */
    private suspend fun bodiesAfter(expected: Int, prefix: String): Int {
        fun count() = commands.count { it.startsWith("BODY <$prefix") }
        withTimeout(10_000) { while (count() < expected) delay(10) }
        delay(300)
        return count()
    }

    // Priority.

    /**
     * On a pool of one connection, a stream's first fetch holds it (its answer held back) while its other one waits;
     * then an availability check at PREPARE and a default stream's fetch ask for it. The commands the server reads
     * once the first answer goes say who got the connection in what order, with the stream's fetches named
     * "held" and "next" (its two segments ask at once, either first).
     */
    private suspend fun orderBehindHeldFetch(options: StreamOptions?): List<String> {
        val held = AtomicReference<String?>()
        val server = server(answer = { name ->
            if (name.startsWith("first") && held.compareAndSet(null, name)) gate.await(30, TimeUnit.SECONDS)
        })
        // One connection, and time enough that nothing here times out.
        streamer(server, connections = 1, streaming = StreamingConfig(segmentFetchTimeoutMs = 30_000)).use { streamer ->
            val first = streamer.start(nzb("first0", "first1"), options)
            withTimeout(10_000) { while (held.get() == null) delay(10) }
            awaitWaiting(server, 1)
            val check = sessions.async { streamer.verifySegments(nzb("check0", "check1"), NntpPriority.PREPARE) }
            awaitWaiting(server, 2)
            val play = streamer.start(nzb("play0"))
            awaitWaiting(server, 3)
            gate.countDown()

            assertContentEquals(contentOf("first0", "first1"), first.channel.readToEnd().first)
            assertContentEquals(contentOf("play0"), play.channel.readToEnd().first)
            assertEquals(VerificationResult.Success, withTimeout(10_000) { check.await() })
            val next = if (held.get() == "first0") "first1" else "first0"
            return commands.map { it.replace("${held.get()}@test", "held").replace("$next@test", "next") }
        }
    }

    @Test
    fun `a background stream's next fetch waits for a check and a play asked after it`() = runBlocking<Unit> {
        assertEquals(
            listOf("BODY <held>", "BODY <play0@test>", "STAT <check0@test>", "STAT <check1@test>", "BODY <next>"),
            orderBehindHeldFetch(StreamOptions(priority = BACKGROUND_PRIORITY)),
        )
    }

    @Test
    fun `with default options a stream's next fetch goes first, as before`() = runBlocking<Unit> {
        assertEquals(
            listOf("BODY <held>", "BODY <next>", "BODY <play0@test>", "STAT <check0@test>", "STAT <check1@test>"),
            orderBehindHeldFetch(null),
        )
    }

    // The segment cache, read only.

    @Test
    fun `a read-only stream reads the segment cache and stores nothing it fetches`(@TempDir dir: Path) = runBlocking<Unit> {
        val server = server()
        SegmentCache(SegmentCacheConfig(dir, maxBytes = 10L * 1024 * 1024)).use { cache ->
            cache.getOrFetch("kept@test") { bytesOf("kept") }
            streamer(server, cache = cache).use { streamer ->
                val readOnly = streamer.start(nzb("kept", "fresh"), StreamOptions(cache = CacheUse.READ_ONLY))
                assertContentEquals(contentOf("kept", "fresh"), readOnly.channel.readToEnd().first)
                assertEquals(listOf("<fresh@test>"), server.bodiesRead.toList(), "articles fetched")
                assertNull(cache.get("fresh@test"), "the read-only stream stored what it fetched")

                val readWrite = streamer.start(nzb("played"), StreamOptions(cache = CacheUse.READ_WRITE))
                assertContentEquals(contentOf("played"), readWrite.channel.readToEnd().first)
                assertContentEquals(bytesOf("played"), cache.get("played@test"), "a read-write stream stores what it fetches")
            }
        }
    }

    /**
     * A background stream that stores what it fetches (a prefetch) waits behind the plays for one connection, held by
     * a play's slow fetch. A play asking for the same article then must ask the pool itself, at its own priority: had it
     * joined the background fetch, it would wait as long, with no time limit, and fail with it.
     */
    @Test
    fun `a play never shares a background stream's fetch of the same article, and what the stream fetches is stored`(@TempDir dir: Path) = runBlocking<Unit> {
        val server = server(answer = { name -> if (name == "slow") gate.await(30, TimeUnit.SECONDS) })
        SegmentCache(SegmentCacheConfig(dir, maxBytes = 10L * 1024 * 1024)).use { cache ->
            streamer(server, connections = 1, streaming = StreamingConfig(segmentFetchTimeoutMs = 30_000), cache = cache).use { streamer ->
                val held = streamer.start(nzb("slow"))
                awaitBodies(server, "slow")
                val prefetch = streamer.start(nzb("shared"), StreamOptions(priority = BACKGROUND_PRIORITY))
                awaitWaiting(server, 1)
                val play = streamer.start(nzb("shared"))
                // Its own ask comes at once; joined to the background fetch, it never makes one.
                withTimeoutOrNull(5_000) { while (waiting(server) < 2) delay(10) }
                assertEquals(2, waiting(server), "fetches waiting for the connection: the play's own, or it joined the background one")

                gate.countDown()
                assertContentEquals(contentOf("slow"), held.channel.readToEnd().first)
                assertContentEquals(contentOf("shared"), play.channel.readToEnd().first)
                assertContentEquals(contentOf("shared"), prefetch.channel.readToEnd().first)
                assertEquals(listOf("BODY <slow@test>", "BODY <shared@test>", "BODY <shared@test>"), commands.toList())
                assertContentEquals(bytesOf("shared"), cache.get("shared@test"), "the article stored")
            }
        }
    }

    // The missing-article policy.

    @Test
    fun `a stream's own missing-article policy decides over the streamer's`() = runBlocking<Unit> {
        val server = server(lacks = { it == "gone" })
        val names = arrayOf("before", "gone", "after")
        streamer(server).use { failing ->
            val gaps = CopyOnWriteArrayList<Gap>()
            val filling = StreamOptions(missingArticles = MissingArticlePolicy.ZERO_FILL)
            val (filled, cause) = failing.start(nzb(*names), filling) { gaps += it }.channel.readToEnd()
            assertNull(cause, "the stream that zero-fills failed: ${cause.chain()}")
            assertContentEquals(bytesOf("before") + ByteArray(segmentSize) + bytesOf("after"), filled)
            assertEquals(listOf(Gap(segmentSize.toLong() until 2L * segmentSize, Gap.Cause.Missing)), gaps.toList())

            val (_, failed) = failing.start(nzb(*names)).channel.readToEnd()
            assertTrue(failed.isMissing("gone"), "the stream without options closed with: ${failed.chain()}")
        }
        streamer(server, streaming = StreamingConfig(missingArticles = MissingArticlePolicy.ZERO_FILL)).use { filling ->
            val (_, failed) = filling.start(nzb(*names), StreamOptions(missingArticles = MissingArticlePolicy.FAIL)).channel.readToEnd()
            assertTrue(failed.isMissing("gone"), "the stream that fails closed with: ${failed.chain()}")

            val (filled, cause) = filling.start(nzb(*names)).channel.readToEnd()
            assertNull(cause, "the stream without options failed: ${cause.chain()}")
            assertContentEquals(bytesOf("before") + ByteArray(segmentSize) + bytesOf("after"), filled)
        }
    }

    // Concurrency and read-ahead.

    @Test
    fun `a stream downloads as many segments at once as its own concurrency allows`() = runBlocking<Unit> {
        val server = server(answer = { gate.await(30, TimeUnit.SECONDS) })
        // The whole read-ahead at once, so only the concurrency bounds the downloads; time enough that none times out.
        val streaming = StreamingConfig(concurrency = 4, initialReadAheadSegments = null, segmentFetchTimeoutMs = 30_000)
        streamer(server, connections = 8, streaming = streaming).use { streamer ->
            val own = streamer.start(nzb(*names("own", 10)), StreamOptions(concurrency = 2, readAheadSegments = 4))
            assertEquals(2, bodiesAfter(2, "own"), "BODYs in flight for a stream of concurrency 2")
            val default = streamer.start(nzb(*names("dflt", 10)))
            assertEquals(4, bodiesAfter(4, "dflt"), "BODYs in flight for a stream with the streamer's concurrency")

            gate.countDown()
            assertContentEquals(contentOf(*names("own", 10)), own.channel.readToEnd().first)
            assertContentEquals(contentOf(*names("dflt", 10)), default.channel.readToEnd().first)
        }
    }

    @Test
    fun `a stream holds as many segments ahead of its reader as its own read-ahead allows`() = runBlocking<Unit> {
        // Past the 1 MiB a channel takes before its writer waits: the first segment written holds the writer up while
        // nothing is read. A whole number of the 64 KiB units a byte budget is counted in.
        val size = 23 * 64 * 1024
        val server = server(size = size)
        val streaming = StreamingConfig(concurrency = 4, readAheadSegments = 6, initialReadAheadSegments = null)
        streamer(server, connections = 8, streaming = streaming).use { streamer ->
            // The one being written, those read ahead and the one waiting to join them.
            val own = streamer.start(nzb(*names("own", 10), size = size), StreamOptions(readAheadSegments = 4))
            assertEquals(6, bodiesAfter(6, "own"), "segments asked for by a stream reading 4 ahead, before anything was read")
            val budget = streamer.start(nzb(*names("budget", 10), size = size), StreamOptions(readAheadBytes = 3L * size))
            assertEquals(3, bodiesAfter(3, "budget"), "segments asked for by a stream of 3 segments' bytes, before anything was read")
            val default = streamer.start(nzb(*names("dflt", 10), size = size))
            assertEquals(8, bodiesAfter(8, "dflt"), "segments asked for by a stream reading the streamer's 6 ahead")

            assertContentEquals(contentOf(*names("own", 10), size = size), own.channel.readToEnd().first)
            assertContentEquals(contentOf(*names("budget", 10), size = size), budget.channel.readToEnd().first)
            assertContentEquals(contentOf(*names("dflt", 10), size = size), default.channel.readToEnd().first)
        }
    }

    // The encrypted path: an encrypted RAR5 block in an archive of 8 KiB articles, as in EncryptedStreamTest.

    private val articleSize = 8 * 1024
    private val password = "Copper Crown"
    private val salt = ByteArray(16) { it.toByte() }
    private val kdfLog2 = 1
    private val headerSize = 21L
    private val dataAreaIv = ByteArray(16) { (it * 3).toByte() }
    private val plaintext = ByteArray(40_000) { (it % 251).toByte() }
    private val blockStart = 100

    private fun blocks(n: Int) = (n + 15) / 16 * 16

    private fun encrypt(data: ByteArray): ByteArray = Cipher.getInstance("AES/CBC/NoPadding").run {
        init(Cipher.ENCRYPT_MODE, SecretKeySpec(Rar5Crypto.deriveKey(password, salt, kdfLog2), "AES"), IvParameterSpec(dataAreaIv))
        doFinal(data.copyOf(blocks(data.size)))
    }

    private val archive = ByteArray(blockStart) + ByteArray(16) { 7 } + ByteArray(blocks(headerSize.toInt())) + encrypt(plaintext) +
        ByteArray(4 * articleSize) { 1 }

    private val encrypted = StreamableFile(
        "Harbor.Lights.mkv", plaintext.size.toLong(), 0, 0, 0, 0,
        preComputedSplits = listOf(
            SplitInfo(
                volumeIndex = 0,
                dataStartPosition = blockStart.toLong(),
                dataSize = plaintext.size.toLong(),
                encryption = SplitEncryptionInfo(headerSize, salt, kdfLog2, dataAreaIv = dataAreaIv),
            )
        ),
    )

    private fun article(i: Int) = archive.copyOfRange(i * articleSize, minOf(archive.size, (i + 1) * articleSize))

    private val archiveNzb = NzbDocument(
        listOf(
            NzbFile(
                poster = "test", date = 0, subject = "Harbor.Lights.part1.rar", groups = listOf("alt.binaries.test"),
                segments = (0 until (archive.size + articleSize - 1) / articleSize).map { NzbSegment(articleSize.toLong(), it + 1, "enc$it@test") },
                yencHeaders = YencHeaders(line = 128, size = archive.size.toLong(), name = "Harbor.Lights.part1.rar", partEnd = articleSize.toLong()),
            )
        ),
        password = password,
    )

    /** The article holding the file's plaintext byte [offset]: its data area follows the IV and the header. */
    private fun articleOfPlaintext(offset: Int) = (blockStart + 16 + blocks(headerSize.toInt()) + offset) / articleSize

    /** Serves the archive's articles but [missing]. */
    private fun archiveServer(missing: Int? = null) = FakeNntpServer(body = { id ->
        val i = id.removePrefix("<enc").substringBefore('@').toInt()
        if (i == missing) null else YencEncoder.encodeSinglePart(article(i), "Harbor.Lights.part1.rar").data
    }).also { servers += it }

    private suspend fun NzbStreamer.startEncrypted(options: StreamOptions?, onGap: ((Gap) -> Unit)? = null): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            writer.complete(
                if (options == null) {
                    launchStreamFile(archiveNzb, encrypted, onGap = onGap)
                } else {
                    launchStreamFile(archiveNzb, encrypted, options = options, onGap = onGap)
                }
            )
        }
        return withTimeout(10_000) { writer.await() }
    }

    @Test
    fun `an encrypted file's stream fetches at its options' priority`() = runBlocking<Unit> {
        val server = archiveServer()
        streamer(server).use { streamer ->
            val (received, cause) = streamer.startEncrypted(StreamOptions(priority = BACKGROUND_PRIORITY)).channel.readToEnd()
            assertNull(cause, "closed with: ${cause.chain()}")
            assertContentEquals(plaintext, received)
            assertEquals(setOf(BACKGROUND_PRIORITY), prioritiesLent(server), "priorities the pool lent its connections at")
        }
    }

    @Test
    fun `an encrypted file's read-only stream reads the segment cache and stores nothing`(@TempDir dir: Path) = runBlocking<Unit> {
        val server = archiveServer()
        val kept = articleOfPlaintext(10_000)
        SegmentCache(SegmentCacheConfig(dir, maxBytes = 10L * 1024 * 1024)).use { cache ->
            cache.getOrFetch("enc$kept@test") { article(kept) }
            streamer(server, cache = cache).use { streamer ->
                val (received, cause) = streamer.startEncrypted(StreamOptions(cache = CacheUse.READ_ONLY)).channel.readToEnd()
                assertNull(cause, "closed with: ${cause.chain()}")
                assertContentEquals(plaintext, received)
                assertFalse("<enc$kept@test>" in server.bodiesRead, "the article the cache holds was fetched: ${server.bodiesRead}")
                assertEquals(1, cache.stats().entryCount, "articles in the cache")
            }
        }
    }

    @Test
    fun `an encrypted file's stream follows its own missing-article policy`() = runBlocking<Unit> {
        val server = archiveServer(missing = articleOfPlaintext(20_000))
        streamer(server).use { failing ->
            val gaps = CopyOnWriteArrayList<Gap>()
            val filling = StreamOptions(missingArticles = MissingArticlePolicy.ZERO_FILL)
            val (received, cause) = failing.startEncrypted(filling) { gaps += it }.channel.readToEnd()
            assertNull(cause, "the stream that zero-fills failed: ${cause.chain()}")
            assertEquals(plaintext.size, received.size)
            assertTrue(gaps.isNotEmpty(), "no gap reported")
            val garbled = gaps.map { it.fileRange }
            assertTrue(plaintext.indices.all { i -> garbled.any { i in it } || received[i] == plaintext[i] }, "plaintext outside the gaps differs")

            val (_, failed) = failing.startEncrypted(null).channel.readToEnd()
            assertTrue(failed.chain().any { it is ArticleNotFoundException }, "the stream without options closed with: ${failed.chain()}")
        }
    }

    // Default options change nothing.

    /**
     * What two streams did, with [options] or without: the first's bytes; the BODYs their server read, the priorities
     * their pool lent at and what their cache stored; and what the second, which meets a missing article, failed with.
     */
    private data class Observed(val bytes: List<Byte>, val bodies: List<String>, val priorities: Set<Int>, val cached: Int, val failure: Class<*>?)

    private fun observe(dir: Path, options: StreamOptions?): Observed = runBlocking {
        val server = server(lacks = { it == "gone" })
        SegmentCache(SegmentCacheConfig(dir, maxBytes = 10L * 1024 * 1024)).use { cache ->
            streamer(server, cache = cache).use { streamer ->
                val (received, _) = streamer.start(nzb("one", "two", "three"), options).channel.readToEnd()
                val (_, cause) = streamer.start(nzb("four", "gone"), options).channel.readToEnd()
                val failure = cause.chain().firstOrNull { it is ArticleNotFoundException }
                Observed(received.toList(), server.bodiesRead.sorted(), prioritiesLent(server), cache.stats().entryCount, failure?.javaClass)
            }
        }
    }

    @Test
    fun `a stream with default options does what one without options does`(@TempDir without: Path, @TempDir withOptions: Path) {
        val plain = observe(without, null)
        assertEquals(contentOf("one", "two", "three").toList(), plain.bytes)
        assertEquals(ArticleNotFoundException::class.java, plain.failure, "the stream without options: $plain")
        assertEquals(setOf(NntpPriority.STREAMING.value), plain.priorities)
        assertEquals(4, plain.cached, "articles the stream without options stored")
        assertEquals(plain, observe(withOptions, StreamOptions()))
    }

    @Test
    fun `trailing lambdas bind as before, with options or without`() = runBlocking<Unit> {
        val server = server(lacks = { it == "gone" })
        val nzb = nzb("one", "gone")
        val filled = bytesOf("one") + ByteArray(segmentSize)
        val zeroFill = StreamingConfig(missingArticles = MissingArticlePolicy.ZERO_FILL)
        streamer(server, streaming = zeroFill).use { streamer ->
            // launchStreamFile's trailing lambda is its onGap.
            val without = CopyOnWriteArrayList<Gap>()
            val read = sessions.async { streamer.launchStreamFile(nzb, nzb.file()) { gap -> without += gap }.channel.toByteArray() }
            assertContentEquals(filled, withTimeout(20_000) { read.await() })
            val with = CopyOnWriteArrayList<Gap>()
            val readWith = sessions.async {
                streamer.launchStreamFile(nzb, nzb.file(), options = StreamOptions()) { gap -> with += gap }.channel.toByteArray()
            }
            assertContentEquals(filled, withTimeout(20_000) { readWith.await() })
            assertEquals(listOf(Gap(segmentSize.toLong() until 2L * segmentSize, Gap.Cause.Missing)), without.toList())
            assertEquals(without.toList(), with.toList())
        }
        service(server, streaming = zeroFill).use { service ->
            // streamSegments' trailing lambda is its consume.
            val queue = { SegmentQueueService.createFileQueue(nzb.files.single(), 0, 0L) }
            var plain = ByteArray(0)
            service.streamSegments(queue(), name = "plain") { plain = it.toByteArray() }
            var withOptions = ByteArray(0)
            service.streamSegments(queue(), name = "options", options = StreamOptions()) { withOptions = it.toByteArray() }
            assertContentEquals(filled, plain)
            assertContentEquals(filled, withOptions)
            // launchStreamSegments' is its onGap.
            val gaps = CopyOnWriteArrayList<Gap>()
            val launched = sessions.async {
                service.launchStreamSegments(queue(), name = "launched", options = StreamOptions()) { gap -> gaps += gap }.channel.toByteArray()
            }
            assertContentEquals(filled, withTimeout(20_000) { launched.await() })
            assertEquals(1, gaps.size)
            // A seekable stream, with options or without.
            for (stream in listOf(
                NntpSeekableInputStream(nzb, service),
                NntpSeekableInputStream(nzb, service, options = StreamOptions(), onGap = null),
            )) {
                val buffer = ByteArray(filled.size)
                var at = 0
                while (at < buffer.size) {
                    val n = stream.read(buffer, at, buffer.size - at)
                    if (n == -1) break
                    at += n
                }
                stream.close()
                assertContentEquals(filled, buffer)
            }
        }
    }

    /** An argument of whatever type its place takes, as a mocking library's any() is; never called. */
    private fun <T> anything(): T = throw AssertionError("never called")

    /**
     * Checked by compiling: each call, with an argument of any type in each place, matched one overload before the
     * options came, and still matches only that one, as a caller stubbing them with a mocking library has it.
     */
    @Test
    fun `calls with arguments of any type in the old overloads' places still pick the old overloads`() {
        val calls = listOf<suspend () -> Any>(
            { anything<NzbStreamer>().launchStreamFile(anything<ExtractedMetadata>(), anything<NamedSplits>(), anything(), anything()) },
            { anything<NzbStreamer>().launchStreamFile(anything<ExtractedMetadata>(), anything<StreamableFile>(), anything(), anything(), anything()) },
            { anything<NzbStreamer>().launchStreamFile(anything<NzbDocument>(), anything<StreamableFile>(), anything(), anything(), anything()) },
            { anything<ArchiveStreamingService>().launchStreamFile(anything(), anything<NamedSplits>(), anything(), anything()) },
            { anything<ArchiveStreamingService>().launchStreamFile(anything(), anything<StreamableFile>(), anything(), anything(), anything()) },
            { anything<NntpStreamingService>().launchStreamSegments(anything(), anything(), anything(), anything(), anything(), anything(), anything()) },
            { NntpSeekableInputStream(anything(), anything(), anything(), anything()) },
        )
        assertEquals(7, calls.size)
    }

    // A background stream's wait for a connection.

    private fun NntpStreamingService.breakerOf(server: FakeNntpServer): CircuitBreaker {
        val field = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        field.isAccessible = true
        return (field.get(this) as CircuitBreakerRegistry).circuitBreaker(poolOf(server))
    }

    private suspend fun NntpStreamingService.start(nzb: NzbDocument, options: StreamOptions?): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch {
            val queue = SegmentQueueService.createFileQueue(nzb.files.single(), 0, 0L)
            writer.complete(
                if (options == null) launchStreamSegments(queue, name = "play") else launchStreamSegments(queue, name = "download", options = options, onGap = null)
            )
        }
        return withTimeout(10_000) { writer.await() }
    }

    /**
     * Two pools of one connection each. A play's fetch holds the first one's past the timeout: its answer is held back,
     * so it times out there and goes to the second, and the pool keeps the connection while it reads off the answer
     * owed. A download's fetch waits for that connection meanwhile, and so does another play's, whose wait runs out.
     */
    @Test
    fun `a background fetch waits out a held connection without failing or counting against its pool`() = runBlocking<Unit> {
        val first = server(answer = { name -> if (name == "slow") gate.await(30, TimeUnit.SECONDS) })
        val second = server()
        service(first, second, connections = 1, streaming = StreamingConfig(segmentFetchTimeoutMs = TIMEOUT_MS)).use { service ->
            val breaker = service.breakerOf(first)
            val play = service.start(nzb("slow"), options = null)
            awaitBodies(first, "slow")
            val download = service.start(nzb("saved"), StreamOptions(priority = BACKGROUND_PRIORITY))
            awaitWaiting(first, 1)
            assertContentEquals(contentOf("slow"), play.channel.readToEnd().first)
            // A play's wait still runs out as before, and the second pool serves it.
            val next = service.start(nzb("next"), options = null)
            assertContentEquals(contentOf("next"), next.channel.readToEnd().first)
            assertFalse(download.job.isCompleted, "the download's fetch ended before the connection was free")

            gate.countDown()
            val (saved, cause) = download.channel.readToEnd()
            assertNull(cause, "the download failed: ${cause.chain()}")
            assertContentEquals(contentOf("saved"), saved)
            assertEquals(listOf("<slow@test>", "<saved@test>"), first.bodiesRead.toList(), "BODYs the first pool's server read")
            assertEquals(listOf("<slow@test>", "<next@test>"), second.bodiesRead.toList(), "BODYs the second pool's server read")
            // The plays' two timeouts count against the first pool, and nothing of the download's.
            assertEquals(2.0, registry.counter("nzb.segments.body_timeouts", "pool.name", poolOf(first)).count(), "timeouts counted")
            assertEquals(2, breaker.metrics.numberOfFailedCalls, "failures the first pool's breaker counted")
        }
    }

    /**
     * Two pools of one connection each, the first's breaker half-open: it lets one call through, whose outcome says
     * whether plays use the pool again. A download's fetch waits for the first pool's connection, held by a play's slow
     * fetch, behind the plays; it must not hold that one call meanwhile, or every play goes to the second pool.
     */
    @Test
    fun `a background fetch waiting on a half-open pool leaves its trial call to a play`() = runBlocking<Unit> {
        val first = server(answer = { name -> if (name == "slow") gate.await(30, TimeUnit.SECONDS) })
        val second = server()
        // Time enough that nothing here times out.
        service(first, second, connections = 1, streaming = StreamingConfig(segmentFetchTimeoutMs = 30_000)).use { service ->
            val breaker = service.breakerOf(first)
            val held = service.start(nzb("slow"), options = null)
            awaitBodies(first, "slow")
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            val download = service.start(nzb("saved"), StreamOptions(priority = BACKGROUND_PRIORITY))
            awaitWaiting(first, 1)

            val play = service.start(nzb("probe"), options = null)
            withTimeout(10_000) { while (waiting(first) < 2 && second.bodiesRead.isEmpty()) delay(10) }
            assertEquals(emptyList(), second.bodiesRead.toList(), "BODYs the second pool's server read: the play was refused the first")
            gate.countDown()
            assertContentEquals(contentOf("slow"), held.channel.readToEnd().first)
            assertContentEquals(contentOf("probe"), play.channel.readToEnd().first)
            val (saved, cause) = download.channel.readToEnd()
            assertNull(cause, "the download failed: ${cause.chain()}")
            assertContentEquals(contentOf("saved"), saved)
            assertEquals(listOf("<slow@test>", "<probe@test>", "<saved@test>"), first.bodiesRead.toList(), "BODYs the first pool's server read")
            assertEquals(CircuitBreaker.State.CLOSED, breaker.state, "the first pool's breaker, its trial call served")
        }
    }

    /** What the pool says when its own wait for a connection (30 s, and as long again on its retry) runs out. */
    private fun poolWaitRanOut() = NntpConnectionException("Timed out waiting for connection to 127.0.0.1:119 (30000ms)")

    @Test
    fun `a background fetch asks the pool again when the pool's own wait runs out, and its time starts at the hand-over`() = runTest {
        var asks = 0
        val tries = Tries()
        val served = withConnectionWithin<Unit, String>(
            withClient = { block ->
                asks++
                delay(60_000)
                if (asks < 3) throw poolWaitRanOut()
                block(Unit)
            },
            timeoutMs = TIMEOUT_MS, retries = 0, tries = tries, background = true, onRetry = {},
        ) {
            delay(TIMEOUT_MS - 1)
            "served"
        }
        assertEquals("served", served)
        assertEquals(3, asks)
        assertEquals(3 * 60_000 + TIMEOUT_MS - 1, currentTime)
        assertEquals(1, tries.made)
        assertEquals(1, tries.connected)
    }

    @Test
    fun `a background fetch waits as long as it takes for the pool's fetch again after its connection dropped`() = runTest {
        var calls = 0
        val tries = Tries()
        val served = withConnectionWithin<Unit, String>(
            // As the pool does: an article whose connection dropped is fetched again on another, once one is free.
            withClient = { block ->
                try {
                    block(Unit)
                } catch (_: NntpConnectionLostException) {
                    delay(60_000)
                    block(Unit)
                }
            },
            timeoutMs = TIMEOUT_MS, retries = 0, tries = tries, background = true, onRetry = {},
        ) {
            if (++calls == 1) throw NntpConnectionLostException("Connection reset in the middle of an article")
            "served"
        }
        assertEquals("served", served)
        assertEquals(60_000, currentTime)
        assertEquals(1, tries.made)
    }

    @Test
    fun `a background fetch's next try after a timeout on its connection waits as long as it takes`() = runTest {
        var asks = 0
        var retried = 0
        val tries = Tries()
        val served = withConnectionWithin<Unit, String>(
            withClient = { block ->
                // The next try's connection comes once the plays are done with them.
                if (++asks == 2) delay(60_000)
                block(Unit)
            },
            timeoutMs = TIMEOUT_MS, retries = 1, tries = tries, background = true, onRetry = { retried++ },
        ) {
            if (asks == 1) awaitCancellation()
            "served"
        }
        assertEquals("served", served)
        assertEquals(1, retried, "timeouts on a connection counted")
        assertEquals(TIMEOUT_MS + 60_000, currentTime)
        assertEquals(2, tries.made)
        assertEquals(2, tries.connected)
    }

    @Test
    fun `a background fetch's time on its connections still has twice the timeout in all`() = runTest {
        var calls = 0
        var retried = 0
        val tries = Tries()
        assertFailsWith<TimeoutCancellationException> {
            withConnectionWithin<Unit, String>(
                // Each connection a minute coming, the pool's fetch again after one dropped included.
                withClient = { block ->
                    delay(60_000)
                    try {
                        block(Unit)
                    } catch (_: NntpConnectionLostException) {
                        delay(60_000)
                        block(Unit)
                    }
                },
                timeoutMs = TIMEOUT_MS, retries = 1, tries = tries, background = true, onRetry = { retried++ },
                clock = { testScheduler.currentTime * 1_000_000 },
            ) {
                if (++calls == 1) {
                    delay(400)
                    throw NntpConnectionLostException("Connection reset in the middle of an article")
                }
                awaitCancellation()
            }
        }
        // 400 ms on the first connection, its own 500 on the next one, and on the next try's the 100 left of the 1,000.
        assertEquals(60_000 + 400 + 60_000 + TIMEOUT_MS + 60_000 + 100, currentTime)
        assertEquals(1, retried, "timeouts on a connection counted before the last")
        assertEquals(2 * TIMEOUT_MS * 1_000_000, tries.heldNs)
    }

    @Test
    fun `a background fetch still has the timeout on its connection`() = runTest {
        val tries = Tries()
        assertFailsWith<TimeoutCancellationException> {
            withConnectionWithin<Unit, String>(
                withClient = { block ->
                    delay(600_000)
                    block(Unit)
                },
                timeoutMs = TIMEOUT_MS, retries = 0, tries = tries, background = true, onRetry = {},
            ) { awaitCancellation() }
        }
        assertEquals(600_000 + TIMEOUT_MS, currentTime)
        assertEquals(1, tries.connected)
    }

    @Test
    fun `a background fetch whose pool can't connect fails at once`() = runTest {
        var asks = 0
        val refused = NntpConnectionException("Could not connect to 127.0.0.1:119: Connection refused")
        val failure = assertFailsWith<NntpConnectionException> {
            withConnectionWithin<Unit, String>(
                // A second ask would be served: the failure must end the call.
                withClient = { block -> if (++asks == 1) throw refused else block(Unit) },
                timeoutMs = TIMEOUT_MS, retries = 0, tries = Tries(), background = true, onRetry = {},
            ) { "served" }
        }
        assertEquals(refused.message, failure.message)
        assertEquals(1, asks)
    }

    @Test
    fun `a default fetch's wait for a connection still runs out after the timeout`() = runTest {
        val tries = Tries()
        assertFailsWith<TimeoutCancellationException> {
            withConnectionWithin<Unit, String>(
                withClient = { block ->
                    delay(10 * TIMEOUT_MS)
                    block(Unit)
                },
                timeoutMs = TIMEOUT_MS, retries = 0, tries = tries, background = false, onRetry = {},
            ) { "served" }
        }
        assertEquals(TIMEOUT_MS, currentTime)
        assertEquals(0, tries.connected)
    }

    private companion object {
        const val TIMEOUT_MS = 500L
    }
}
