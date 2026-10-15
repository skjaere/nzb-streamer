package io.skjaere.nzbstreamer

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.WriterJob
import io.ktor.utils.io.cancel
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readFully
import io.skjaere.compressionutils.Rar5Crypto
import io.skjaere.compressionutils.SplitEncryptionInfo
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.NamedSplits
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.YencEncoder
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * The stream an encrypted RAR5 file is decrypted from: a download that fails says why, it belongs
 * to the coroutine reading it, and it stops where the file's encrypted block ends.
 */
class EncryptedStreamTest {
    private val servers = mutableListOf<FakeNntpServer>()
    private val gate = CountDownLatch(1)

    // Where the app starts its streams; anything that reaches the handler is recorded.
    private val uncaught = CopyOnWriteArrayList<Throwable>()
    private val sessions = CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, e -> uncaught += e })

    @AfterEach
    fun tearDown() {
        gate.countDown()
        sessions.cancel()
        servers.forEach { it.close() }
    }

    // An archive of 8 KiB segments: 100 bytes, an encrypted block (IV, header, data area), then
    // other files' bytes.
    private val segmentSize = 8 * 1024
    private val password = "secret"
    private val salt = ByteArray(16) { it.toByte() }
    private val kdfLog2 = 1
    private val headerSize = 21L
    private val dataAreaIv = ByteArray(16) { (it * 3).toByte() }
    private val plaintext = ByteArray(40_000) { (it % 251).toByte() }
    private val blockStart = 100
    private val block = ByteArray(16) { 7 } + ByteArray(blocks(headerSize.toInt())) + encrypt(plaintext)
    private val blockEnd = blockStart + block.size
    private val archive = ByteArray(blockStart) + block + ByteArray(12 * segmentSize) { 1 }

    private fun blocks(n: Int) = (n + 15) / 16 * 16

    private fun encrypt(data: ByteArray, iv: ByteArray = dataAreaIv): ByteArray = Cipher.getInstance("AES/CBC/NoPadding").run {
        init(Cipher.ENCRYPT_MODE, SecretKeySpec(Rar5Crypto.deriveKey(password, salt, kdfLog2), "AES"), IvParameterSpec(iv))
        doFinal(data.copyOf(blocks(data.size)))
    }

    private val split = SplitInfo(
        volumeIndex = 0,
        dataStartPosition = blockStart.toLong(),
        dataSize = plaintext.size.toLong(),
        encryption = SplitEncryptionInfo(headerSize, salt, kdfLog2, dataAreaIv = dataAreaIv),
    )

    private fun segmentOf(position: Int) = position / segmentSize

    // A file of four decryption chunks (the decryptor reads 64 KiB of ciphertext at a time).
    private val chunk = 64 * 1024
    private val long = ByteArray(4 * chunk) { (it % 247).toByte() }
    private val longArchive = ByteArray(blockStart) + ByteArray(16) { 7 } + ByteArray(blocks(headerSize.toInt())) +
        encrypt(long) + ByteArray(12 * segmentSize) { 1 }
    private val longSplit = split.copy(dataSize = long.size.toLong())

    /** The segment holding the file's plaintext byte [offset]: its data area follows the IV and the header. */
    private fun segmentOfPlaintext(offset: Int) = segmentOf(blockStart + 16 + blocks(headerSize.toInt()) + offset)

    private fun nzb(archive: ByteArray = this.archive) = NzbDocument(
        listOf(
            NzbFile(
                poster = "test", date = 0, subject = "release.part1.rar", groups = listOf("alt.binaries.test"),
                segments = (0 until (archive.size + segmentSize - 1) / segmentSize)
                    .map { NzbSegment(segmentSize.toLong(), it + 1, "seg$it@test") },
                yencHeaders = YencHeaders(line = 128, size = archive.size.toLong(), name = "release.part1.rar", partEnd = segmentSize.toLong()),
            )
        ),
        password = password,
    )

    /** Serves the archive's segments but [missing]; [answer] runs before each answer and may block. */
    private fun server(missing: Int? = null, archive: ByteArray = this.archive, answer: (Int) -> Unit = {}) = FakeNntpServer(body = { id ->
        val i = id.removePrefix("<seg").substringBefore('@').toInt()
        answer(i)
        if (i == missing) {
            null
        } else {
            val slice = archive.copyOfRange(i * segmentSize, minOf(archive.size, (i + 1) * segmentSize))
            YencEncoder.encodeSinglePart(slice, "release.part1.rar").data
        }
    }).also { servers += it }

    private fun service(server: FakeNntpServer) = NntpStreamingService(
        NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)
    ).also { runBlocking { it.connect() } }

    /** Starts streaming the decrypted file from a session coroutine (in [context]), as the app does. */
    private suspend fun startStream(
        service: NntpStreamingService,
        split: SplitInfo = this.split,
        archive: ByteArray = this.archive,
        context: CoroutineContext = EmptyCoroutineContext,
    ): WriterJob {
        val writer = CompletableDeferred<WriterJob>()
        sessions.launch(context) {
            writer.complete(
                ArchiveStreamingService(service)
                    .launchStreamFile(nzb(archive), NamedSplits(listOf(split), split.dataSize, "video.mkv"))
            )
        }
        return writer.await()
    }

    /** Everything readable, and why it ended: the close cause a read threw or left behind. */
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

    private fun Throwable?.chain() = generateSequence(this) { it.cause }.toList()

    /** A dispatcher with one thread, as Android's main thread is. A daemon: a test that hangs it can't hold up the JVM. */
    private fun singleThread() =
        Executors.newSingleThreadExecutor { Thread(it, "single").apply { isDaemon = true } }.asCoroutineDispatcher()

    @Test
    fun `an encrypted file decrypts whole`() = runBlocking<Unit> {
        service(server()).use { service ->
            val (received, cause) = startStream(service).channel.readToEnd()
            assertEquals(null, cause)
            assertContentEquals(plaintext, received)
        }
    }

    @Test
    fun `an encrypted file without a per-file IV (header and data one CBC stream) decrypts whole`() = runBlocking<Unit> {
        val iv = ByteArray(16) { 9 }
        val legacy = ByteArray(blockStart) + iv + encrypt(ByteArray(headerSize.toInt()) { 5 } + plaintext, iv) +
            ByteArray(12 * segmentSize) { 1 }
        val legacySplit = split.copy(encryption = split.encryption!!.copy(dataAreaIv = null))
        service(server(archive = legacy)).use { service ->
            val (received, cause) = startStream(service, legacySplit, legacy).channel.readToEnd()
            assertEquals(null, cause)
            assertContentEquals(plaintext, received)
        }
    }

    /** Each decrypted chunk goes to the reader as it's decrypted, not once a megabyte has piled up. */
    @Test
    fun `an encrypted file's first decrypted chunk reaches the reader while the rest is still downloading`() = runBlocking<Unit> {
        // The last chunk's article is held back.
        val held = segmentOfPlaintext(3 * chunk)
        service(server(archive = longArchive, answer = { if (it == held) gate.await(60, TimeUnit.SECONDS) })).use { service ->
            val channel = startStream(service, longSplit, longArchive).channel
            val first = ByteArray(chunk)
            val read = withTimeoutOrNull(5_000) { channel.readFully(first) } != null
            gate.countDown()
            assertTrue(read, "no decrypted byte reached the reader while a later article was downloading")
            assertContentEquals(long.copyOf(chunk), first)
        }
    }

    /** Nor does a failure further on take them with it: Ktor drops what the reader hasn't taken. */
    @Test
    fun `the bytes decrypted in front of a missing article reach the reader before the stream fails`() = runBlocking<Unit> {
        // The last chunk's article is missing, and says so once the reader has the first chunk (or 5 s on).
        val missing = segmentOfPlaintext(3 * chunk)
        val readFirst = CountDownLatch(1)
        val provider = server(missing, longArchive, answer = { if (it == missing) readFirst.await(5, TimeUnit.SECONDS) })
        service(provider).use { service ->
            val channel = startStream(service, longSplit, longArchive).channel
            val first = ByteArray(chunk)
            val read = try {
                withTimeoutOrNull(10_000) { channel.readFully(first) } != null
            } catch (_: IOException) {
                false
            }
            readFirst.countDown()
            assertTrue(read, "the bytes decrypted in front of the missing article never reached the reader")
            assertContentEquals(long.copyOf(chunk), first)

            val (_, cause) = channel.readToEnd()
            assertTrue(cause.chain().any { it is ArticleNotFoundException }, "closed with: ${cause.chain()}")
        }
    }

    @Test
    fun `a missing article in an encrypted file fails the stream with that cause`() = runBlocking<Unit> {
        val missing = segmentOf(blockStart + 16 + blocks(headerSize.toInt()) + 20_000)
        service(server(missing)).use { service ->
            val (_, cause) = startStream(service).channel.readToEnd()
            assertTrue(cause.chain().any { it is ArticleNotFoundException }, "closed with: ${cause.chain()}")
            assertEquals(emptyList(), uncaught.toList(), "reached the exception handler")
        }
    }

    @Test
    fun `streamFile throws a missing article in an encrypted file even when consume takes the early end for the end`() = runBlocking<Unit> {
        val missing = segmentOf(blockStart + 16 + blocks(headerSize.toInt()) + 20_000)
        service(server(missing)).use { service ->
            val failure = runCatching {
                ArchiveStreamingService(service)
                    .streamFile(nzb(), NamedSplits(listOf(split), plaintext.size.toLong(), "video.mkv")) { it.readToEnd() }
            }.exceptionOrNull()
            assertTrue(failure.chain().any { it is ArticleNotFoundException }, "failed with: ${failure.chain()}")
        }
    }

    /**
     * A consume that has what it wants closes the channel and returns: the writer, held up in a
     * flush by the megabyte nobody reads, then finds the channel closed, which is not the stream
     * failing.
     */
    @Test
    fun `streamFile returns normally when consume closes an encrypted file's channel early`() = runBlocking<Unit> {
        val large = ByteArray(3 * 1024 * 1024) { (it % 253).toByte() }
        val largeArchive = ByteArray(blockStart) + ByteArray(16) { 7 } + ByteArray(blocks(headerSize.toInt())) +
            encrypt(large) + ByteArray(12 * segmentSize) { 1 }
        val largeSplit = split.copy(dataSize = large.size.toLong())
        service(server(archive = largeArchive)).use { service ->
            withTimeout(60_000) {
                ArchiveStreamingService(service)
                    .streamFile(nzb(largeArchive), NamedSplits(listOf(largeSplit), large.size.toLong(), "video.mkv")) { channel ->
                        channel.awaitWriterHeldUp()
                        val first = ByteArray(1000).also { channel.readFully(it) }
                        assertContentEquals(large.copyOf(1000), first)
                        channel.cancel()
                    }
            }
        }
    }

    @Test
    fun `an encrypted file downloads nothing past its encrypted block`() = runBlocking<Unit> {
        val lastOfBlock = segmentOf(blockEnd - 1)
        val askedPast = CountDownLatch(1)
        val asked = CopyOnWriteArrayList<Int>()
        // The block's last segment is answered once a segment past it is asked for (or 2 s on).
        val provider = server(answer = { i ->
            asked += i
            if (i > lastOfBlock) askedPast.countDown()
            if (i == lastOfBlock) askedPast.await(2, TimeUnit.SECONDS)
        })
        service(provider).use { service ->
            val (received, cause) = startStream(service).channel.readToEnd()
            assertEquals(null, cause)
            assertContentEquals(plaintext, received)
            assertEquals(emptyList(), asked.filter { it > lastOfBlock }, "segments past the block's last, $lastOfBlock")
        }
    }

    @Test
    fun `a read after the download stopped at a missing article throws why instead of ending`() = runBlocking<Unit> {
        // Segment 1 is missing, and says so once the reader has segment 0.
        service(server(missing = 1, answer = { if (it == 1) gate.await(60, TimeUnit.SECONDS) })).use { service ->
            val stream = NntpSeekableInputStream(nzb(), service)
            try {
                val buffer = ByteArray(segmentSize)
                var read = 0
                while (read < segmentSize) {
                    val n = stream.read(buffer, read, segmentSize - read)
                    if (n == -1) fail("segment 0 ended after $read bytes")
                    read += n
                }
                gate.countDown()
                // Let the download get to the missing segment and close the channel with it.
                val channel = NntpSeekableInputStream::class.java.getDeclaredField("currentChannel")
                    .apply { isAccessible = true }.get(stream) as ByteReadChannel
                try {
                    withTimeoutOrNull(5_000) { while (channel.awaitContent()) channel.readAvailable(ByteArray(segmentSize)) }
                } catch (_: IOException) {
                }

                val outcome = try {
                    "returned ${stream.read(buffer, 0, buffer.size)}"
                } catch (e: IOException) {
                    e
                }
                if (outcome !is Throwable) fail("read past the missing article $outcome")
                assertTrue(outcome.chain().any { it is ArticleNotFoundException }, "threw: ${outcome.chain()}")
            } finally {
                stream.close()
            }
        }
    }

    @Test
    fun `an encrypted file decrypts whole when its session runs on a single thread`() = runBlocking<Unit> {
        singleThread().use { single ->
            service(server()).use { service ->
                // The writer closes its stream once the block is decrypted, on the session's thread.
                val (received, cause) = withTimeoutOrNull(30_000) {
                    startStream(service, context = single).channel.readToEnd()
                } ?: fail("the stream never ended: closing its download blocked the session's only thread")
                assertEquals(null, cause)
                assertContentEquals(plaintext, received)
            }
        }
    }

    @Test
    fun `close on a single-threaded reader ends the download without blocking the thread`() = runBlocking<Unit> {
        singleThread().use { single ->
            service(server()).use { service ->
                val closed = CompletableDeferred<Unit>()
                val reader = sessions.launch(single) {
                    val stream = NntpSeekableInputStream(nzb(), service)
                    stream.read(ByteArray(16), 0, 16)
                    stream.close()
                    closed.complete(Unit)
                }
                withTimeoutOrNull(10_000) { closed.await() } ?: fail("close() blocked the reading coroutine's only thread")
                // The download stops all the same: the reading coroutine's job completes.
                withTimeoutOrNull(10_000) { reader.join() } ?: fail("the reading coroutine never completed")
            }
        }
    }

    @Test
    fun `the download a read starts belongs to the reading coroutine until close`() = runBlocking<Unit> {
        service(server()).use { service ->
            val stream = NntpSeekableInputStream(nzb(), service)
            val read = CompletableDeferred<Unit>()
            val reader = sessions.launch {
                stream.read(ByteArray(16), 0, 16)
                read.complete(Unit)
            }
            read.await()
            assertTrue(reader.children.any(), "the read's download is not the reading coroutine's child")
            assertFalse(reader.isCompleted)
            stream.close()
            reader.join()
        }
    }
}
