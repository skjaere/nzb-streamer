package io.skjaere.nzbstreamer

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readAvailable
import io.skjaere.compressionutils.generation.ArchiveFile
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ArchiveVolume
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.nntp.NntpMockResponses
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.startNntpServer
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.ByteArrayOutputStream
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * A scene-style nested release end to end over NNTP: an outer RAR5 set wrapping uniform inner
 * RAR5 volumes, posted as multi-segment yEnc articles to the in-process mock server, prepared
 * and streamed through [NzbStreamer]. Also guards the header walk's cost: preparing must not
 * download (much of) the release, which the read-ahead seekable stream used to do.
 */
class NestedArchivePrepareIntegrationTest {
    private lateinit var socket: ServerSocket
    private lateinit var serverScope: CoroutineScope

    private val segmentSize = 64 * 1024

    @BeforeEach
    fun setUp() {
        socket = ServerSocket(0)
        serverScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        serverScope.launch { startNntpServer(socket) }
        NntpMockResponses.clearMockResponses()
    }

    @AfterEach
    fun tearDown() {
        NntpMockResponses.clearMockResponses()
        serverScope.cancel()
        socket.close()
    }

    /** Posts [volumes] as multipart yEnc articles and returns the NZB describing them. */
    private fun post(volumes: List<ArchiveVolume>): ByteArray {
        val files = volumes.mapIndexed { v, volume ->
            val parts = volume.data.asList().chunked(segmentSize).map { it.toByteArray() }
            var begin = 1L
            val segments = parts.mapIndexed { p, part ->
                val id = "v$v-p${p + 1}@it"
                val end = begin + part.size - 1
                val article = ("=ybegin part=${p + 1} total=${parts.size} line=128 size=${volume.data.size} name=${volume.filename}\r\n" +
                    "=ypart begin=$begin end=$end\r\n").toByteArray(Charsets.ISO_8859_1) +
                    RapidYenc.encodeEx(part).data +
                    "\r\n=yend size=${part.size} part=${p + 1} pcrc32=${RapidYenc.crc32(part).toString(16).padStart(8, '0')}\r\n".toByteArray(Charsets.ISO_8859_1)
                NntpMockResponses.addYencBodyMock("<$id>", article)
                begin = end + 1
                """<segment bytes="${article.size}" number="${p + 1}">$id</segment>"""
            }
            """<file poster="it" date="0" subject="[${v + 1}/${volumes.size}] - &quot;${volume.filename}&quot; yEnc (1/${parts.size})"><groups><group>alt.binaries.test</group></groups><segments>${segments.joinToString("")}</segments></file>"""
        }
        return """<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""".toByteArray()
    }

    private suspend fun ByteReadChannel.readAll(): ByteArray {
        val out = ByteArrayOutputStream()
        val buf = ByteArray(64 * 1024)
        while (true) {
            val n = readAvailable(buf, 0, buf.size)
            if (n == -1) break
            out.write(buf, 0, n)
        }
        return out.toByteArray()
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    fun `nested RAR5 release prepares cheaply and streams byte-exact`() = runBlocking<Unit> {
        val payload = Random(7).nextBytes(12 * 1024 * 1024)
        // 12 uniform inner volumes inside ~4 outer volumes, like scene releases repacked by posters.
        val inner = ArchiveGenerator.generate(payload, 1024L * 1024, ContainerType.RAR5, "Show.S01E01.1080p.WEB.mkv")
        val outer = ArchiveGenerator.generate(inner.map { ArchiveFile(it.filename, it.data) }, 4L * 1024 * 1024, ContainerType.RAR5)
        assertTrue(inner.size >= 10, "inner volumes: ${inner.size}")
        assertTrue(outer.size >= 3, "outer volumes: ${outer.size}")
        val nzb = post(outer)
        val totalSegments = outer.sumOf { (it.data.size + segmentSize - 1) / segmentSize }

        val config = NntpConfig(host = "localhost", port = socket.localPort, username = "", password = "", useTls = false, maxConnections = 4)
        NzbStreamer.fromConfig(config).use { streamer ->
            val before = NntpMockResponses.getCommandCallCount("BODY")
            val prepared = streamer.prepare(nzb)
            val prepareBodies = NntpMockResponses.getCommandCallCount("BODY") - before
            assertIs<PrepareResult.Success>(prepared, "prepare: $prepared")
            assertIs<ExtractedMetadata.NestedArchive>(prepared.metadata)

            // Header reads only: well under half of the release's segments.
            println("prepare fetched $prepareBodies of $totalSegments segments")
            assertTrue(prepareBodies < totalSegments / 2, "prepare fetched $prepareBodies of $totalSegments segments")

            val file = streamer.resolveStreamableFiles(prepared.metadata).single { it.path.endsWith(".mkv") }
            assertEquals(payload.size.toLong(), file.totalSize)
            var streamed = ByteArray(0)
            streamer.streamFile(prepared.metadata, file) { channel -> streamed = channel.readAll() }
            assertContentEquals(payload, streamed)
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    fun `scene-shaped nested release - 72 inner volumes in many outer volumes - costs little more than one article per header`() = runBlocking<Unit> {
        // A large nested set scaled down: 73 inner volumes, ~14 outer volumes, headers spread out.
        val payload = Random(11).nextBytes(72 * 256 * 1024)
        val inner = ArchiveGenerator.generate(payload, 256L * 1024, ContainerType.RAR5, "Show.S02E10.1080p.BluRay.x264.mkv")
        val outer = ArchiveGenerator.generate(inner.map { ArchiveFile(it.filename, it.data) }, 5L * 256 * 1024 + 7_000, ContainerType.RAR5)
        val nzb = post(outer)
        val totalSegments = outer.sumOf { (it.data.size + segmentSize - 1) / segmentSize }

        val config = NntpConfig(host = "localhost", port = socket.localPort, username = "", password = "", useTls = false, maxConnections = 4)
        NzbStreamer.fromConfig(config).use { streamer ->
            val before = NntpMockResponses.getCommandCallCount("BODY")
            val prepared = streamer.prepare(nzb)
            val bodies = NntpMockResponses.getCommandCallCount("BODY") - before
            assertIs<PrepareResult.Success>(prepared, "prepare: $prepared")
            println("scene-shaped: ${inner.size} inner in ${outer.size} outer volumes, prepare fetched $bodies of $totalSegments segments")
            // Needed: ~1 per outer file (enrichment) and ~1 per outer entry or volume header. Inner
            // volumes have real names, so their starts aren't read (the parser infers the split
            // from the first one). The old read-ahead stream needed several times the release.
            assertTrue(bodies <= outer.size + inner.size + 30, "prepare fetched $bodies for ${inner.size} inner volumes")
            val file = streamer.resolveStreamableFiles(prepared.metadata).single { it.path.endsWith(".mkv") }
            assertEquals(payload.size.toLong(), file.totalSize)
        }
    }
}
