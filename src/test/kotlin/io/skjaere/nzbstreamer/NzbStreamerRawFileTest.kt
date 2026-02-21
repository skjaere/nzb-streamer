package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.NzbMetadataResponse
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.stream.FileResolveResult
import io.skjaere.nntp.YencHeaders
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.ServerSocket
import kotlin.test.assertIs
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NzbStreamerRawFileTest {

    private val server: ServerSocket
    private val streamer: NzbStreamer

    init {
        server = ServerSocket(0)
        Thread {
            try {
                while (!server.isClosed) {
                    val client = server.accept()
                    client.use { socket ->
                        val out = socket.getOutputStream()
                        out.write("200 welcome\r\n".toByteArray())
                        out.flush()
                        // keep connection open until client disconnects
                        socket.getInputStream().read()
                    }
                }
            } catch (_: Exception) {
            }
        }.start()

        streamer = NzbStreamer {
            nntp {
                host = "localhost"
                port = server.localPort
                useTls = false
                concurrency = 1
                maxConnections = 1
            }
        }
    }

    @AfterAll
    fun tearDown() {
        streamer.close()
        server.close()
    }

    private fun createRawMetadata(
        obfuscatedName: String,
        resolvedName: String,
        fileSize: Long
    ): ExtractedMetadata.Raw {
        val nzbFile = NzbFile(
            poster = "test",
            date = 0,
            subject = "test",
            groups = listOf("alt.binaries.test"),
            segments = listOf(NzbSegment(bytes = 1000, number = 1, articleId = "seg@test")),
            yencHeaders = YencHeaders(line = 128, size = fileSize, name = obfuscatedName, partEnd = 1000)
        )
        return ExtractedMetadata.Raw(
            response = NzbMetadataResponse(
                volumes = listOf(resolvedName),
                obfuscated = true,
                entries = emptyList()
            ),
            orderedArchiveNzb = NzbDocument(listOf(nzbFile))
        )
    }

    @Test
    fun `resolveStreamableFiles uses resolved name from volumes, not obfuscated yenc name`() {
        val metadata = createRawMetadata(
            obfuscatedName = "7e3e86335eb7410334339c5320dec20efa1fb5aef4307ae50c019db0cde6",
            resolvedName = "A.Knight.of.the.Seven.Kingdoms.S01E02.1080p.WEB-DL.mkv",
            fileSize = 2_400_000_000L
        )

        val files = streamer.resolveStreamableFiles(metadata)

        assertEquals(1, files.size)
        assertEquals("A.Knight.of.the.Seven.Kingdoms.S01E02.1080p.WEB-DL.mkv", files[0].path)
        assertEquals(2_400_000_000L, files[0].totalSize)
    }

    @Test
    fun `resolveFile finds file by resolved name`() {
        val metadata = createRawMetadata(
            obfuscatedName = "7e3e86335eb7410334339c5320dec20efa1fb5aef4307ae50c019db0cde6",
            resolvedName = "A.Knight.of.the.Seven.Kingdoms.S01E02.1080p.WEB-DL.mkv",
            fileSize = 2_400_000_000L
        )

        val result = streamer.resolveFile(metadata, "A.Knight.of.the.Seven.Kingdoms.S01E02.1080p.WEB-DL.mkv")

        val ok = assertIs<FileResolveResult.Ok>(result)
        assertEquals(2_400_000_000L, ok.totalSize)
        assertEquals(1, ok.splits.size)
        assertEquals(0, ok.splits[0].volumeIndex)
    }

    @Test
    fun `resolveFile returns NotFound when searching by obfuscated name`() {
        val metadata = createRawMetadata(
            obfuscatedName = "7e3e86335eb7410334339c5320dec20efa1fb5aef4307ae50c019db0cde6",
            resolvedName = "A.Knight.of.the.Seven.Kingdoms.S01E02.1080p.WEB-DL.mkv",
            fileSize = 2_400_000_000L
        )

        val result = streamer.resolveFile(
            metadata,
            "7e3e86335eb7410334339c5320dec20efa1fb5aef4307ae50c019db0cde6"
        )

        assertIs<FileResolveResult.NotFound>(result)
    }
}
