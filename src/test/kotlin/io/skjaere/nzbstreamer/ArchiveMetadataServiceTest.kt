package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.metadata.ArchiveMetadataService
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import kotlin.test.assertIs
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
            concurrency = 1
        )
        val service = NntpStreamingService(config)
        runBlocking { service.connect() }
        return service
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
                val metadataService = ArchiveMetadataService(streamingService, 102400L)
                val nzb = createNzb("article1@test")

                val result = metadataService.prepare(nzb)

                val success = assertIs<PrepareResult.Success>(result)
                assertIs<ExtractedMetadata.Raw>(success.metadata)
                assertTrue(success.metadata.response.entries.isEmpty())
                Unit
            }
        }
    }
}
