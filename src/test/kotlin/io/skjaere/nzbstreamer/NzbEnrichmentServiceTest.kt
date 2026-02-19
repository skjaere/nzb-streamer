package io.skjaere.nzbstreamer

import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
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
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class NzbEnrichmentServiceTest {

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
    fun `enrich returns Success when all files enriched`() = runBlocking {
        val data = "test file content".toByteArray()
        val server = startServer { reader, out ->
            val cmd = reader.readLine() // BODY <article1@test>
            assertTrue(cmd.startsWith("BODY"))
            writeYencResponse(out, "testfile.rar", data)
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val enrichmentService = NzbEnrichmentService(streamingService)
                val nzb = createNzb("article1@test")

                val result = enrichmentService.enrich(nzb)

                assertIs<EnrichmentResult.Success>(result)
                assertNotNull(result.enrichedNzb.files[0].yencHeaders)
                kotlin.test.assertEquals("testfile.rar", result.enrichedNzb.files[0].yencHeaders!!.name)
                assertNotNull(result.enrichedNzb.files[0].first16kb)
            }
        }
    }

    @Test
    fun `enrich returns MissingArticles when server responds 430`() = runBlocking {
        val server = startServer { reader, out ->
            reader.readLine() // BODY command
            out.write("430 No Such Article Found\r\n".toByteArray())
            out.flush()
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val enrichmentService = NzbEnrichmentService(streamingService)
                val nzb = createNzb("missing@test")

                val result = enrichmentService.enrich(nzb)

                assertIs<EnrichmentResult.MissingArticles>(result)
                assertIs<ArticleNotFoundException>(result.cause)
            }
        }
    }

    @Test
    fun `enrich returns Failure on NNTP connection error`() = runBlocking {
        // Server immediately closes the connection after welcome, causing a connection error
        val server = startServer { _, _ ->
            // Don't read or respond - just exit, closing the socket
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val enrichmentService = NzbEnrichmentService(streamingService)
                val nzb = createNzb("article@test")

                val result = enrichmentService.enrich(nzb)

                assertIs<EnrichmentResult.Failure>(result)
                assertIs<NntpConnectionException>(result.cause)
            }
        }
    }

    @Test
    fun `enrich returns Success with multiple files`() = runBlocking {
        val data1 = "content of file 1".toByteArray()
        val data2 = "content of file 2".toByteArray()
        val server = startServer { reader, out ->
            // Handle two BODY requests sequentially (concurrency=1)
            val cmd1 = reader.readLine()
            assertTrue(cmd1.startsWith("BODY"))
            writeYencResponse(out, "file1.rar", data1)

            val cmd2 = reader.readLine()
            assertTrue(cmd2.startsWith("BODY"))
            writeYencResponse(out, "file2.rar", data2)
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val enrichmentService = NzbEnrichmentService(streamingService)
                val nzb = createNzb("file1@test", "file2@test")

                val result = enrichmentService.enrich(nzb)

                assertIs<EnrichmentResult.Success>(result)
                val files = result.enrichedNzb.files
                kotlin.test.assertEquals(2, files.size)
                assertTrue(files.all { it.yencHeaders != null })
            }
        }
    }

    @Test
    fun `enrich returns MissingArticles when second file has missing article`() = runBlocking {
        val data1 = "content of file 1".toByteArray()
        val server = startServer { reader, out ->
            // First file succeeds
            val cmd1 = reader.readLine()
            assertTrue(cmd1.startsWith("BODY"))
            writeYencResponse(out, "file1.rar", data1)

            // Second file: article not found
            val cmd2 = reader.readLine()
            assertTrue(cmd2.startsWith("BODY"))
            out.write("430 No Such Article Found\r\n".toByteArray())
            out.flush()
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val enrichmentService = NzbEnrichmentService(streamingService)
                val nzb = createNzb("file1@test", "missing@test")

                val result = enrichmentService.enrich(nzb)

                assertIs<EnrichmentResult.MissingArticles>(result)
                assertIs<ArticleNotFoundException>(result.cause)
            }
        }
    }
}
