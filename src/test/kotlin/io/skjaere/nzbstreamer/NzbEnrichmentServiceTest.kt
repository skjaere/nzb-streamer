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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
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

    @Test
    fun `enrich detects PAR2 base index by magic bytes with obfuscated filenames`() = runBlocking {
        val par2IndexData = buildPar2IndexFile("movie.mkv", 1_000_000L)
        val par2RecoveryData = buildPar2RecoveryVolume()
        val archiveData = ByteArray(512) { (it % 256).toByte() }

        // All yenc names are obfuscated hex hashes — no .par2 extension anywhere
        val responses = mapOf(
            "abc123@obfs" to Pair("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", archiveData),
            "def456@obfs" to Pair("d4e5f6a7b8c9d0e1f2a3b4c5d6a7b8c9", par2IndexData),
            "ghi789@obfs" to Pair("f1e2d3c4b5a6f7e8d9c0b1a2f3e4d5c6", par2RecoveryData),
        )

        val server = startServer { reader, out ->
            while (true) {
                val line = reader.readLine() ?: break
                if (line.startsWith("BODY")) {
                    val articleId = line.removePrefix("BODY ").trim()
                        .removeSurrounding("<", ">")
                    val (name, data) = responses[articleId]
                        ?: error("Unexpected article: $articleId")
                    writeYencResponse(out, name, data)
                }
            }
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val enrichmentService = NzbEnrichmentService(streamingService)
                val nzb = createNzb("abc123@obfs", "def456@obfs", "ghi789@obfs")

                val result = enrichmentService.enrich(nzb)

                assertIs<EnrichmentResult.Success>(result)
                val files = result.enrichedNzb.files

                // All files should be enriched
                assertTrue(files.all { it.yencHeaders != null })
                assertTrue(files.all { it.first16kb != null })

                // None of the filenames have a .par2 extension — detection is by magic bytes only
                assertTrue(files.none { it.yencHeaders!!.name.endsWith(".par2", ignoreCase = true) })

                // Base PAR2 index file should have par2Data downloaded
                val par2IndexFile = files.first { it.segments[0].articleId == "def456@obfs" }
                assertNotNull(par2IndexFile.par2Data, "Base PAR2 index should be detected and downloaded")

                // PAR2 recovery volume should NOT have par2Data
                val par2RecoveryFile = files.first { it.segments[0].articleId == "ghi789@obfs" }
                assertNull(par2RecoveryFile.par2Data, "PAR2 recovery volume should not be downloaded")

                // Archive file should NOT have par2Data
                val archiveFile = files.first { it.segments[0].articleId == "abc123@obfs" }
                assertNull(archiveFile.par2Data, "Archive file should not be downloaded as PAR2")
            }
        }
    }

    @Test
    fun `enrich detects PAR2 base index larger than 16KB with obfuscated filenames`() = runBlocking {
        // Build a PAR2 index file that exceeds 16KB (two FileDesc packets, second has a long name)
        val smallPkt = buildPar2IndexFile("movie.mkv", 1_000_000L)
        val largePkt = buildPar2IndexFile("a".repeat(16300), 500_000L)
        val par2IndexData = smallPkt + largePkt // ~16.5KB total

        val par2RecoveryData = buildPar2RecoveryVolume()
        val archiveData = ByteArray(512) { (it % 256).toByte() }

        val responses = mapOf(
            "arc@obfs" to Pair("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", archiveData),
            "par2idx@obfs" to Pair("d4e5f6a7b8c9d0e1f2a3b4c5d6a7b8c9", par2IndexData),
            "par2vol@obfs" to Pair("f1e2d3c4b5a6f7e8d9c0b1a2f3e4d5c6", par2RecoveryData),
        )

        val server = startServer { reader, out ->
            while (true) {
                val line = reader.readLine() ?: break
                if (line.startsWith("BODY")) {
                    val articleId = line.removePrefix("BODY ").trim()
                        .removeSurrounding("<", ">")
                    val (name, data) = responses[articleId]
                        ?: error("Unexpected article: $articleId")
                    writeYencResponse(out, name, data)
                }
            }
        }

        server.use {
            val streamingService = createStreamingService(server.localPort)
            streamingService.use {
                val enrichmentService = NzbEnrichmentService(streamingService)
                val nzb = createNzb("arc@obfs", "par2idx@obfs", "par2vol@obfs")

                val result = enrichmentService.enrich(nzb)

                assertIs<EnrichmentResult.Success>(result)

                // PAR2 index > 16KB should still be detected (parser finds FileDesc before truncation)
                val par2IndexFile = result.enrichedNzb.files
                    .first { it.segments[0].articleId == "par2idx@obfs" }
                assertNotNull(par2IndexFile.par2Data,
                    "PAR2 index >16KB should be detected via FileDesc in first16kb")

                // Recovery volume still excluded
                val par2RecoveryFile = result.enrichedNzb.files
                    .first { it.segments[0].articleId == "par2vol@obfs" }
                assertNull(par2RecoveryFile.par2Data)
            }
        }
    }

    /**
     * Builds a minimal PAR2 index file containing one FileDesc packet.
     */
    private fun buildPar2IndexFile(filename: String, fileSize: Long): ByteArray {
        val filenameBytes = filename.toByteArray(Charsets.UTF_8)
        val paddedLen = (filenameBytes.size + 3) and 3.inv()
        val bodySize = 56 + paddedLen
        val packetLength = 64L + bodySize

        val buf = ByteBuffer.allocate(packetLength.toInt()).order(ByteOrder.LITTLE_ENDIAN)
        // Header (64 bytes)
        buf.put("PAR2".toByteArray()); buf.put(0); buf.put("PKT".toByteArray())
        buf.putLong(packetLength)
        buf.put(ByteArray(16)) // packet hash
        buf.put(ByteArray(16)) // recovery set ID
        buf.put("PAR 2.0".toByteArray()); buf.put(0); buf.put("FileDesc".toByteArray())
        // Body
        buf.put(ByteArray(16)) // file ID
        buf.put(ByteArray(16)) // file hash
        buf.put(ByteArray(16)) // hash16k
        buf.putLong(fileSize)
        buf.put(filenameBytes)
        if (paddedLen > filenameBytes.size) buf.put(ByteArray(paddedLen - filenameBytes.size))

        return buf.array()
    }

    /**
     * Builds a PAR2 recovery volume: valid magic + header claiming a huge body,
     * but only a few bytes of actual data. The parser returns empty results on
     * truncation, so hasFileDescriptions returns false.
     */
    private fun buildPar2RecoveryVolume(): ByteArray {
        val buf = ByteBuffer.allocate(128).order(ByteOrder.LITTLE_ENDIAN)
        // Header (64 bytes)
        buf.put("PAR2".toByteArray()); buf.put(0); buf.put("PKT".toByteArray())
        buf.putLong(1_000_064L) // claims 1MB body
        buf.put(ByteArray(16)) // packet hash
        buf.put(ByteArray(16)) // recovery set ID
        buf.put("PAR 2.0".toByteArray()); buf.put(0); buf.put("RecvSlic".toByteArray())
        // 64 bytes of filler (parser expects ~1MB, gets 64 → throws)
        buf.put(ByteArray(64))

        return buf.array()
    }
}
