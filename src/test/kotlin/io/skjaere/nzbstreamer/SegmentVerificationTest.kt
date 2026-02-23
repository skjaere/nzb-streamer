package io.skjaere.nzbstreamer

import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nntp.NntpMockResponses
import io.skjaere.nntp.StatResult
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.PrepareConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.metadata.ArchiveMetadataService
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.startNntpServer
import io.skjaere.yenc.YencEncoder
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.ServerSocket

class SegmentVerificationTest {

    private lateinit var socket: ServerSocket
    private lateinit var serverScope: CoroutineScope

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

    private fun createMultiSegmentNzb(vararg files: Pair<String, List<String>>): NzbDocument {
        val nzbFiles = files.map { (subject, articleIds) ->
            NzbFile(
                poster = "test",
                date = 0,
                subject = subject,
                groups = listOf("alt.binaries.test"),
                segments = articleIds.mapIndexed { index, articleId ->
                    NzbSegment(bytes = 1000, number = index + 1, articleId = articleId)
                }
            )
        }
        return NzbDocument(nzbFiles)
    }

    private fun registerYencBody(articleId: String, name: String, data: ByteArray) {
        val yencArticle = YencEncoder.encodeSinglePart(data, name)
        NntpMockResponses.addYencBodyMock(articleId, yencArticle.data)
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
    fun `verifySegments returns Success when all segments present`() = runBlocking {
        val data = "test file content".toByteArray()
        registerYencBody("<seg1@test>", "testfile.rar", data)
        // seg2 and seg3 have no yenc body mock, so register explicit stat mocks
        NntpMockResponses.addStatMock("<seg2@test>", true)
        NntpMockResponses.addStatMock("<seg3@test>", true)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val enrichmentService = NzbEnrichmentService(streamingService)
            val verificationService = VerificationService(streamingService, concurrency = 1)
            val nzb = createMultiSegmentNzb(
                "file1" to listOf("seg1@test", "seg2@test", "seg3@test")
            )

            val enrichResult = enrichmentService.enrich(nzb)
            assertIs<EnrichmentResult.Success>(enrichResult)

            val verifyResult = verificationService.verifySegments(enrichResult.enrichedNzb)
            assertIs<VerificationResult.Success>(verifyResult)
        }
    }

    @Test
    fun `verifySegments returns MissingArticles when segments respond 430`() = runBlocking {
        val data = "test file content".toByteArray()
        registerYencBody("<seg1@test>", "testfile.rar", data)
        NntpMockResponses.addStatMock("<missing@test>", false)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val enrichmentService = NzbEnrichmentService(streamingService)
            val verificationService = VerificationService(streamingService, concurrency = 1)
            val nzb = createMultiSegmentNzb(
                "file1" to listOf("seg1@test", "missing@test")
            )

            val enrichResult = enrichmentService.enrich(nzb)
            assertIs<EnrichmentResult.Success>(enrichResult)

            val verifyResult = verificationService.verifySegments(enrichResult.enrichedNzb)
            assertIs<VerificationResult.MissingArticles>(verifyResult)
            assertTrue(verifyResult.message.contains("missing@test"))
        }
    }

    @Test
    fun `verifySegments returns Success for single-segment files`() = runBlocking {
        val data = "test file content".toByteArray()
        registerYencBody("<seg1@test>", "testfile.rar", data)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val enrichmentService = NzbEnrichmentService(streamingService)
            val verificationService = VerificationService(streamingService, concurrency = 1)
            val nzb = createMultiSegmentNzb(
                "file1" to listOf("seg1@test")
            )

            val enrichResult = enrichmentService.enrich(nzb)
            assertIs<EnrichmentResult.Success>(enrichResult)

            val verifyResult = verificationService.verifySegments(enrichResult.enrichedNzb)
            assertIs<VerificationResult.Success>(verifyResult)
        }
    }

    @Test
    fun `prepare skips verification when verifySegments is false`() = runBlocking {
        val data = "test file content".toByteArray()
        registerYencBody("<seg1@test>", "testfile.txt", data)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val metadataService = ArchiveMetadataService(
                streamingService,
                forwardThresholdBytes = 102400L,
                prepareConfig = PrepareConfig(verifySegments = false),
                concurrency = 1
            )
            val nzb = createMultiSegmentNzb(
                "file1" to listOf("seg1@test", "seg2@test")
            )

            val result = metadataService.prepare(nzb)
            // Should succeed without attempting STAT on seg2
            assertIs<PrepareResult.Success>(result)
        }
    }

    @Test
    fun `prepare returns MissingArticles when verification enabled and segments missing`() = runBlocking {
        val data = "test file content".toByteArray()
        registerYencBody("<seg1@test>", "testfile.txt", data)
        NntpMockResponses.addStatMock("<missing@test>", false)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val metadataService = ArchiveMetadataService(
                streamingService,
                forwardThresholdBytes = 102400L,
                prepareConfig = PrepareConfig(verifySegments = true),
                concurrency = 1
            )
            val nzb = createMultiSegmentNzb(
                "file1" to listOf("seg1@test", "missing@test")
            )

            val result = metadataService.prepare(nzb)
            assertIs<PrepareResult.MissingArticles>(result)
            assertTrue(result.message.contains("missing@test"))
        }
    }

    @Test
    fun `verifySegments returns Failure on NNTP connection error`() = runBlocking {
        val data = "test file content".toByteArray()
        registerYencBody("<seg1@test>", "testfile.rar", data)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val enrichmentService = NzbEnrichmentService(streamingService)
            val verificationService = VerificationService(streamingService, concurrency = 1)
            val nzb = createMultiSegmentNzb(
                "file1" to listOf("seg1@test", "seg2@test")
            )

            val enrichResult = enrichmentService.enrich(nzb)
            assertIs<EnrichmentResult.Success>(enrichResult)

            // Shut down the mock server to cause a connection error on STAT
            serverScope.cancel()
            socket.close()

            val verifyResult = verificationService.verifySegments(enrichResult.enrichedNzb)
            assertIs<VerificationResult.Failure>(verifyResult)
            assertIs<NntpConnectionException>(verifyResult.cause)
        }
    }

    // --- StatResult integration tests ---

    @Test
    fun `stat returns Found for existing article`() = runBlocking {
        NntpMockResponses.addStatMock("<article@test>", true)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val result = streamingService.withClient { client ->
                client.stat("<article@test>")
            }
            assertIs<StatResult.Found>(result)
            assertEquals(223, result.code)
            assertEquals("<article@test>", result.messageId)
        }
    }

    @Test
    fun `stat returns NotFound for missing article`() = runBlocking {
        NntpMockResponses.addStatMock("<missing@test>", false)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val result = streamingService.withClient { client ->
                client.stat("<missing@test>")
            }
            assertIs<StatResult.NotFound>(result)
            assertEquals(430, result.code)
        }
    }

    @Test
    fun `stat returns mix of Found and NotFound`() = runBlocking {
        NntpMockResponses.addStatMock("<present@test>", true)
        NntpMockResponses.addStatMock("<missing@test>", false)
        NntpMockResponses.addStatMock("<also-present@test>", true)

        val streamingService = createStreamingService(socket.localPort)
        streamingService.use {
            val articleIds = listOf("<present@test>", "<missing@test>", "<also-present@test>")
            val results = articleIds.map { id ->
                streamingService.withClient { client -> client.stat(id) }
            }

            assertIs<StatResult.Found>(results[0])
            assertIs<StatResult.NotFound>(results[1])
            assertIs<StatResult.Found>(results[2])

            val missing = results.filterIsInstance<StatResult.NotFound>()
            val found = results.filterIsInstance<StatResult.Found>()
            assertEquals(1, missing.size)
            assertEquals(2, found.size)
        }
    }
}
