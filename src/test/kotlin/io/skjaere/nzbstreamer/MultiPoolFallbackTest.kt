package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.mocknntp.testcontainer.MockNntpServerContainer
import io.skjaere.nntp.StatResult
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.assertEquals

class MultiPoolFallbackTest {

    companion object {
        private val primaryContainer = MockNntpServerContainer()
        private val fillContainer = MockNntpServerContainer()

        @BeforeAll
        @JvmStatic
        fun startContainers() {
            primaryContainer.start()
            fillContainer.start()
        }

        @AfterAll
        @JvmStatic
        fun stopContainers() {
            primaryContainer.stop()
            fillContainer.stop()
        }
    }

    @AfterEach
    fun tearDown() {
        runBlocking {
            primaryContainer.client.clearYencBodyExpectations()
            primaryContainer.client.clearStatExpectations()
            primaryContainer.client.clearStats()
            fillContainer.client.clearYencBodyExpectations()
            fillContainer.client.clearStatExpectations()
            fillContainer.client.clearStats()
        }
    }

    private fun createConfigs(): List<NntpConfig> = listOf(
        NntpConfig(
            host = primaryContainer.nntpHost,
            port = primaryContainer.nntpPort,
            username = "",
            password = "",
            useTls = false,
            maxConnections = 4
        ),
        NntpConfig(
            host = fillContainer.nntpHost,
            port = fillContainer.nntpPort,
            username = "",
            password = "",
            useTls = false,
            maxConnections = 4
        )
    )

    private fun createStreamingService(configs: List<NntpConfig>): NntpStreamingService {
        val service = NntpStreamingService(configs)
        runBlocking { service.connect() }
        return service
    }

    /**
     * Creates an enriched NzbFile with yencHeaders set, suitable for SegmentQueueService.
     */
    private fun enrichedNzbFile(
        segments: List<Pair<String, Int>>,
        filename: String = "data.bin"
    ): NzbFile {
        val totalSize = segments.sumOf { it.second }.toLong()
        val partSize = segments.first().second.toLong()
        return NzbFile(
            poster = "test",
            date = 0,
            subject = filename,
            groups = listOf("alt.binaries.test"),
            segments = segments.mapIndexed { index, (articleId, bytes) ->
                NzbSegment(bytes = bytes.toLong(), number = index + 1, articleId = articleId)
            },
            yencHeaders = YencHeaders(
                line = 128,
                size = totalSize,
                name = filename,
                partEnd = if (segments.size > 1) partSize else null
            )
        )
    }

    @Test
    fun `downloadSegment falls back to fill pool when primary returns 430`() = runBlocking {
        val seg1Data = ByteArray(16 * 1024) { (it % 256).toByte() }
        val seg2Data = ByteArray(16 * 1024) { ((it + 128) % 256).toByte() }
        val seg1Id = "seg1@primary"
        val seg2Id = "seg2@fill"

        // seg1 on primary, seg2 only on fill
        primaryContainer.client.addYencBodyExpectation("<$seg1Id>", seg1Data, "data.bin")
        fillContainer.client.addYencBodyExpectation("<$seg2Id>", seg2Data, "data.bin")

        primaryContainer.client.clearStats()
        fillContainer.client.clearStats()

        val file = enrichedNzbFile(
            listOf(seg1Id to seg1Data.size, seg2Id to seg2Data.size)
        )
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)

        val service = createStreamingService(createConfigs())
        service.use {
            service.streamSegments(queue, name = "test") { channel ->
                val result = channel.toByteArray()
                assertEquals(seg1Data.size + seg2Data.size, result.size)
            }
        }

        // Verify fill server handled at least one BODY request
        val fillStats = fillContainer.client.getStats()
        assertTrue(
            (fillStats["BODY"] ?: 0) >= 1,
            "Fill server should have received at least 1 BODY request, got: $fillStats"
        )
    }

    @Test
    fun `streamSegments succeeds when all segments on primary pool`() = runBlocking {
        val segData = ByteArray(32 * 1024) { (it % 256).toByte() }
        val segId = "seg-all-primary@test"

        primaryContainer.client.addYencBodyExpectation("<$segId>", segData, "data.bin")

        primaryContainer.client.clearStats()
        fillContainer.client.clearStats()

        val file = enrichedNzbFile(listOf(segId to segData.size))
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)

        val service = createStreamingService(createConfigs())
        service.use {
            service.streamSegments(queue, name = "test") { channel ->
                val result = channel.toByteArray()
                assertEquals(segData.size, result.size)
                assertTrue(result.contentEquals(segData))
            }
        }

        // Fill server should not have been called
        val fillStats = fillContainer.client.getStats()
        assertEquals(0, fillStats.getOrDefault("BODY", 0), "Fill server should not have received any BODY requests")
    }

    @Test
    fun `statAcrossPools finds article on fill when primary returns NotFound`() = runBlocking {
        val articleId = "<test-article@mock>"

        primaryContainer.client.addStatExpectation(articleId, false)
        fillContainer.client.addStatExpectation(articleId, true)

        val service = createStreamingService(createConfigs())
        service.use {
            val result = service.statAcrossPools(articleId)
            assertIs<StatResult.Found>(result)
        }
    }

    @Test
    fun `statAcrossPools returns NotFound when missing on all pools`() = runBlocking {
        val articleId = "<missing-everywhere@mock>"

        primaryContainer.client.addStatExpectation(articleId, false)
        fillContainer.client.addStatExpectation(articleId, false)

        val service = createStreamingService(createConfigs())
        service.use {
            val result = service.statAcrossPools(articleId)
            assertIs<StatResult.NotFound>(result)
        }
    }

    @Test
    fun `statAcrossPools returns Found immediately when primary has article`() = runBlocking {
        val articleId = "<on-primary@mock>"

        primaryContainer.client.addStatExpectation(articleId, true)

        primaryContainer.client.clearStats()
        fillContainer.client.clearStats()

        val service = createStreamingService(createConfigs())
        service.use {
            val result = service.statAcrossPools(articleId)
            assertIs<StatResult.Found>(result)
        }

        // Fill server should not have been called for STAT
        val fillStats = fillContainer.client.getStats()
        assertEquals(0, fillStats.getOrDefault("STAT", 0), "Fill server should not have received STAT requests")
    }

    @Test
    fun `verifySegments succeeds when article found on fill pool`() = runBlocking {
        val articleOnPrimary = "<seg1@primary>"
        val articleOnFill = "<seg2@fill>"

        primaryContainer.client.addStatExpectation(articleOnPrimary, true)
        primaryContainer.client.addStatExpectation(articleOnFill, false)
        fillContainer.client.addStatExpectation(articleOnFill, true)

        val nzb = NzbDocument(
            listOf(
                NzbFile(
                    poster = "test",
                    date = 0,
                    subject = "test",
                    groups = listOf("alt.binaries.test"),
                    segments = listOf(
                        NzbSegment(bytes = 1000, number = 1, articleId = "seg1@primary"),
                        NzbSegment(bytes = 1000, number = 2, articleId = "seg2@fill")
                    )
                )
            )
        )

        val service = createStreamingService(createConfigs())
        service.use {
            val verificationService = VerificationService(service, concurrency = 1)
            val result = verificationService.verifySegments(nzb)
            assertIs<VerificationResult.Success>(result)
        }
    }

    @Test
    fun `single-config constructor backward compatibility`() = runBlocking {
        val segData = ByteArray(16 * 1024) { (it % 256).toByte() }
        val segId = "seg-single@test"

        primaryContainer.client.addYencBodyExpectation("<$segId>", segData, "data.bin")

        val singleConfig = NntpConfig(
            host = primaryContainer.nntpHost,
            port = primaryContainer.nntpPort,
            username = "",
            password = "",
            useTls = false,
            maxConnections = 4
        )
        val service = NntpStreamingService(singleConfig)
        runBlocking { service.connect() }

        val file = enrichedNzbFile(listOf(segId to segData.size))
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)

        service.use {
            service.streamSegments(queue, name = "test") { channel ->
                val result = channel.toByteArray()
                assertEquals(segData.size, result.size)
            }
        }
    }
}
