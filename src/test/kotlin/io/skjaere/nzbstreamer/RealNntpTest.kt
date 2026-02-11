package io.skjaere.nzbstreamer

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.nntp.NntpClientPool
import io.skjaere.nntp.YencEvent
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import io.skjaere.nzbstreamer.metadata.ArchiveMetadataService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbParser
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.TimeUnit

/**
 * Tests against a real NNTP server using credentials from .env.
 * Skipped if .env is not present.
 */
class RealNntpTest {
    private val logger = LoggerFactory.getLogger(RealNntpTest::class.java)

    private fun loadEnv(): Map<String, String> {
        val envFile = File("/home/william/IdeaProjects/nzb-streamer/.env")
        if (!envFile.exists()) return emptyMap()
        return envFile.readLines()
            .filter { it.contains('=') && !it.startsWith('#') }
            .associate { line ->
                val (key, value) = line.split('=', limit = 2)
                key.trim() to value.trim()
            }
    }

    @Test
    fun `sequential enrichment of first 6 files`() = runBlocking {
        val env = loadEnv()
        assumeTrue(env.containsKey("NNTP_HOST"), "No .env file with NNTP credentials")

        val nzbBytes = File("/home/william/Downloads/Avengers.Age.of.Ultron.2015.1080p.WEB-DL.X264.AC3-EVO.nzb").readBytes()
        val nzb = NzbParser.parse(nzbBytes)
        logger.info("Parsed NZB with {} files", nzb.files.size)

        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val selectorManager = SelectorManager(Dispatchers.IO)

        val pool = NntpClientPool(
            host = env["NNTP_HOST"]!!,
            port = env["NNTP_PORT"]!!.toInt(),
            selectorManager = selectorManager,
            useTls = env["NNTP_USE_TLS"]!!.toBoolean(),
            username = env["NNTP_USERNAME"]!!,
            password = env["NNTP_PASSWORD"]!!,
            maxConnections = 1,
            scope = scope
        )
        pool.connect()

        try {
            for ((i, file) in nzb.files.take(6).withIndex()) {
                val articleId = file.segments.first().articleId
                logger.info("File {}: requesting article <{}>", i, articleId)

                var headers: YencHeaders? = null
                var dataSize = 0
                pool.bodyYenc("<$articleId>").collect { event ->
                    when (event) {
                        is YencEvent.Headers -> {
                            headers = event.yencHeaders
                            logger.info("File {}: got yenc headers: name={}, size={}", i, headers!!.name, headers!!.size)
                        }
                        is YencEvent.Body -> {
                            val data = event.data.toByteArray()
                            dataSize = data.size
                        }
                    }
                }
                logger.info("File {}: read {} bytes of decoded data", i, dataSize)
            }
            logger.info("All 6 sequential enrichments succeeded!")
        } finally {
            pool.close()
            scope.cancel()
        }
    }

    @Test
    fun `concurrent enrichment with connection pool`() = runBlocking {
        val env = loadEnv()
        assumeTrue(env.containsKey("NNTP_HOST"), "No .env file with NNTP credentials")

        val nzbBytes = File("/home/william/Downloads/Avengers.Age.of.Ultron.2015.1080p.WEB-DL.X264.AC3-EVO.nzb").readBytes()
        val nzb = NzbParser.parse(nzbBytes)

        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val selectorManager = SelectorManager(Dispatchers.IO)

        val pool = NntpClientPool(
            host = env["NNTP_HOST"]!!,
            port = env["NNTP_PORT"]!!.toInt(),
            selectorManager = selectorManager,
            useTls = env["NNTP_USE_TLS"]!!.toBoolean(),
            username = env["NNTP_USERNAME"]!!,
            password = env["NNTP_PASSWORD"]!!,
            maxConnections = 3,
            scope = scope
        )
        pool.connect()
        logger.info("Connection pool initialized with 3 connections")

        try {
            val files = nzb.files.take(20)
            coroutineScope {
                files.mapIndexed { i, file ->
                    async {
                        try {
                            enrichFileWithPool(i, file, pool)
                        } catch (e: Exception) {
                            logger.error("File {} FAILED: {}", i, e.message)
                        }
                    }
                }.awaitAll()
            }
            logger.info("All concurrent enrichments completed!")
        } finally {
            pool.close()
            scope.cancel()
        }
    }

    @Test
    @Timeout(5, unit = TimeUnit.MINUTES)
    fun `full pipeline with real NZB`() = runBlocking {
        val env = loadEnv()
        assumeTrue(env.containsKey("NNTP_HOST"), "No .env file with NNTP credentials")
        assumeTrue(
            File("/home/william/Downloads/Avengers.Age.of.Ultron.2015.1080p.WEB-DL.X264.AC3-EVO.nzb").exists(),
            "NZB file not found"
        )

        val nntpConfig = NntpConfig(
            host = env["NNTP_HOST"]!!,
            port = env["NNTP_PORT"]!!.toInt(),
            username = env["NNTP_USERNAME"]!!,
            password = env["NNTP_PASSWORD"]!!,
            useTls = env["NNTP_USE_TLS"]!!.toBoolean(),
            concurrency = 3
        )

        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val streamingService = NntpStreamingService(nntpConfig, scope)
        streamingService.connect()

        val enrichmentService = NzbEnrichmentService(streamingService)
        val metadataService = ArchiveMetadataService(streamingService, 102400L)

        try {
            val nzbBytes = File("/home/william/Downloads/Avengers.Age.of.Ultron.2015.1080p.WEB-DL.X264.AC3-EVO.nzb").readBytes()
            val nzb = NzbParser.parse(nzbBytes)

            enrichmentService.enrich(nzb)
            val enrichedNzb = NzbDocument(nzb.files.filter { it.yencHeaders != null })
            logger.info("Enriched {} of {} files", enrichedNzb.files.size, nzb.files.size)
            assertTrue(enrichedNzb.files.isNotEmpty(), "Should have enriched files")

            val extracted = metadataService.extractMetadata(enrichedNzb)
            val metadata = extracted.response
            logger.info("Got {} archive entries, {} volumes, obfuscated={}",
                metadata.entries.size, metadata.volumes.size, metadata.obfuscated)
            assertTrue(metadata.entries.isNotEmpty(), "Should have archive file entries")

            for (entry in metadata.entries) {
                logger.info("  Entry: {}", entry)
            }
        } finally {
            streamingService.close()
            scope.cancel()
        }
    }

    private suspend fun enrichFileWithPool(
        index: Int,
        file: NzbFile,
        pool: NntpClientPool
    ) {
        val articleId = file.segments.first().articleId
        logger.info("File {}: requesting article <{}>", index, articleId)

        pool.bodyYenc("<$articleId>").collect { event ->
            when (event) {
                is YencEvent.Headers -> {
                    logger.info("File {}: got headers name={}", index, event.yencHeaders.name)
                }
                is YencEvent.Body -> {
                    val data = event.data.toByteArray()
                    logger.info("File {}: read {} bytes", index, data.size)
                }
            }
        }
    }
}
