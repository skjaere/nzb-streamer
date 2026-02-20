package io.skjaere.nzbstreamer.enrichment

import io.ktor.utils.io.*
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpException
import io.skjaere.nntp.YencEvent
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import io.skjaere.compressionutils.Par2Parser
import org.slf4j.LoggerFactory
import kotlin.math.min

class NzbEnrichmentService(
    private val streamingService: NntpStreamingService
) {
    private val logger = LoggerFactory.getLogger(NzbEnrichmentService::class.java)

    suspend fun enrich(nzb: NzbDocument): EnrichmentResult {
        try {
            coroutineScope {
                nzb.files.map { file ->
                    async {
                        enrichFile(file)
                    }
                }.awaitAll()
            }
        } catch (e: ArticleNotFoundException) {
            logger.warn("Article not found during enrichment: {}", e.message)
            return EnrichmentResult.MissingArticles(
                e.message ?: "Article not found",
                e
            )
        } catch (e: NntpException) {
            logger.error("NNTP failure during enrichment: {}", e.message, e)
            return EnrichmentResult.Failure(
                e.message ?: "NNTP failure",
                e
            )
        }

        val enrichedCount = nzb.files.count { it.yencHeaders != null }
        logger.info("Enriched {} of {} files", enrichedCount, nzb.files.size)

        // After enrichment, download the base PAR2 file (contains recovery set metadata).
        // Skip .volXXX+YYY.par2 recovery volume files — they contain recovery blocks
        // and can be very large (tens of MB each), causing OOM if loaded into memory.
        try {
            coroutineScope {
                nzb.files
                    .filter { file ->
                        val first16kb = file.first16kb ?: return@filter false
                        Par2Parser.isPar2(first16kb) && Par2Parser.hasFileDescriptions(first16kb)
                    }
                    .map { file ->
                        async {
                            downloadPar2(file)
                        }
                    }.awaitAll()
            }
        } catch (e: ArticleNotFoundException) {
            logger.warn("PAR2 article not found: {}", e.message)
            return EnrichmentResult.MissingArticles(
                e.message ?: "PAR2 article not found",
                e
            )
        } catch (e: NntpException) {
            logger.error("NNTP failure during PAR2 download: {}", e.message, e)
            return EnrichmentResult.Failure(
                e.message ?: "NNTP failure during PAR2 download",
                e
            )
        }

        return EnrichmentResult.Success(nzb)
    }

    private suspend fun enrichFile(file: NzbFile) {
        val firstSegment = file.segments.first()
        streamingService.withClient { client ->
            client.bodyYenc("<${firstSegment.articleId}>").collect { event ->
                when (event) {
                    is YencEvent.Headers -> file.yencHeaders = event.yencHeaders
                    is YencEvent.Body -> {
                        val data = event.data.toByteArray()
                        file.first16kb = data.copyOf(min(16384, data.size))
                    }
                }
            }
        }
        logger.debug("Enriched file: {}", file.yencHeaders?.name)
    }

    private suspend fun downloadPar2(file: NzbFile) {
        logger.info("Downloading PAR2 file: {}", file.yencHeaders?.name)
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)
        streamingService.streamSegments(queue) { channel ->
            file.par2Data = channel.toByteArray()
        }
        logger.info("Downloaded PAR2 file: {} ({} bytes)", file.yencHeaders?.name, file.par2Data?.size)
    }
}
