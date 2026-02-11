package io.skjaere.nzbstreamer.enrichment

import io.ktor.utils.io.*
import io.skjaere.nntp.YencEvent
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import kotlin.math.min

class NzbEnrichmentService(
    private val streamingService: NntpStreamingService
) {
    private val logger = LoggerFactory.getLogger(NzbEnrichmentService::class.java)

    suspend fun enrich(nzb: NzbDocument) {
        coroutineScope {
            nzb.files.map { file ->
                async {
                    try {
                        enrichFile(file)
                    } catch (e: Exception) {
                        logger.error("Failed to enrich file: {}", file.subject, e)
                    }
                }
            }.awaitAll()
        }
        val enrichedCount = nzb.files.count { it.yencHeaders != null }
        logger.info("Enriched {} of {} files", enrichedCount, nzb.files.size)

        // After enrichment, download the base PAR2 file (contains recovery set metadata).
        // Skip .volXXX+YYY.par2 recovery volume files — they contain recovery blocks
        // and can be very large (tens of MB each), causing OOM if loaded into memory.
        coroutineScope {
            nzb.files
                .filter { file ->
                    val name = file.yencHeaders?.name ?: return@filter false
                    name.endsWith(".par2", ignoreCase = true)
                            && !name.contains(".vol", ignoreCase = true)
                }
                .map { file ->
                    async {
                        try {
                            downloadPar2(file)
                        } catch (e: Exception) {
                            logger.error("Failed to download PAR2 file: {}", file.yencHeaders?.name, e)
                        }
                    }
                }.awaitAll()
        }
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
        // fileIndex and globalOffset are only metadata on queue items;
        // they don't affect the actual download behavior, so we pass 0.
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)
        val (channel, _) = streamingService.streamSegments(queue)
        file.par2Data = channel.toByteArray()
        logger.info("Downloaded PAR2 file: {} ({} bytes)", file.yencHeaders?.name, file.par2Data?.size)
    }
}
