package io.skjaere.nzbstreamer.enrichment

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpException
import io.skjaere.nntp.StatResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.stream.NntpPriority
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory

class VerificationService(
    private val streamingService: NntpStreamingService,
    private val concurrency: Int = 1
) {
    private val logger = LoggerFactory.getLogger(VerificationService::class.java)
    private val registry = Metrics.globalRegistry
    private val verificationTimer = registry.timer("nzb.verification.duration")
    private val verificationSegments = registry.counter("nzb.verification.segments")
    private val verificationMissing = registry.counter("nzb.verification.missing")

    suspend fun verifySegments(nzb: NzbDocument): VerificationResult {
        val sample = Timer.start(registry)
        val segmentsToVerify = nzb.files.flatMap { file ->
            file.segments
        }

        if (segmentsToVerify.isEmpty()) {
            sample.stop(verificationTimer)
            logger.debug("No additional segments to verify")
            return VerificationResult.Success
        }

        logger.debug("Verifying {} additional segments", segmentsToVerify.size)

        val missingArticles = mutableListOf<String>()
        try {
            coroutineScope {
                val semaphore = Semaphore(concurrency)
                segmentsToVerify.map { segment ->
                    async {
                        semaphore.acquire()
                        try {
                            val result = streamingService.withClient(NntpPriority.HEALTH_CHECK) { client ->
                                client.stat("<${segment.articleId}>")
                            }
                            if (result is StatResult.NotFound) {
                                synchronized(missingArticles) {
                                    missingArticles.add(segment.articleId)
                                }
                            }
                        } finally {
                            semaphore.release()
                        }
                    }
                }.awaitAll()
            }
        } catch (e: NntpException) {
            logger.error("NNTP failure during segment verification: {}", e.message, e)
            return VerificationResult.Failure(
                e.message ?: "NNTP failure during segment verification",
                e
            )
        }

        sample.stop(verificationTimer)
        verificationSegments.increment(segmentsToVerify.size.toDouble())

        if (missingArticles.isNotEmpty()) {
            verificationMissing.increment(missingArticles.size.toDouble())
            val message = "Missing ${missingArticles.size} articles: ${missingArticles.joinToString(", ")}"
            logger.warn(message)
            return VerificationResult.MissingArticles(
                message,
                ArticleNotFoundException("Missing articles: ${missingArticles.joinToString(", ")}")
            )
        }

        logger.debug("All {} segments verified successfully", segmentsToVerify.size)
        return VerificationResult.Success
    }
}
