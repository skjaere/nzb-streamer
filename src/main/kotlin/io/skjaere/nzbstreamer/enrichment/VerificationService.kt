package io.skjaere.nzbstreamer.enrichment

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpException
import io.skjaere.nntp.StatResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

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

        val firstMissing = AtomicReference<String>(null)
        var checkedCount = 0
        try {
            coroutineScope {
                val semaphore = Semaphore(concurrency)
                segmentsToVerify.map { segment ->
                    async {
                        if (firstMissing.get() != null) return@async
                        semaphore.acquire()
                        try {
                            if (firstMissing.get() != null) return@async
                            val result = streamingService.statAcrossPools("<${segment.articleId}>")
                            if (result is StatResult.NotFound) {
                                firstMissing.compareAndSet(null, segment.articleId)
                            }
                        } finally {
                            semaphore.release()
                        }
                    }
                }.awaitAll()
            }
            checkedCount = segmentsToVerify.size
        } catch (e: NntpException) {
            logger.error("NNTP failure during segment verification: {}", e.message, e)
            return VerificationResult.Failure(
                e.message ?: "NNTP failure during segment verification",
                e
            )
        }

        sample.stop(verificationTimer)
        verificationSegments.increment(checkedCount.toDouble())

        val missingArticleId = firstMissing.get()
        if (missingArticleId != null) {
            verificationMissing.increment(1.0)
            val message = "Missing article: $missingArticleId"
            logger.warn(message)
            return VerificationResult.MissingArticles(
                message,
                ArticleNotFoundException("Missing article: $missingArticleId")
            )
        }

        logger.debug("All {} segments verified successfully", segmentsToVerify.size)
        return VerificationResult.Success
    }
}
