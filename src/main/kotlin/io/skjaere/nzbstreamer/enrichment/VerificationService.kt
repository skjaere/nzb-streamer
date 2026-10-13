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
    concurrency: Int = 1
) {
    private val logger = LoggerFactory.getLogger(VerificationService::class.java)
    private val registry = Metrics.globalRegistry
    private val verificationTimer = registry.timer("nzb.verification.duration")
    private val verificationSegments = registry.counter("nzb.verification.segments")
    private val verificationMissing = registry.counter("nzb.verification.missing")

    // Mutable so the live override path in debridav can bump it without restarting.
    // Read fresh into the per-call Semaphore below; in-flight verifications keep their
    // existing semaphore. Volatile is sufficient — single-int write, no compound
    // check-then-act on the field itself.
    @Volatile
    private var currentConcurrency: Int = concurrency

    fun setConcurrency(value: Int) {
        require(value > 0) { "concurrency must be > 0, got $value" }
        currentConcurrency = value
    }

    fun getConcurrency(): Int = currentConcurrency

    suspend fun verifySegments(nzb: NzbDocument): VerificationResult {
        val sample = Timer.start(registry)
        val segmentsToVerify = segmentsOf(nzb)

        if (segmentsToVerify.isEmpty()) {
            sample.stop(verificationTimer)
            logger.debug("No additional segments to verify")
            return VerificationResult.Success
        }

        logger.debug("Verifying {} additional segments", segmentsToVerify.size)
        val result = check(segmentsToVerify)
        sample.stop(verificationTimer)
        if (result is VerificationResult.Success) {
            logger.debug("All {} segments verified successfully", segmentsToVerify.size)
        }
        return result
    }

    /**
     * Checks a sample of segments instead of all of them: the first and last segment of
     * every file plus evenly spaced segments in between, [samples] in total (more when the
     * NZB has many files). STATing every segment of a 7 GB release takes tens of seconds;
     * a sample takes about one round trip per [currentConcurrency] segments, and a release
     * with expired articles is usually missing whole stretches, which a spread sample hits.
     */
    suspend fun verifySample(nzb: NzbDocument, samples: Int = 32): VerificationResult {
        require(samples > 0) { "samples must be > 0, got $samples" }
        val all = segmentsOf(nzb)
        if (all.isEmpty()) return VerificationResult.Success
        val ends = all.indices.filter { all[it].positionInFile == 1 || all[it].positionInFile == all[it].totalSegmentsInFile }
        val spread = (0 until samples).map { i -> (i.toLong() * (all.size - 1) / maxOf(samples - 1, 1)).toInt() }
        val chosen = (ends + spread).distinct().sorted().map { all[it] }
        logger.debug("Verifying a sample of {} of {} segments", chosen.size, all.size)
        return check(chosen)
    }

    // Carry the per-segment position within its file (and the file's name/size)
    // alongside each unit of work, so when a segment is missing we can log WHERE
    // in the file it sits — useful for distinguishing "this NZB is mostly broken"
    // from "just one segment lost at position N/M".
    private data class SegmentToCheck(
        val segment: io.skjaere.nzbstreamer.nzb.NzbSegment,
        val fileName: String,
        val positionInFile: Int,  // 1-based
        val totalSegmentsInFile: Int
    )

    private fun segmentsOf(nzb: NzbDocument): List<SegmentToCheck> = nzb.files.flatMap { file ->
        val name = file.yencHeaders?.name ?: file.subject
        val total = file.segments.size
        file.segments.mapIndexed { idx, segment ->
            SegmentToCheck(segment, name, idx + 1, total)
        }
    }

    private suspend fun check(segmentsToVerify: List<SegmentToCheck>): VerificationResult {
        val firstMissing = AtomicReference<SegmentToCheck>(null)
        try {
            coroutineScope {
                val semaphore = Semaphore(currentConcurrency)
                segmentsToVerify.map { unit ->
                    async {
                        if (firstMissing.get() != null) return@async
                        semaphore.acquire()
                        try {
                            if (firstMissing.get() != null) return@async
                            val result = streamingService.statAcrossPools("<${unit.segment.articleId}>")
                            if (result is StatResult.NotFound) {
                                firstMissing.compareAndSet(null, unit)
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

        verificationSegments.increment(segmentsToVerify.size.toDouble())

        val missing = firstMissing.get() ?: return VerificationResult.Success
        verificationMissing.increment(1.0)
        // Include position/total/filename so we can see WHERE in the file the
        // first missing segment sits — telling us whether the NZB is broken at
        // the head, the middle, or the tail (which often correlates with the
        // root cause: head=indexer indexing miss, tail=expired retention).
        // segment.number is the NZB-declared segment number (sometimes differs
        // from list position when the NZB enumerates out of order).
        val message = "Missing article ${missing.segment.articleId} at " +
            "position ${missing.positionInFile}/${missing.totalSegmentsInFile} " +
            "(segment.number=${missing.segment.number}) " +
            "of file '${missing.fileName}'"
        logger.warn(message)
        return VerificationResult.MissingArticles(
            message,
            ArticleNotFoundException(message)
        )
    }
}
