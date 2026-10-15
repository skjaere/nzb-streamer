package io.skjaere.nzbstreamer.enrichment

import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpException
import io.skjaere.nzbstreamer.nzb.FilenameResolver
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.printable
import io.skjaere.nzbstreamer.queue.SegmentGap
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpPriority
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.StatCheck
import org.slf4j.LoggerFactory

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

    suspend fun verifySegments(
        nzb: NzbDocument,
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        progress: ((checked: Int, total: Int) -> Unit)? = null,
    ): VerificationResult {
        val sample = Timer.start(registry)
        val segmentsToVerify = segmentsOf(nzb)

        if (segmentsToVerify.isEmpty()) {
            sample.stop(verificationTimer)
            logger.debug("No additional segments to verify")
            return VerificationResult.Success
        }

        logger.debug("Verifying {} additional segments", segmentsToVerify.size)
        val result = check(segmentsToVerify, unlistedParts(nzb), priority, progress)
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
     * Parts the NZB lists no article for are found in every file, as [verifySegments] finds them.
     */
    suspend fun verifySample(
        nzb: NzbDocument,
        samples: Int = 32,
        priority: NntpPriority = NntpPriority.PREPARE,
        progress: ((checked: Int, total: Int) -> Unit)? = null,
    ): VerificationResult {
        require(samples > 0) { "samples must be > 0, got $samples" }
        val all = segmentsOf(nzb)
        if (all.isEmpty()) return VerificationResult.Success
        val ends = all.indices.filter { all[it].positionInFile == 1 || all[it].positionInFile == all[it].totalSegmentsInFile }
        val spread = (0 until samples).map { i -> (i.toLong() * (all.size - 1) / maxOf(samples - 1, 1)).toInt() }
        val chosen = (ends + spread).distinct().sorted().map { all[it] }
        logger.debug("Verifying a sample of {} of {} segments", chosen.size, all.size)
        return check(chosen, unlistedParts(nzb), priority, progress)
    }

    // Carry the per-segment position within its file (and the file's name/size)
    // alongside each unit of work, so when a segment is missing we can log WHERE
    // in the file it sits — useful for distinguishing "this NZB is mostly broken"
    // from "just one segment lost at position N/M". The name, and the id where it's
    // reported, are escaped: an NZB can put a CR/LF in either.
    private data class SegmentToCheck(
        val segment: io.skjaere.nzbstreamer.nzb.NzbSegment,
        val fileName: String,
        val positionInFile: Int,  // 1-based
        val totalSegmentsInFile: Int
    )

    private fun segmentsOf(nzb: NzbDocument): List<SegmentToCheck> = nzb.files.flatMap { file ->
        val name = (file.yencHeaders?.name ?: file.subject).printable()
        val total = file.segments.size
        file.segments.mapIndexed { idx, segment ->
            SegmentToCheck(segment, name, idx + 1, total)
        }
    }

    /**
     * The runs of parts the files of [nzb] were posted in that its NZB lists no article for, which
     * no provider can have: an enriched file's as its stream lays them out (by its yEnc size and
     * first part, see [SegmentQueueService.unlistedParts]), another's by its subject's yEnc part
     * count, "(1/N)", when its articles are numbered within it.
     */
    private fun unlistedParts(nzb: NzbDocument): List<SegmentGap> = nzb.files.flatMap { file ->
        if (file.yencHeaders != null) SegmentQueueService.unlistedParts(file) else unlistedBySubject(file)
    }

    private fun unlistedBySubject(file: NzbFile): List<SegmentGap> {
        val posted = SUBJECT_PART_COUNT.find(file.subject)?.groupValues?.get(1)?.toIntOrNull()
            ?.takeIf { it < Int.MAX_VALUE } ?: return emptyList()
        val numbers = file.segments.map { it.number }.toSortedSet()
        if (numbers.isEmpty() || numbers.size >= posted || numbers.first() < 1 || numbers.last() > posted) return emptyList()
        val name = FilenameResolver.extractSubjectFilename(file.subject) ?: file.subject
        return (listOf(0) + numbers + (posted + 1)).zipWithNext()
            .filter { (before, after) -> after - before > 1 }
            .map { (before, after) -> SegmentGap(name, before + 1 until after, posted) }
    }

    private suspend fun check(
        segmentsToVerify: List<SegmentToCheck>,
        unlisted: List<SegmentGap>,
        priority: NntpPriority,
        progress: ((checked: Int, total: Int) -> Unit)?,
    ): VerificationResult {
        // Pipelined STATs across the connections (one round trip per batch, not per article),
        // stopping at the first article no provider has or can answer for.
        val check = try {
            streamingService.statUntilMissing(
                segmentsToVerify.map { it.segment.articleId }, currentConcurrency, priority, progress,
            )
        } catch (e: NntpException) {
            logger.error("NNTP failure during segment verification: {}", e.message, e)
            return unlistedMissing(unlisted, answered = 0, total = segmentsToVerify.size) ?: VerificationResult.Failure(
                e.message ?: "NNTP failure during segment verification",
                e
            )
        }

        verificationSegments.increment(check.answered.toDouble())

        val missing = check.missing.firstOrNull()?.let { segmentsToVerify[it] }
            ?: return unlistedMissing(unlisted, check.answered, check.total)
                ?: check.failure?.let { failed(check, segmentsToVerify, it) } ?: VerificationResult.Success
        verificationMissing.increment(1.0)
        // Include position/total/filename so we can see WHERE in the file the
        // first missing segment sits — telling us whether the NZB is broken at
        // the head, the middle, or the tail (which often correlates with the
        // root cause: head=indexer indexing miss, tail=expired retention).
        // segment.number is the NZB-declared segment number (sometimes differs
        // from list position when the NZB enumerates out of order). The counts tell
        // a takedown (every answered article missing) from a gap.
        val message = "Missing article ${missing.segment.articleId.printable()} at " +
            "position ${missing.positionInFile}/${missing.totalSegmentsInFile} " +
            "(segment.number=${missing.segment.number}) " +
            "of file '${missing.fileName}' " +
            "(${check.missing.size} of ${check.answered} answered articles missing, ${check.total} to check)"
        logger.warn(message)
        return VerificationResult.MissingArticles(
            message,
            ArticleNotFoundException(message),
            missingCount = check.missing.size,
            answeredCount = check.answered,
            totalCount = check.total,
        )
    }

    /**
     * The release lacks the [unlisted] parts whatever the providers say, though none of the listed
     * articles they answered for ([answered] of [total]) is missing: they count as answered and
     * missing. Null when the NZB lists every part.
     */
    private fun unlistedMissing(unlisted: List<SegmentGap>, answered: Int, total: Int): VerificationResult.MissingArticles? {
        val first = unlisted.firstOrNull() ?: return null
        val parts = unlisted.sumOf { it.parts.last - it.parts.first + 1 }
        verificationMissing.increment(1.0)
        val message = "${first.toException().message} " +
            "($parts unlisted part${if (parts == 1) "" else "s"} in all; $answered of $total listed articles answered)"
        logger.warn(message)
        return VerificationResult.MissingArticles(
            message,
            ArticleNotFoundException(message),
            missingCount = parts,
            answeredCount = answered + parts,
            totalCount = total + parts,
        )
    }

    /**
     * No provider could answer for an article (each failed other than with a 430, or the rest said
     * 430): whether the release is complete is unknown, so the check fails with the [cause].
     */
    private fun failed(check: StatCheck, segmentsToVerify: List<SegmentToCheck>, cause: Throwable): VerificationResult {
        val where = check.unanswered.firstOrNull()?.let { segmentsToVerify[it] }?.let {
            " for article ${it.segment.articleId.printable()} at position ${it.positionInFile}/${it.totalSegmentsInFile} " +
                "of file '${it.fileName}'"
        }.orEmpty()
        val message = "No provider could answer$where: ${cause.message ?: cause}"
        logger.warn(message)
        return VerificationResult.Failure(message, cause)
    }

    private companion object {
        // A yEnc subject's part count: the last "(x/N)" after "yEnc", e.g. "file.mkv" yEnc (1/120).
        val SUBJECT_PART_COUNT = Regex("""(?i)\byEnc\b.*\(\s*\d+\s*/\s*(\d+)\s*\)""")
    }
}
