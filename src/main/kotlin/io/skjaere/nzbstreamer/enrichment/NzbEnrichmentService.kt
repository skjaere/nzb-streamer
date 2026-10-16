package io.skjaere.nzbstreamer.enrichment

import io.ktor.utils.io.*
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.NntpException
import io.skjaere.nntp.YencEvent
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpPriority
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Semaphore
import io.skjaere.compressionutils.Par2Parser
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min

class NzbEnrichmentService(
    private val streamingService: NntpStreamingService,
    enrichmentConcurrency: Int = DEFAULT_ENRICHMENT_CONCURRENCY,
) {
    companion object {
        const val DEFAULT_ENRICHMENT_CONCURRENCY = 8
    }
    private val logger = LoggerFactory.getLogger(NzbEnrichmentService::class.java)
    private val registry = Metrics.globalRegistry
    private val enrichmentTimer = registry.timer("nzb.enrichment.duration")
    private val enrichmentFiles = registry.counter("nzb.enrichment.files")
    // Bound the per-file fan-out. A multi-volume release can contain 100+ wire-level
    // NZB files; without this gate, the inner `awaitAll` would open one priority=PREPARE
    // NNTP connection per file simultaneously and starve every other caller (and
    // overflow most provider account session limits).
    private val fanOutGate = Semaphore(enrichmentConcurrency)
    // Gauge: how many permits are still free right now. 0 = the gate is the bottleneck
    // and raising enrichmentConcurrency would let more imports run in parallel
    // (assuming pool capacity is also available). Pair with [permitWaitTimer] to
    // confirm callers are actually queueing.
    private val fanOutPermitsAvailable = Gauge.builder("nzb.enrichment.fan_out.permits.available", fanOutGate) {
        it.availablePermits.toDouble()
    }.register(registry)
    // Timer: how long each enrichFile/downloadPar2 waited inside withPermit before
    // it ran. Sustained non-zero waits mean imports are being serialized at the
    // gate — diagnostic for "is the new bound too tight?"
    private val permitWaitTimer = registry.timer("nzb.enrichment.fan_out.permit.wait")

    // Acquires a fan-out permit, recording the wait time. Equivalent to
    // `fanOutGate.withPermit { block() }` but isolates the "time spent queued
    // behind the bound" so we can tell whether the bound is the bottleneck.
    private suspend fun <T> withTimedPermit(block: suspend () -> T): T {
        val sample = Timer.start(registry)
        fanOutGate.acquire()
        sample.stop(permitWaitTimer)
        try {
            return block()
        } finally {
            fanOutGate.release()
        }
    }

    /**
     * Fetches the first article of each file of [nzb] that prepare needs to open the release (see
     * [ReleaseFiles]: its archive volumes, or its content when it has none, and the PAR2 index or,
     * without one, a recovery volume), then downloads the PAR2 index, then fetches any file that may
     * be a volume posted under another extension and tells by the PAR2 index or its content whether
     * it is one ([ReleaseFiles.Volumes]). The enriched NZB holds those files, the volumes found among
     * the last too, but for any left out: a file the release opens without (a PAR2 file, another
     * episode's video or archive set) that lacks its first article, or another episode's set that
     * lacks a volume its PAR2 index names, which is logged. A file it needs that lacks it, or a
     * volume its PAR2 index names that it doesn't hold, is MissingArticles; a volume posted under a
     * name that doesn't place it, with no PAR2 index to, is Failure.
     */
    suspend fun enrich(nzb: NzbDocument): EnrichmentResult {
        val sample = Timer.start(registry)
        val release = ReleaseFiles(nzb.files)
        val lacking = ConcurrentHashMap<Int, ArticleNotFoundException>()
        try {
            fetchFirstArticles(nzb, release.toFetch) { i, e ->
                if (!release.mayLack(i)) throw e
                lacking[i] = e
            }
            release.needed(lacking.keys)?.let { throw lacking.getValue(it) }
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            rethrowCancellation(e)
            classifyEnrichmentFailure(e, phase = "enrichment")?.let { return it }
            throw e
        }
        val kept = release.leftOut(lacking.keys).let { leftOut -> release.toFetch.filterNot { it in leftOut } }

        // After enrichment, download the base PAR2 file (contains recovery set metadata).
        // Skip .volXXX+YYY.par2 recovery volume files — they contain recovery blocks
        // and can be very large (tens of MB each), causing OOM if loaded into memory. The one
        // fetched in place of a missing index is downloaded only when its first 16 KB already
        // name files: its blocks are that small.
        try {
            coroutineScope {
                kept.map { nzb.files[it] }
                    .filter { file ->
                        val first16kb = file.first16kb ?: return@filter false
                        Par2Parser.isPar2(first16kb) && Par2Parser.hasFileDescriptions(first16kb)
                    }
                    .map { file ->
                        async {
                            withTimedPermit { downloadPar2(file) }
                        }
                    }.awaitAll()
            }
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            rethrowCancellation(e)
            classifyEnrichmentFailure(e, phase = "PAR2 download")?.let { return it }
            throw e
        }

        // Then the volumes posted under another extension (library issue N52), which the PAR2 index
        // names or their content shows to be: a file that may be one is fetched only when the index
        // names a volume no file holds, or doesn't name every volume the release has.
        val volumes = release.Volumes(nzb.files, kept)
        val found = try {
            val unknown = ConcurrentHashMap<Int, ArticleNotFoundException>()
            fetchFirstArticles(nzb, volumes.toCheck) { i, e -> unknown[i] = e }
            val result = when (val outcome = volumes.found(unknown)) {
                is ReleaseFiles.Found.Missing -> throw outcome.cause
                is ReleaseFiles.Found.Unplaced -> {
                    val message = "Archive volume '${outcome.name}' was posted under a name that doesn't give its " +
                        "place in its archive, and no PAR2 index names it"
                    logger.warn(message)
                    return EnrichmentResult.Failure(message, IllegalStateException(message))
                }
                is ReleaseFiles.Found.Volumes -> outcome
            }
            lacking += result.incomplete
            release.needed(lacking.keys)?.let { throw lacking.getValue(it) }
            result.volumes
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            rethrowCancellation(e)
            classifyEnrichmentFailure(e, phase = "volume check")?.let { return it }
            throw e
        }

        val leftOut = release.leftOut(lacking.keys, found)
        if (leftOut.isNotEmpty()) {
            logger.warn(
                "Leaving out {} file(s) the release opens without, as they or their archive set lack a first article " +
                    "or a volume: {}",
                leftOut.size, leftOut.sorted().joinToString { release.nameOf(it) },
            )
        }
        val files = (release.toFetch + found.keys).sorted().filterNot { it in leftOut }.map { nzb.files[it] }
        logger.debug("Enriched {} of {} files", files.size, nzb.files.size)

        sample.stop(enrichmentTimer)
        enrichmentFiles.increment((release.toFetch.size + volumes.toCheck.size).toDouble())
        return EnrichmentResult.Success(NzbDocument(files, nzb.password))
    }

    /**
     * Fetches the first article of each file of [nzb] at [indices], at most enrichmentConcurrency at a
     * time. One that lacks it goes to [lacks], which may throw to fail them all.
     */
    private suspend fun fetchFirstArticles(
        nzb: NzbDocument,
        indices: List<Int>,
        lacks: (Int, ArticleNotFoundException) -> Unit,
    ) {
        coroutineScope {
            indices.map { i ->
                async {
                    withTimedPermit {
                        try {
                            enrichFile(nzb.files[i])
                        } catch (e: ArticleNotFoundException) {
                            lacks(i, e)
                        }
                    }
                }
            }.awaitAll()
        }
    }

    /**
     * Classifies an exception thrown from a per-file fan-out as either a missing-article
     * condition or a generic NNTP failure, returning the corresponding [EnrichmentResult].
     *
     * Walks the cause chain rather than relying on `e is ArticleNotFoundException` because
     * a missing article thrown inside a Ktor writer coroutine ends up wrapped as
     * `ClosedByteChannelException(cause = ArticleNotFoundException)` by the channel's
     * `cancel(cause)` path — visible to consumers reading from the channel. A plain `is`
     * check would miss the wrapper and propagate up, where it gets logged at ERROR with
     * a stack trace and shipped to GlitchTip as if it were an application bug.
     *
     * A connection that drops can surface as a plain [IOException] (Ktor's EOFException when the
     * socket closes mid-answer), with no NntpException in its chain: that is a failure too.
     *
     * Returns null for exceptions that carry neither an NntpException nor an IOException
     * anywhere in the cause chain — those are genuinely unexpected and the caller propagates.
     */
    private fun classifyEnrichmentFailure(e: Throwable, phase: String): EnrichmentResult? {
        e.firstCauseOfType<ArticleNotFoundException>()?.let { cause ->
            logger.warn("Article not found during {}: {}", phase, cause.message)
            return EnrichmentResult.MissingArticles(
                cause.message ?: "Article not found",
                e,
            )
        }
        e.firstCauseOfType<NntpException>()?.let { cause ->
            logger.error("NNTP failure during {}: {}", phase, cause.message, e)
            return EnrichmentResult.Failure(
                cause.message ?: "NNTP failure",
                e,
            )
        }
        e.firstCauseOfType<IOException>()?.let { cause ->
            logger.error("Connection failure during {}: {}", phase, cause.toString(), e)
            return EnrichmentResult.Failure(
                cause.message ?: cause.toString(),
                e,
            )
        }
        return null
    }

    private suspend fun enrichFile(file: NzbFile) {
        val firstSegment = file.segments.first()
        streamingService.withClient(NntpPriority.PREPARE) { client ->
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

    /**
     * Downloads the PAR2 index [file] into its par2Data. One that lacks an article is left without:
     * it only names obfuscated volumes, and the release opens without it.
     */
    private suspend fun downloadPar2(file: NzbFile) {
        logger.debug("Downloading PAR2 file: {}", file.yencHeaders?.name)
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)
        try {
            streamingService.streamSegments(queue, name = "par2:${file.yencHeaders?.name}") { channel ->
                file.par2Data = channel.toByteArray()
            }
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            rethrowCancellation(e)
            val missing = e.firstCauseOfType<ArticleNotFoundException>() ?: throw e
            file.par2Data = null
            logger.warn("Going on without PAR2 file {}, which lacks an article: {}", file.yencHeaders?.name, missing.message)
            return
        }
        logger.debug("Downloaded PAR2 file: {} ({} bytes)", file.yencHeaders?.name, file.par2Data?.size)
    }
}

/**
 * Throws the caller's cancellation, and any other, rather than let it be classified: a
 * cancellation's cause may be an NNTP failure that isn't this NZB's (a sibling's that cancelled
 * the caller), and a cancelled caller's scope throws its children's failures in its place.
 */
internal suspend fun rethrowCancellation(e: Exception) {
    currentCoroutineContext().ensureActive()
    if (e is CancellationException) throw e
}

/** The first throwable of type [T] in this one's cause chain, this one included. */
internal inline fun <reified T : Throwable> Throwable.firstCauseOfType(): T? {
    var current: Throwable? = this
    val seen = mutableSetOf<Throwable>()
    while (current != null && seen.add(current)) {
        if (current is T) return current
        current = current.cause
    }
    return null
}
