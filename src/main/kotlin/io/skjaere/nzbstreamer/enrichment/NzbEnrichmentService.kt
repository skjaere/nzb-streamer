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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import io.skjaere.compressionutils.Par2Parser
import org.slf4j.LoggerFactory
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

    suspend fun enrich(nzb: NzbDocument): EnrichmentResult {
        val sample = Timer.start(registry)
        try {
            coroutineScope {
                nzb.files.map { file ->
                    async {
                        withTimedPermit { enrichFile(file) }
                    }
                }.awaitAll()
            }
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            classifyEnrichmentFailure(e, phase = "enrichment")?.let { return it }
            throw e
        }

        val enrichedCount = nzb.files.count { it.yencHeaders != null }
        logger.debug("Enriched {} of {} files", enrichedCount, nzb.files.size)

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
                            withTimedPermit { downloadPar2(file) }
                        }
                    }.awaitAll()
            }
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            classifyEnrichmentFailure(e, phase = "PAR2 download")?.let { return it }
            throw e
        }

        sample.stop(enrichmentTimer)
        enrichmentFiles.increment(nzb.files.size.toDouble())
        return EnrichmentResult.Success(nzb)
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
     * Returns null for exceptions that don't carry an NntpException anywhere in the
     * cause chain — those are genuinely unexpected and the caller propagates.
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
        return null
    }

    private inline fun <reified T : Throwable> Throwable.firstCauseOfType(): T? {
        var current: Throwable? = this
        val seen = mutableSetOf<Throwable>()
        while (current != null && seen.add(current)) {
            if (current is T) return current
            current = current.cause
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

    private suspend fun downloadPar2(file: NzbFile) {
        logger.debug("Downloading PAR2 file: {}", file.yencHeaders?.name)
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)
        streamingService.streamSegments(queue, name = "par2:${file.yencHeaders?.name}") { channel ->
            file.par2Data = channel.toByteArray()
        }
        logger.debug("Downloaded PAR2 file: {} ({} bytes)", file.yencHeaders?.name, file.par2Data?.size)
    }
}
