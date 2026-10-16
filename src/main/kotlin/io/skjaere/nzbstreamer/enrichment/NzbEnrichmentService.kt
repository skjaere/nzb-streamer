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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import io.skjaere.compressionutils.Par2Parser
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min

class NzbEnrichmentService(
    private val streamingService: NntpStreamingService,
    enrichmentConcurrency: Int = DEFAULT_ENRICHMENT_CONCURRENCY,
) {
    companion object {
        const val DEFAULT_ENRICHMENT_CONCURRENCY = 8

        // How many of a recovery set's volumes, smallest first, are read for its file descriptions.
        private const val STAND_INS_PER_SET = 2

        // How many bytes of a PAR2 file are read for its file descriptions past what is held of it, in
        // how many reads: one, and one past each recovery slice skipped ([SKIP_OVER]), whose bytes
        // aren't read (NR-7).
        private const val PAR2_READ_LIMIT = 16L * 1024 * 1024
        private const val MAX_PAR2_READS = 16

        // A recovery slice this big or bigger is skipped by reading on from past it, not read through.
        private const val SKIP_OVER = 1024L * 1024
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
     * without one, a recovery volume), then downloads the PAR2 index (or, when it names nothing,
     * reads a recovery volume's file descriptions in its place), then fetches any file that may
     * be a volume posted under another extension and tells by the PAR2 index or its content whether
     * it is one ([ReleaseFiles.Volumes]). The enriched NZB holds those files, the volumes found among
     * the last too, but for any left out: a file the release opens without (a PAR2 file, another
     * episode's video or archive set) that lacks its first article, or another episode's set that
     * lacks a volume its PAR2 index names, which is logged. A file it needs that lacks it, or a
     * volume its PAR2 index names that it doesn't hold, is MissingArticles, the latter's cause an
     * [ArchiveVolumeNotInNzbException] where no provider can supply it; a volume posted under a name
     * that doesn't place it, with no PAR2 index to, is Failure, its cause an
     * [UnplacedArchiveVolumeException] (NR-18).
     */
    suspend fun enrich(nzb: NzbDocument): EnrichmentResult {
        val sample = Timer.start(registry)
        val release = ReleaseFiles(nzb.files)
        // Files that lack their first article, and sets that lack a volume, with the cause.
        val lacking = ConcurrentHashMap<Int, Exception>()
        // The first articles, whole, of the PAR2 files by name, which their reads start from (NR-10):
        // not those of recovery volumes posted under an index's name, which aren't read (NR-9).
        val heads = ConcurrentHashMap<Int, ByteArray>()
        try {
            fetchFirstArticles(nzb, release.toFetch, { i, article -> if (holds(release, i, article)) hold(heads, i, nzb.files[i], article) }) { i, e ->
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

        // After enrichment, read the base PAR2 file's file descriptions (it names the files).
        // Skip .volXXX+YYY.par2 recovery volume files — they contain recovery blocks
        // and can be very large (tens of MB each), causing OOM if loaded into memory: a file
        // counts as an index when it is named as one (and is PAR2), or else when its first 16 KB
        // name files, read packet by packet without taking their lengths on trust; only its
        // descriptions are held ([readPar2]). An index over a thousand files or more names none in
        // its first 16 KB, its Main packet's list of them longer (NR-9), but a recovery volume posted
        // under an index's name (<hash>.par2) starts with its recovery slice: it counts as one only
        // as any other file does. A recovery set's smallest volume stands in for its index when that
        // names nothing ([readStandIns]), where the release needs names (NR-10).
        var standIns = emptyList<Int>()
        try {
            coroutineScope {
                kept.filterNot(release::isRecovery)
                    .filter { i ->
                        val first16kb = nzb.files[i].first16kb
                        if (release.isIndex(i) && first16kb?.let(Par2Packets::startsWithRecoverySlice) == false) {
                            Par2Parser.isPar2(first16kb)
                        } else {
                            namesFiles(first16kb)
                        }
                    }
                    .map { i ->
                        async {
                            withTimedPermit { readPar2(nzb.files[i], heads.remove(i)) }
                        }
                    }.awaitAll()
            }
            if (release.needsNames(nzb.files, kept)) standIns = readStandIns(nzb, release, heads)
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            rethrowCancellation(e)
            classifyEnrichmentFailure(e, phase = "PAR2 download")?.let { return it }
            throw e
        }
        val named = (kept + standIns).distinct()
        // The first articles not read (another set's stand-ins, or all where no names are needed) aren't held on.
        heads.clear()

        // Then the volumes posted under another extension (library issue N52), which the PAR2 index
        // names or their content shows to be: a file that may be one is fetched only when the index
        // names a volume no file holds, or doesn't name every volume the release has.
        val volumes = release.Volumes(nzb.files, named)
        val found = try {
            val unknown = ConcurrentHashMap<Int, ArticleNotFoundException>()
            fetchFirstArticles(nzb, volumes.toCheck) { i, e -> unknown[i] = e }
            val result = when (val outcome = volumes.found(unknown)) {
                is ReleaseFiles.Found.Missing -> throw outcome.cause
                is ReleaseFiles.Found.Unplaced -> {
                    val message = "Archive volume '${outcome.name}' was posted under a name that doesn't give its " +
                        "place in its archive, and no PAR2 index names it"
                    logger.warn(message)
                    return EnrichmentResult.Failure(message, UnplacedArchiveVolumeException(outcome.name, message))
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
        val files = (named + found.keys + release.toFetch).distinct().sorted().filterNot { it in leftOut }
            .map { nzb.files[it] }
        logger.debug("Enriched {} of {} files", files.size, nzb.files.size)

        sample.stop(enrichmentTimer)
        enrichmentFiles.increment((release.toFetch.size + volumes.toCheck.size).toDouble())
        return EnrichmentResult.Success(NzbDocument(files, nzb.password))
    }

    /**
     * Fetches the first article of each file of [nzb] at [indices], at most enrichmentConcurrency at a
     * time, each passed to [fetched] whole, decoded, with its index. One that lacks it goes to
     * [lacks], which may throw to fail them all.
     */
    private suspend fun fetchFirstArticles(
        nzb: NzbDocument,
        indices: List<Int>,
        fetched: (Int, ByteArray) -> Unit = { _, _ -> },
        lacks: (Int, ArticleNotFoundException) -> Unit,
    ) {
        coroutineScope {
            indices.map { i ->
                async {
                    withTimedPermit {
                        try {
                            enrichFile(nzb.files[i])?.let { fetched(i, it) }
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

    /** Fetches [file]'s first article into its yencHeaders and first16kb; the article, decoded. */
    private suspend fun enrichFile(file: NzbFile): ByteArray? {
        val firstSegment = file.segments.first()
        var article: ByteArray? = null
        streamingService.withClient(NntpPriority.PREPARE) { client ->
            client.bodyYenc("<${firstSegment.articleId}>").collect { event ->
                when (event) {
                    is YencEvent.Headers -> file.yencHeaders = event.yencHeaders
                    is YencEvent.Body -> {
                        val data = event.data.toByteArray()
                        file.first16kb = data.copyOf(min(16384, data.size))
                        article = data
                    }
                }
            }
        }
        logger.debug("Enriched file: {}", file.yencHeaders?.name)
        return article
    }

    /**
     * Whether to hold [article], fetched as file [i]'s first, for its PAR2 read: a recovery volume's,
     * and an index's by its name, but for one that starts with a recovery slice, a recovery volume
     * posted under an index's name, which is read only when its first 16 KB name files (NR-9).
     */
    private fun holds(release: ReleaseFiles, i: Int, article: ByteArray): Boolean =
        release.isRecovery(i) || release.isIndex(i) && !Par2Packets.startsWithRecoverySlice(article)

    /**
     * Holds [article], fetched as PAR2 file [file]'s first, in [heads] at [i] when it is PAR2's and
     * the file's first part, which its bytes start (an NZB that lacks part 1 lists a later one first).
     */
    private fun hold(heads: MutableMap<Int, ByteArray>, i: Int, file: NzbFile, article: ByteArray) {
        if (file.yencHeaders?.partBegin.let { it == null || it == 1L } && Par2Parser.isPar2(article)) heads[i] = article
    }

    /**
     * Reads recovery volumes' file descriptions in place of the PAR2 index of each recovery set
     * ([ReleaseFiles.standIns]) that has none, or whose index named nothing (it lacks an article):
     * its smallest volume's, and the next-smallest's when that names nothing, each set's apart
     * (NR-5). Another set's index (a sample's) names only its own set's files. Those that named files.
     * [heads] holds the first articles fetched with the rest.
     */
    private suspend fun readStandIns(nzb: NzbDocument, release: ReleaseFiles, heads: MutableMap<Int, ByteArray>): List<Int> =
        coroutineScope {
            release.standIns
                .filterKeys { set -> release.indexesOf(set).none { nzb.files[it].par2Data != null } }
                .values.map { volumes ->
                    async { volumes.take(STAND_INS_PER_SET).firstOrNull { readStandIn(nzb, release, it, heads.remove(it)) } }
                }.awaitAll().filterNotNull()
        }

    /**
     * Reads the file descriptions of recovery volume [i] into its par2Data, in place of its set's
     * PAR2 index's, fetching its first article first when it wasn't with the rest ([head], when held
     * whole); whether it named files. It is read as [readPar2] reads a PAR2 file, from that article:
     * its recovery block not read at all, however big (NR-7). As the index's, a read that lacks an
     * article (its first included) leaves it naming nothing, and one that fails any other way fails
     * prepare: the volumes may need its names for their order, and it may be tried again (NR-7).
     */
    private suspend fun readStandIn(nzb: NzbDocument, release: ReleaseFiles, i: Int, head: ByteArray?): Boolean {
        val file = nzb.files[i]
        var held = head
        if (i !in release.toFetch) {
            enrichmentFiles.increment()
            val heads = mutableMapOf<Int, ByteArray>()
            var lacks = false
            fetchFirstArticles(nzb, listOf(i), { _, article -> hold(heads, i, file, article) }) { _, e ->
                logger.warn("Going on without recovery volume {}, which lacks its first article: {}", release.nameOf(i), e.message)
                lacks = true
            }
            if (lacks) return false
            held = heads[i]
        }
        if (file.yencHeaders == null || file.first16kb?.let(Par2Parser::isPar2) != true) return false
        withTimedPermit { readPar2(file, held) }
        return file.par2Data != null
    }

    /**
     * Whether [first16kb] are those of a PAR2 file that names files in them, an index's: its packets
     * are read one by one ([Par2Packets]), a length a packet gives never allocated before it is read.
     */
    private suspend fun namesFiles(first16kb: ByteArray?): Boolean =
        first16kb != null && Par2Parser.isPar2(first16kb) && Par2Packets.fileDescriptions(ByteReadChannel(first16kb)) != null

    /**
     * Reads the file descriptions (its FileDesc packets) of the PAR2 file [file] into its par2Data
     * ([Par2Packets]): from its first article, [head], when held whole (it isn't fetched again,
     * NR-10), else its first 16 KB, then as it streams from where those end, its other packets
     * skipped, never held. A recovery slice in the way of them, whose length its header gives,
     * isn't read at all when it is [SKIP_OVER] bytes or more (or runs past what is held): the next
     * read starts past it, so a recovery volume's block is never downloaded (NR-7). No more
     * than [PAR2_READ_LIMIT] bytes are read, in [MAX_PAR2_READS] reads ([readFrom]), nor any more once
     * one gets nothing past where it starts (a file that holds fewer bytes than its size claims), and
     * what fails past where they stop is none of theirs (NR-15); nor is what fails once they cover
     * every file the Main packet lists ([Par2Packets.complete], NR-7). It only names files: one that
     * lacks an article before then is left without, and the release opens without it. One whose read
     * fails any other way fails prepare, as the release may need its names to order its volumes.
     */
    private suspend fun readPar2(file: NzbFile, head: ByteArray?) {
        val size = file.yencHeaders?.size ?: return
        logger.debug("Reading PAR2 file: {}", file.yencHeaders?.name)
        val packets = Par2Packets()
        var stop = packets.read(ByteReadChannel(head ?: file.first16kb ?: ByteArray(0)), 0)
        var left = PAR2_READ_LIMIT
        var reads = 0
        try {
            while (reads < MAX_PAR2_READS && left > 0) {
                val at = when (val s = stop) {
                    Par2Packets.Stop.Done -> break
                    is Par2Packets.Stop.Next -> s.at
                    is Par2Packets.Stop.End -> s.at
                }
                // Past the end (a slice that claims more than the file holds, its end wrapped round included).
                if (at !in 0 until size) break
                val before = packets.bytesRead
                stop = readFrom(file, at, minOf(size - at, left), packets)
                left -= packets.bytesRead - before
                reads++
                // Nothing past where it started: the file holds no more, whatever its size claims.
                if (stop == Par2Packets.Stop.End(at)) break
            }
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            rethrowCancellation(e)
            if (packets.complete) {
                logger.debug("Read PAR2 file {} to its file descriptions; past them: {}", file.yencHeaders?.name, e.toString())
            } else {
                val missing = e.firstCauseOfType<ArticleNotFoundException>() ?: throw e
                file.par2Data = null
                logger.warn("Going on without PAR2 file {}, which lacks an article: {}", file.yencHeaders?.name, missing.message)
                return
            }
        }
        file.par2Data = packets.descriptions
        logger.debug("Read PAR2 file: {} ({} bytes of file descriptions)", file.yencHeaders?.name, file.par2Data?.size)
    }

    /**
     * Has [packets] read the bytes of [file] from [from], [limit] of them, their articles fetched
     * one ahead of it ([NntpStreamingService.fetchSegment], through the segment cache and provider
     * fallback); where it stopped. An article that fails ends the bytes there, all of them up to it
     * delivered, and is thrown only when [packets] read on to that end ([Par2Packets.Stop.End]): one
     * that stopped before, by its own rule, never wanted what failed past it, whenever it failed
     * (NR-15). A stream ([NntpStreamingService.streamSegments]) drops what its reader hasn't taken
     * yet when a failure closes it, so a read-ahead article's failure could lose descriptions read
     * whole, or not, by the timing.
     */
    private suspend fun readFrom(file: NzbFile, from: Long, limit: Long, packets: Par2Packets): Par2Packets.Stop = coroutineScope {
        val items = SegmentQueueService.createRangeQueue(NzbDocument(listOf(file)), from, limit).toList()
        val channel = ByteChannel()
        val failure = AtomicReference<Exception?>()
        val writer = launch {
            try {
                // A download that fails fails neither the next nor the writer until it is its turn.
                supervisorScope {
                    @OptIn(ExperimentalCoroutinesApi::class)
                    produce(capacity = Channel.RENDEZVOUS) {
                        items.forEach { item ->
                            send(item to this@supervisorScope.async {
                                item.gap?.let { throw it.toException() }
                                streamingService.fetchSegment(item.segment.articleId)
                            })
                        }
                    }.consumeEach { (item, download) ->
                        val data = download.await()
                        val start = minOf(item.readStart, data.size.toLong()).toInt()
                        val end = minOf(item.readEnd, data.size.toLong()).toInt()
                        if (end > start) {
                            channel.writeFully(data, start, end)
                            channel.flush()
                        }
                    }
                }
            } catch (e: CancellationException) {
                // Cancelled once the reader stopped, or with the caller; one that isn't the writer's
                // own is a download's failure, thrown from here as any other.
                if (!isActive) throw e
                failure.set(e)
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                failure.set(e)
            } finally {
                // Closed without a cause, so the reader gets every byte written before it (each
                // flushed as it is), and is never left waiting.
                channel.close()
            }
        }
        val stop = packets.read(channel, from, SKIP_OVER)
        writer.cancel()
        if (stop is Par2Packets.Stop.End) failure.get()?.let { throw it }
        stop
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
