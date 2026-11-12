package io.skjaere.nzbstreamer

import io.ktor.utils.io.*
import io.skjaere.compressionutils.Par2FileNames
import io.skjaere.compressionutils.Par2Parser
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nzbstreamer.config.MissingArticlePolicy
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.PrepareConfig
import io.skjaere.nzbstreamer.config.StreamingConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.metadata.ArchiveMetadataService
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.nzb.FilenameResolver
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbParser
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.FileResolveResult
import io.skjaere.nzbstreamer.stream.Gap
import io.skjaere.nzbstreamer.stream.NamedSplits
import io.skjaere.nzbstreamer.stream.NntpPriority
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.SegmentCache
import io.skjaere.nzbstreamer.stream.StreamableFile
import kotlinx.coroutines.runBlocking
import java.io.Closeable

class NzbStreamer private constructor(
    private val streamingService: NntpStreamingService,
    private val metadataService: ArchiveMetadataService,
    private val verificationService: VerificationService,
    private val archiveStreamingService: ArchiveStreamingService
) : Closeable {

    /** Like the NZB-bytes overload, for an NZB already parsed (or a part of one, e.g. the volumes of one file). */
    suspend fun verifySegments(
        nzb: NzbDocument,
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        progress: ((checked: Int, total: Int) -> Unit)? = null,
    ): VerificationResult = verificationService.verifySegments(nzb, priority, progress)

    /**
     * STATs every segment of the NZB (pipelined across connections, falling back across pools),
     * reporting (checked, total) as answers arrive. Minutes on huge releases without pipelining;
     * seconds to tens of seconds with it. A file whose NZB lists fewer parts than its subject's
     * "(1/N)" (or, once enriched, its yEnc sizes) says is missing articles too.
     */
    suspend fun verifySegments(
        nzbBytes: ByteArray,
        priority: NntpPriority = NntpPriority.HEALTH_CHECK,
        progress: ((checked: Int, total: Int) -> Unit)? = null,
    ): VerificationResult = verificationService.verifySegments(NzbParser.parse(nzbBytes), priority, progress)

    /** STATs a spread sample of segments; see [VerificationService.verifySample]. */
    suspend fun verifySample(
        nzb: NzbDocument,
        samples: Int = 32,
        progress: ((checked: Int, total: Int) -> Unit)? = null,
    ): VerificationResult = verificationService.verifySample(nzb, samples, progress = progress)

    suspend fun verifySample(
        nzbBytes: ByteArray,
        samples: Int = 32,
        progress: ((checked: Int, total: Int) -> Unit)? = null,
    ): VerificationResult = verificationService.verifySample(NzbParser.parse(nzbBytes), samples, progress = progress)

    suspend fun enrich(nzbBytes: ByteArray): EnrichmentResult {
        val nzb = NzbParser.parse(nzbBytes)
        return metadataService.enrich(nzb)
    }

    /**
     * @param passwordOverride if non-null, supersedes any `<head><meta type="password">` in the
     *   NZB XML. Useful for CLI invocations and integration tests against releases that ship
     *   the password out-of-band rather than in the NZB metadata.
     */
    suspend fun prepare(nzbBytes: ByteArray, passwordOverride: String? = null): PrepareResult {
        val parsed = NzbParser.parse(nzbBytes)
        val nzb = if (passwordOverride != null) parsed.copy(password = passwordOverride) else parsed
        return metadataService.prepare(nzb)
    }

    fun resolveFile(metadata: ExtractedMetadata, path: String): FileResolveResult {
        return when (metadata) {
            is ExtractedMetadata.Archive ->
                archiveStreamingService.resolveFile(metadata.entries, metadata.orderedArchiveNzb, path)
            is ExtractedMetadata.NestedArchive ->
                archiveStreamingService.resolveFile(metadata.innerEntries, metadata.orderedArchiveNzb, path)
            is ExtractedMetadata.Raw -> resolveRawFile(metadata, path)
        }
    }

    private fun resolveRawFile(metadata: ExtractedMetadata.Raw, path: String): FileResolveResult {
        val nzb = metadata.orderedArchiveNzb
        val index = metadata.response.volumes.indexOfFirst { it == path }
        if (index < 0) return FileResolveResult.NotFound
        val size = nzb.files[index].yencHeaders!!.size
        return FileResolveResult.Streamable(
            NamedSplits(
                splits = listOf(
                    SplitInfo(
                        volumeIndex = index,
                        dataStartPosition = ArchiveStreamingService.computeVolumeOffsets(nzb)[index],
                        dataSize = size
                    )
                ),
                totalSize = size,
                name = path
            )
        )
    }

    suspend fun streamFile(
        metadata: ExtractedMetadata,
        namedSplits: NamedSplits,
        range: LongRange? = null,
        onGap: ((Gap) -> Unit)? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        archiveStreamingService.streamFile(metadata.orderedArchiveNzb, namedSplits, range, onGap, consume)
    }

    /**
     * Starts streaming the file (or [range] of it) and returns at once with the [WriterJob] whose
     * channel delivers it. The writer runs as a child of the calling coroutine's job: cancelling
     * that job (or [WriterJob.job], or the channel) stops it, and that job doesn't complete until
     * the writer has, so call this from a coroutine that outlives the reading. A failure (an
     * article no provider has, a timeout) never fails the caller's job: the channel closes with it
     * as its cause ([ByteReadChannel.closedCause]) once everything in front of it has been
     * written. Ktor drops the bytes the reader hasn't taken yet when a channel closes with a cause.
     */
    suspend fun launchStreamFile(
        metadata: ExtractedMetadata,
        namedSplits: NamedSplits,
        range: LongRange? = null,
        onGap: ((Gap) -> Unit)? = null,
    ): WriterJob {
        return archiveStreamingService.launchStreamFile(metadata.orderedArchiveNzb, namedSplits, range, onGap)
    }

    fun resolveStreamableFiles(metadata: ExtractedMetadata): List<StreamableFile> {
        return when (metadata) {
            is ExtractedMetadata.Archive ->
                archiveStreamingService.resolveStreamableFiles(metadata.entries, metadata.orderedArchiveNzb)
            is ExtractedMetadata.NestedArchive ->
                archiveStreamingService.resolveStreamableFiles(metadata.innerEntries, metadata.orderedArchiveNzb)
            is ExtractedMetadata.Raw -> resolveRawStreamableFiles(metadata)
        }
    }

    private fun resolveRawStreamableFiles(metadata: ExtractedMetadata.Raw): List<StreamableFile> {
        // For RAW (non-archive) NZBs the streamable filename is what users will see on
        // their mount, so it's worth picking the most-trustworthy source instead of the
        // on-wire yenc header name. The par2 index is built once per NZB (par2 bytes
        // can be on any file in the doc, normally only one slot has them). prepare keeps the
        // PAR2 files out of the doc, though: the name their data gives a file is the one it
        // lists the file under in the response's volumes, where that isn't its yEnc name.
        // Either names a file by its first 16 KB and its length, and none that start alike
        // and are as long; and no name is given two files.
        val files = metadata.orderedArchiveNzb.files
        val volumes = metadata.response.volumes
        val par2Names = Par2FileNames.parse(files.firstOrNull { it.par2Data != null }?.par2Data)
        val listedOnce = volumes.groupingBy { it }.eachCount().filterValues { it == 1 }.keys
        val par2Filenames = files.mapIndexed { index, file ->
            val headers = file.yencHeaders ?: return@mapIndexed null
            FilenameResolver.hash16kHex(file.first16kb)?.let { par2Names.nameOf(it, headers.size) }
                ?: volumes.getOrNull(index)?.takeIf { it != headers.name && it in listedOnce }
        }
        val best = files.mapIndexed { index, file ->
            file.yencHeaders?.let { FilenameResolver.bestFilename(it.name, file.subject, par2Filenames[index]) }
        }
        val taken = best.filterNotNull().groupingBy { it }.eachCount()
        return files.mapIndexedNotNull { index, file ->
            val headers = file.yencHeaders ?: return@mapIndexedNotNull null
            // A PAR2 name another file is listed under too is neither's: this one keeps its own.
            val name = best[index]!!.takeUnless { it == par2Filenames[index] && taken.getValue(it) > 1 }
                ?: FilenameResolver.bestFilename(headers.name, file.subject, par2Filename = null)
            // Fall back to the previously-surfaced name only if FilenameResolver
            // returned an empty string (every source was blank — should be impossible
            // for a real NZB but guard against malformed inputs).
            StreamableFile(
                path = name.ifBlank { volumes[index] },
                totalSize = headers.size,
                startVolumeIndex = index,
                startOffsetInVolume = 0,
                continuationHeaderSize = 0,
                endOfArchiveSize = 0
            )
        }
    }

    suspend fun streamFile(
        metadata: ExtractedMetadata,
        file: StreamableFile,
        range: LongRange? = null,
        onGap: ((Gap) -> Unit)? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        archiveStreamingService.streamFile(metadata.orderedArchiveNzb, file, range, onGap = onGap, consume = consume)
    }

    suspend fun streamFile(
        nzbDocument: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null,
        streamName: String? = null,
        onGap: ((Gap) -> Unit)? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        archiveStreamingService.streamFile(nzbDocument, file, range, streamName, onGap, consume)
    }

    /** Starts streaming [file]; see the [NamedSplits] overload for the writer's lifetime and failures. */
    suspend fun launchStreamFile(
        metadata: ExtractedMetadata,
        file: StreamableFile,
        range: LongRange? = null,
        streamName: String? = null,
        onGap: ((Gap) -> Unit)? = null,
    ): WriterJob {
        return archiveStreamingService.launchStreamFile(metadata.orderedArchiveNzb, file, range, streamName, onGap)
    }

    /** Starts streaming [file]; see the [NamedSplits] overload for the writer's lifetime and failures. */
    suspend fun launchStreamFile(
        nzbDocument: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null,
        streamName: String? = null,
        onGap: ((Gap) -> Unit)? = null,
    ): WriterJob {
        return archiveStreamingService.launchStreamFile(nzbDocument, file, range, streamName, onGap)
    }

    suspend fun streamVolume(
        metadata: ExtractedMetadata,
        volumeIndex: Int,
        onGap: ((Gap) -> Unit)? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        streamVolume(metadata.orderedArchiveNzb, volumeIndex, onGap, consume)
    }

    suspend fun streamVolume(
        nzb: NzbDocument,
        volumeIndex: Int,
        onGap: ((Gap) -> Unit)? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val file = nzb.files[volumeIndex]
        val queue = SegmentQueueService.createFileQueue(file, volumeIndex, 0L)
        streamingService.streamSegments(queue, name = "volume-$volumeIndex", onGap = onGap, consume = consume)
    }

    fun addPool(config: NntpConfig) {
        streamingService.addPool(config)
    }

    fun removePool(config: NntpConfig) {
        streamingService.removePool(config)
    }

    fun getPoolConfigs(): List<NntpConfig> {
        return streamingService.getPoolConfigs()
    }

    fun setStreamingConcurrency(value: Int) {
        streamingService.setStreamingConcurrency(value)
    }

    fun setReadAheadSegments(value: Int) {
        streamingService.setReadAheadSegments(value)
    }

    /** See [io.skjaere.nzbstreamer.config.StreamingConfig.readAheadBytes]. */
    fun setReadAheadBytes(value: Long?) {
        streamingService.setReadAheadBytes(value)
    }

    /** See [io.skjaere.nzbstreamer.config.StreamingConfig.initialReadAheadSegments]. */
    fun setInitialReadAheadSegments(value: Int?) {
        streamingService.setInitialReadAheadSegments(value)
    }

    // NOTE: only mutates the top-level VerificationService used by [verifySegments].
    // ArchiveMetadataService holds its own internal VerificationService for
    // prepare-time enrichment (sized from streamingConfig.concurrency at build time)
    // and is not affected — that path runs once per NZB import, not per stream, so
    // it's a lower-impact restart-required knob.
    fun setVerificationConcurrency(value: Int) {
        verificationService.setConcurrency(value)
    }

    fun getStreamingConcurrency(): Int = streamingService.getStreamingConcurrency()

    fun getReadAheadSegments(): Int = streamingService.getReadAheadSegments()

    fun getReadAheadBytes(): Long? = streamingService.getReadAheadBytes()

    fun getInitialReadAheadSegments(): Int? = streamingService.getInitialReadAheadSegments()

    fun getVerificationConcurrency(): Int = verificationService.getConcurrency()

    fun setCircuitBreakerConfig(failureThreshold: Int, cooldownMs: Long) {
        streamingService.setCircuitBreakerConfig(failureThreshold, cooldownMs)
    }

    fun getCircuitBreakerFailureThreshold(): Int = streamingService.getCircuitBreakerFailureThreshold()

    fun getCircuitBreakerCooldownMs(): Long = streamingService.getCircuitBreakerCooldownMs()

    fun setSegmentFetchTimeoutMs(value: Long) {
        streamingService.setSegmentFetchTimeoutMs(value)
    }

    fun getSegmentFetchTimeoutMs(): Long = streamingService.getSegmentFetchTimeoutMs()

    /** See [io.skjaere.nzbstreamer.config.StreamingConfig.missingArticles]. Applies to streams opened from now on. */
    fun setMissingArticles(value: MissingArticlePolicy) {
        streamingService.setMissingArticles(value)
    }

    fun getMissingArticles(): MissingArticlePolicy = streamingService.getMissingArticles()

    override fun close() {
        streamingService.close()
    }

    private fun isPar2(first16kb: ByteArray?): Boolean {
        return first16kb != null && Par2Parser.isPar2(first16kb)
    }

    class NntpBuilder {
        var host: String = ""
        var port: Int = 563
        var username: String = ""
        var password: String = ""
        var useTls: Boolean = true
        var maxConnections: Int = 8
        /** See [NntpConfig.verifyHostname]. */
        var verifyHostname: Boolean = true

        internal fun toConfig(): NntpConfig = NntpConfig(
            host = host,
            port = port,
            username = username,
            password = password,
            useTls = useTls,
            maxConnections = maxConnections,
            verifyHostname = verifyHostname
        )
    }

    class PrepareBuilder {
        var verifySegments: Boolean = false
    }

    class Builder {
        private val poolBuilders = mutableListOf<NntpBuilder>()
        private var prepareBuilder: PrepareBuilder = PrepareBuilder()
        var concurrency: Int = 4
        var verificationConcurrency: Int? = null
        var readAheadSegments: Int? = null
        /** See [StreamingConfig.readAheadBytes]. */
        var readAheadBytes: Long? = null
        /** See [StreamingConfig.initialReadAheadSegments]. */
        var initialReadAheadSegments: Int? = StreamingConfig.DEFAULT_INITIAL_READ_AHEAD_SEGMENTS
        var segmentCache: SegmentCache? = null
        /** See [NzbStreamer.fromConfig]'s closeSegmentCache. */
        var closeSegmentCache: Boolean = true
        /** See [StreamingConfig.missingArticles]. */
        var missingArticles: MissingArticlePolicy = MissingArticlePolicy.FAIL

        /** Add an NNTP pool. First pool added is the primary; subsequent pools are fill/fallback. */
        fun nntp(block: NntpBuilder.() -> Unit) {
            poolBuilders.add(NntpBuilder().apply(block))
        }

        fun prepare(block: PrepareBuilder.() -> Unit) {
            prepareBuilder = PrepareBuilder().apply(block)
        }

        fun build(): NzbStreamer {
            val streamingConfig = StreamingConfig(
                concurrency = concurrency,
                verificationConcurrency = verificationConcurrency ?: concurrency,
                readAheadSegments = readAheadSegments ?: (concurrency * 3),
                readAheadBytes = readAheadBytes,
                initialReadAheadSegments = initialReadAheadSegments,
                missingArticles = missingArticles,
            )
            val prepareConfig = PrepareConfig(verifySegments = prepareBuilder.verifySegments)
            return create(poolBuilders.map { it.toConfig() }, streamingConfig, prepareConfig, segmentCache, closeSegmentCache)
        }
    }

    companion object {
        operator fun invoke(block: Builder.() -> Unit): NzbStreamer {
            return Builder().apply(block).build()
        }

        /**
         * A streamer with exactly these configs: every setting in them applies, and [getPoolConfigs] returns [nntpConfigs].
         *
         * @param closeSegmentCache whether the streamer's [close] closes [segmentCache], saving its
         *   index: the streamer owns the cache it is given unless this is false, as for a cache the
         *   caller keeps across the streamers it builds (one instance per cache directory).
         */
        fun fromConfig(
            nntpConfigs: List<NntpConfig>,
            streamingConfig: StreamingConfig = StreamingConfig(),
            prepareConfig: PrepareConfig = PrepareConfig(),
            segmentCache: SegmentCache? = null,
            closeSegmentCache: Boolean = true,
        ): NzbStreamer = create(nntpConfigs, streamingConfig, prepareConfig, segmentCache, closeSegmentCache)

        // The configs go through as they are: copying them field by field (through the builder)
        // dropped the fields nobody remembered to copy.
        private fun create(
            nntpConfigs: List<NntpConfig>,
            streamingConfig: StreamingConfig,
            prepareConfig: PrepareConfig,
            segmentCache: SegmentCache?,
            closeSegmentCache: Boolean,
        ): NzbStreamer {
            val streamingService = NntpStreamingService(nntpConfigs, streamingConfig, segmentCache, closeSegmentCache)
            runBlocking { streamingService.connect() }

            val metadataService = ArchiveMetadataService(
                streamingService, prepareConfig, streamingConfig.concurrency
            )
            val verificationService = VerificationService(streamingService, streamingConfig.verificationConcurrency)
            val archiveStreamingService = ArchiveStreamingService(streamingService)

            return NzbStreamer(streamingService, metadataService, verificationService, archiveStreamingService)
        }

        /** Single-pool convenience overload. */
        fun fromConfig(
            nntpConfig: NntpConfig,
            streamingConfig: StreamingConfig = StreamingConfig(),
            prepareConfig: PrepareConfig = PrepareConfig(),
            segmentCache: SegmentCache? = null,
            closeSegmentCache: Boolean = true,
        ): NzbStreamer = fromConfig(listOf(nntpConfig), streamingConfig, prepareConfig, segmentCache, closeSegmentCache)
    }
}
