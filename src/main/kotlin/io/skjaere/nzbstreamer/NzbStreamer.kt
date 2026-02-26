package io.skjaere.nzbstreamer

import io.ktor.utils.io.*
import io.skjaere.compressionutils.Par2Parser
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.PrepareConfig
import io.skjaere.nzbstreamer.config.SeekConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.metadata.ArchiveMetadataService
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbParser
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.FileResolveResult
import io.skjaere.nzbstreamer.stream.NamedSplits
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.StreamableFile
import kotlinx.coroutines.runBlocking
import java.io.Closeable

class NzbStreamer private constructor(
    private val streamingService: NntpStreamingService,
    private val metadataService: ArchiveMetadataService,
    private val verificationService: VerificationService,
    private val archiveStreamingService: ArchiveStreamingService
) : Closeable {

    suspend fun verifySegments(nzb: NzbDocument): VerificationResult {
        return verificationService.verifySegments(nzb)
    }

    suspend fun enrich(nzbBytes: ByteArray): EnrichmentResult {
        val nzb = NzbParser.parse(nzbBytes)
        return metadataService.enrich(nzb)
    }

    suspend fun prepare(nzbBytes: ByteArray): PrepareResult {
        val nzb = NzbParser.parse(nzbBytes)
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
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        archiveStreamingService.streamFile(metadata.orderedArchiveNzb, namedSplits, range, consume = consume)
    }

    suspend fun launchStreamFile(
        metadata: ExtractedMetadata,
        namedSplits: NamedSplits,
        range: LongRange? = null
    ): WriterJob {
        return archiveStreamingService.launchStreamFile(metadata.orderedArchiveNzb, namedSplits, range)
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
        return metadata.orderedArchiveNzb.files.mapIndexedNotNull { index, file ->
            val headers = file.yencHeaders ?: return@mapIndexedNotNull null
            StreamableFile(
                path = metadata.response.volumes[index],
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
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        archiveStreamingService.streamFile(metadata.orderedArchiveNzb, file, range, consume)
    }

    suspend fun streamFile(
        nzbDocument: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        archiveStreamingService.streamFile(nzbDocument, file, range, consume)
    }

    suspend fun launchStreamFile(
        metadata: ExtractedMetadata,
        file: StreamableFile,
        range: LongRange? = null
    ): WriterJob {
        return archiveStreamingService.launchStreamFile(metadata.orderedArchiveNzb, file, range)
    }

    suspend fun launchStreamFile(
        nzbDocument: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null
    ): WriterJob {
        return archiveStreamingService.launchStreamFile(nzbDocument, file, range)
    }

    suspend fun streamVolume(
        metadata: ExtractedMetadata,
        volumeIndex: Int,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        streamVolume(metadata.orderedArchiveNzb, volumeIndex, consume)
    }

    suspend fun streamVolume(
        nzb: NzbDocument,
        volumeIndex: Int,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val file = nzb.files[volumeIndex]
        val queue = SegmentQueueService.createFileQueue(file, volumeIndex, 0L)
        streamingService.streamSegments(queue, consume = consume)
    }

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
        var concurrency: Int = 4
        var maxConnections: Int = 8
        var readAheadSegments: Int? = null
    }

    class SeekBuilder {
        var forwardThresholdBytes: Long = 102400L
    }

    class PrepareBuilder {
        var verifySegments: Boolean = false
    }

    class Builder {
        private var nntpBuilder: NntpBuilder? = null
        private var seekBuilder: SeekBuilder = SeekBuilder()
        private var prepareBuilder: PrepareBuilder = PrepareBuilder()

        fun nntp(block: NntpBuilder.() -> Unit) {
            nntpBuilder = NntpBuilder().apply(block)
        }

        fun seek(block: SeekBuilder.() -> Unit) {
            seekBuilder = SeekBuilder().apply(block)
        }

        fun prepare(block: PrepareBuilder.() -> Unit) {
            prepareBuilder = PrepareBuilder().apply(block)
        }

        fun build(): NzbStreamer {
            val nb = nntpBuilder ?: error("nntp {} block is required")
            val nntpConfig = NntpConfig(
                host = nb.host,
                port = nb.port,
                username = nb.username,
                password = nb.password,
                useTls = nb.useTls,
                concurrency = nb.concurrency,
                maxConnections = nb.maxConnections,
                readAheadSegments = nb.readAheadSegments ?: (nb.concurrency * 3)
            )
            val seekConfig = SeekConfig(forwardThresholdBytes = seekBuilder.forwardThresholdBytes)
            val prepareConfig = PrepareConfig(verifySegments = prepareBuilder.verifySegments)
            val streamingService = NntpStreamingService(nntpConfig)
            runBlocking { streamingService.connect() }

            val metadataService = ArchiveMetadataService(
                streamingService, seekConfig.forwardThresholdBytes, prepareConfig, nntpConfig.concurrency
            )
            val verificationService = VerificationService(streamingService, nntpConfig.concurrency)
            val archiveStreamingService = ArchiveStreamingService(streamingService)

            return NzbStreamer(streamingService, metadataService, verificationService, archiveStreamingService)
        }
    }

    companion object {
        operator fun invoke(block: Builder.() -> Unit): NzbStreamer {
            return Builder().apply(block).build()
        }

        fun fromConfig(
            nntpConfig: NntpConfig,
            seekConfig: SeekConfig,
            prepareConfig: PrepareConfig = PrepareConfig()
        ): NzbStreamer {
            return invoke {
                nntp {
                    host = nntpConfig.host
                    port = nntpConfig.port
                    username = nntpConfig.username
                    password = nntpConfig.password
                    useTls = nntpConfig.useTls
                    concurrency = nntpConfig.concurrency
                    maxConnections = nntpConfig.maxConnections
                    readAheadSegments = nntpConfig.readAheadSegments
                }
                seek { forwardThresholdBytes = seekConfig.forwardThresholdBytes }
                prepare { verifySegments = prepareConfig.verifySegments }
            }
        }
    }
}
