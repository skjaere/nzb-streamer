package io.skjaere.nzbstreamer

import io.ktor.utils.io.*
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.SeekConfig
import io.skjaere.nzbstreamer.metadata.ArchiveMetadataService
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.nzb.NzbParser
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.ArchiveStreamingService
import io.skjaere.nzbstreamer.stream.FileResolveResult
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.io.Closeable

class NzbStreamer private constructor(
    private val streamingService: NntpStreamingService,
    private val metadataService: ArchiveMetadataService,
    private val archiveStreamingService: ArchiveStreamingService,
    private val scope: CoroutineScope,
    private val ownsScope: Boolean
) : Closeable {

    suspend fun prepare(nzbBytes: ByteArray): ExtractedMetadata {
        val nzb = NzbParser.parse(nzbBytes)
        return metadataService.prepare(nzb)
    }

    fun resolveFile(metadata: ExtractedMetadata, path: String): FileResolveResult {
        return archiveStreamingService.resolveFile(metadata.entries, metadata.orderedArchiveNzb, path)
    }

    fun streamFile(
        metadata: ExtractedMetadata,
        splits: List<SplitInfo>,
        range: LongRange? = null
    ): Pair<ByteReadChannel, Job> {
        return archiveStreamingService.streamFile(metadata.orderedArchiveNzb, splits, range)
    }

    suspend fun streamFile(
        metadata: ExtractedMetadata,
        splits: List<SplitInfo>,
        range: LongRange? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val (channel, job) = streamFile(metadata, splits, range)
        try {
            consume(channel)
        } finally {
            job.cancel()
        }
    }

    fun streamVolume(metadata: ExtractedMetadata, volumeIndex: Int): Pair<ByteReadChannel, Job> {
        val file = metadata.orderedArchiveNzb.files[volumeIndex]
        val queue = SegmentQueueService.createFileQueue(file, volumeIndex, 0L)
        return streamingService.streamSegments(queue)
    }

    suspend fun streamVolume(
        metadata: ExtractedMetadata,
        volumeIndex: Int,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val (channel, job) = streamVolume(metadata, volumeIndex)
        try {
            consume(channel)
        } finally {
            job.cancel()
        }
    }

    override fun close() {
        streamingService.close()
        if (ownsScope) {
            scope.cancel()
        }
    }

    class NntpBuilder {
        var host: String = ""
        var port: Int = 563
        var username: String = ""
        var password: String = ""
        var useTls: Boolean = true
        var concurrency: Int = 4
    }

    class SeekBuilder {
        var forwardThresholdBytes: Long = 102400L
    }

    class Builder {
        private var nntpBuilder: NntpBuilder? = null
        private var seekBuilder: SeekBuilder = SeekBuilder()
        private var scope: CoroutineScope? = null

        fun nntp(block: NntpBuilder.() -> Unit) {
            nntpBuilder = NntpBuilder().apply(block)
        }

        fun seek(block: SeekBuilder.() -> Unit) {
            seekBuilder = SeekBuilder().apply(block)
        }

        fun scope(scope: CoroutineScope) {
            this.scope = scope
        }

        fun build(): NzbStreamer {
            val nb = nntpBuilder ?: error("nntp {} block is required")
            val nntpConfig = NntpConfig(
                host = nb.host,
                port = nb.port,
                username = nb.username,
                password = nb.password,
                useTls = nb.useTls,
                concurrency = nb.concurrency
            )
            val seekConfig = SeekConfig(forwardThresholdBytes = seekBuilder.forwardThresholdBytes)

            val ownsScope = scope == null
            val effectiveScope = scope ?: CoroutineScope(Dispatchers.IO + SupervisorJob())

            val streamingService = NntpStreamingService(nntpConfig, effectiveScope)
            runBlocking { streamingService.connect() }

            val metadataService = ArchiveMetadataService(streamingService, seekConfig.forwardThresholdBytes)
            val archiveStreamingService = ArchiveStreamingService(streamingService)

            return NzbStreamer(streamingService, metadataService, archiveStreamingService, effectiveScope, ownsScope)
        }
    }

    companion object {
        operator fun invoke(block: Builder.() -> Unit): NzbStreamer {
            return Builder().apply(block).build()
        }

        fun fromConfig(nntpConfig: NntpConfig, seekConfig: SeekConfig, scope: CoroutineScope? = null): NzbStreamer {
            return invoke {
                nntp {
                    host = nntpConfig.host
                    port = nntpConfig.port
                    username = nntpConfig.username
                    password = nntpConfig.password
                    useTls = nntpConfig.useTls
                    concurrency = nntpConfig.concurrency
                }
                seek { forwardThresholdBytes = seekConfig.forwardThresholdBytes }
                if (scope != null) scope(scope)
            }
        }
    }
}
