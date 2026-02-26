package io.skjaere.nzbstreamer.metadata

import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.ArchiveService
import io.skjaere.compressionutils.ListFilesResult
import io.skjaere.compressionutils.Par2Parser
import io.skjaere.compressionutils.VolumeMetaData
import io.skjaere.nzbstreamer.config.PrepareConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import org.slf4j.LoggerFactory

class ArchiveMetadataService(
    private val streamingService: NntpStreamingService,
    private val forwardThresholdBytes: Long,
    private val prepareConfig: PrepareConfig = PrepareConfig(),
    concurrency: Int = 1
) {
    private val logger = LoggerFactory.getLogger(ArchiveMetadataService::class.java)
    private val enrichmentService = NzbEnrichmentService(streamingService)
    private val verificationService = VerificationService(streamingService, concurrency)
    private val nestedArchiveService = NestedArchiveMetadataService(streamingService, forwardThresholdBytes)

    suspend fun enrich(nzb: NzbDocument): EnrichmentResult {
        val result = enrichmentService.enrich(nzb)
        if (result is EnrichmentResult.Success) {
            return EnrichmentResult.Success(
                NzbDocument(result.enrichedNzb.files.filter { it.yencHeaders != null })
            )
        }
        return result
    }

    suspend fun prepare(nzb: NzbDocument): PrepareResult {
        return when (val result = enrich(nzb)) {
            is EnrichmentResult.Success -> {
                val enrichedNzb = result.enrichedNzb

                if (prepareConfig.verifySegments) {
                    when (val v = verificationService.verifySegments(enrichedNzb)) {
                        is VerificationResult.MissingArticles ->
                            return PrepareResult.MissingArticles(v.message, v.cause)

                        is VerificationResult.Failure ->
                            return PrepareResult.Failure(v.message, v.cause)

                        is VerificationResult.Success -> { /* continue */
                        }
                    }
                }

                if (enrichedNzb.files.isEmpty()) {
                    PrepareResult.Success(
                        ExtractedMetadata.Raw(
                            response = NzbMetadataResponse(
                                volumes = emptyList(),
                                obfuscated = false,
                                entries = emptyList()
                            ),
                            orderedArchiveNzb = NzbDocument(emptyList())
                        )
                    )
                } else {
                    PrepareResult.Success(extractMetadata(enrichedNzb))
                }
            }

            is EnrichmentResult.MissingArticles ->
                PrepareResult.MissingArticles(result.message, result.cause)

            is EnrichmentResult.Failure ->
                PrepareResult.Failure(result.message, result.cause)
        }
    }

    suspend fun extractMetadata(nzb: NzbDocument): ExtractedMetadata {
        val resolved = resolveVolumes(nzb)
        val (orderedArchiveNzb, orderedVolumes, obfuscated) = resolved

        val rawEntries = listArchiveEntries(resolved)
            ?: return ExtractedMetadata.Raw(
                response = NzbMetadataResponse(
                    volumes = orderedVolumes.map { it.filename },
                    obfuscated = obfuscated,
                    entries = emptyList()
                ),
                orderedArchiveNzb = orderedArchiveNzb
            )

        return buildMetadata(rawEntries, orderedArchiveNzb, orderedVolumes, obfuscated)
    }

    private fun resolveVolumes(nzb: NzbDocument): ResolvedVolumes {
        val par2Data = nzb.files.firstOrNull { it.par2Data != null }?.par2Data

        val archiveFiles = nzb.files.filter { file ->
            val first16kb = file.first16kb
            first16kb == null || !Par2Parser.isPar2(first16kb)
        }

        val volumes = archiveFiles.map { file ->
            VolumeMetaData(
                filename = file.yencHeaders!!.name,
                size = file.yencHeaders!!.size,
                first16kb = file.first16kb
            )
        }

        val obfuscated = volumes.any { !ArchiveService.fileHasKnownExtension(it.filename) }
        val orderedVolumes = ArchiveService.resolveVolumes(volumes, par2Data)

        val orderedArchiveFiles = orderedVolumes.map { vol ->
            archiveFiles[volumes.indexOfFirst { it.first16kb === vol.first16kb }]
        }

        logger.debug("Extracting metadata from {} volumes (obfuscated={})", orderedVolumes.size, obfuscated)

        return ResolvedVolumes(
            orderedArchiveNzb = NzbDocument(orderedArchiveFiles),
            orderedVolumes = orderedVolumes,
            obfuscated = obfuscated,
            par2Data = par2Data
        )
    }

    private suspend fun listArchiveEntries(resolved: ResolvedVolumes): List<ArchiveFileEntry>? {
        val seekableStream = NntpSeekableInputStream(
            resolved.orderedArchiveNzb, streamingService, forwardThresholdBytes
        )
        val listFilesResult = seekableStream.use { stream ->
            ArchiveService.listFiles(stream, resolved.orderedVolumes, resolved.par2Data)
        }
        return when (listFilesResult) {
            is ListFilesResult.Success -> listFilesResult.entries
            is ListFilesResult.UnsupportedFormat -> {
                logger.debug("No supported archive format detected; returning volumes without archive entries")
                null
            }
        }
    }

    private suspend fun buildMetadata(
        rawEntries: List<ArchiveFileEntry>,
        orderedArchiveNzb: NzbDocument,
        orderedVolumes: List<VolumeMetaData>,
        obfuscated: Boolean
    ): ExtractedMetadata {
        val innerArchiveFiles = rawEntries.filter { entry ->
            !entry.isDirectory && ArchiveService.fileHasKnownExtension(entry.path)
        }

        if (innerArchiveFiles.isNotEmpty() &&
            nestedArchiveService.looksLikeNestedArchive(innerArchiveFiles, rawEntries)
        ) {
            logger.debug("Detected nested archive with {} inner archive files", innerArchiveFiles.size)
            return nestedArchiveService.extractNestedMetadata(
                outerEntries = rawEntries,
                innerArchiveEntries = innerArchiveFiles,
                orderedArchiveNzb = orderedArchiveNzb,
                orderedVolumes = orderedVolumes,
                obfuscated = obfuscated
            )
        }

        return ExtractedMetadata.Archive(
            response = NzbMetadataResponse(
                volumes = orderedVolumes.map { it.filename },
                obfuscated = obfuscated,
                entries = rawEntries.map { it.toResponse() }
            ),
            orderedArchiveNzb = orderedArchiveNzb,
            entries = rawEntries
        )
    }

    private data class ResolvedVolumes(
        val orderedArchiveNzb: NzbDocument,
        val orderedVolumes: List<VolumeMetaData>,
        val obfuscated: Boolean,
        val par2Data: ByteArray?
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ResolvedVolumes

            if (obfuscated != other.obfuscated) return false
            if (orderedArchiveNzb != other.orderedArchiveNzb) return false
            if (orderedVolumes != other.orderedVolumes) return false
            if (!par2Data.contentEquals(other.par2Data)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = obfuscated.hashCode()
            result = 31 * result + orderedArchiveNzb.hashCode()
            result = 31 * result + orderedVolumes.hashCode()
            result = 31 * result + (par2Data?.contentHashCode() ?: 0)
            return result
        }
    }
}
