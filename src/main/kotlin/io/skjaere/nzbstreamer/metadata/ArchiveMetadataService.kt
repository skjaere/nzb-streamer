package io.skjaere.nzbstreamer.metadata

import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.ArchiveService
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.VolumeMetaData
import io.skjaere.nzbstreamer.enrichment.NzbEnrichmentService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory

@Serializable
data class NzbMetadataResponse(
    val cacheKey: String = "",
    val volumes: List<String>,
    val obfuscated: Boolean,
    val entries: List<ArchiveFileEntryResponse>
)

data class ExtractedMetadata(
    val response: NzbMetadataResponse,
    val orderedArchiveNzb: NzbDocument,
    val entries: List<ArchiveFileEntry>
)

@Serializable
sealed interface ArchiveFileEntryResponse

@Serializable
@SerialName("rar")
data class RarFileEntryResponse(
    val path: String,
    val uncompressedSize: Long,
    val compressedSize: Long,
    val isDirectory: Boolean,
    val volumeIndex: Int,
    val compressionMethod: Int,
    val isUncompressed: Boolean,
    val isSplit: Boolean,
    val splitParts: List<SplitInfoResponse>,
    val crc32: Long? = null
) : ArchiveFileEntryResponse

@Serializable
data class SplitInfoResponse(
    val volumeIndex: Int,
    val dataStartPosition: Long,
    val dataSize: Long
)

@Serializable
@SerialName("7zip")
data class SevenZipFileEntryResponse(
    val path: String,
    val size: Long,
    val packedSize: Long,
    val isDirectory: Boolean,
    val method: String? = null,
    val crc32: Long? = null
) : ArchiveFileEntryResponse

class ArchiveMetadataService(
    private val streamingService: NntpStreamingService,
    private val forwardThresholdBytes: Long
) {
    private val logger = LoggerFactory.getLogger(ArchiveMetadataService::class.java)
    private val enrichmentService = NzbEnrichmentService(streamingService)

    suspend fun enrich(nzb: NzbDocument): NzbDocument {
        enrichmentService.enrich(nzb)
        return NzbDocument(nzb.files.filter { it.yencHeaders != null })
    }

    suspend fun prepare(nzb: NzbDocument): ExtractedMetadata {
        val enrichedNzb = enrich(nzb)
        return if (enrichedNzb.files.isEmpty()) {
            ExtractedMetadata(
                response = NzbMetadataResponse(
                    volumes = emptyList(),
                    obfuscated = false,
                    entries = emptyList()
                ),
                orderedArchiveNzb = NzbDocument(emptyList()),
                entries = emptyList()
            )
        } else {
            extractMetadata(enrichedNzb)
        }
    }

    suspend fun extractMetadata(nzb: NzbDocument): ExtractedMetadata {
        val par2Data = nzb.files.firstOrNull { it.par2Data != null }?.par2Data

        // Filter to only archive volumes (exclude PAR2 files) for the seekable stream
        // and volume metadata. PAR2 data is passed separately.
        val archiveFiles = nzb.files.filter { file ->
            val name = file.yencHeaders?.name ?: return@filter false
            !name.endsWith(".par2", ignoreCase = true)
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

        // Reorder archiveFiles to match the resolved volume order
        val orderedArchiveFiles = orderedVolumes.map { vol ->
            archiveFiles[volumes.indexOfFirst { it.first16kb === vol.first16kb }]
        }

        logger.info("Extracting metadata from {} volumes (obfuscated={})", orderedVolumes.size, obfuscated)

        val orderedArchiveNzb = NzbDocument(orderedArchiveFiles)
        val seekableStream = NntpSeekableInputStream(
            orderedArchiveNzb, streamingService, forwardThresholdBytes
        )
        val rawEntries = seekableStream.use { stream ->
            ArchiveService.listFiles(stream, orderedVolumes, par2Data)
        }

        val response = NzbMetadataResponse(
            volumes = orderedVolumes.map { it.filename },
            obfuscated = obfuscated,
            entries = rawEntries.map { it.toResponse() }
        )

        return ExtractedMetadata(
            response = response,
            orderedArchiveNzb = orderedArchiveNzb,
            entries = rawEntries
        )
    }
}

private fun ArchiveFileEntry.toResponse(): ArchiveFileEntryResponse {
    return when (this) {
        is RarFileEntry -> RarFileEntryResponse(
            path = path,
            uncompressedSize = uncompressedSize,
            compressedSize = compressedSize,
            isDirectory = isDirectory,
            volumeIndex = volumeIndex,
            compressionMethod = compressionMethod,
            isUncompressed = isUncompressed,
            isSplit = isSplit,
            splitParts = splitParts.map {
                SplitInfoResponse(
                    volumeIndex = it.volumeIndex,
                    dataStartPosition = it.dataStartPosition,
                    dataSize = it.dataSize
                )
            },
            crc32 = crc32
        )
        is SevenZipFileEntry -> SevenZipFileEntryResponse(
            path = path,
            size = size,
            packedSize = packedSize,
            isDirectory = isDirectory,
            method = method,
            crc32 = crc32
        )
    }
}
