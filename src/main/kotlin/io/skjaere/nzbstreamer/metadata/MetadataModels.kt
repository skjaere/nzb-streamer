package io.skjaere.nzbstreamer.metadata

import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.TranslatedFileEntry
import io.skjaere.nzbstreamer.nzb.NzbDocument
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class NzbMetadataResponse(
    val cacheKey: String = "",
    val volumes: List<String>,
    val obfuscated: Boolean,
    val entries: List<ArchiveFileEntryResponse>
)

sealed interface ExtractedMetadata {
    val response: NzbMetadataResponse
    val orderedArchiveNzb: NzbDocument

    data class Archive(
        override val response: NzbMetadataResponse,
        override val orderedArchiveNzb: NzbDocument,
        val entries: List<ArchiveFileEntry>
    ) : ExtractedMetadata

    data class Raw(
        override val response: NzbMetadataResponse,
        override val orderedArchiveNzb: NzbDocument
    ) : ExtractedMetadata

    data class NestedArchive(
        override val response: NzbMetadataResponse,
        override val orderedArchiveNzb: NzbDocument,
        val innerEntries: List<ArchiveFileEntry>,
        val outerEntries: List<ArchiveFileEntry>
    ) : ExtractedMetadata
}

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

@Serializable
@SerialName("translated")
data class TranslatedFileEntryResponse(
    val path: String,
    val size: Long,
    val isDirectory: Boolean,
    val splitParts: List<SplitInfoResponse>,
    val crc32: Long? = null
) : ArchiveFileEntryResponse

internal fun ArchiveFileEntry.toResponse(): ArchiveFileEntryResponse {
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

        is TranslatedFileEntry -> TranslatedFileEntryResponse(
            path = path,
            size = size,
            isDirectory = isDirectory,
            splitParts = splitParts.map {
                SplitInfoResponse(
                    volumeIndex = it.volumeIndex,
                    dataStartPosition = it.dataStartPosition,
                    dataSize = it.dataSize
                )
            },
            crc32 = crc32
        )
    }
}
