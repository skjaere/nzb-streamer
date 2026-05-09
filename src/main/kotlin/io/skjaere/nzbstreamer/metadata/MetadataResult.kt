package io.skjaere.nzbstreamer.metadata

import io.skjaere.compressionutils.EncryptionInfo

/**
 * Result of [ArchiveMetadataService.extractMetadata]. The Success case carries the
 * fully-resolved [ExtractedMetadata]; the Encrypted case short-circuits when the
 * underlying RAR archive is password-protected (RAR5 `HEAD_CRYPT`) so the caller
 * can persist the crypto parameters and surface a typed [PrepareResult.Encrypted]
 * without having to fold the encrypted state into the broader ExtractedMetadata
 * hierarchy (which the streaming/resolution paths assume is fully-extracted).
 */
sealed interface MetadataResult {
    data class Success(val metadata: ExtractedMetadata) : MetadataResult
    data class Encrypted(val info: EncryptionInfo) : MetadataResult
}
