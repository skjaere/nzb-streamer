package io.skjaere.nzbstreamer.metadata

import io.skjaere.compressionutils.EncryptionInfo

sealed interface PrepareResult {
    data class Success(val metadata: ExtractedMetadata) : PrepareResult
    data class MissingArticles(val message: String, val cause: Throwable) : PrepareResult
    data class Failure(val message: String, val cause: Throwable) : PrepareResult
    data class UnsupportedArchive(val message: String, val cause: Throwable) : PrepareResult

    /**
     * Archive is RAR5 with `HEAD_CRYPT` (password-protected). Carries the crypto
     * parameters so callers can persist them and re-attempt parsing once a
     * password is available. Distinct from [Failure] so this isn't logged as an
     * error in operational logs / Sentry — it's an expected state, not a bug.
     */
    data class Encrypted(val message: String, val info: EncryptionInfo) : PrepareResult
}
