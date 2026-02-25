package io.skjaere.nzbstreamer.enrichment

sealed interface VerificationResult {
    data object Success : VerificationResult
    data class MissingArticles(val message: String, val cause: Throwable) : VerificationResult
    data class Failure(val message: String, val cause: Throwable) : VerificationResult
}
