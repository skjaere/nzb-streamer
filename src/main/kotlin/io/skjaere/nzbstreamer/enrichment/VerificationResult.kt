package io.skjaere.nzbstreamer.enrichment

sealed interface VerificationResult {
    data object Success : VerificationResult

    /**
     * Articles are missing on every provider. A check stops at the first such article, so the
     * counts cover what was answered by then: [missingCount] of [answeredCount] answered articles
     * were missing, out of [totalCount] to check. All answered articles missing suggests the
     * release was taken down; one among many, a gap.
     */
    data class MissingArticles @JvmOverloads constructor(
        val message: String,
        val cause: Throwable,
        val missingCount: Int = 1,
        val answeredCount: Int = missingCount,
        val totalCount: Int = answeredCount,
    ) : VerificationResult

    data class Failure(val message: String, val cause: Throwable) : VerificationResult
}
