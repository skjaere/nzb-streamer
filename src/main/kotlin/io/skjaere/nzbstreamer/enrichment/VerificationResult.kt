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

    /**
     * The check couldn't tell, e.g. no provider could answer for an article: each failed other
     * than with a 430 (its login refused, unreachable, its connection dropped) or the rest said
     * 430. [cause] is the first provider's failure.
     */
    data class Failure(val message: String, val cause: Throwable) : VerificationResult
}
