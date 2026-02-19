package io.skjaere.nzbstreamer.metadata

sealed interface PrepareResult {
    data class Success(val metadata: ExtractedMetadata) : PrepareResult
    data class MissingArticles(val message: String, val cause: Throwable) : PrepareResult
    data class Failure(val message: String, val cause: Throwable) : PrepareResult
}
