package io.skjaere.nzbstreamer.enrichment

import io.skjaere.nzbstreamer.nzb.NzbDocument

sealed interface EnrichmentResult {
    data class Success(val enrichedNzb: NzbDocument) : EnrichmentResult
    data class MissingArticles(val message: String, val cause: Throwable) : EnrichmentResult
    data class Failure(val message: String, val cause: Throwable) : EnrichmentResult
}
