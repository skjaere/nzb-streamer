package io.skjaere.nzbstreamer.stream

/**
 * What [NntpStreamingService.statUntilMissing] learnt before it stopped. [missing] holds the
 * indices (into the ids it was given) of the articles no pool has, in the order they were found;
 * [answered] counts the articles with a final answer (found on a pool, or missing on all of them)
 * out of [total].
 */
data class StatCheck(
    val missing: List<Int>,
    val answered: Int,
    val total: Int,
)
