package io.skjaere.nzbstreamer.stream

/**
 * What [NntpStreamingService.statUntilMissing] learnt before it stopped. [missing] holds the
 * indices (into the ids it was given) of the articles every pool answered 430 for, in the order
 * they were found; [answered] counts the articles with a final answer (found on a pool, or
 * missing on all of them) out of [total].
 *
 * [unanswered] holds the articles no pool could answer for: each pool asked failed other than with
 * a 430, or was skipped for its open circuit breaker, and the rest answered 430. The check stops
 * at the first one, and [failure] is the first failure of a pool it went through.
 */
data class StatCheck @JvmOverloads constructor(
    val missing: List<Int>,
    val answered: Int,
    val total: Int,
    val unanswered: List<Int> = emptyList(),
    val failure: Throwable? = null,
)
