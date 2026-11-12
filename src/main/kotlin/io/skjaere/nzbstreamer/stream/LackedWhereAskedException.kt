package io.skjaere.nzbstreamer.stream

/**
 * Prepare's read of an article ([NntpStreamingService.fetchArticle]) where every pool asked lacks it, each answering
 * 430, and a pool its breaker had passed over, asked after them, didn't serve it: [failure], its connection
 * dropping, its login rejected, no answer in time, or only a bad copy. Not missing, as that pool may have it; but no
 * worse than before that pool was asked, when the article was missing on the pools asked. So prepare leaves out a file
 * the release opens without for it, as for a 430, and a file the release needs fails with [failure], unavailable rather
 * than missing. Never thrown beyond prepare.
 */
internal class LackedWhereAskedException(val failure: Exception) : Exception(failure.message, failure)
