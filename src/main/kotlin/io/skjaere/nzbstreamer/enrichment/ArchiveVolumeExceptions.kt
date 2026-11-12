package io.skjaere.nzbstreamer.enrichment

import io.skjaere.nntp.ArticleNotFoundException

/**
 * Archive volume [volume] was posted under a name that doesn't give its place in its archive, and
 * no PAR2 data names it: nothing tells where it goes, so the release can't be
 * opened in order. The release is broken as posted, whichever providers serve it. prepare's
 * [io.skjaere.nzbstreamer.metadata.PrepareResult.Failure] has it as its cause; an
 * [IllegalStateException], as that cause was before it had a type of its own.
 */
class UnplacedArchiveVolumeException(val volume: String, message: String) : IllegalStateException(message)

/**
 * An archive volume of a set the release opens that its NZB doesn't hold under any name prepare
 * reads: one its PAR2 data names, or one its set's names skip.
 * No provider can supply a file the NZB doesn't list, so the release can't open whole whichever
 * providers serve it. prepare's [io.skjaere.nzbstreamer.metadata.PrepareResult.MissingArticles]
 * has it as its cause, with an [ArticleNotFoundException] of its words as its own, as the missing
 * article it was reported as before.
 */
class ArchiveVolumeNotInNzbException(message: String) : Exception(message, ArticleNotFoundException(message))
