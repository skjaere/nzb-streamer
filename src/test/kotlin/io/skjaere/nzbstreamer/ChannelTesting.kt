package io.skjaere.nzbstreamer

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.availableForRead

/** What a Ktor channel takes flushed and unread before a writer's flush waits for the reader. */
private const val CHANNEL_LIMIT = 1024 * 1024

/**
 * Waits until the channel's writer is held up in a flush, reading nothing: first a [CHANNEL_LIMIT]
 * or more moves into the read buffer, then the writer flushes another [CHANNEL_LIMIT] behind it,
 * which stays where it is (reads move flushed bytes only into a read buffer holding less).
 */
internal suspend fun ByteReadChannel.awaitWriterHeldUp() {
    awaitContent(CHANNEL_LIMIT)
    awaitContent(availableForRead + CHANNEL_LIMIT)
}
