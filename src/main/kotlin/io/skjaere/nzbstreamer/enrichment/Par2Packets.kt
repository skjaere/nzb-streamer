package io.skjaere.nzbstreamer.enrichment

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.discard
import io.ktor.utils.io.readAvailable
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Reads the FileDesc packets of a PAR2 file as it streams, without holding the rest: a recovery
 * volume's recovery blocks can be tens of MB each. One reader reads one file,
 * in one read or several ([read]): a recovery slice need not be read at all, as its length is in its
 * header, so a read can stop there and the next start past it.
 */
internal class Par2Packets {
    private val kept = ByteArrayOutputStream()
    private val described = mutableSetOf<String>()
    private var listed: Set<String>? = null

    /**
     * The FileDesc packets read so far, one after another as a PAR2 file holds them (which the PAR2
     * parser reads as one), or null when none.
     */
    val descriptions: ByteArray? get() = kept.toByteArray().takeIf { it.isNotEmpty() }

    /** How many bytes the reads have taken from their channels, skipped ones too. */
    var bytesRead = 0L
        private set

    /** Whether the descriptions read so far cover every file the Main packet lists: none more is needed. */
    val complete: Boolean get() = kept.size() > 0 && listed?.let(described::containsAll) == true

    /** Where a [read] stopped. */
    sealed interface Stop {
        /** At a recovery slice once the descriptions are complete, or at bytes that aren't a packet: nothing more to read. */
        data object Done : Stop

        /** At a recovery slice it was to skip, before the descriptions are complete: they go on from [at], its end. */
        data class Next(val at: Long) : Stop

        /** Where [channel][read] ended: the file goes on from [at], the start of the packet it was in or the end of one it skipped. */
        data class End(val at: Long) : Stop
    }

    /**
     * Reads the packets [channel] delivers, the file's from byte [from] on (a packet's start), up to
     * its end, the first bytes that aren't a packet, or a recovery block once the descriptions it
     * read cover every file the Main packet lists: a recovery volume names its files before or after
     * its blocks, or spreads them between its blocks as par2cmdline does. A recovery
     * slice of [skipOver] bytes or more before then ends the read too ([Stop.Next]): the next read
     * starts past it. Other packets are skipped, but for the Main packet's list of file ids.
     */
    suspend fun read(channel: ByteReadChannel, from: Long, skipOver: Long = Long.MAX_VALUE): Stop {
        var position = from
        val header = ByteArray(HEADER)
        while (true) {
            if (channel.readUpTo(header) < HEADER) return Stop.End(position)
            if (!header.copyOf(MAGIC.size).contentEquals(MAGIC)) return Stop.Done
            val length = ByteBuffer.wrap(header, 8, 8).order(ByteOrder.LITTLE_ENDIAN).long
            if (length < HEADER) return Stop.Done
            val type = header.copyOfRange(48, HEADER)
            val body = length - HEADER
            if (type.contentEquals(RECOVERY_SLICE)) {
                if (complete) return Stop.Done
                if (body >= skipOver) return Stop.Next(position + length)
            }
            val description = type.contentEquals(FILE_DESC) && kept.size() + length <= MAX_DESCRIPTIONS
            if ((description || type.contentEquals(MAIN)) && length <= MAX_DESCRIPTION) {
                val bytes = ByteArray(body.toInt())
                if (channel.readUpTo(bytes) < bytes.size) return Stop.End(position)
                if (description) {
                    kept.write(header)
                    kept.write(bytes)
                    if (bytes.size >= FILE_ID) described += bytes.copyOf(FILE_ID).toHex()
                } else {
                    listed = (MAIN_FIXED..bytes.size - FILE_ID step FILE_ID)
                        .map { bytes.copyOfRange(it, it + FILE_ID).toHex() }.toSet()
                }
            } else if (channel.discard(body).also { bytesRead += it } < body) {
                // Nothing of a packet skipped is needed: the file goes on past it.
                return Stop.End(position + length)
            }
            position += length
        }
    }

    companion object {
        private const val HEADER = 64
        private val MAGIC = "PAR2\u0000PKT".toByteArray(Charsets.ISO_8859_1)
        private val FILE_DESC = "PAR 2.0\u0000FileDesc".toByteArray(Charsets.ISO_8859_1)
        private val MAIN = "PAR 2.0\u0000Main\u0000\u0000\u0000\u0000".toByteArray(Charsets.ISO_8859_1)
        private val RECOVERY_SLICE = "PAR 2.0\u0000RecvSlic".toByteArray(Charsets.ISO_8859_1)

        // A FileDesc packet holds a file's name and hashes, a Main packet its files' ids: none is near
        // this big (a Main packet this big lists some 4000 files), nor are a release's descriptions
        // together near the second.
        private const val MAX_DESCRIPTION = 64 * 1024
        private const val MAX_DESCRIPTIONS = 4 * 1024 * 1024

        // A file's id, the first field of its FileDesc packet; the Main packet's list of them follows
        // the slice size and the count of the recovery set's files.
        private const val FILE_ID = 16
        private const val MAIN_FIXED = 12

        /** The FileDesc packets [channel] delivers, read as [read] reads them, or null when it delivers none. */
        suspend fun fileDescriptions(channel: ByteReadChannel): ByteArray? = Par2Packets().apply { read(channel, 0) }.descriptions

        /** Whether [bytes], a PAR2 file's first, start with a recovery slice: a recovery volume's, whatever it is named. */
        fun startsWithRecoverySlice(bytes: ByteArray): Boolean =
            bytes.size >= HEADER && bytes.copyOf(MAGIC.size).contentEquals(MAGIC) &&
                bytes.copyOfRange(48, HEADER).contentEquals(RECOVERY_SLICE)

        private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
    }

    /** Reads into [buffer] until it is full or the channel ends; how many bytes it got. */
    private suspend fun ByteReadChannel.readUpTo(buffer: ByteArray): Int {
        var read = 0
        while (read < buffer.size) {
            val count = readAvailable(buffer, read, buffer.size - read)
            if (count < 0) break
            read += count
        }
        bytesRead += read
        return read
    }
}
