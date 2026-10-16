package io.skjaere.nzbstreamer.enrichment

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.discard
import io.ktor.utils.io.readAvailable
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Reads the FileDesc packets of a PAR2 file as it streams, without holding the rest: a recovery
 * volume's recovery blocks can be tens of MB each (library issue N53).
 */
internal object Par2Packets {
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

    /**
     * The FileDesc packets [channel] delivers, one after another as a PAR2 file holds them (which
     * the PAR2 parser reads as one), or null when it delivers none. It is read up to its end, the
     * first bytes that aren't a packet, or a recovery block once the descriptions it delivered
     * cover every file its Main packet lists: a recovery volume names its files before or after its
     * blocks, or spreads them between its blocks as par2cmdline does (NR-4, NR-6). Other packets
     * are skipped, but for the Main packet's list of file ids.
     */
    suspend fun fileDescriptions(channel: ByteReadChannel): ByteArray? {
        val kept = ByteArrayOutputStream()
        val described = mutableSetOf<String>()
        var listed: Set<String>? = null
        val header = ByteArray(HEADER)
        while (channel.readUpTo(header) == HEADER && header.copyOf(MAGIC.size).contentEquals(MAGIC)) {
            val length = ByteBuffer.wrap(header, 8, 8).order(ByteOrder.LITTLE_ENDIAN).long
            if (length < HEADER) break
            val type = header.copyOfRange(48, HEADER)
            val body = length - HEADER
            if (type.contentEquals(RECOVERY_SLICE) && kept.size() > 0 && listed?.let(described::containsAll) == true) break
            val description = type.contentEquals(FILE_DESC) && kept.size() + length <= MAX_DESCRIPTIONS
            if ((description || type.contentEquals(MAIN)) && length <= MAX_DESCRIPTION) {
                val bytes = ByteArray(body.toInt())
                if (channel.readUpTo(bytes) < bytes.size) break
                if (description) {
                    kept.write(header)
                    kept.write(bytes)
                    if (bytes.size >= FILE_ID) described += bytes.copyOf(FILE_ID).toHex()
                } else {
                    listed = (MAIN_FIXED..bytes.size - FILE_ID step FILE_ID)
                        .map { bytes.copyOfRange(it, it + FILE_ID).toHex() }.toSet()
                }
            } else if (channel.discard(body) < body) {
                break
            }
        }
        return kept.toByteArray().takeIf { it.isNotEmpty() }
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

    /** Reads into [buffer] until it is full or the channel ends; how many bytes it got. */
    private suspend fun ByteReadChannel.readUpTo(buffer: ByteArray): Int {
        var read = 0
        while (read < buffer.size) {
            val count = readAvailable(buffer, read, buffer.size - read)
            if (count < 0) break
            read += count
        }
        return read
    }
}
