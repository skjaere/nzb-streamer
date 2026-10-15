package io.skjaere.nzbstreamer

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest

/**
 * Builds a minimal PAR2 index file containing one FileDesc packet.
 */
internal fun buildPar2IndexFile(filename: String, fileSize: Long): ByteArray {
    val filenameBytes = filename.toByteArray(Charsets.UTF_8)
    val paddedLen = (filenameBytes.size + 3) and 3.inv()
    val bodySize = 56 + paddedLen
    val packetLength = 64L + bodySize

    val buf = ByteBuffer.allocate(packetLength.toInt()).order(ByteOrder.LITTLE_ENDIAN)
    // Header (64 bytes)
    buf.put("PAR2".toByteArray()); buf.put(0); buf.put("PKT".toByteArray())
    buf.putLong(packetLength)
    buf.put(ByteArray(16)) // packet hash
    buf.put(ByteArray(16)) // recovery set ID
    buf.put("PAR 2.0".toByteArray()); buf.put(0); buf.put("FileDesc".toByteArray())
    // Body
    buf.put(ByteArray(16)) // file ID
    buf.put(ByteArray(16)) // file hash
    buf.put(ByteArray(16)) // hash16k
    buf.putLong(fileSize)
    buf.put(filenameBytes)
    if (paddedLen > filenameBytes.size) buf.put(ByteArray(paddedLen - filenameBytes.size))

    return buf.array()
}

/**
 * A PAR2 file naming [files] (name to bytes) in FileDesc packets, each with its size and the MD5 of
 * its first 16 KB, as a poster's PAR2 index does. When [recoveryBlock] is set, a recovery slice
 * packet of that many bytes comes first, as in a recovery volume of that block size.
 */
internal fun buildPar2File(files: List<Pair<String, ByteArray>>, recoveryBlock: Int? = null): ByteArray {
    val recovery = recoveryBlock?.let { par2Packet("RecvSlic", ByteArray(4 + it)) } ?: ByteArray(0)
    return files.fold(recovery) { all, (name, data) ->
        val padded = name.toByteArray(Charsets.UTF_8).let { it.copyOf((it.size + 3) and 3.inv()) }
        val hash16k = MessageDigest.getInstance("MD5").digest(data.copyOf(minOf(data.size, 16 * 1024)))
        val body = ByteBuffer.allocate(56 + padded.size).order(ByteOrder.LITTLE_ENDIAN)
            .put(ByteArray(16)).put(ByteArray(16)).put(hash16k).putLong(data.size.toLong()).put(padded)
        all + par2Packet("FileDesc", body.array())
    }
}

/** A PAR2 packet of [type] ("FileDesc", "RecvSlic") around [body], with zeros for its hash and set id. */
private fun par2Packet(type: String, body: ByteArray): ByteArray =
    ByteBuffer.allocate(64 + body.size).order(ByteOrder.LITTLE_ENDIAN)
        .put("PAR2".toByteArray()).put(0).put("PKT".toByteArray())
        .putLong(64L + body.size)
        .put(ByteArray(16)) // packet hash
        .put(ByteArray(16)) // recovery set ID
        .put("PAR 2.0".toByteArray()).put(0).put(type.padEnd(8, '\u0000').toByteArray())
        .put(body)
        .array()
