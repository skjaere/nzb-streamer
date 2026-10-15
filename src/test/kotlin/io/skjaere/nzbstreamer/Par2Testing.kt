package io.skjaere.nzbstreamer

import java.nio.ByteBuffer
import java.nio.ByteOrder

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
