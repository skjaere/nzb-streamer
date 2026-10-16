package io.skjaere.nzbstreamer

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.util.zip.CRC32

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
 * A PAR2 file naming [files] (name to bytes) as par2cmdline writes one. Its critical packets: a Main
 * packet giving the slice size and the files' ids, then for each file, by file id, a FileDesc packet
 * (its id, its MD5, the MD5 of its first 16 KB and its length, its name), then for each an IFSC
 * packet (each slice's MD5 and CRC32); then a Creator packet. Each packet carries the MD5 of the Main
 * packet's body as its recovery set id and its own MD5 over that id onward. A file id is the MD5 of
 * the file's first 16 KB's MD5, its length and its name.
 *
 * With [recoveryBlock] set, it is a recovery volume of [slices] recovery slices of that size: its
 * slices come first and its critical packets after them (or before, with [descriptionsFirst]).
 * With several slices, par2cmdline spreads the critical packets evenly between them: after slice
 * b of n, those from P*b/n up to P*(b+1)/n of the P in the order above.
 */
internal fun buildPar2File(
    files: List<Pair<String, ByteArray>>,
    recoveryBlock: Int? = null,
    descriptionsFirst: Boolean = false,
    slices: Int = 1,
): ByteArray {
    val sliceSize = recoveryBlock ?: (16 * 1024)
    val described = files.map { (name, data) ->
        val hash16k = md5(data.copyOf(minOf(data.size, 16 * 1024)))
        val length = le64(data.size.toLong())
        val nameBytes = name.toByteArray(Charsets.UTF_8)
        Triple(md5(hash16k + length + nameBytes), name, data)
    }.sortedWith { a, b -> compareIds(a.first, b.first) }
    val main = le64(sliceSize.toLong()) + le32(described.size) + described.fold(ByteArray(0)) { all, (id) -> all + id }
    val setId = md5(main)
    val descriptions = described.map { (id, name, data) ->
        val padded = name.toByteArray(Charsets.UTF_8).let { it.copyOf((it.size + 3) and 3.inv()) }
        val hash16k = md5(data.copyOf(minOf(data.size, 16 * 1024)))
        par2Packet(setId, "FileDesc", id + md5(data) + hash16k + le64(data.size.toLong()) + padded)
    }
    val checksums = described.map { (id, _, data) ->
        val sums = (0 until maxOf(1, (data.size + sliceSize - 1) / sliceSize)).fold(ByteArray(0)) { all, s ->
            all + sliceChecksums(data, s * sliceSize, sliceSize)
        }
        par2Packet(setId, "IFSC", id + sums)
    }
    val critical = listOf(par2Packet(setId, "Main", main)) + descriptions + checksums
    val creator = par2Packet(setId, "Creator", "Created by Par2Testing".toByteArray().let { it.copyOf((it.size + 3) and 3.inv()) })
    if (recoveryBlock == null) return ByteArrayOutputStream().apply { (critical + creator).forEach(::write) }.toByteArray()
    val recovery = (0 until slices).map { e -> par2Packet(setId, "RecvSlic", le32(e) + ByteArray(recoveryBlock)) }
    val packets = when {
        slices > 1 -> recovery.indices.flatMap { b ->
            listOf(recovery[b]) + critical.subList(critical.size * b / slices, critical.size * (b + 1) / slices)
        }
        descriptionsFirst -> critical + recovery
        else -> recovery + critical
    }
    return ByteArrayOutputStream().apply { (packets + creator).forEach(::write) }.toByteArray()
}

/** The MD5 and CRC32 of the slice of [data] from [from], [size] bytes long, padded with zeros past its end. */
private fun sliceChecksums(data: ByteArray, from: Int, size: Int): ByteArray {
    val md5 = MessageDigest.getInstance("MD5")
    val crc = CRC32()
    val held = minOf(size, data.size - from)
    md5.update(data, from, held)
    crc.update(data, from, held)
    var left = size - held
    val zeros = ByteArray(minOf(left, 64 * 1024))
    while (left > 0) {
        val count = minOf(left, zeros.size)
        md5.update(zeros, 0, count)
        crc.update(zeros, 0, count)
        left -= count
    }
    return md5.digest() + le32(crc.value.toInt())
}

/** A PAR2 packet of [type] ("Main", "FileDesc", "RecvSlic", ...) around [body], with its MD5 over [setId] onward. */
private fun par2Packet(setId: ByteArray, type: String, body: ByteArray): ByteArray {
    val typed = setId + "PAR 2.0\u0000${type.padEnd(8, '\u0000')}".toByteArray(Charsets.ISO_8859_1) + body
    return "PAR2\u0000PKT".toByteArray(Charsets.ISO_8859_1) + le64(32L + typed.size) + md5(typed) + typed
}

/** File ids in the order PAR2 sorts them: as unsigned bytes, the first that differs. */
private fun compareIds(a: ByteArray, b: ByteArray): Int =
    a.indices.firstOrNull { a[it] != b[it] }?.let { (a[it].toInt() and 0xFF) - (b[it].toInt() and 0xFF) } ?: 0

private fun md5(data: ByteArray): ByteArray = MessageDigest.getInstance("MD5").digest(data)

private fun le64(value: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array()

private fun le32(value: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array()
