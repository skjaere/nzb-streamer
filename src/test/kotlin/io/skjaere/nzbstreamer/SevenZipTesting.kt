package io.skjaere.nzbstreamer

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * A stored 7z archive of [files] (name to bytes), laid out as 7-Zip's `-mx0` writes one: each file
 * in a Copy folder of its own, their bytes one after another behind the 32-byte signature header,
 * then an uncompressed header naming them. kotlin-compression-utils' ArchiveGenerator writes a
 * single file only.
 */
internal fun storedSevenZip(files: List<Pair<String, ByteArray>>): ByteArray {
    val header = ByteArrayOutputStream().apply {
        write(K_HEADER)
        write(K_MAIN_STREAMS_INFO)
        write(K_PACK_INFO)
        writeNumber(0) // pack position, from the end of the signature header
        writeNumber(files.size.toLong())
        write(K_SIZE)
        files.forEach { (_, data) -> writeNumber(data.size.toLong()) }
        write(K_END)
        write(K_UNPACK_INFO)
        write(K_FOLDER)
        writeNumber(files.size.toLong())
        write(0) // not external
        repeat(files.size) {
            write(1) // one coder
            write(1) // a one-byte codec id, simple, no properties
            write(0) // Copy
        }
        write(K_CODERS_UNPACK_SIZE)
        files.forEach { (_, data) -> writeNumber(data.size.toLong()) }
        write(K_END)
        write(K_SUB_STREAMS_INFO)
        write(K_CRC)
        write(1) // all defined
        files.forEach { (_, data) -> write(le32(crc32(data))) }
        write(K_END)
        write(K_END)
        write(K_FILES_INFO)
        writeNumber(files.size.toLong())
        write(K_NAME)
        val names = files.flatMap { (name, _) -> (name + '\u0000').toByteArray(Charsets.UTF_16LE).toList() }.toByteArray()
        writeNumber(names.size + 1L)
        write(0) // not external
        write(names)
        write(K_END)
        write(K_END)
    }.toByteArray()
    val data = files.fold(ByteArray(0)) { all, (_, bytes) -> all + bytes }
    val startHeader = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN)
        .putLong(data.size.toLong()).putLong(header.size.toLong()).putInt(crc32(header)).array()
    return MAGIC + byteArrayOf(0, 4) + le32(crc32(startHeader)) + startHeader + data + header
}

/** [archive] cut into volumes of [volumeSize] named [name].001, .002, ..., as `7z -v` writes them. */
internal fun splitVolumes(archive: ByteArray, volumeSize: Int, name: String): List<Pair<String, ByteArray>> =
    archive.indices.step(volumeSize).mapIndexed { i, start ->
        "%s.%03d".format(name, i + 1) to archive.copyOfRange(start, minOf(start + volumeSize, archive.size))
    }

private val MAGIC = byteArrayOf(0x37, 0x7A, 0xBC.toByte(), 0xAF.toByte(), 0x27, 0x1C)
private const val K_END = 0x00
private const val K_HEADER = 0x01
private const val K_MAIN_STREAMS_INFO = 0x04
private const val K_FILES_INFO = 0x05
private const val K_PACK_INFO = 0x06
private const val K_UNPACK_INFO = 0x07
private const val K_SUB_STREAMS_INFO = 0x08
private const val K_SIZE = 0x09
private const val K_CRC = 0x0A
private const val K_FOLDER = 0x0B
private const val K_CODERS_UNPACK_SIZE = 0x0C
private const val K_NAME = 0x11

private fun crc32(data: ByteArray): Int = CRC32().apply { update(data) }.value.toInt()

private fun le32(value: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array()

/**
 * Writes a 7z number: the first byte's leading 1 bits count the bytes that follow (the value's low
 * bytes, low first), and its other bits hold the value above them.
 */
private fun ByteArrayOutputStream.writeNumber(value: Long) {
    val extra = (0..8).first { n -> n == 8 || value ushr (8 * n) < (1L shl (7 - n)) }
    val high = if (extra == 8) 0 else (value ushr (8 * extra)).toInt()
    write(((0xFF00 ushr extra) and 0xFF) or high)
    write(ByteArray(extra) { i -> (value ushr (8 * i)).toByte() })
}
