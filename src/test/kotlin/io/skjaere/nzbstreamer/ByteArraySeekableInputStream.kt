package io.skjaere.nzbstreamer

import io.skjaere.compressionutils.SeekableInputStream

class ByteArraySeekableInputStream(private val data: ByteArray) : SeekableInputStream {
    private var pos = 0

    override suspend fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (pos >= data.size) return -1
        val toRead = minOf(length, data.size - pos)
        System.arraycopy(data, pos, buffer, offset, toRead)
        pos += toRead
        return toRead
    }

    override suspend fun read(): Int {
        if (pos >= data.size) return -1
        return data[pos++].toInt() and 0xFF
    }

    override suspend fun seek(position: Long) {
        pos = position.toInt()
    }

    override fun position(): Long = pos.toLong()

    override fun size(): Long = data.size.toLong()

    override fun close() {}
}
