package io.skjaere.nzbstreamer

import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nzbstreamer.seekable.NestedSeekableInputStream
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * [NestedSeekableInputStream] over inner volumes whose data lies in one or more chunks of a
 * synthetic outer stream, with gaps between the chunks.
 */
class ChunkedNestedSeekableInputStreamTest {

    @Test
    fun `read across chunk boundary skips gaps`() = runBlocking {
        // Outer: [ABCD____EFGH] where ____ is a 4-byte gap
        // Volume 0: chunk at 0 size 4, chunk at 8 size 4 → inner size = 8
        val outerData = ByteArray(12)
        outerData[0] = 0x41; outerData[1] = 0x42; outerData[2] = 0x43; outerData[3] = 0x44
        outerData[8] = 0x45; outerData[9] = 0x46; outerData[10] = 0x47; outerData[11] = 0x48

        val stream = NestedSeekableInputStream(
            outerStream = ByteArraySeekableInputStream(outerData),
            innerVolumeSizes = listOf(8),
            outerChunks = listOf(listOf(SplitInfo(0, 0, 4), SplitInfo(0, 8, 4)))
        )

        val buf = ByteArray(8)
        val n1 = stream.read(buf, 0, 8)
        assertEquals(4, n1, "First read should clamp at chunk boundary")
        assertEquals(0x41, buf[0].toInt() and 0xFF)
        assertEquals(0x44, buf[3].toInt() and 0xFF)

        val n2 = stream.read(buf, 0, 8)
        assertEquals(4, n2, "Second read should return second chunk")
        assertEquals(0x45, buf[0].toInt() and 0xFF)
        assertEquals(0x48, buf[3].toInt() and 0xFF)

        assertEquals(-1, stream.read(buf, 0, 1))
    }

    @Test
    fun `clamp at chunk boundary returns partial read`() = runBlocking {
        val outerData = ByteArray(20) { it.toByte() }
        val stream = NestedSeekableInputStream(
            outerStream = ByteArraySeekableInputStream(outerData),
            innerVolumeSizes = listOf(10),
            outerChunks = listOf(listOf(SplitInfo(0, 0, 5), SplitInfo(0, 10, 5)))
        )

        val buf = ByteArray(10)
        val n = stream.read(buf, 0, 10)
        assertEquals(5, n)
        for (i in 0 until 5) assertEquals(i.toByte(), buf[i])
    }

    @Test
    fun `seek into second chunk works across gap`() = runBlocking {
        val outerData = ByteArray(100) { it.toByte() }
        val stream = NestedSeekableInputStream(
            outerStream = ByteArraySeekableInputStream(outerData),
            innerVolumeSizes = listOf(10),
            outerChunks = listOf(listOf(SplitInfo(0, 10, 5), SplitInfo(0, 50, 5)))
        )

        stream.seek(7) // 2 bytes into second chunk → outer pos 52
        val buf = ByteArray(3)
        val n = stream.read(buf, 0, 3)
        assertEquals(3, n)
        assertEquals(52.toByte(), buf[0])
        assertEquals(53.toByte(), buf[1])
        assertEquals(54.toByte(), buf[2])
    }

    @Test
    fun `single-chunk degenerate case works like old API`() = runBlocking {
        val data = ByteArray(256) { it.toByte() }
        val outerData = ByteArray(400)
        System.arraycopy(data, 0, outerData, 100, 256)

        val stream = NestedSeekableInputStream(
            outerStream = ByteArraySeekableInputStream(outerData),
            innerVolumeSizes = listOf(256),
            outerChunks = listOf(listOf(SplitInfo(0, 100, 256)))
        )

        val buf = ByteArray(256)
        var totalRead = 0
        while (totalRead < 256) {
            val n = stream.read(buf, totalRead, 256 - totalRead)
            if (n <= 0) break
            totalRead += n
        }
        assertEquals(256, totalRead)
        for (i in 0 until 256) assertEquals(i.toByte(), buf[i])
    }

    @Test
    fun `multi-volume with mixed chunk counts`() = runBlocking {
        // Volume 0: single chunk at pos 0, size 10
        // Volume 1: two chunks at pos 20 size 5 and pos 30 size 5
        val outerData = ByteArray(40)
        for (i in 0 until 10) outerData[i] = (i + 1).toByte()
        for (i in 0 until 5) outerData[20 + i] = (11 + i).toByte()
        for (i in 0 until 5) outerData[30 + i] = (16 + i).toByte()

        val stream = NestedSeekableInputStream(
            outerStream = ByteArraySeekableInputStream(outerData),
            innerVolumeSizes = listOf(10, 10),
            outerChunks = listOf(
                listOf(SplitInfo(0, 0, 10)),
                listOf(SplitInfo(0, 20, 5), SplitInfo(0, 30, 5))
            )
        )

        val result = ByteArray(20)
        var pos = 0
        while (pos < 20) {
            val n = stream.read(result, pos, 20 - pos)
            if (n <= 0) break
            pos += n
        }
        assertEquals(20, pos)
        for (i in 0 until 20) assertEquals((i + 1).toByte(), result[i])
    }
}
