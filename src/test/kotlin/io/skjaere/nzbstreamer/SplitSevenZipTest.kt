package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.stream.FileResolveResult
import io.skjaere.nzbstreamer.stream.toSplits
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * A stored 7z set of several files split into volumes (a 7z season pack): each entry's offset is
 * into the volumes laid end to end, and every entry streams its own bytes, those that start at or
 * past the end of the first volume too.
 */
class SplitSevenZipTest {
    private val volumeSize = 16 * 1024
    private val articles = mutableMapOf<String, ByteArray>()
    private val files = mutableListOf<String>()
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    // Behind the 32-byte signature header, E01 fills the first volume: E02 starts the second, runs
    // into the third, E03 starts inside the third, and E04 runs from there into the eighth.
    private val episodes = listOf(16_352, 20_000, 9_000, 70_000).mapIndexed { i, size ->
        "Show.S01E0${i + 1}.mkv" to Random(i).nextBytes(size)
    }.toMap()

    private val expectedSplits = mapOf(
        "Show.S01E01.mkv" to listOf(SplitInfo(0, 32, 16_352)),
        "Show.S01E02.mkv" to listOf(SplitInfo(1, 16_384, 16_384), SplitInfo(2, 32_768, 3_616)),
        "Show.S01E03.mkv" to listOf(SplitInfo(2, 36_384, 9_000)),
        "Show.S01E04.mkv" to listOf(
            SplitInfo(2, 45_384, 3_768), SplitInfo(3, 49_152, 16_384), SplitInfo(4, 65_536, 16_384),
            SplitInfo(5, 81_920, 16_384), SplitInfo(6, 98_304, 16_384), SplitInfo(7, 114_688, 696),
        ),
    )

    /** Posts each volume of the set in articles of 8 KiB. */
    private fun postSet() {
        splitVolumes(storedSevenZip(episodes.toList()), volumeSize, "Show.S01.7z").forEach { (name, data) ->
            val parts = YencEncoder.encodeMultiPart(data, name, 8 * 1024)
            parts.forEachIndexed { i, part -> articles["<$name-${i + 1}@test>"] = part.data }
            val segments = parts.indices.joinToString("") { i ->
                """<segment bytes="${parts[i].data.size}" number="${i + 1}">$name-${i + 1}@test</segment>"""
            }
            files += """<file poster="t" date="0" subject="&quot;$name&quot; yEnc (1/${parts.size})">""" +
                """<groups><group>alt.binaries.test</group></groups><segments>$segments</segments></file>"""
        }
    }

    private fun nzb() = ("""<?xml version="1.0" encoding="UTF-8"?>""" +
        """<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""").toByteArray()

    private fun <T> withPrepared(block: suspend (NzbStreamer, PrepareResult.Success) -> T): T {
        postSet()
        val server = FakeNntpServer(body = { id -> articles[id] }).also { servers += it }
        return NzbStreamer.fromConfig(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)).use {
            runBlocking { block(it, assertIs<PrepareResult.Success>(it.prepare(nzb()), "prepare")) }
        }
    }

    @Test
    fun `each entry's splits lie in the volumes its bytes are in`() = withPrepared<Unit> { streamer, prepared ->
        val listed = streamer.resolveStreamableFiles(prepared.metadata)
        assertEquals(expectedSplits.keys.toList(), listed.map { it.path })
        listed.forEach { file ->
            assertEquals(expectedSplits[file.path], file.toSplits(prepared.metadata.orderedArchiveNzb), file.path)
        }
    }

    @Test
    fun `every entry of a split stored 7z streams its own bytes`() = withPrepared<Unit> { streamer, prepared ->
        streamer.resolveStreamableFiles(prepared.metadata).forEach { file ->
            var streamed = ByteArray(0)
            streamer.streamFile(prepared.metadata, file) { streamed = it.toByteArray() }
            assertContentEquals(episodes[file.path], streamed, file.path)
        }
    }

    @Test
    fun `a range of an entry that starts past the first volume streams its bytes`() = withPrepared<Unit> { streamer, prepared ->
        val file = streamer.resolveStreamableFiles(prepared.metadata).single { it.path == "Show.S01E04.mkv" }
        var streamed = ByteArray(0)
        streamer.streamFile(prepared.metadata, file, 100L..60_099L) { streamed = it.toByteArray() }
        assertContentEquals(episodes.getValue("Show.S01E04.mkv").copyOfRange(100, 60_100), streamed)
    }

    @Test
    fun `an entry resolved by its path streams its bytes`() = withPrepared<Unit> { streamer, prepared ->
        val resolved = assertIs<FileResolveResult.Streamable>(streamer.resolveFile(prepared.metadata, "Show.S01E03.mkv"))
        var streamed = ByteArray(0)
        streamer.streamFile(prepared.metadata, resolved.namedSplits) { streamed = it.toByteArray() }
        assertContentEquals(episodes["Show.S01E03.mkv"], streamed)
    }
}
