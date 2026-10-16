package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.stream.FileResolveResult
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * A release of files posted as they are (no archive) lists them by the best name it has for each:
 * the PAR2 index's first, which survives a poster's obfuscation (library issue N54).
 */
class RawReleaseNamesTest {
    private val articles = mutableMapOf<String, ByteArray>()
    private val files = mutableListOf<String>()
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    /** Posts [data] as [name], in articles of 32 KiB. */
    private fun post(name: String, data: ByteArray) {
        val parts = YencEncoder.encodeMultiPart(data, name, 32 * 1024)
        val segments = parts.indices.joinToString("") { i ->
            articles["<$name-${i + 1}@test>"] = parts[i].data
            """<segment bytes="${parts[i].data.size}" number="${i + 1}">$name-${i + 1}@test</segment>"""
        }
        files += """<file poster="t" date="0" subject="&quot;$name&quot; yEnc (1/${parts.size})">""" +
            """<groups><group>alt.binaries.test</group></groups><segments>$segments</segments></file>"""
    }

    private fun nzb() = ("""<?xml version="1.0" encoding="UTF-8"?>""" +
        """<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""").toByteArray()

    private fun <T> withPrepared(block: suspend (NzbStreamer, PrepareResult.Success) -> T): T {
        val server = FakeNntpServer(body = { id -> articles[id] }).also { servers += it }
        return NzbStreamer.fromConfig(NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)).use {
            runBlocking { block(it, assertIs<PrepareResult.Success>(it.prepare(nzb()), "prepare")) }
        }
    }

    @Test
    fun `files posted under obfuscated names are listed and found by the names the PAR2 index gives them`() {
        val episodes = listOf("Show.S01E01.mkv", "Show.S01E02.mkv").associateWith { Random(it.hashCode()).nextBytes(50_000) }
        episodes.values.zip(listOf("a8f7d6e5.mkv", "b91c2d3e.mkv")).forEach { (data, posted) -> post(posted, data) }
        post("c07e4f1a.par2", buildPar2File(episodes.toList()))

        withPrepared { streamer, prepared ->
            assertIs<ExtractedMetadata.Raw>(prepared.metadata)
            assertEquals(episodes.keys.toList(), streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            episodes.forEach { (name, data) -> assertStreams(streamer, prepared, name, data) }
        }
    }

    /**
     * Disc images start alike (their first 32 KB are zeros), so the PAR2 index's descriptions of
     * both give the same first 16 KB: their lengths tell them apart, and where they don't either,
     * no description names either (NR-8).
     */
    @ParameterizedTest
    @CsvSource("300000, 350000, false", "300000, 300000, false", "300000, 350000, true")
    fun `disc images that start alike keep their own names`(first: Int, second: Int, obfuscated: Boolean) {
        val discs = listOf("Show.S01.Disc1.iso" to first, "Show.S01.Disc2.iso" to second)
            .associate { (name, size) -> name to ByteArray(32 * 1024) + Random(name.hashCode()).nextBytes(size - 32 * 1024) }
        val postedAs = if (obfuscated) listOf("a8f7d6e5.iso", "b91c2d3e.iso") else discs.keys.toList()
        discs.values.zip(postedAs).forEach { (data, posted) -> post(posted, data) }
        post("Show.S01.par2", buildPar2File(discs.toList()))

        withPrepared { streamer, prepared ->
            val expected = if (first == second) postedAs else discs.keys.toList()
            assertEquals(expected, streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            expected.zip(discs.values).forEach { (name, data) -> assertStreams(streamer, prepared, name, data) }
        }
    }

    /** A DVD's VIDEO_TS.BUP is a copy of its VIDEO_TS.IFO (and VTS_01_0.BUP of VTS_01_0.IFO): each keeps its name (NR-12). */
    @Test
    fun `a DVD folder posted as it is lists each file under its own name`() {
        val ifo = Random(1).nextBytes(12_000)
        val titleIfo = Random(2).nextBytes(20_000)
        val dvd = listOf(
            "VIDEO_TS.IFO" to ifo, "VIDEO_TS.BUP" to ifo.copyOf(),
            "VTS_01_0.IFO" to titleIfo, "VTS_01_0.BUP" to titleIfo.copyOf(),
            "VTS_01_1.VOB" to Random(3).nextBytes(100_000),
        )
        dvd.forEach { (name, data) -> post(name, data) }
        post("VIDEO_TS.par2", buildPar2File(dvd))

        withPrepared { streamer, prepared ->
            assertEquals(dvd.map { it.first }, streamer.resolveStreamableFiles(prepared.metadata).map { it.path })
            dvd.forEach { (name, data) -> assertStreams(streamer, prepared, name, data) }
        }
    }

    /** [name] resolves to a file that streams [data]. */
    private suspend fun assertStreams(streamer: NzbStreamer, prepared: PrepareResult.Success, name: String, data: ByteArray) {
        val resolved = assertIs<FileResolveResult.Streamable>(streamer.resolveFile(prepared.metadata, name), name)
        var streamed = ByteArray(0)
        streamer.streamFile(prepared.metadata, resolved.namedSplits) { streamed = it.toByteArray() }
        assertContentEquals(data, streamed, name)
    }
}
