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
            episodes.forEach { (name, data) ->
                val resolved = assertIs<FileResolveResult.Streamable>(streamer.resolveFile(prepared.metadata, name), name)
                var streamed = ByteArray(0)
                streamer.streamFile(prepared.metadata, resolved.namedSplits) { streamed = it.toByteArray() }
                assertContentEquals(data, streamed, name)
            }
        }
    }
}
