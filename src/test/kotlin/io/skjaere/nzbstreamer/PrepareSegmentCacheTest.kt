package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.config.SegmentCacheConfig
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.stream.SegmentCache
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * prepare reads a file's name, size and first 16 KB from its first article, which it fetched past
 * the segment cache: a release prepared again (a replay, a resume) fetched every file's first
 * article again, and streaming fetched each once more.
 */
class PrepareSegmentCacheTest {
    private val articles = mutableMapOf<String, ByteArray>()
    private val files = mutableListOf<String>()
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    /** Posts [data] as [name], in articles of 16 KB (with yEnc part headers). */
    private fun post(name: String, data: ByteArray) {
        val parts = YencEncoder.encodeMultiPart(data, name, 16 * 1024)
        val ids = parts.indices.map { "$name-${it + 1}@test" }
        ids.zip(parts) { id, part -> articles["<$id>"] = part.data }
        val segments = ids.withIndex().joinToString("") { (i, id) ->
            """<segment bytes="${parts[i].data.size}" number="${i + 1}">$id</segment>"""
        }
        files += """<file poster="t" date="0" subject="&quot;$name&quot; yEnc (1/${parts.size})">""" +
            """<groups><group>alt.binaries.test</group></groups><segments>$segments</segments></file>"""
    }

    private fun nzb() = ("""<?xml version="1.0" encoding="UTF-8"?>""" +
        """<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""").toByteArray()

    private suspend fun NzbStreamer.streamed(prepared: PrepareResult.Success): ByteArray {
        var streamed = ByteArray(0)
        streamFile(prepared.metadata, resolveStreamableFiles(prepared.metadata).single()) { streamed = it.toByteArray() }
        return streamed
    }

    @Test
    fun `a release prepared and played, then prepared again, fetches no article twice`(@TempDir tmp: Path) {
        val payload = Random(7).nextBytes(200_000)
        ArchiveGenerator.generate(payload, 48L * 1024, ContainerType.RAR5, "Movie.2020.mkv")
            .forEachIndexed { v, volume -> post("Movie.2020.part${v + 1}.rar", volume.data) }
        val server = FakeNntpServer(body = { id -> articles[id] }).also { servers += it }
        val config = NntpConfig("127.0.0.1", server.port, "", "", useTls = false, maxConnections = 4)

        SegmentCache(SegmentCacheConfig(tmp, maxBytes = 100L * 1024 * 1024)).use { cache ->
            NzbStreamer.fromConfig(config, segmentCache = cache, closeSegmentCache = false).use { streamer ->
                runBlocking {
                    val first = assertIs<PrepareResult.Success>(streamer.prepare(nzb()))
                    assertContentEquals(payload, streamer.streamed(first))
                    val fetched = server.bodiesRead.toList()
                    assertEquals(fetched.distinct(), fetched, "articles fetched twice to prepare and play")

                    val again = assertIs<PrepareResult.Success>(streamer.prepare(nzb()))
                    assertEquals(streamer.resolveStreamableFiles(first.metadata), streamer.resolveStreamableFiles(again.metadata))
                    assertContentEquals(payload, streamer.streamed(again))
                    assertEquals(emptyList(), server.bodiesRead.drop(fetched.size), "articles fetched again")
                }
            }
        }
    }
}
