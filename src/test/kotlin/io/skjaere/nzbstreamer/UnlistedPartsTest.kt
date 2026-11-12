package io.skjaere.nzbstreamer

import io.ktor.utils.io.readAvailable
import io.skjaere.compressionutils.generation.ArchiveGenerator
import io.skjaere.compressionutils.generation.ContainerType
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.EnrichmentResult
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.metadata.ExtractedMetadata
import io.skjaere.nzbstreamer.metadata.PrepareResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbParser
import io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import io.skjaere.nzbstreamer.stream.StreamableFile
import io.skjaere.yenc.YencEncoder
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A file whose NZB lists fewer articles than the parts it was posted in (an indexer that missed
 * some): reading reaches the parts it doesn't list and fails there with a missing article, rather
 * than ending early or putting a later part's bytes in their place, and the availability check
 * finds them missing.
 */
class UnlistedPartsTest {
    private val partSize = 16 * 1024
    private val articles = mutableMapOf<String, ByteArray>()
    private val files = mutableListOf<String>()
    private val servers = mutableListOf<FakeNntpServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
    }

    /**
     * Posts [data] as [name] in parts of [partSize]; the NZB lists all but the parts in [unlisted]
     * (from 1), under a subject that gives their count unless [counted] is false.
     */
    private fun post(name: String, data: ByteArray, unlisted: Set<Int> = emptySet(), counted: Boolean = true) {
        val parts = YencEncoder.encodeMultiPart(data, name, partSize)
        parts.forEachIndexed { i, part -> articles["<$name-${i + 1}@test>"] = part.data }
        val segments = parts.indices.filterNot { it + 1 in unlisted }.joinToString("") { i ->
            """<segment bytes="${parts[i].data.size}" number="${i + 1}">$name-${i + 1}@test</segment>"""
        }
        val count = if (counted) " (1/${parts.size})" else ""
        files += """<file poster="t" date="0" subject="&quot;$name&quot; yEnc$count">""" +
            """<groups><group>alt.binaries.test</group></groups><segments>$segments</segments></file>"""
    }

    private fun nzb() = ("""<?xml version="1.0" encoding="UTF-8"?>""" +
        """<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files.joinToString("")}</nzb>""").toByteArray()

    private fun server() = FakeNntpServer(stat = { id -> id in articles }, body = { id -> articles[id] }).also { servers += it }

    private fun <T> withStreamer(block: suspend (NzbStreamer) -> T): T =
        NzbStreamer.fromConfig(NntpConfig("127.0.0.1", server().port, "", "", useTls = false, maxConnections = 4))
            .use { runBlocking { block(it) } }

    /** 5 whole parts and a sixth of 5000 bytes. */
    private val movie = Random(1).nextBytes(5 * partSize + 5000)

    /** What [file] streams, and the failure it ends with (null at a plain end). */
    private suspend fun NzbStreamer.readAll(metadata: ExtractedMetadata, file: StreamableFile): Pair<ByteArray, Throwable?> {
        val out = ByteArrayOutputStream()
        val failure = try {
            streamFile(metadata, file) { channel ->
                val buffer = ByteArray(8192)
                while (true) {
                    val n = channel.readAvailable(buffer)
                    if (n == -1) break
                    out.write(buffer, 0, n)
                }
            }
            null
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            e
        }
        return out.toByteArray() to failure
    }

    /** What the availability check, of every article and of a sample, says of [nzb]. */
    private suspend fun NzbStreamer.checks(nzb: NzbDocument) =
        listOf("verifySegments" to verifySegments(nzb), "verifySample" to verifySample(nzb))

    private fun Throwable?.missingArticle() = generateSequence(this) { it.cause }.filterIsInstance<ArticleNotFoundException>().firstOrNull()

    /** Streams the release's only file and checks it fails at the parts its NZB doesn't list, saying which. */
    private fun assertFailsAtUnlistedParts(parts: String) = withStreamer { streamer ->
        val prepared = assertIs<PrepareResult.Success>(streamer.prepare(nzb()), "prepare")
        val (bytes, failure) = streamer.readAll(prepared.metadata, streamer.resolveStreamableFiles(prepared.metadata).single())
        val missing = assertNotNull(failure.missingArticle(), "the stream ended after ${bytes.size} of ${movie.size} bytes with $failure")
        assertTrue(parts in missing.message.orEmpty(), "message: ${missing.message}")
        assertContentEquals(movie.copyOf(bytes.size), bytes, "streamed bytes that aren't the file's")
    }

    @Test
    fun `a file whose NZB lacks its last parts fails at them instead of ending early`() {
        post("Movie.2020.mkv", movie, unlisted = setOf(5, 6))

        assertFailsAtUnlistedParts("parts 5-6 of 6")
    }

    @Test
    fun `a file whose NZB lacks a part in the middle fails there instead of streaming the next part in its place`() {
        post("Movie.2020.mkv", movie, unlisted = setOf(3))

        assertFailsAtUnlistedParts("part 3 of 6")
    }

    @Test
    fun `a seek into parts the NZB doesn't list fails instead of ending the stream`() = runBlocking<Unit> {
        post("Movie.2020.mkv", movie, unlisted = setOf(5, 6))
        val nzb = NzbParser.parse(nzb())
        nzb.files.single().yencHeaders = YencHeaders(
            line = 128, size = movie.size.toLong(), name = "Movie.2020.mkv", part = 1, total = 6, partBegin = 1, partEnd = partSize.toLong(),
        )
        val service = NntpStreamingService(NntpConfig("127.0.0.1", server().port, "", "", useTls = false, maxConnections = 4))
        service.use {
            it.connect()
            val stream = NntpSeekableInputStream(nzb, it)
            stream.seek(4L * partSize + 100)
            // The stream throws the download's failure itself.
            val result = try {
                stream.read(ByteArray(100), 0, 100)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                e
            } finally {
                stream.close()
            }
            assertIs<ArticleNotFoundException>(result, "read in the unlisted parts: $result")
        }
    }

    @Test
    fun `an archive whose headers lie in parts its NZB doesn't list is missing articles, not malformed`() = runBlocking<Unit> {
        // A 7z keeps its headers at the end of its last volume.
        val volumes = ArchiveGenerator.generate(Random(2).nextBytes(150_000), 64L * 1024, ContainerType.SEVENZIP, "Movie.2020.mkv")
        volumes.dropLast(1).forEach { post(it.filename, it.data) }
        val last = volumes.last()
        assertEquals(2, (last.data.size + partSize - 1) / partSize, "the last volume's parts")
        post(last.filename, last.data, unlisted = setOf(2))

        withStreamer { streamer ->
            val result = streamer.prepare(nzb())
            assertIs<PrepareResult.MissingArticles>(result, "prepare: $result")
        }
    }

    /** The availability check finds the parts an NZB doesn't list missing. */
    @Test
    fun `the check of an enriched NZB finds the parts it lacks by the yEnc sizes`() {
        post("Movie.2020.mkv", movie, unlisted = setOf(3), counted = false)

        withStreamer { streamer ->
            val enriched = assertIs<EnrichmentResult.Success>(streamer.enrich(nzb()), "enrich").enrichedNzb
            streamer.checks(enriched).forEach { (check, result) ->
                val missing = assertIs<VerificationResult.MissingArticles>(result, "$check: $result")
                assertTrue("part 3 of 6" in missing.message, "$check: ${missing.message}")
                assertEquals(1, missing.missingCount, check)
            }
        }
    }

    @Test
    fun `the check of an NZB not enriched finds the parts it lacks by its subject's part count`() {
        post("Movie.2020.mkv", movie, unlisted = setOf(5, 6))

        withStreamer { streamer ->
            streamer.checks(NzbParser.parse(nzb())).forEach { (check, result) ->
                val missing = assertIs<VerificationResult.MissingArticles>(result, "$check: $result")
                assertTrue("parts 5-6 of 6" in missing.message, "$check: ${missing.message}")
                assertEquals(2, missing.missingCount, check)
            }
        }
    }

    /**
     * NZB byte counts are only near the articles' sizes, and a file's first part needn't be the
     * size of the others: a whole file whose first part, by its yEnc size, implies more parts than
     * its NZB lists still passes, as long as the byte counts or the yEnc total tell otherwise.
     */
    @Test
    fun `the check passes whole files whose sizes the NZB and the first part give only roughly`() {
        post("Movie.2020.mkv", movie)

        withStreamer { streamer ->
            val parsed = NzbParser.parse(nzb())
            val enriched = assertIs<EnrichmentResult.Success>(streamer.enrich(nzb()), "enrich").enrichedNzb
            val file = enriched.files.single()
            val rounded = file.copy(segments = file.segments.map { it.copy(bytes = (it.bytes + 999) / 1000 * 1000) })
            // A first part of 12 KiB would make 8 of the file's size; its byte count says it is smaller than the rest.
            val smallFirst = file.copy(
                segments = file.segments.mapIndexed { i, segment -> if (i == 0) segment.copy(bytes = 12_600) else segment },
                yencHeaders = file.yencHeaders!!.copy(partEnd = 12L * 1024, total = null),
            )
            // Its yEnc total says 6.
            val totalled = smallFirst.copy(segments = file.segments, yencHeaders = smallFirst.yencHeaders!!.copy(total = 6))
            val releases = listOf("parsed" to parsed, "enriched" to enriched) +
                listOf("rounded" to rounded, "smallFirst" to smallFirst, "totalled" to totalled).map { (how, it) -> how to NzbDocument(listOf(it)) }
            releases.forEach { (how, nzb) ->
                streamer.checks(nzb).forEach { (check, result) -> assertIs<VerificationResult.Success>(result, "$how, $check: $result") }
            }
        }
    }
}
