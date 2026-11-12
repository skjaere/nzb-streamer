package io.skjaere.nzbstreamer.stream

import io.ktor.utils.io.*
import io.skjaere.compressionutils.ArchiveFileEntry
import io.skjaere.compressionutils.Rar5Crypto
import io.skjaere.compressionutils.Rar5DataAreaDecryptor
import io.skjaere.compressionutils.RarFileEntry
import io.skjaere.compressionutils.SevenZipFileEntry
import io.skjaere.compressionutils.SplitInfo
import io.skjaere.compressionutils.TranslatedFileEntry
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.seekable.NntpSeekableInputStream
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

sealed interface FileResolveResult {
    data class Streamable(val namedSplits: NamedSplits) : FileResolveResult {
        val splits get() = namedSplits.splits
        val totalSize get() = namedSplits.totalSize
    }
    data object NotFound : FileResolveResult
    data object IsDirectory : FileResolveResult
    data class Compressed(val description: String) : FileResolveResult
}

class ArchiveStreamingService(
    private val streamingService: NntpStreamingService
) {
    private val logger = LoggerFactory.getLogger(ArchiveStreamingService::class.java)

    // Cache of derived AES keys. PBKDF2 at 2^15 iterations is meaningfully slow
    // (~50–200ms), and the same archive is typically streamed many times across
    // a media session — derive once per (password, salt, iters) tuple.
    private val keyCache = ConcurrentHashMap<KeyCacheKey, ByteArray>()

    private data class KeyCacheKey(val password: String, val saltHex: String, val iters: Int)

    private fun deriveKeyCached(password: String, salt: ByteArray, kdfIterationsLog2: Int): ByteArray {
        val saltHex = salt.joinToString("") { "%02x".format(it) }
        return keyCache.computeIfAbsent(KeyCacheKey(password, saltHex, kdfIterationsLog2)) {
            Rar5Crypto.deriveKey(password, salt, kdfIterationsLog2)
        }
    }

    fun resolveFile(
        entries: List<ArchiveFileEntry>,
        archiveNzb: NzbDocument,
        path: String
    ): FileResolveResult {
        val entry = entries.firstOrNull { it.path == path }
            ?: return FileResolveResult.NotFound

        if (entry.isDirectory) return FileResolveResult.IsDirectory

        return resolveExistingFileEntry(entry, archiveNzb)
    }

    private fun resolveExistingFileEntry(entry: ArchiveFileEntry, archiveNzb: NzbDocument): FileResolveResult {
        when (entry) {
            is RarFileEntry -> if (!entry.isUncompressed) {
                return FileResolveResult.Compressed("File is compressed (RAR method=${entry.compressionMethod})")
            }
            is SevenZipFileEntry -> if (entry.method != null && entry.method != "Copy") {
                return FileResolveResult.Compressed("File is compressed (7z method=${entry.method})")
            }
            is TranslatedFileEntry -> { /* always uncompressed — already validated during translation */ }
        }

        val splits = getSplitsForEntry(entry, archiveNzb)

        return FileResolveResult.Streamable(NamedSplits(splits, entry.size, entry.path))
    }

    private fun getSplitsForEntry(
        entry: ArchiveFileEntry,
        archiveNzb: NzbDocument
    ): List<SplitInfo> = when (entry) {
        is RarFileEntry -> {
            entry.splitParts.ifEmpty {
                val volumeOffsets = computeVolumeOffsets(archiveNzb)
                listOf(
                    SplitInfo(
                        volumeIndex = entry.volumeIndex,
                        dataStartPosition = volumeOffsets[entry.volumeIndex] + entry.dataPosition,
                        dataSize = entry.uncompressedSize
                    )
                )
            }
        }

        is SevenZipFileEntry -> listOf(SplitInfo(0, entry.dataOffset, entry.size))

        is TranslatedFileEntry -> entry.splitParts
    }

    /**
     * Streams the file (or [range] of it) to [consume]. Under
     * [io.skjaere.nzbstreamer.config.MissingArticlePolicy.ZERO_FILL], [onGap] gets each stretch that
     * zeros fill as offsets in the file (from its start, whatever [range] is), before they're read; in
     * an encrypted file, the plaintext they garble: the zeroed ciphertext rounded out to whole AES
     * blocks, and the block after (decrypted with it as its IV). See
     * [NntpStreamingService.streamSegments] for when it is called.
     */
    suspend fun streamFile(
        archiveNzb: NzbDocument,
        namedSplits: NamedSplits,
        range: LongRange? = null,
        onGap: ((Gap) -> Unit)? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val effectiveSplits = if (range != null) {
            adjustSplitsForRange(namedSplits.splits, range.first, range.last - range.first + 1)
        } else {
            namedSplits.splits
        }

        logger.debug(
            "Streaming {} splits (range={}, encrypted={})",
            effectiveSplits.size,
            range?.let { "${it.first}-${it.last}" } ?: "full",
            effectiveSplits.any { it.encryption != null },
        )

        if (effectiveSplits.any { it.encryption != null }) {
            return streamEncrypted(archiveNzb, effectiveSplits, namedSplits.name, range?.first ?: 0L, onGap, consume)
        }

        val combinedQueue = flow {
            for (split in effectiveSplits) {
                SegmentQueueService.createRangeQueue(
                    archiveNzb, split.dataStartPosition, split.dataSize
                ).collect { emit(it) }
            }
        }
        streamingService.streamSegments(
            combinedQueue, name = namedSplits.name, initialReadAheadSegments = firstSegmentsFor(range),
            onGap = onGap?.from(range?.first ?: 0L), consume = consume,
        )
    }

    /**
     * Starts streaming the file (or [range] of it) and returns at once with the [WriterJob] whose
     * channel delivers it. The writer runs as a child of the calling coroutine's job: cancelling
     * that job (or [WriterJob.job], or the channel) stops it, and that job doesn't complete until
     * the writer has. A failure (an article no provider has, a timeout) never fails the caller's
     * job: the channel closes with it as its cause ([ByteReadChannel.closedCause]) once everything
     * in front of it has been written. Ktor drops the bytes the reader hasn't taken yet when a
     * channel closes with a cause. [onGap] as in [streamFile].
     */
    suspend fun launchStreamFile(
        archiveNzb: NzbDocument,
        namedSplits: NamedSplits,
        range: LongRange? = null,
        onGap: ((Gap) -> Unit)? = null,
    ): WriterJob {
        val effectiveSplits = if (range != null) {
            adjustSplitsForRange(namedSplits.splits, range.first, range.last - range.first + 1)
        } else {
            namedSplits.splits
        }

        if (effectiveSplits.any { it.encryption != null }) {
            return launchEncryptedFileWriter(archiveNzb, effectiveSplits, namedSplits.name, range?.first ?: 0L, onGap)
        }

        val combinedQueue = flow {
            for (split in effectiveSplits) {
                SegmentQueueService.createRangeQueue(
                    archiveNzb, split.dataStartPosition, split.dataSize
                ).collect { emit(it) }
            }
        }
        return streamingService.launchStreamSegments(
            combinedQueue, name = namedSplits.name, initialReadAheadSegments = firstSegmentsFor(range),
            onGap = onGap?.from(range?.first ?: 0L),
        )
    }

    /** This listener, for gaps a stream reports from its first byte, given them as offsets from [start]. */
    private fun ((Gap) -> Unit).from(start: Long): (Gap) -> Unit = { gap ->
        this(gap.copy(fileRange = gap.fileRange.first + start..gap.fileRange.last + start))
    }

    /**
     * The first segments a stream of a file's [range] starts with
     * ([io.skjaere.nzbstreamer.config.StreamingConfig.initialReadAheadSegments]). From the file's
     * first byte, where a player's first open, a probe of the file and a media server's scan read a
     * few KB and stop, the configured few. From anywhere else (a seek, a resume, an index at the
     * end) the whole read-ahead at once: a reader that opens there reads on, and a player waits for
     * seconds of it, a few segments of a high-bitrate release, before it plays.
     */
    private fun firstSegmentsFor(range: LongRange?): Int? =
        if (range == null || range.first == 0L) streamingService.getInitialReadAheadSegments() else null

    /**
     * Streams an encrypted RAR5 file by AES-CBC decrypting on the fly.
     *
     * Each [SplitInfo] points at the IV of an encrypted block (one per volume for a split file);
     * `Rar5DataAreaDecryptor` reads ciphertext from `NntpSeekableInputStream` in 64 KiB chunks,
     * decrypts, and writes plaintext to the output channel. Memory bound is the chunk size,
     * not the requested range — important for video seeking where media servers ask for multi-MB ranges.
     */
    private suspend fun streamEncrypted(
        archiveNzb: NzbDocument,
        splits: List<SplitInfo>,
        name: String,
        fileStart: Long,
        onGap: ((Gap) -> Unit)?,
        consume: suspend (ByteReadChannel) -> Unit,
    ) {
        val failure = AtomicReference<Throwable?>()
        val early = coroutineScope {
            val writerJob = launchEncryptedFileWriter(archiveNzb, splits, name, fileStart, onGap) { failure.compareAndSet(null, it) }
            try {
                consume(writerJob.channel)
                // As in NntpStreamingService.streamSegments: what failed before consume returned is
                // the early end it may have taken for the end; what fails from here on, it didn't
                // wait for, and the closed channel keeps the writer from recording it.
                failure.get().also { writerJob.channel.cancel() }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // Most likely the channel closing with the writer's failure: throw that.
                throw failure.get() ?: e
            } finally {
                writerJob.cancel()
            }
        }
        // consume may have taken the early end of the data for its end.
        early?.let { throw it }
    }

    /**
     * [onFailure] gets the writer's failure (not its cancellation) before the channel closes with it,
     * unless the reader has closed the channel first: a write to the closed channel then is not the
     * stream failing. [onGap] gets the plaintext that zeroed ciphertext garbles ([garbledPlaintext]),
     * as offsets in the file, whose byte [fileStart] the stream starts at.
     */
    private suspend fun launchEncryptedFileWriter(
        archiveNzb: NzbDocument,
        splits: List<SplitInfo>,
        name: String,
        fileStart: Long,
        onGap: ((Gap) -> Unit)?,
        onFailure: (Throwable) -> Unit = {},
    ): WriterJob {
        val password = archiveNzb.password
            ?: error("Encrypted RAR5 archive but NzbDocument has no password metadata (file=$name)")
        val callerScope = CoroutineScope(currentCoroutineContext())
        return callerScope.writer(autoFlush = false) {
            try {
                // A scoped coroutine: a failure in here is thrown to the writer, which closes the
                // channel with it, instead of failing the caller's job.
                coroutineScope {
                    // The ciphertext's gaps come in the volumes' bytes; the reader wants the plaintext's.
                    val cipherGaps = onGap?.let { listener ->
                        { gap: Gap ->
                            garbledPlaintext(splits, gap.fileRange).forEach { plaintext ->
                                listener(Gap(plaintext.first + fileStart..plaintext.last + fileStart, gap.cause))
                            }
                        }
                    }
                    val seekableStream = NntpSeekableInputStream(archiveNzb, streamingService, end = splits.maxOf(::encryptedReadEnd), onGap = cipherGaps)
                    try {
                        for (split in splits) {
                            val enc = checkNotNull(split.encryption) {
                                "encrypted-streaming path got an unencrypted split (file=$name, split=$split)"
                            }
                            val key = deriveKeyCached(password, enc.salt, enc.kdfIterationsLog2)
                            Rar5DataAreaDecryptor(key).streamDataAreaPlaintext(
                                sourceStream = seekableStream,
                                blockIvPosition = split.dataStartPosition,
                                plaintextHeaderSize = enc.plaintextHeaderSize,
                                dataAreaPlaintextOffset = enc.dataAreaPlaintextOffset,
                                length = split.dataSize,
                                dataAreaIv = enc.dataAreaIv,
                            ) { buf, off, len ->
                                // ktor's writeFully takes (startIndex, endIndex), not (offset, length).
                                channel.writeFully(buf, off, off + len)
                                // Hand each chunk to the reader as it's decrypted, not once 1 MB has
                                // piled up: what a failure further on would otherwise drop.
                                channel.flush()
                            }
                        }
                    } finally {
                        // Ends the download; this scope waits for it to stop.
                        seekableStream.close()
                    }
                }
            } catch (e: Throwable) {
                if (e !is CancellationException && !channel.isClosedForWrite) onFailure(e)
                throw e
            }
        }
    }

    companion object {
        private const val AES_BLOCK = 16

        /**
         * Where decrypting [split] stops reading: its IV, the header's ciphertext, then the data
         * area's up to the end of the range. Exact with a per-file IV; past the end without one
         * (header and data area one CBC stream), by at most a block.
         */
        private fun encryptedReadEnd(split: SplitInfo): Long {
            val enc = checkNotNull(split.encryption)
            fun ciphertext(plaintext: Long) = (plaintext + AES_BLOCK - 1) / AES_BLOCK * AES_BLOCK
            return split.dataStartPosition + AES_BLOCK + ciphertext(enc.plaintextHeaderSize) +
                ciphertext(enc.dataAreaPlaintextOffset + split.dataSize)
        }

        /**
         * The plaintext a zeroed stretch of ciphertext garbles, [ciphertext] in the volumes' bytes laid
         * end to end, as offsets in the stream of [splits]' plaintext (each split's data area, one after
         * another): per split, from the AES block holding its first zeroed byte to the end of the block
         * after its last, which CBC decrypts with the zeroed block as its IV, within what the split
         * streams. A data area with its own IV (dataAreaIv) chains from its first block; without one,
         * header and data area are one chain from the IV on disk ahead of it, which the first block
         * decrypts with.
         */
        internal fun garbledPlaintext(splits: List<SplitInfo>, ciphertext: LongRange): List<LongRange> {
            var streamAt = 0L
            return splits.mapNotNull { split ->
                val enc = checkNotNull(split.encryption) { "garbledPlaintext of an unencrypted split: $split" }
                val splitStart = streamAt
                streamAt += split.dataSize
                fun ciphertextOf(plaintext: Long) = (plaintext + AES_BLOCK - 1) / AES_BLOCK * AES_BLOCK
                val ownIv = enc.dataAreaIv != null
                // Where the chain's block 0 lies on disk, and the first block of ciphertext in the chain:
                // the IV on disk (block -1) decrypts block 0 when the data area has no IV of its own.
                val blockZero = split.dataStartPosition + AES_BLOCK + if (ownIv) ciphertextOf(enc.plaintextHeaderSize) else 0L
                val firstBlock = (ciphertext.first - blockZero).floorDiv(AES_BLOCK.toLong()).coerceAtLeast(if (ownIv) 0L else -1L)
                val lastBlock = (ciphertext.last - blockZero).floorDiv(AES_BLOCK.toLong())
                if (lastBlock < firstBlock) return@mapNotNull null
                // The chain's plaintext from the first garbled block to the end of the one after the last zeroed,
                // as offsets in the data area (past the header, in one chain with it).
                val before = if (ownIv) 0L else enc.plaintextHeaderSize
                val from = firstBlock.coerceAtLeast(0L) * AES_BLOCK - before
                val to = (lastBlock + 2) * AES_BLOCK - before
                // Of that, what the split streams: its data area from dataAreaPlaintextOffset.
                val start = maxOf(from, enc.dataAreaPlaintextOffset)
                val end = minOf(to, enc.dataAreaPlaintextOffset + split.dataSize)
                if (start >= end) null else splitStart + (start - enc.dataAreaPlaintextOffset) until splitStart + (end - enc.dataAreaPlaintextOffset)
            }
        }

        private fun splitOverlapsRange(
            fileOffset: Long,
            split: SplitInfo,
            rangeStart: Long,
            rangeEnd: Long
        ): Boolean = fileOffset + split.dataSize > rangeStart && fileOffset < rangeEnd

        internal fun adjustSplitsForRange(
            splits: List<SplitInfo>,
            rangeStart: Long,
            rangeLength: Long
        ): List<SplitInfo> {
            val rangeEnd = rangeStart + rangeLength

            return splits.runningFold(0L) { offset, split -> offset + split.dataSize }
                .zip(splits)
                .filter { (fileOffset, split) -> splitOverlapsRange(fileOffset, split, rangeStart, rangeEnd) }
                .map { (fileOffset, split) ->
                    val trimStart = maxOf(0L, rangeStart - fileOffset)
                    val trimEnd = minOf(split.dataSize, rangeEnd - fileOffset)
                    val enc = split.encryption
                    if (enc != null) {
                        // Encrypted splits: dataStartPosition must keep pointing at the IV on disk —
                        // moving it forward would break the AES block index math. Encode the trim into
                        // the plaintext-offset field on the encryption metadata; the decryptor consumes
                        // it as `dataAreaPlaintextOffset`.
                        SplitInfo(
                            volumeIndex = split.volumeIndex,
                            dataStartPosition = split.dataStartPosition,
                            dataSize = trimEnd - trimStart,
                            encryption = enc.copy(
                                dataAreaPlaintextOffset = enc.dataAreaPlaintextOffset + trimStart,
                            ),
                        )
                    } else {
                        SplitInfo(
                            volumeIndex = split.volumeIndex,
                            dataStartPosition = split.dataStartPosition + trimStart,
                            dataSize = trimEnd - trimStart
                        )
                    }
                }
        }

        internal fun computeVolumeOffsets(archiveNzb: NzbDocument): List<Long> {
            val offsets = mutableListOf<Long>()
            var cumOffset = 0L
            for (file in archiveNzb.files) {
                offsets.add(cumOffset)
                cumOffset += file.yencHeaders!!.size
            }
            return offsets
        }

        internal fun computeVolumeSizes(archiveNzb: NzbDocument): List<Long> {
            return archiveNzb.files.map { it.yencHeaders!!.size }
        }
    }

    fun resolveStreamableFile(
        entry: ArchiveFileEntry,
        archiveNzb: NzbDocument
    ): StreamableFile? {
        return when (entry) {
            is RarFileEntry -> resolveRarFile(entry, archiveNzb)
            is SevenZipFileEntry -> resolveSevenZipFile(entry)
            is TranslatedFileEntry -> resolveTranslatedFile(entry)
        }
    }

    private fun resolveSevenZipFile(
        entry: SevenZipFileEntry
    ): StreamableFile? {
        if (entry.isDirectory) return null
        if (entry.method != null && entry.method != "Copy") return null

        return StreamableFile(
            path = entry.path,
            totalSize = entry.size,
            startVolumeIndex = 0,
            startOffsetInVolume = entry.dataOffset,
            continuationHeaderSize = 0,
            endOfArchiveSize = 0
        )
    }

    private fun resolveRarFile(
        entry: RarFileEntry,
        archiveNzb: NzbDocument
    ): StreamableFile? {
        if (entry.isDirectory) return null
        if (!entry.isUncompressed) return null

        return if (entry.splitParts.isEmpty()) {
            // Non-split RAR file — dataPosition is already the local offset within the volume
            StreamableFile(
                path = entry.path,
                totalSize = entry.uncompressedSize,
                startVolumeIndex = entry.volumeIndex,
                startOffsetInVolume = entry.dataPosition,
                continuationHeaderSize = 0,
                endOfArchiveSize = 0
            )
        } else {
            // Split RAR file — use pre-computed splits directly.
            // RAR5 volumes can have varying overhead (headers, end-of-archive sections)
            // across volumes, so reconstructing positions from uniform overhead values
            // produces incorrect byte ranges. The parser's split positions are authoritative.
            StreamableFile(
                path = entry.path,
                totalSize = entry.uncompressedSize,
                startVolumeIndex = entry.splitParts[0].volumeIndex,
                startOffsetInVolume = 0,
                continuationHeaderSize = 0,
                endOfArchiveSize = 0,
                preComputedSplits = entry.splitParts
            )
        }
    }

    private fun resolveTranslatedFile(entry: TranslatedFileEntry): StreamableFile? {
        if (entry.isDirectory) return null
        if (entry.splitParts.isEmpty()) return null

        return StreamableFile(
            path = entry.path,
            totalSize = entry.size,
            startVolumeIndex = 0,
            startOffsetInVolume = 0,
            continuationHeaderSize = 0,
            endOfArchiveSize = 0,
            preComputedSplits = entry.splitParts
        )
    }

    fun resolveStreamableFiles(
        entries: List<ArchiveFileEntry>,
        archiveNzb: NzbDocument
    ): List<StreamableFile> {
        return entries.mapNotNull { resolveStreamableFile(it, archiveNzb) }
    }

    suspend fun streamFile(
        archiveNzb: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null,
        streamName: String? = null,
        onGap: ((Gap) -> Unit)? = null,
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        // See the NamedSplits overload for onGap.
        val namedSplits = NamedSplits(file.toSplits(archiveNzb), file.totalSize, streamName ?: file.path)
        streamFile(archiveNzb, namedSplits, range, onGap, consume)
    }

    /** Starts streaming [file]; see the [NamedSplits] overload for the writer's lifetime and failures. */
    suspend fun launchStreamFile(
        archiveNzb: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null,
        streamName: String? = null,
        onGap: ((Gap) -> Unit)? = null,
    ): WriterJob {
        val namedSplits = NamedSplits(file.toSplits(archiveNzb), file.totalSize, streamName ?: file.path)
        return launchStreamFile(archiveNzb, namedSplits, range, onGap)
    }
}
