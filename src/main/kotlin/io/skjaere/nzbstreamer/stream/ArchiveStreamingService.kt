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

    suspend fun streamFile(
        archiveNzb: NzbDocument,
        namedSplits: NamedSplits,
        range: LongRange? = null,
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
            return streamEncrypted(archiveNzb, effectiveSplits, namedSplits.name, consume)
        }

        val combinedQueue = flow {
            for (split in effectiveSplits) {
                SegmentQueueService.createRangeQueue(
                    archiveNzb, split.dataStartPosition, split.dataSize
                ).collect { emit(it) }
            }
        }
        streamingService.streamSegments(combinedQueue, name = namedSplits.name, consume = consume)
    }

    suspend fun launchStreamFile(
        archiveNzb: NzbDocument,
        namedSplits: NamedSplits,
        range: LongRange? = null
    ): WriterJob {
        val effectiveSplits = if (range != null) {
            adjustSplitsForRange(namedSplits.splits, range.first, range.last - range.first + 1)
        } else {
            namedSplits.splits
        }

        if (effectiveSplits.any { it.encryption != null }) {
            return launchEncryptedFileWriter(archiveNzb, effectiveSplits, namedSplits.name)
        }

        val combinedQueue = flow {
            for (split in effectiveSplits) {
                SegmentQueueService.createRangeQueue(
                    archiveNzb, split.dataStartPosition, split.dataSize
                ).collect { emit(it) }
            }
        }
        return streamingService.launchStreamSegments(combinedQueue, name = namedSplits.name)
    }

    /**
     * Streams an encrypted RAR5 file by AES-CBC decrypting on the fly.
     *
     * Each [SplitInfo] points at the IV of an encrypted block (one per volume for a split file);
     * `Rar5DataAreaDecryptor` reads ciphertext from `NntpSeekableInputStream` in 64 KiB chunks,
     * decrypts, and writes plaintext to the output channel. Memory bound is the chunk size,
     * not the requested range — important for video seeking where Plex issues multi-MB ranges.
     */
    private suspend fun streamEncrypted(
        archiveNzb: NzbDocument,
        splits: List<SplitInfo>,
        name: String,
        consume: suspend (ByteReadChannel) -> Unit,
    ) {
        coroutineScope {
            val writerJob = launchEncryptedFileWriter(archiveNzb, splits, name)
            try {
                consume(writerJob.channel)
            } finally {
                writerJob.cancel()
            }
        }
    }

    private suspend fun launchEncryptedFileWriter(
        archiveNzb: NzbDocument,
        splits: List<SplitInfo>,
        name: String,
    ): WriterJob {
        val password = archiveNzb.password
            ?: error("Encrypted RAR5 archive but NzbDocument has no password metadata (file=$name)")
        val callerScope = CoroutineScope(currentCoroutineContext())
        return callerScope.writer(autoFlush = false) {
            val seekableStream = NntpSeekableInputStream(archiveNzb, streamingService)
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
                    }
                }
            } finally {
                seekableStream.close()
            }
        }
    }

    companion object {
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
        consume: suspend (ByteReadChannel) -> Unit
    ) {
        val namedSplits = NamedSplits(file.toSplits(archiveNzb), file.totalSize, file.path)
        streamFile(archiveNzb, namedSplits, range, consume = consume)
    }

    suspend fun launchStreamFile(
        archiveNzb: NzbDocument,
        file: StreamableFile,
        range: LongRange? = null
    ): WriterJob {
        val namedSplits = NamedSplits(file.toSplits(archiveNzb), file.totalSize, file.path)
        return launchStreamFile(archiveNzb, namedSplits, range)
    }
}
