package io.skjaere.nzbstreamer.enrichment

import io.skjaere.compressionutils.ArchiveService
import io.skjaere.compressionutils.ArchiveTypeDetector
import io.skjaere.compressionutils.Par2FileNames
import io.skjaere.compressionutils.Par2Parser
import io.skjaere.nzbstreamer.nzb.NzbFile

/**
 * What tells that a file is an archive volume, and which, beyond the name it was posted under
 * (library issue N52): the PAR2 data, which names each file by the MD5 of its first 16 KB and its
 * length, and its content, which starts with an archive's signature (every RAR volume's does, a 7z
 * set's first).
 */
internal object VolumeNames {
    private val PART_NUMBER = Regex("""(?i)\.part\d+$""")

    /**
     * The file descriptions in the PAR2 data of [files], every PAR2 file's (a season pack can have
     * an index per episode), each by the name its path ends in; none from data that doesn't parse.
     * An index made over a folder names its files with their path in it
     * (Show.S01/Show.S01E02.part3.rar), which no name they were posted under has (NR-11).
     */
    fun descriptions(files: List<NzbFile>): List<Par2Parser.Par2FileDescription> =
        files.mapNotNull { it.par2Data }.flatMap { data ->
            try {
                Par2Parser.parse(data).files
                    .map { it.copy(filename = baseName(it.filename)) }
                    .filter { it.filename.isNotBlank() }
            } catch (@Suppress("TooGenericExceptionCaught") _: Exception) {
                emptyList()
            }
        }

    /** [path] without the folders it is in, by either separator. */
    private fun baseName(path: String): String = path.substringAfterLast('/').substringAfterLast('\\')

    /**
     * The names [descriptions] give files, each by its first 16 KB and its length: none to files
     * that start alike and are as long, which several with different names describe (NR-8).
     */
    fun names(descriptions: List<Par2Parser.Par2FileDescription>): Par2FileNames = Par2FileNames(descriptions)

    /** The MD5 of [first16kb] in hex, as [Par2Parser.Par2FileDescription.hash16kHex] gives a file's. */
    fun md5Hex(first16kb: ByteArray): String = Par2FileNames.md5Hex(first16kb)

    /** Whether [name] is an archive volume's (.partN.rar, .rar, .r00, .7z, .7z.001, ...). */
    fun isVolume(name: String): Boolean = ArchiveService.fileHasKnownExtension(name)

    /**
     * The volume name [name] gives: itself when it is one, else, posted under another extension or
     * none, its name without that extension when that is one ("Movie.part2.rar.bin", "Movie.r00.bin")
     * or ends in a part number ("Movie.part2.bin" and "Movie.part2" for "Movie.part2.rar"). Null
     * when it gives none.
     */
    fun of(name: String): String? = listOf(name, name.substringBeforeLast('.', "")).firstNotNullOfOrNull { stem ->
        when {
            isVolume(stem) -> stem
            PART_NUMBER.containsMatchIn(stem) -> "$stem.rar"
            else -> null
        }
    }

    /** Whether [first16kb] start with an archive's signature (RAR4, RAR5, 7z). */
    fun isArchive(first16kb: ByteArray?): Boolean =
        first16kb != null && ArchiveTypeDetector.detect(first16kb).type != ArchiveTypeDetector.ArchiveType.UNKNOWN
}
