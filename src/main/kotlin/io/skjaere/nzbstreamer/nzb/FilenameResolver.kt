package io.skjaere.nzbstreamer.nzb

import io.skjaere.compressionutils.Par2Parser
import java.security.MessageDigest

/**
 * Picks the best filename for an NZB file from up to three sources, ranked by trust:
 *
 *  1. **par2** — the parity file's `FileDesc` packet carries the original filename
 *     signed at upload time. If the uploader obfuscated the wire filename but produced
 *     a normal par2 set, this is the only place the real name survives.
 *  2. **subject** — the NZB XML's `<file subject="...">` line, usually formatted as
 *     `[1/N] "filename.ext" yEnc (M/K)`. The uploader chooses what goes here; on
 *     well-formed releases it matches the par2 name, on obscured releases it may be
 *     a hash.
 *  3. **yenc header** — the `=ybegin name=...` field on the wire. Always present but
 *     least trusted because it's the on-wire filename and gets obscured most often.
 *
 * Weighting follows nzbdav's GetFilenamePriority (a port of SABnzbd's approach):
 *  - Starting priority: par2=3, subject=2, yenc=1
 *  - `-5000` if blank
 *  - `-1000` if [ObfuscationHeuristic.isProbablyObfuscated] says so
 *  - `+50` if the extension is in [IMPORTANT_FILE_EXTENSIONS] (video / archive)
 *  - `+10` if the extension is 2..4 chars (normal release-style extensions)
 *
 * Highest priority wins. Ties broken by source-rank (par2 first).
 *
 * Reference:
 *   https://github.com/nzbdav-dev/nzbdav/blob/main/nzbdav/backend/Queue/DeobfuscationSteps/3.GetFileInfos/GetFileInfosStep.cs
 */
object FilenameResolver {

    // 2-4 char extensions get a small bonus; matches what real release names typically use.
    private const val EXT_BONUS_MIN_LEN = 2
    private const val EXT_BONUS_MAX_LEN = 4
    private const val EXT_BONUS = 10

    private const val IMPORTANT_FILE_BONUS = 50
    private const val OBFUSCATED_PENALTY = -1000
    private const val BLANK_PENALTY = -5000

    private const val PAR2_PRIORITY = 3
    private const val SUBJECT_PRIORITY = 2
    private const val YENC_PRIORITY = 1

    // Video and archive containers that we care about ending up visible to clients.
    // Matches nzbdav's IsImportantFileType (https://github.com/nzbdav-dev/nzbdav/...).
    private val IMPORTANT_FILE_EXTENSIONS = setOf(
        "mkv", "mp4", "avi", "mov", "wmv", "flv", "m4v", "mpg", "mpeg", "ts", "webm",
        "rar", "zip", "7z", "tar", "gz",
    )

    private val QUOTED_FILENAME = Regex("\"([^\"]+)\"")

    // SABnzbd's fallback when the filename isn't in quotes — looks for a word-ish run
    // ending in .EXT where EXT is 2-4 alphanumeric chars. Permissive enough to handle
    // the variety of subject formats seen on Usenet.
    // https://github.com/sabnzbd/sabnzbd/blob/b6b0d10367fd4960bad73edd1d3812cafa7fc002/sabnzbd/nzbstuff.py#L106
    private val FALLBACK_FILENAME = Regex(
        "\\b([\\w\\-+()' .,]+(?:\\[[\\w\\-/+()' .,]*][\\w\\-+()' .,]*)*\\.[A-Za-z0-9]{2,4})\\b"
    )

    /**
     * Extract a filename from an NZB `<file subject="...">` line.
     *
     * Tries the most-common `"filename.ext"` quoted form first, then falls back to a
     * loose regex matching any word ending in a 2-4 char extension. Returns null if
     * neither matches or the match isn't a bare filename (any extracted token containing
     * a path separator is rejected — the subject should reference a single file, not
     * a path).
     */
    fun extractSubjectFilename(subject: String): String? {
        val quoted = QUOTED_FILENAME.find(subject)?.groupValues?.get(1)
        if (!quoted.isNullOrBlank() && !quoted.contains('/') && !quoted.contains('\\')) {
            return quoted
        }
        val fallback = FALLBACK_FILENAME.find(subject)?.groupValues?.get(1)
        if (!fallback.isNullOrBlank() && !fallback.contains('/') && !fallback.contains('\\')) {
            return fallback
        }
        return null
    }

    /**
     * Index a par2 byte blob by the SHA-of-first-16KB of each described file. The keys
     * are the same `hash16k` field used by [Par2Parser]; debridav callers compute it
     * from the NZB file's first 16KB and look up the matching descriptor here.
     *
     * Returns an empty map if [par2Data] is null, malformed, or empty. We never throw
     * out of this — par2 is best-effort metadata.
     */
    @Suppress("TooGenericExceptionCaught")
    fun indexPar2ByHash16k(par2Data: ByteArray?): Map<String, String> {
        if (par2Data == null) return emptyMap()
        return try {
            Par2Parser.parse(par2Data).files
                .filter { it.filename.isNotBlank() }
                .associate { it.hash16kHex() to it.filename }
        } catch (_: Exception) {
            emptyMap()
        }
    }

    /**
     * Pick the best filename for an NZB file from the three candidate sources. Inputs
     * may be blank or null — the priority math handles missing sources via the blank
     * penalty.
     *
     * Returns the highest-priority non-blank candidate. If all three are blank, returns
     * an empty string (the caller should never persist a streamable file with an empty
     * path; this is a guard against malformed NZBs only).
     */
    fun bestFilename(yencName: String?, subject: String, par2Filename: String?): String {
        val subjectName = extractSubjectFilename(subject)
        val candidates = listOf(
            FilenameCandidate(par2Filename, PAR2_PRIORITY),
            FilenameCandidate(subjectName, SUBJECT_PRIORITY),
            FilenameCandidate(yencName, YENC_PRIORITY),
        )
        return candidates
            .maxByOrNull { priority(it.name, it.startingPriority) }
            ?.name
            ?.takeUnless { it.isNullOrBlank() }
            ?: ""
    }

    /**
     * Convenience wrapper for the common case where the caller has par2 bytes (not yet
     * indexed) and a single hash16k to look up.
     */
    fun bestFilename(
        yencName: String?,
        subject: String,
        par2Data: ByteArray?,
        hash16kHex: String,
    ): String {
        val par2Filename = indexPar2ByHash16k(par2Data)[hash16kHex]
        return bestFilename(yencName, subject, par2Filename)
    }

    /**
     * Compute the MD5 of the first 16KB of a buffer, formatted as a lowercase hex
     * string. Match format produced by [Par2Parser.Par2FileDescription.hash16kHex].
     * Returns null if [data] is null or shorter than 16KB (we can't match against a
     * par2 hash16k otherwise).
     */
    fun hash16kHex(data: ByteArray?): String? {
        if (data == null || data.size < FIRST_16KB) return null
        val md5 = MessageDigest.getInstance("MD5")
        md5.update(data, 0, FIRST_16KB)
        return md5.digest().joinToString("") { "%02x".format(it) }
    }

    private const val FIRST_16KB = 16 * 1024

    private data class FilenameCandidate(val name: String?, val startingPriority: Int)

    private fun priority(name: String?, startingPriority: Int): Int {
        var p = startingPriority
        if (name.isNullOrBlank()) return p + BLANK_PENALTY
        if (ObfuscationHeuristic.isProbablyObfuscated(name)) p += OBFUSCATED_PENALTY

        val ext = name.substringAfterLast('.', "").lowercase()
        if (ext in IMPORTANT_FILE_EXTENSIONS) p += IMPORTANT_FILE_BONUS
        if (ext.length in EXT_BONUS_MIN_LEN..EXT_BONUS_MAX_LEN) p += EXT_BONUS

        return p
    }
}
