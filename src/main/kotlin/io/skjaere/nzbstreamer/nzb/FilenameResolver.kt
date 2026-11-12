package io.skjaere.nzbstreamer.nzb

import io.skjaere.compressionutils.Par2FileNames
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
 * The weights and [IMPORTANT_FILE_EXTENSIONS] come from nzbdav (MIT licence; see THIRD_PARTY_NOTICES.md).
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
    // Matches nzbdav's IsImportantFileType.
    private val IMPORTANT_FILE_EXTENSIONS = setOf(
        "mkv", "mp4", "avi", "mov", "wmv", "flv", "m4v", "mpg", "mpeg", "ts", "webm",
        "rar", "zip", "7z", "tar", "gz",
    )

    private val QUOTED_FILENAME = Regex("\"([^\"]+)\"")

    // An unquoted subject is cut into pieces at white space, control characters and these.
    private const val PIECE_SEPARATORS = "\"/\\:|[](){}<>"

    // Trimmed from a piece's end, and from its start, before it is read as a file name.
    private const val TRIMMED_AT_END = ".,;!?"
    private const val TRIMMED_AT_START = ",;!?"

    // By convention the file name comes right before this word, in any case.
    private const val SUBJECT_KEYWORD = "yenc"

    // File systems cap a name at 255 bytes.
    private const val MAX_SUBJECT_NAME_LENGTH = 255

    // A piece such as `1.37GB` or `700MB` is a size, never a name.
    private val SIZE_UNITS = setOf("b", "kb", "mb", "gb", "tb", "kib", "mib", "gib", "tib", "bytes")

    /**
     * Extract a filename from an NZB `<file subject="...">` line.
     *
     * The first quoted string wins when it is a bare name: posters are meant to quote it.
     * Otherwise the name is read from the bare text: the piece that looks like a file name (a
     * name, a dot and a short extension) nearest before `yEnc`, or before the part counter
     * such as `(1/50)` when `yEnc` is missing, else the first one after it. Sizes such as
     * `1.37GB` are never names. Returns null when nothing qualifies, or when the name found
     * holds a path separator (the subject should reference a single file, not a path).
     */
    fun extractSubjectFilename(subject: String): String? {
        val quoted = QUOTED_FILENAME.find(subject)?.groupValues?.get(1)
        if (!quoted.isNullOrBlank() && !quoted.contains('/') && !quoted.contains('\\')) {
            return quoted
        }
        val unquoted = unquotedFilename(subject)
        if (!unquoted.isNullOrBlank() && !unquoted.contains('/') && !unquoted.contains('\\')) {
            return unquoted
        }
        return null
    }

    /**
     * The file name in a subject that doesn't quote it, or null. The subject is cut into pieces
     * at separators and split at its anchor: the first `yEnc`, else the last part counter, else
     * nothing. The name is the last file name piece before the anchor, else the first after it.
     */
    private fun unquotedFilename(subject: String): String? {
        val pieces = piecesOf(subject)
        val keyword = pieces.indexOfFirst { asciiEqualsIgnoreCase(subject, it, SUBJECT_KEYWORD) }
        val head: List<IntRange>
        val tail: List<IntRange>
        if (keyword >= 0) {
            head = pieces.subList(0, keyword)
            tail = pieces.subList(keyword + 1, pieces.size)
        } else {
            val counter = lastPartCounter(subject)
            head = if (counter == null) pieces else pieces.filter { it.last < counter.first }
            tail = if (counter == null) emptyList() else pieces.filter { it.first > counter.last }
        }
        return head.asReversed().firstNotNullOfOrNull { fileNameIn(subject, it) }
            ?: tail.firstNotNullOfOrNull { fileNameIn(subject, it) }
    }

    /** Where each run of characters between separators lies in [subject]. */
    private fun piecesOf(subject: String): List<IntRange> {
        val pieces = ArrayList<IntRange>()
        var start = -1
        for (i in subject.indices) {
            val c = subject[i]
            if (c.isWhitespace() || c.isISOControl() || c in PIECE_SEPARATORS) {
                if (start >= 0) pieces += start until i
                start = -1
            } else if (start < 0) {
                start = i
            }
        }
        if (start >= 0) pieces += start until subject.length
        return pieces
    }

    /** Where the last part counter, such as `(1/50)` or `( 1 / 50 )`, lies in [subject], or null. */
    private fun lastPartCounter(subject: String): IntRange? {
        var found: IntRange? = null
        var open = subject.indexOf('(')
        while (open >= 0) {
            val close = partCounterClose(subject, open)
            if (close >= 0) found = open..close
            open = subject.indexOf('(', open + 1)
        }
        return found
    }

    /** The index of the `)` that closes a part counter opened at [open], or -1. */
    private fun partCounterClose(subject: String, open: Int): Int {
        var i = skipSpaces(subject, open + 1)
        val part = i
        i = digitsEnd(subject, i)
        if (i == part) return -1
        i = skipSpaces(subject, i)
        if (i >= subject.length || subject[i] != '/') return -1
        i = skipSpaces(subject, i + 1)
        val parts = i
        i = digitsEnd(subject, i)
        if (i == parts) return -1
        i = skipSpaces(subject, i)
        return if (i < subject.length && subject[i] == ')') i else -1
    }

    /**
     * The file name [piece] of [subject] holds, trimmed of punctuation, or null: it must have
     * something before its last dot and an extension after it, and must not be a size.
     */
    private fun fileNameIn(subject: String, piece: IntRange): String? {
        var start = piece.first
        var end = piece.last + 1
        while (end > start && subject[end - 1] in TRIMMED_AT_END) end--
        while (start < end && subject[start] in TRIMMED_AT_START) start++
        if (end - start > MAX_SUBJECT_NAME_LENGTH) return null
        val name = subject.substring(start, end)
        if (isSize(name)) return null
        val dot = name.lastIndexOf('.')
        if (dot < 0 || !isExtension(name.substring(dot + 1))) return null
        if ((0 until dot).none { name[it].isLetter() || isDigit(name[it]) }) return null
        return name
    }

    /** `mkv`, `r00`, `7z`, `par2`: 2 to 4 ASCII letters or digits with a letter. Or `001`, a split part. */
    private fun isExtension(extension: String): Boolean {
        val letters = extension.count { it in 'a'..'z' || it in 'A'..'Z' }
        val digits = extension.count(::isDigit)
        if (letters + digits != extension.length) return false
        return (extension.length in 2..4 && letters > 0) || (extension.length == 3 && digits == 3)
    }

    /** `1.37GB`, `700MB`, `1,4GiB`, `734003200bytes`: digits, maybe a fraction, then a unit. */
    private fun isSize(piece: String): Boolean {
        var i = digitsEnd(piece, 0)
        if (i == 0) return false
        if (i < piece.length && (piece[i] == '.' || piece[i] == ',')) {
            val fraction = digitsEnd(piece, i + 1)
            if (fraction > i + 1) i = fraction
        }
        val unit = piece.substring(i)
        return SIZE_UNITS.any { asciiEqualsIgnoreCase(unit, unit.indices, it) }
    }

    /** Whether [range] of [text] is [lowerCaseWord], ignoring the case of ASCII letters. */
    private fun asciiEqualsIgnoreCase(text: String, range: IntRange, lowerCaseWord: String): Boolean {
        if (range.last - range.first + 1 != lowerCaseWord.length) return false
        return lowerCaseWord.indices.all { i ->
            val c = text[range.first + i]
            (if (c in 'A'..'Z') c + ('a' - 'A') else c) == lowerCaseWord[i]
        }
    }

    private fun skipSpaces(text: String, from: Int): Int {
        var i = from
        while (i < text.length && text[i] == ' ') i++
        return i
    }

    private fun digitsEnd(text: String, from: Int): Int {
        var i = from
        while (i < text.length && isDigit(text[i])) i++
        return i
    }

    private fun isDigit(c: Char): Boolean = c in '0'..'9'

    /**
     * Index a par2 byte blob by the MD5-of-first-16KB of each described file. The keys
     * are the same `hash16k` field used by [Par2Parser]; callers compute it
     * from the NZB file's first 16KB and look up the matching descriptor here. A hash
     * that descriptions with different names share (files that start alike: a DVD's
     * VIDEO_TS.IFO and its copy VIDEO_TS.BUP, disc images) names nothing: it would name
     * every such file the same. [Par2FileNames.nameOf] tells those apart by length too.
     *
     * Returns an empty map if [par2Data] is null, malformed, or empty. We never throw
     * out of this — par2 is best-effort metadata.
     */
    fun indexPar2ByHash16k(par2Data: ByteArray?): Map<String, String> = Par2FileNames.parse(par2Data).byHash16k

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
