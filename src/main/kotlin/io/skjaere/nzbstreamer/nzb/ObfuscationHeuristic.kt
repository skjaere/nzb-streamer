package io.skjaere.nzbstreamer.nzb

/**
 * Tells a file name a person chose from one a tool generated: a hash, a UUID, or letters and
 * digits in no readable order.
 *
 * The name is judged by its stem: the file's own name without its folder, its extension, and the
 * volume, part and file type suffixes before that. The stem is cut into tokens, each a run of
 * letters and digits. A word or an episode code means a person named the file, whatever else the
 * name holds. Without one, a single token that looks generated makes the name random.
 */
object ObfuscationHeuristic {

    // File systems cap a name at 255 bytes: anything longer was never a file's name.
    private const val MAX_NAME_LENGTH = 255

    // The last `.x` is the extension when x is this long, in ASCII letters and digits.
    private const val MAX_EXTENSION_LENGTH = 5

    private const val UUID_LENGTH = 36
    private val UUID_DASHES = setOf(8, 13, 18, 23)
    private val UUID_WRAPPERS = charArrayOf('[', ']', '{', '}', '(', ')', ' ')

    private const val LONG_HEX = 8
    private const val LONG_NUMBER = 7
    private const val INTERLEAVED_RUNS = 3
    private const val CASE_NOISE_CHANGES = 4
    private const val WORD_LETTERS = 3

    // Suffixes that say what kind of file it is, not what it is. Dropped from the end of the stem.
    private val FILE_TYPES = setOf(
        "mkv", "mp4", "m4v", "avi", "mov", "wmv", "ts", "m2ts", "mts", "mpg", "mpeg", "vob", "webm",
        "rar", "7z", "zip", "tar", "gz", "bz2", "xz", "iso", "img",
        "par2", "nfo", "sfv", "srr", "srs", "txt", "jpg", "jpeg", "png",
    )

    // Words that say what kind of file it is but not what it belongs to.
    private val GENERIC_WORDS = setOf("sample", "proof")

    private enum class Kind { TELLING, NEUTRAL, RANDOM }

    /** Whether [filename] looks random rather than a name a person gave the file. */
    fun isProbablyObfuscated(filename: String): Boolean {
        val name = filename.substringAfterLast('/').substringAfterLast('\\').trim()
        if (name.isEmpty() || name.length > MAX_NAME_LENGTH) return true
        val stem = stemOf(name)
        if (stem.none(::isLetterOrDigit)) return true
        if (isUuid(stem.trim(*UUID_WRAPPERS))) return true

        var random = false
        for (token in tokensOf(stem)) {
            when (kindOf(token)) {
                Kind.TELLING -> return false
                Kind.RANDOM -> random = true
                Kind.NEUTRAL -> Unit
            }
        }
        return random
    }

    /** [name] without its extension, then without each volume, part or file type suffix left at its end. */
    private fun stemOf(name: String): String {
        var end = name.length
        val extension = name.substringAfterLast('.', missingDelimiterValue = "")
        if (extension.length in 1..MAX_EXTENSION_LENGTH && extension.all(::isAsciiLetterOrDigit)) {
            end -= extension.length + 1
        }
        while (true) {
            val dot = name.lastIndexOf('.', end - 1)
            if (dot < 0 || !isDroppableSuffix(asciiLowercase(name.substring(dot + 1, end)))) return name.substring(0, end)
            end = dot
        }
    }

    /** Whether the lower-case [suffix] is a part, recovery, old-style or split volume marker, or a file type. */
    private fun isDroppableSuffix(suffix: String): Boolean =
        isNumbered(suffix, "part") ||
            isRecoveryMarker(suffix) ||
            (suffix.length in 3..4 && suffix[0] in "rsz" && isAllDigits(suffix, 1)) ||
            (suffix.length == 3 && isAllDigits(suffix, 0)) ||
            suffix in FILE_TYPES

    /** `vol03+04` or `vol03-04`. */
    private fun isRecoveryMarker(suffix: String): Boolean {
        if (!suffix.startsWith("vol")) return false
        val firstEnd = digitsEnd(suffix, 3)
        if (firstEnd == 3 || firstEnd >= suffix.length) return false
        if (suffix[firstEnd] != '+' && suffix[firstEnd] != '-') return false
        return isAllDigits(suffix, firstEnd + 1)
    }

    /** [suffix] is [word] followed by one or more digits. */
    private fun isNumbered(suffix: String, word: String): Boolean =
        suffix.startsWith(word) && isAllDigits(suffix, word.length)

    /** Eight, four, four, four and twelve hex digits joined by dashes. */
    private fun isUuid(text: String): Boolean =
        text.length == UUID_LENGTH &&
            text.indices.all { i -> if (i in UUID_DASHES) text[i] == '-' else isHexDigit(text[i]) }

    /** The runs of letters and digits in [stem]. */
    private fun tokensOf(stem: String): List<String> {
        val tokens = ArrayList<String>()
        var start = -1
        for (i in stem.indices) {
            if (isLetterOrDigit(stem[i])) {
                if (start < 0) start = i
            } else if (start >= 0) {
                tokens += stem.substring(start, i)
                start = -1
            }
        }
        if (start >= 0) tokens += stem.substring(start)
        return tokens
    }

    private fun kindOf(token: String): Kind = when {
        isEpisodeCode(token) -> Kind.TELLING
        asciiLowercase(token) in GENERIC_WORDS -> Kind.NEUTRAL
        isLongHex(token) -> Kind.RANDOM
        token.length >= LONG_NUMBER && isAllDigits(token, 0) -> Kind.RANDOM
        runsOf(token) >= INTERLEAVED_RUNS -> Kind.RANDOM
        isCaseNoise(token) -> Kind.RANDOM
        hasLettersInARow(token, WORD_LETTERS) -> Kind.TELLING
        else -> Kind.NEUTRAL
    }

    /** `S01`, `S01E02`, `S01E02E03`, `S2019E05` or `1x02`, in any case. */
    private fun isEpisodeCode(token: String): Boolean = isSeasonEpisode(token) || isCrossEpisode(token)

    private fun isSeasonEpisode(token: String): Boolean {
        if (token[0] != 's' && token[0] != 'S') return false
        var i = digitsEnd(token, 1)
        if (i - 1 !in 1..4) return false
        while (i < token.length) {
            if (token[i] != 'e' && token[i] != 'E') return false
            val end = digitsEnd(token, i + 1)
            if (end - (i + 1) !in 1..4) return false
            i = end
        }
        return true
    }

    private fun isCrossEpisode(token: String): Boolean {
        val x = digitsEnd(token, 0)
        if (x !in 1..2 || x >= token.length || (token[x] != 'x' && token[x] != 'X')) return false
        val end = digitsEnd(token, x + 1)
        return end == token.length && end - (x + 1) in 2..3
    }

    /** A hash: hex digits only, long enough, with both digits and letters. */
    private fun isLongHex(token: String): Boolean =
        token.length >= LONG_HEX && token.all(::isHexDigit) && token.any(::isDigit) && !token.all(::isDigit)

    /** How many runs of only letters or only digits [token] is made of. */
    private fun runsOf(token: String): Int {
        var runs = 1
        for (i in 1 until token.length) {
            if (isDigit(token[i]) != isDigit(token[i - 1])) runs++
        }
        return runs
    }

    /**
     * Whether the case changes between neighbouring letters often enough to be noise. The first
     * pair isn't counted, so an initial capital (or a lower-case first letter) doesn't add one.
     */
    private fun hasFrequentCaseChanges(token: String): Boolean {
        var changes = 0
        for (i in 2 until token.length) {
            val a = token[i - 1]
            val b = token[i]
            if ((a.isUpperCase() && b.isLowerCase()) || (a.isLowerCase() && b.isUpperCase())) changes++
        }
        val letters = token.count { it.isLetter() }
        return changes >= CASE_NOISE_CHANGES && 2 * changes > letters
    }

    private fun hasLettersInARow(token: String, count: Int): Boolean {
        var row = 0
        for (c in token) {
            row = if (c.isLetter()) row + 1 else 0
            if (row >= count) return true
        }
        return false
    }

    private fun digitsEnd(text: String, from: Int): Int {
        var i = from
        while (i < text.length && isDigit(text[i])) i++
        return i
    }

    /** Whether [text] from [from] on is one or more digits. */
    private fun isAllDigits(text: String, from: Int): Boolean = from < text.length && digitsEnd(text, from) == text.length

    private fun asciiLowercase(text: String): String =
        buildString(text.length) { for (c in text) append(if (c in 'A'..'Z') c + ('a' - 'A') else c) }

    private fun isDigit(c: Char): Boolean = c in '0'..'9'

    private fun isLetterOrDigit(c: Char): Boolean = c.isLetter() || isDigit(c)

    private fun isAsciiLetterOrDigit(c: Char): Boolean = c in 'a'..'z' || c in 'A'..'Z' || isDigit(c)

    private fun isHexDigit(c: Char): Boolean = isDigit(c) || c in 'a'..'f' || c in 'A'..'F'

    // Case noise: the case changes often, or breaks a word's shape where no word does (RandomCase).
    private fun isCaseNoise(token: String): Boolean = hasFrequentCaseChanges(token) || RandomCase.isRandom(token)
}
