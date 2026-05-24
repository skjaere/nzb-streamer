package io.skjaere.nzbstreamer.nzb

/**
 * Detects whether a filename looks obfuscated (random hash) versus a real release name.
 * Used by [FilenameResolver] to break ties between par2 / subject / yenc-header sources
 * when picking the best name to surface for a streamable file.
 *
 * Port of SABnzbd's `is_probably_obfuscated`, with three documented extensions:
 *  1. `-` counted as a separator (SABnzbd uses only ` `, `.`, `_`, but real release names
 *     are hyphen-heavy — `Group-Quality-Year-...`).
 *  2. Short all-lowercase single-word names (`testfile`, `sample`, `intro`) treated as
 *     clean. SABnzbd's pure algorithm misclassifies them because every positive case
 *     requires uppercase or separators.
 *  3. Letter-dominant rule: at least 4 letters + at least 1 separator + letters more than
 *     twice the digit count → clean. Catches lowercase hyphenated release names that
 *     don't have enough digits to trip SABnzbd's `Beast 2020` rule.
 *
 * The same algorithm lives in debridav (also a port of nzbdav/SABnzbd); they're kept in
 * sync by convention rather than by sharing a module — the algorithm is small and stable.
 *
 *  Reference: https://github.com/sabnzbd/sabnzbd/blob/64034c5/sabnzbd/deobfuscate_filenames.py#L105
 */
object ObfuscationHeuristic {

    private val HEX_32 = Regex("^[a-f0-9]{32}$")
    private val HEX_DOT_40_PLUS = Regex("^[a-f0-9.]{40,}$")
    private val HEX_30 = Regex("[a-f0-9]{30}")
    private val BRACKET_TAG = Regex("\\[\\w+]")
    private val ABC_XYZ_PREFIX = Regex("^abc\\.xyz")

    @Suppress("ReturnCount", "MagicNumber")
    fun isProbablyObfuscated(filename: String): Boolean {
        val base = stripLastExtension(filename)

        if (HEX_32.matches(base)) return true
        if (HEX_DOT_40_PLUS.matches(base)) return true
        if (HEX_30.containsMatchIn(base) && BRACKET_TAG.findAll(base).count() >= 2) return true
        if (ABC_XYZ_PREFIX.containsMatchIn(base)) return true

        val digits = base.count { it.isDigit() }
        val upper = base.count { it.isUpperCase() }
        val lower = base.count { it.isLowerCase() }
        val separators = base.count { it == ' ' || it == '.' || it == '_' || it == '-' }

        if (upper >= 2 && lower >= 2 && separators >= 1) return false
        if (separators >= 3) return false
        if (upper + lower >= 4 && digits >= 4 && separators >= 1) return false
        if (base.isNotEmpty() && base[0].isUpperCase() && lower > 2 && upper.toDouble() / lower <= 0.25) {
            return false
        }
        if (lower >= 2 && upper == 0 && digits == 0 && separators == 0 && base.length <= 16) return false
        if ((upper + lower) >= 4 && separators >= 1 && (upper + lower) > 2 * digits) return false

        return true
    }

    private fun stripLastExtension(filename: String): String {
        val withoutDir = filename.substringAfterLast('/')
        val dot = withoutDir.lastIndexOf('.')
        return if (dot <= 0) withoutDir else withoutDir.substring(0, dot)
    }
}
