package io.skjaere.nzbstreamer.nzb

/**
 * Letter case that no word has. A camel-case word is capitals each followed by two or more
 * lower-case letters (HarborLights), at most with an acronym before a word (WEBRip) or a short
 * word inside it (iPhoneXsMax). A random mixed-case string breaks that shape again and again.
 */
internal object RandomCase {
    private const val MIN_LETTERS = 8
    private const val MIN_BREAKS = 2

    /**
     * Whether [token] has at least 8 letters and breaks a word's shape in two places or more
     * ([breaks]): KTpqWfbxRNcZ, not HarborLights, BluRayRemux or iPhoneXsMax.
     */
    fun isRandom(token: String): Boolean = token.count(::isLetter) >= MIN_LETTERS && breaks(token) >= MIN_BREAKS

    /**
     * The places in [token] where its case breaks a word's shape: a single lower-case letter
     * between two capitals (the q in KqW), and two or more capitals followed directly by a
     * lower-case letter (the KTp in KTpq). Camel-case words have neither. An acronym before a
     * word has one (WEBRip), and so has a one-letter word between two others (iPhoneXsMax).
     */
    fun breaks(token: String): Int {
        var count = 0
        var capitals = 0
        for (i in token.indices) {
            val c = token[i]
            when {
                isUpper(c) -> capitals++
                isLower(c) -> {
                    if (capitals >= 2) count++
                    if (capitals >= 1 && i + 1 < token.length && isUpper(token[i + 1])) count++
                    capitals = 0
                }
                else -> capitals = 0
            }
        }
        return count
    }

    private fun isUpper(c: Char): Boolean = c in 'A'..'Z'

    private fun isLower(c: Char): Boolean = c in 'a'..'z'

    private fun isLetter(c: Char): Boolean = isUpper(c) || isLower(c)
}
