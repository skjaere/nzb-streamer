package io.skjaere.nzbstreamer.nzb

/**
 * This text with its control characters and line separators escaped (`\r`, `\n`, `\t`, the others
 * as `\uXXXX`), for log lines and messages. Text from an NZB (a segment id, a subject) can hold a
 * CR/LF, which would start a forged log line or split a message. This text itself when it has
 * nothing to escape.
 */
internal fun String.printable(): String {
    if (none(::needsEscape)) return this
    return buildString(length + 8) {
        this@printable.forEach { c ->
            when {
                c == '\r' -> append("\\r")
                c == '\n' -> append("\\n")
                c == '\t' -> append("\\t")
                needsEscape(c) -> append("\\u%04x".format(c.code))
                else -> append(c)
            }
        }
    }
}

private fun needsEscape(c: Char) = c.isISOControl() || c == ' ' || c == ' '
