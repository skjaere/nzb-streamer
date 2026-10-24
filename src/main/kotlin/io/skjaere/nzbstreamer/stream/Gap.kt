package io.skjaere.nzbstreamer.stream

/**
 * A stretch of a streamed file that zeros fill, as no provider could give its bytes
 * ([io.skjaere.nzbstreamer.config.MissingArticlePolicy.ZERO_FILL]). [fileRange] is where, in the
 * file's bytes: offsets from the start of the file streamed (an archive's inner file), as a range
 * asked of it counts them; from [NntpStreamingService.streamSegments], offsets from the stream's
 * first byte. In an encrypted file it is the plaintext the zeroed ciphertext garbles, which reaches
 * a little past the zeros on both sides.
 */
data class Gap(val fileRange: LongRange, val cause: Cause) {
    enum class Cause {
        /** Every provider asked answered 430. */
        Missing,

        /** The providers that have the article have only copies whose yEnc header can't be read; the rest answered 430. */
        Malformed,
    }
}
