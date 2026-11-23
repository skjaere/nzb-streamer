package io.skjaere.nzbstreamer.play

/**
 * The container a video file's extension names, as a MIME type: for the player, which reads the file through its data
 * source, and for a server that streams it to a Cast device, which tells the device what it serves. Here, free of the
 * player's types, so both can have it. The names are Media3's (`MimeTypes`), written out.
 */
object ContainerTypes {
    private val types = mapOf(
        "avi" to "video/x-msvideo",
        "mkv" to "video/x-matroska",
        "mp4" to "video/mp4",
        "m4v" to "video/mp4",
        "ts" to "video/mp2t",
        "m2ts" to "video/mp2t",
        "webm" to "video/webm",
        "mov" to "video/quicktime",
        "wmv" to "video/x-ms-wmv",
        "mpg" to "video/mp2p",
        "mpeg" to "video/mp2p",
    )

    /**
     * What a Cast device plays, as measured on a receiver: MKV, WebM, MP4, M4V and MOV play; TS and AVI are
     * refused with no error code. WMV, M2TS and MPEG weren't tried, and are refused by rule.
     */
    private val castable = setOf("mkv", "webm", "mp4", "m4v", "mov")

    /** The container MIME type [fileName]'s extension names, or null for one it does not know. */
    fun mimeOf(fileName: String): String? = types[fileName.substringAfterLast('.', "").lowercase()]

    /** Whether a Cast device plays a file of [fileName]'s type, as far as its name tells. */
    fun castable(fileName: String): Boolean = extension(fileName) in castable

    /** [fileName]'s extension, lowercased; empty when it has none. */
    fun extension(fileName: String): String = fileName.substringAfterLast('/').substringAfterLast('.', "").lowercase()
}
