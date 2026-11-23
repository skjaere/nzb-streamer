package io.skjaere.nzbstreamer.play

/**
 * A copy of a disc's own files, which plays as no single video: a Blu-ray's, by its `BDMV` folder, its
 * navigation files or its streams beside them; a DVD's, by its `VIDEO_TS` folder or its own files; a disc image, `.iso`.
 * Told by the files a release lists once they are known, never by its name: a folder's name compares whole, whatever its
 * case, split on either separator (a 7z made on Windows may keep its own).
 */
object DiscFiles {
    /** A Blu-ray's navigation files: its index and movie objects, playlists, clip information and BD-J objects. */
    private val navigation = setOf("bdmv", "mpls", "clpi", "bdjo")

    /** A Blu-ray's streams, 2D and 3D: a disc's only beside its navigation files, as a lone one may be any recording. */
    private val streams = setOf("m2ts", "ssif")

    /** A DVD's own files: its information, their backups and its video objects. */
    private val dvd = setOf("ifo", "bup", "vob")

    /** [path]'s folders and its name, lower case. */
    private fun parts(path: String): List<String> = VideoFileFilter.parts(path.lowercase())

    private fun extension(path: String): String = parts(path).last().substringAfterLast('.', "")

    /** Whether one of [path]'s folders is a Blu-ray's `BDMV`, by its whole name: nothing in it is a video. */
    fun inBdmv(path: String): Boolean = "bdmv" in parts(path).dropLast(1)

    /**
     * The kind of disc [files] are a copy of: they hold a disc's file, and no video of the release's own besides (one
     * that isn't a disc's, a sample or an extra, [VideoFileFilter.isSample]), which plays instead. Its largest disc
     * file says which. Null when they aren't one.
     */
    fun kindOf(files: List<ListedFile>): DiscKind? {
        val withNavigation = files.any { extension(it.path) in navigation }
        val kinds = files.map { it to kindOf(it.path, withNavigation) }
        if (kinds.any { (file, kind) -> kind == null && file.isVideo && !VideoFileFilter.isSample(file.path) }) return null
        return kinds.filter { it.second != null }.maxByOrNull { it.first.size }?.second
    }

    /** The kind of disc [path] is a file of, in a listing that holds a Blu-ray's navigation files or not ([withNavigation]). */
    private fun kindOf(path: String, withNavigation: Boolean): DiscKind? {
        val folders = parts(path).dropLast(1)
        val extension = extension(path)
        return when {
            "bdmv" in folders || extension in navigation || extension in streams && withNavigation -> DiscKind.BluRay
            "video_ts" in folders || extension in dvd -> DiscKind.Dvd
            extension == "iso" -> DiscKind.Image
            else -> null
        }
    }
}

object VideoFileFilter {
    private val extensions = setOf("mkv", "mp4", "m4v", "avi", "ts", "m2ts", "wmv", "mov", "webm", "mpg", "mpeg")
    private val sampleName = Regex("""(^|[._\-\s\[(])sample([._\-\s\])]|$)""", RegexOption.IGNORE_CASE)

    /** The folders a release keeps its samples and extras in, by name, whatever their case. */
    private val extrasFolders = setOf("sample", "samples", "featurettes", "extras", "bonus")

    /** A RAR set's volumes: `.rar` (`.part01.rar` among them) and the old `.r00` on. */
    private val rarVolume = Regex("""\.(rar|r\d{2})$""", RegexOption.IGNORE_CASE)

    /** Whether [path] is a video by its extension; nothing in a Blu-ray's `BDMV` folder is one, its streams no film the player reads. */
    fun isVideo(path: String): Boolean = path.substringAfterLast('.', "").lowercase() in extensions && !DiscFiles.inBdmv(path)

    /**
     * Whether [path] is a video that isn't what a release is for: a sample clip by its name, or any video in a folder of
     * samples or extras (featurettes, deleted scenes, a trailer), which a release with folders keeps apart, whatever the
     * files' own names say ([inExtras]).
     */
    fun isSample(path: String): Boolean =
        isVideo(path) && (sampleName.containsMatchIn(parts(path).last().substringBeforeLast('.')) || inExtras(path))

    /** Whether one of [path]'s folders is one a release keeps its samples or extras in: "Sample/", "Featurettes/", "Extras/". */
    fun inExtras(path: String): Boolean = parts(path).dropLast(1).any { it.lowercase() in extrasFolders }

    /**
     * [path]'s folders and its name, split on either separator: a 7z made on Windows may keep its own, and a disc's
     * sample or extras beside it are told as its folders are.
     */
    internal fun parts(path: String): List<String> = path.split('/', '\\')

    /** Whether [path] is a volume of a RAR set. */
    fun isRarVolume(path: String): Boolean = rarVolume.containsMatchIn(path)

    /** Largest video that is no sample clip or extra ([isSample]); falls back to the largest video. */
    fun <F : ListedFile> defaultPick(files: List<F>): F? {
        val videos = files.filter { it.isVideo }
        return videos.filterNot { isSample(it.path) && videos.size > 1 }
            .maxByOrNull { it.size }
            ?: videos.maxByOrNull { it.size }
    }
}
