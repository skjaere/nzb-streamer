package io.skjaere.nzbstreamer.play

/**
 * A release's files as its source lists them, whatever the source: a prepared NZB's, or another's listed by path and
 * size. What plays from it is picked from these ([FilePicker], [FileChooser]) by the same rules for every source. [id]
 * names the listing in its source's store; [title] is the release's name.
 */
interface FileListing<out F : ListedFile> {
    val id: String
    val title: String

    /** In the source's order, each at its [ListedFile.index]. */
    val files: List<F>

    /** The largest video that isn't a sample, else the largest video ([VideoFileFilter.defaultPick]). */
    val defaultPick: F? get() = VideoFileFilter.defaultPick(files)

    /** Whether it holds no more than one video besides samples and extras: whatever plays from it plays that one. */
    val oneVideo: Boolean get() = files.count { it.isVideo && !VideoFileFilter.isSample(it.path) } <= 1

    /**
     * The file a play picked: the one named [name] (of [size] bytes, when known), [index] telling apart two of one name;
     * else, for a play from before names were kept, the one at [index]. Null when it isn't there.
     */
    fun find(index: Int, name: String?, size: Long?): F? = files.named(index, name, size)
}

/** One file of a [FileListing]: its place in the listing ([index]), its [path] there (with any folders), and its [size] in bytes. */
interface ListedFile {
    val index: Int
    val path: String
    val size: Long

    /** Its name without its folders. */
    val name: String get() = path.substringAfterLast('/')

    val isVideo: Boolean get() = VideoFileFilter.isVideo(path)
}

/**
 * Why a source that streams a release's files as it lists them (a debrid service's) can't play [this]: its video is packed in
 * RAR volumes, as a torrent's sometimes is, and no file comes out of them; null otherwise. A sample clip or an extra
 * beside the volumes is no video of the release's own, which a pick would otherwise take for the film or the episode it is
 * named for. A prepared NZB lists the files inside its archives instead, so this is never so of one.
 */
fun FileListing<*>.unstreamable(): AttemptFailure.Unsupported? =
    AttemptFailure.Unsupported(AttemptFailure.Unsupported.PACKED_IN_RAR).takeIf {
        files.none { it.isVideo && !VideoFileFilter.isSample(it.path) } && files.any { VideoFileFilter.isRarVolume(it.path) }
    }

/**
 * Why [this] can't play when its files are a copy of a disc ([DiscFiles]): a Blu-ray's, a DVD's or a disc image, which
 * play as no single video; null otherwise. Every source tells it once the listing is known, before anything is picked from
 * it.
 */
fun FileListing<*>.discCopy(): AttemptFailure.DiscCopy? = DiscFiles.kindOf(files)?.let(AttemptFailure::DiscCopy)

/**
 * [FileListing.find] by name in these files, which are listed in order: the one named [name] (of [size] bytes, when known),
 * [index] telling apart two of one name; the one at [index] when no [name] was kept. Public for a listing that overrides
 * [FileListing.find] (a prepared NZB's, which also looks for a renamed file) and falls back on the plain rule.
 */
fun <F : ListedFile> List<F>.named(index: Int, name: String?, size: Long?): F? {
    if (name == null) return getOrNull(index)
    val named = filter { it.name == name && (size == null || it.size == size) }
    return named.firstOrNull { it.index == index } ?: named.firstOrNull()
}
