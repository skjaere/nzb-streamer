package io.skjaere.nzbstreamer.enrichment

import io.skjaere.compressionutils.ArchiveService
import io.skjaere.nzbstreamer.nzb.FilenameResolver
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.ObfuscationHeuristic
import io.skjaere.nzbstreamer.nzb.printable

/**
 * Which files of a release prepare fetches the first article of, and which of those may lack it,
 * judged by the names in their NZB subjects before anything is fetched.
 *
 * A release with archive volumes is opened from them alone (prepare lists nothing else): prepare
 * fetches those, the PAR2 index (which names obfuscated volumes) and any file whose name tells
 * nothing. A release without them is streamed file by file: prepare fetches its content (videos,
 * other files, unnamed ones) and the PAR2 index. PAR2 recovery volumes are never fetched, nor are
 * NFOs, SFVs and the like, and samples, unless they are all the release has.
 *
 * A file prepare doesn't need to open the release may lack its first article: it is left out. So
 * may another episode's: a video (or other content beside videos) while one of its size is there,
 * and an archive set (with all its volumes) when the release has several, each from its first
 * volume, while another set of its size is whole. Of its size: at least half as big, so a subtitle
 * or a trailer doesn't stand in for the movie. Any other file that lacks it fails the release.
 */
internal class ReleaseFiles(files: List<NzbFile>) {
    private enum class Role { ARCHIVE, VIDEO, PAR2, RECOVERY, SIDE, OTHER, UNKNOWN }

    private val names = files.map { FilenameResolver.extractSubjectFilename(it.subject) }
    private val sizes = files.map { file -> file.segments.sumOf { it.bytes } }
    private val roles = rolesOf(names)
    private val hasArchives = Role.ARCHIVE in roles
    private val hasVideo = Role.VIDEO in roles

    // NFOs and the like are the content of a release that has nothing else, listed as they are.
    private val sideIsContent = roles.none { it in CONTENT }

    /** Archive volumes by set: the volumes of one archive share a name but for their suffix. */
    private val sets = roles.indices.filter { roles[it] == Role.ARCHIVE }
        .groupBy { names[it]!!.replace(VOLUME_SUFFIX, "").lowercase() }.values

    // Several sets, each from its first volume: one per episode, which open on their own. Names
    // that don't group like that (obfuscated per volume) are one archive as far as anyone can tell.
    private val setsApart = sets.size > 1 && sets.all { set -> set.any { isFirstVolume(names[it]!!) } }

    /** Indices of the files whose first article prepare fetches, in order. */
    val toFetch: List<Int> = roles.indices.filter { fetched(roles[it]) }

    private fun fetched(role: Role) = when (role) {
        Role.ARCHIVE, Role.PAR2, Role.UNKNOWN -> true
        Role.VIDEO, Role.OTHER -> !hasArchives
        Role.SIDE -> sideIsContent
        Role.RECOVERY -> false
    }

    /**
     * Whether file [i] may lack its first article without failing the release at once; [needed]
     * then decides once every file has been fetched.
     */
    fun mayLack(i: Int): Boolean = when (roles[i]) {
        Role.PAR2, Role.VIDEO -> true
        Role.ARCHIVE -> setsApart
        Role.OTHER -> hasVideo
        Role.SIDE, Role.UNKNOWN, Role.RECOVERY -> false
    }

    /**
     * Of [lacking] (files that [mayLack] and lack their first article), one the release can't open
     * without: content (a video, or other content beside videos) or an archive set that no whole
     * one of its size stands in for. Null when it opens without them all.
     */
    fun needed(lacking: Set<Int>): Int? {
        val content = toFetch.filter { roles[it] == Role.VIDEO || roles[it] == Role.OTHER }
        val whole = content.filterNot { it in lacking }
        lacking.filter { it in content }.sorted()
            .firstOrNull { file -> whole.none { ofItsSize(sizes[it], sizes[file]) } }
            ?.let { return it }
        val (broken, intact) = sets.partition { set -> set.any { it in lacking } }
        return broken.firstOrNull { set -> intact.none { ofItsSize(it.sumOf(sizes::get), set.sumOf(sizes::get)) } }
            ?.first { it in lacking }
    }

    private fun ofItsSize(standIn: Long, size: Long) = standIn * 2 >= size

    /** The files to leave out when [lacking] lack their first article: those, and the rest of their archive sets. */
    fun leftOut(lacking: Set<Int>): Set<Int> = lacking + sets.filter { set -> set.any { it in lacking } }.flatten()

    /** The name a file is known by before it is fetched (its subject's), escaped for log lines. */
    fun nameOf(i: Int): String = names[i]?.printable() ?: "file ${i + 1}"

    private companion object {
        val CONTENT = setOf(Role.ARCHIVE, Role.VIDEO, Role.OTHER, Role.UNKNOWN)
        val SAMPLED = setOf(Role.ARCHIVE, Role.VIDEO)
        val RECOVERY_VOLUME = Regex("""(?i)\.vol\d+[+-]\d+\.par2$""")
        val VOLUME_SUFFIX = Regex("""(?i)(\.part\d+\.rar|\.rar|\.[rs]\d{2,3}|\.7z(\.\d{3})?)$""")
        val MODERN_RAR = Regex("""(?i)\.part(\d+)\.rar$""")
        val SPLIT_7Z = Regex("""(?i)\.7z\.(\d{3})$""")

        // "Movie-sample.mkv", "sample-movie.mkv": a sample is named so at the start or end.
        val SAMPLE = Regex("""(?i)(^sample([.\-_ ]|$))|([.\-_ ]sample$)""")
        val VIDEO_EXTENSIONS = setOf(
            "mkv", "mp4", "m4v", "avi", "mov", "wmv", "flv", "webm", "ts", "m2ts", "mts", "mpg", "mpeg", "vob", "ogm", "divx",
        )
        val SIDE_EXTENSIONS = setOf("nfo", "sfv", "srr", "srs", "md5", "sha1", "sha256", "txt", "url", "nzb", "diz")

        fun isFirstVolume(name: String): Boolean {
            MODERN_RAR.find(name)?.let { return it.groupValues[1].toIntOrNull() == 1 }
            SPLIT_7Z.find(name)?.let { return it.groupValues[1].toIntOrNull() == 1 }
            return name.endsWith(".rar", ignoreCase = true) || name.endsWith(".7z", ignoreCase = true)
        }

        /**
         * Each file's role. A sample counts as one only beside a file of its kind that isn't one: a
         * release whose only video (or archive) is named like a sample is that.
         */
        private fun rolesOf(names: List<String?>): List<Role> {
            val roles = names.map(::roleOf)
            val samples = names.indices.filter { i ->
                roles[i] in SAMPLED && names[i]?.let(::stem)?.let(SAMPLE::containsMatchIn) == true
            }
            return roles.mapIndexed { i, role ->
                val others = roles.indices.any { j -> roles[j] == role && j !in samples }
                if (i in samples && others) Role.SIDE else role
            }
        }

        private fun roleOf(name: String?): Role {
            if (name.isNullOrBlank()) return Role.UNKNOWN
            val extension = name.substringAfterLast('.', "").lowercase()
            return when {
                RECOVERY_VOLUME.containsMatchIn(name) -> Role.RECOVERY
                extension == "par2" -> Role.PAR2
                ArchiveService.fileHasKnownExtension(name) -> Role.ARCHIVE
                extension in VIDEO_EXTENSIONS -> Role.VIDEO
                extension in SIDE_EXTENSIONS -> Role.SIDE
                // Possibly an obfuscated archive volume: fetched, and needed.
                extension.isEmpty() || ObfuscationHeuristic.isProbablyObfuscated(name) -> Role.UNKNOWN
                else -> Role.OTHER
            }
        }

        /** [name] without its archive volume suffix or its extension. */
        private fun stem(name: String): String =
            if (VOLUME_SUFFIX.containsMatchIn(name)) name.replace(VOLUME_SUFFIX, "") else name.substringBeforeLast('.')
    }
}
