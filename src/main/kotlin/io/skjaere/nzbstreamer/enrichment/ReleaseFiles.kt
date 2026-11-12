package io.skjaere.nzbstreamer.enrichment

import io.skjaere.compressionutils.ArchiveService
import io.skjaere.compressionutils.Par2Parser
import io.skjaere.compressionutils.Rar5Parser
import io.skjaere.nntp.ArticleNotFoundException
import io.skjaere.nzbstreamer.nzb.FilenameResolver
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.ObfuscationHeuristic
import io.skjaere.nzbstreamer.nzb.printable
import kotlin.math.abs

/**
 * Which files of a release prepare fetches the first article of, and which of those may lack it,
 * judged by the names in their NZB subjects before anything is fetched.
 *
 * A release with archive volumes is opened from them alone (prepare lists nothing else): prepare
 * fetches those, the PAR2 index (which names obfuscated volumes) and any file whose name tells
 * nothing. A release without them is streamed file by file: prepare fetches its content (videos,
 * other files, unnamed ones) and the PAR2 index. PAR2 recovery volumes are not fetched, but for a
 * recovery set's smallest when it has no index, or the one it has names nothing (each carries its
 * index's names, [standIns]), nor are NFOs, SFVs and the like, and samples, unless they are all the
 * release has. Where names tell nothing (an obfuscated post), the files that may be recovery volumes
 * wait for the PAR2 data of the others, which tells which of them to fetch ([deferred]).
 *
 * A file prepare doesn't need to open the release may lack its first article: it is left out. So
 * may another episode's: a video (or other content beside videos) while one of its size is there,
 * and an archive set (with all its volumes) when the release has several, each from its first
 * volume and named like a release, while another set of its size is whole. Of its size: at least
 * half as big, so a subtitle or a trailer doesn't stand in for the movie. Any other file that lacks
 * it fails the release.
 *
 * A volume posted under another extension than its archive's isn't one by its name: once
 * those files are fetched, [Volumes] finds it among the videos and other files beside
 * the archives ([mayHideVolume]), by the PAR2 data or by its content. A volume the PAR2 index names
 * that the release doesn't hold leaves its set out where an episode's may be, and fails it elsewhere,
 * and so does one a set's names or RAR5 headers show it lacks.
 */
internal class ReleaseFiles(private val files: List<NzbFile>) {
    private enum class Role { ARCHIVE, VIDEO, PAR2, RECOVERY, SIDE, OTHER, UNKNOWN }

    private val names = files.map { FilenameResolver.extractSubjectFilename(it.subject) }
    private val sizes = files.map { file -> file.segments.sumOf { it.bytes } }
    private val roles = rolesOf(names)
    private val hasArchives = Role.ARCHIVE in roles
    private val hasVideo = Role.VIDEO in roles

    // NFOs and the like are the content of a release that has nothing else, listed as they are.
    private val sideIsContent = roles.none { it in CONTENT }

    /**
     * The recovery volumes that name the files in place of a PAR2 index (each carries its index's
     * file descriptions), by recovery set: the name its volumes share but for .volNN+MM.par2, in
     * lower case, and those volumes, smallest first. A sample's set names only the sample:
     * it is none beside another. The smallest of them is fetched with the rest when the release has
     * no index, and a set's are read after them when it has none or the one it has names nothing
     * (it lacks an article).
     */
    val standIns: Map<String, List<Int>> = roles.indices.filter { roles[it] == Role.RECOVERY }
        .groupBy { names[it]!!.replace(RECOVERY_VOLUME, "").lowercase() }
        .let { sets -> sets.filterKeys { !SAMPLE.containsMatchIn(it) }.ifEmpty { sets } }
        .mapValues { (_, volumes) -> volumes.sortedBy { sizes[it] } }

    private val indexStandIn = standIns.values.map { it.first() }.minByOrNull { sizes[it] }.takeIf { Role.PAR2 !in roles }

    /** Archive volumes by set: the volumes of one archive share a name but for their suffix. */
    private val sets = roles.indices.filter { roles[it] == Role.ARCHIVE }.groupBy { stemOf(names[it]!!) }.values

    // Several sets, each from its first volume and named like a release: one per episode, which
    // open on their own. Names that don't group like that, or that look obfuscated (a8f7d6e5.rar,
    // b91c2d3e.rar: a volume each, each named like a first one), are one archive as far as anyone
    // can tell.
    private val setsApart = sets.size > 1 &&
        sets.all { set -> set.any { isFirstVolume(names[it]!!) } && !isObfuscatedSet(names[set.first()]!!) }

    /** Indices of the files whose first article prepare fetches, in order. */
    val toFetch: List<Int> = roles.indices.filter { fetched(roles[it]) || it == indexStandIn }

    /**
     * Indices of the files of [toFetch] that may be PAR2 recovery volumes, whose first articles wait
     * until the PAR2 data of the others is read: their names tell nothing or are
     * an index's (an obfuscating poster names every PAR2 file so), and they are bigger than an index
     * can be ([INDEX_LIMIT]) by the sizes the NZB gives each of their articles. An obfuscated post
     * names none of its files, so its recovery volumes, whose first articles are as big as any, were
     * fetched with its content: 16 of 3.84 MB for one remux. A file whose NZB leaves out an article's
     * size doesn't wait: its size can't tell it from content.
     */
    val deferred: List<Int> = toFetch.filter { i ->
        (roles[i] == Role.UNKNOWN || roles[i] == Role.PAR2) && sizes[i] > INDEX_LIMIT && files[i].segments.all { it.bytes > 0 }
    }

    /**
     * Of [deferred], the files to fetch once the PAR2 data of the others names [described]: all of
     * them when it names none. Else those whose names tell nothing that are at least half as big as
     * the smallest file it describes that is longer than a quarter of [INDEX_LIMIT], or all of those
     * when it describes none so long (it is a sample's set, or an extra's): a file it describes,
     * posted at least as long as it is (yEnc lengthens what it encodes), and one of its size that it
     * doesn't describe (another episode's, posted without its index). A shorter one, posted at most
     * twice as long (yEnc escapes a byte as two at most), isn't deferred. Not the rest: recovery
     * volumes, smaller than what they cover, and PAR2 files under an index's name.
     */
    fun deferredToFetch(described: List<Par2Parser.Par2FileDescription>): List<Int> {
        if (described.isEmpty()) return deferred
        val smallest = described.map { it.fileSize }.filter { it * 4 > INDEX_LIMIT }.minOrNull() ?: 0L
        return deferred.filter { roles[it] == Role.UNKNOWN && ofItsSize(sizes[it], smallest) }
    }

    /**
     * Indices of the files [toFetch] leaves that may yet be archive volumes posted under another
     * extension: the videos and other files beside archives, but for samples. Not NFOs and the
     * like, nor PAR2 files.
     */
    val mayHideVolume: List<Int> = if (!hasArchives) emptyList() else roles.indices.filter { i ->
        (roles[i] == Role.VIDEO || roles[i] == Role.OTHER) && !isSample(names[i]!!)
    }

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
        Role.PAR2, Role.RECOVERY, Role.VIDEO -> true
        Role.ARCHIVE -> setsApart
        Role.OTHER -> hasVideo
        Role.SIDE, Role.UNKNOWN -> false
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

    /**
     * The files to leave out when [lacking] lack their first article: those, the rest of their
     * archive sets, and those of [found] (volumes [Volumes] found, by the name that places them) in
     * those sets.
     */
    fun leftOut(lacking: Set<Int>, found: Map<Int, String> = emptyMap()): Set<Int> {
        val broken = sets.filter { set -> set.any { it in lacking } }
        val stems = broken.flatMap(::stemsOf).toSet()
        return lacking + broken.flatten() + found.filterValues { stemOf(it) in stems }.keys
    }

    /**
     * The names archive set [set]'s volumes go by but for their volume suffix: their subjects', and,
     * once fetched, their yEnc names', which the archive is ordered by and which an indexer that
     * rewrites subjects leaves as posted.
     */
    private fun stemsOf(set: List<Int>): Set<String> = set
        .flatMap { listOfNotNull(names[it], files[it].yencHeaders?.name) }
        .filter(VolumeNames::isVolume).map(::stemOf).toSet()

    /** The PAR2 indexes, by name, of recovery set [set] ([standIns]). */
    fun indexesOf(set: String): List<Int> =
        roles.indices.filter { roles[it] == Role.PAR2 && names[it]!!.replace(INDEX, "").lowercase() == set }

    /** Whether file [i] is a PAR2 recovery volume by its name. */
    fun isRecovery(i: Int): Boolean = roles[i] == Role.RECOVERY

    /** Whether file [i] is a PAR2 index by its name (a .par2 file that isn't a recovery volume). */
    fun isIndex(i: Int): Boolean = roles[i] == Role.PAR2

    /**
     * Whether the release needs the names its PAR2 data gives files, once the files of [kept] are
     * fetched: it has archive volumes (the PAR2 data shows one posted under another name, or one it
     * lacks), files whose names tell nothing (one may be a volume), or files it would list under
     * obfuscated names, which the PAR2 data's replace. A release of files posted under their
     * own names has its names, so no recovery volume is read for them.
     */
    fun needsNames(files: List<NzbFile>, kept: List<Int>): Boolean = kept.any { i ->
        when (roles[i]) {
            Role.ARCHIVE, Role.UNKNOWN -> true
            Role.PAR2, Role.RECOVERY -> false
            Role.VIDEO, Role.OTHER, Role.SIDE ->
                FilenameResolver.bestFilename(files[i].yencHeaders?.name, files[i].subject, par2Filename = null)
                    .let { it.isBlank() || ObfuscationHeuristic.isProbablyObfuscated(it) }
        }
    }

    /** The name a file is known by before it is fetched (its subject's), escaped for log lines. */
    fun nameOf(i: Int): String = names[i]?.printable() ?: "file ${i + 1}"

    /**
     * The archive volumes the names don't show, once the files of [kept] (those of [toFetch] not
     * left out) are fetched, their PAR2 data with them. The PAR2 data names each file by the MD5 of
     * its first 16 KB and its length. When it names a volume of a set the release opens that no file
     * holds (by that name or those 16 KB and length), or doesn't name a volume the release has
     * (there is no index, or not that set's), [toCheck] lists the files of [mayHideVolume] to fetch:
     * one the PAR2 data names a volume is one, and, where it doesn't name them all, one that starts
     * with an archive's signature is too.
     */
    inner class Volumes(private val files: List<NzbFile>, private val kept: List<Int>) {
        private val descriptions = VolumeNames.descriptions(kept.map { files[it] })
        private val par2Names = VolumeNames.names(descriptions)
        private val byName = descriptions.associate { it.filename.lowercase() to it.filename }
        private val hashed = hashes()

        // A volume kept that the PAR2 data doesn't name: the content tells which of the others is one.
        private val byContent = kept.any { i ->
            par2Name(i, hashed)?.let(VolumeNames::isVolume) != true &&
                (roles[i] == Role.ARCHIVE || roles[i] == Role.UNKNOWN && VolumeNames.isArchive(files[i].first16kb))
        }

        // The volumes the PAR2 data names that no file holds, before any of [toCheck] is fetched.
        private val unheld = unheld(opened(hashed, emptyMap()), hashed)

        /** Indices of the files whose first article to fetch to find the volumes the names don't show. */
        val toCheck: List<Int> = if (byContent || unheld.isNotEmpty()) mayHideVolume else emptyList()

        /**
         * Once [toCheck] are fetched, those [unknown] (lacking their first article) left: the volumes
         * among them, each by the name that places it in its archive (the PAR2 data's, or its own but
         * for the extension it was posted under), and the sets that lack a volume the PAR2 data
         * names, by one of their files with the cause. [Found.Missing] when the release can't open
         * whole: a set that lacks a volume isn't an episode's, or a file whose content was to tell
         * lacks its first article, and is a volume the release lacks of a set that isn't an episode's
         * or that nothing places it in: one its name places in a set the release opens, whatever its
         * size (a set's last volume is smaller than the rest), or one its name places nowhere (or,
         * where every set the release opens is named obfuscated, in a set of its own) that may be a
         * volume by its size ([mayBeVolume]). One its name places where the release holds a volume,
         * or in another archive, and one that can't be a volume fail nothing.
         * [Found.Unplaced] when nothing tells where a volume goes. One its
         * content shows to be a volume whose name places it in a set the release doesn't open, by
         * any name it goes by ([opened]), is another archive's (an extra's), none of the release's:
         * it is left alone, as files under another extension were before such volumes were looked
         * for; but where every set the release opens is named obfuscated, so that a volume of its
         * own may go by a name of its own, it is [Found.Unplaced].
         * A set whose volumes skip one ([skipping]) lacks it as one that lacks a volume the PAR2 data
         * names does. A volume known by its content alone that nothing places, beside
         * others the PAR2 data names, is [Found.Unplaced] too ([unplacedBesideNamed]), and so is one
         * that only its own name places, in a set of its own beside a volume of another set, where
         * every set the release opens is named obfuscated ([placedByItsNameAlone]). A release that is
         * one set, or one volume, posted under another extension opens by the names it gives.
         */
        fun found(unknown: Map<Int, Exception>): Found {
            val hashes = hashes()
            val incomplete = mutableMapOf<Int, Exception>()
            val openedSets = opened(hashes, emptyMap())
            val held = kept.flatMap { listOfNotNull(par2Name(it, hashes) ?: names[it], files[it].yencHeaders?.name) }
                .map { it.lowercase() }.toMutableSet()
            if (byContent) {
                val setSizes = volumeSizes(hashes)
                for ((i, cause) in unknown.toSortedMap()) {
                    val placed = (files[i].yencHeaders?.name ?: names[i])?.let(VolumeNames::of)
                    val lacked = when {
                        placed == null -> mayBeVolume(i, setSizes)
                        placed.lowercase() in held -> false
                        stemOf(placed) in openedSets -> true
                        else -> obfuscatedOnly(hashes) && mayBeVolume(i, setSizes)
                    }
                    if (!lacked) continue
                    val set = placed?.let { sets.firstOrNull { set -> stemOf(it) in stemsOf(set) } }
                    if (set == null || !setsApart) return Found.Missing(cause)
                    incomplete.putIfAbsent(set.first(), cause)
                }
            }
            unplacedBesideNamed(hashes)?.let { return Found.Unplaced(nameOf(it)) }
            placedByItsNameAlone(hashes)?.let { return Found.Unplaced(nameOf(it)) }
            val found = mutableMapOf<Int, String>()
            for (i in toCheck.filterNot { it in unknown }) {
                val named = describedAs(i, hashes)
                val volume = when {
                    named != null -> named.takeIf { it in unheld }
                    byContent && VolumeNames.isArchive(files[i].first16kb) -> {
                        val placed = VolumeNames.of(files[i].yencHeaders?.name ?: names[i]!!) ?: return Found.Unplaced(nameOf(i))
                        when {
                            stemOf(placed) in openedSets -> placed
                            obfuscatedOnly(hashes) -> return Found.Unplaced(nameOf(i))
                            else -> null
                        }
                    }
                    else -> null
                }
                if (volume != null && held.add(volume.lowercase())) found[i] = volume
            }
            for (volume in unheld(opened(hashes, found), hashes)) {
                // With no file that may be it lacking its first article, no provider can supply it.
                val cause = if (unknown.isEmpty()) {
                    ArchiveVolumeNotInNzbException("Archive volume '${volume.printable()}', which the PAR2 index names, isn't in the NZB")
                } else {
                    ArticleNotFoundException(
                        "Archive volume '${volume.printable()}', which the PAR2 index names, isn't in the NZB " +
                            "or lacks its first article",
                    )
                }
                val set = sets.firstOrNull { stemOf(volume) in stemsOf(it) }
                if (set == null || !setsApart) return Found.Missing(cause)
                incomplete.putIfAbsent(set.first(), cause)
            }
            for ((first, cause) in skipping(hashes, found, toCheck.filterNot { it in unknown })) {
                if (!setsApart) return Found.Missing(cause)
                incomplete.putIfAbsent(first, cause)
            }
            return Found.Volumes(found, incomplete)
        }

        /**
         * The size the NZB gives the volumes of each set the release opens but its last, by the
         * set's name: its biggest volume's. A set's are of a size, but for its last.
         */
        private fun volumeSizes(hashes: Map<Int, String>): Map<String, Long> = kept
            .filter(::isArchiveVolume)
            .groupBy { stemOf(par2Name(it, hashes) ?: files[it].yencHeaders?.name ?: names[it].orEmpty()) }
            .mapValues { (_, set) -> set.maxOf(sizes::get) }

        /**
         * Whether file [i], which lacks its first article and whose name doesn't place it in a set
         * the release opens, may be a volume of one whose absence nothing else shows: one but the
         * set's last, as big as its biggest ([volumeSizes]) by the size the NZB gives it, within the
         * few bytes yEnc's escapes and names add to a volume posted as big. A bigger file is none of
         * its volumes, and a smaller one could be its last only, which, under a name that doesn't
         * place it, is seen missing no more than one not posted.
         */
        private fun mayBeVolume(i: Int, volumeSizes: Map<String, Long>): Boolean =
            volumeSizes.values.any { size -> abs(sizes[i] - size) * SIZE_TOLERANCE <= size }

        /**
         * Of the kept files that are archive volumes by their content alone (posted under names
         * that tell nothing), one that nothing places (no PAR2 name nor its own name, nor a RAR5
         * header that numbers it) while others are named (by the PAR2 data, or their own names):
         * ordered by those names, it would follow them wherever it goes. Null when there is
         * none, or none of them is named. RAR5 volumes are placed by their headers, the named and
         * the unnamed together.
         */
        private fun unplacedBesideNamed(hashes: Map<Int, String>): Int? {
            val (named, unnamed) = kept.filter { roles[it] == Role.UNKNOWN && VolumeNames.isArchive(files[it].first16kb) }
                .partition { i ->
                    par2Name(i, hashes)?.let(VolumeNames::isVolume) == true ||
                        (files[i].yencHeaders?.name ?: names[i])?.let(VolumeNames::of) != null
                }
            if (named.isEmpty()) return null
            return unnamed.firstOrNull { files[it].first16kb?.let(Rar5Parser::volumeNumber) == null }
        }

        /**
         * Of the kept files that are archive volumes by their content alone, one placed by nothing
         * but its own name (without the extension it was posted under), in a set no other kept file
         * gives, beside a kept volume of another set, while every volume the release opens is named
         * obfuscated ([obfuscatedOnly]). Its name may be as obfuscated as theirs, so the place it
         * gives can't be trusted: x7f3k9q2.r00.bin beside a8f7d6e5.rar and c07e4f1a.rar would be
         * ordered after them, whichever volume it is. A file's sets are those its names give as
         * volume names ([VolumeNames.of]): x7f3k9q2.rar.bin, x7f3k9q2.r00.bin and x7f3k9q2.r01.bin
         * make one, so a release that is one set, or one volume, posted under another extension
         * opens by its names. Null when there is none. One the PAR2 data names is placed by that
         * name, and a RAR5 volume whose headers number it by them ([Rar5Parser.volumeNumber]).
         */
        private fun placedByItsNameAlone(hashes: Map<Int, String>): Int? {
            if (!obfuscatedOnly(hashes)) return null
            // The sets each kept file's names give: its PAR2 name or subject name, and its yEnc name.
            val setsOf = kept.associateWith { j ->
                listOfNotNull(par2Name(j, hashes) ?: names[j], files[j].yencHeaders?.name)
                    .mapNotNull(VolumeNames::of).map(::stemOf).toSet()
            }
            return kept.firstOrNull { i ->
                val placed = (files[i].yencHeaders?.name ?: names[i])?.let(VolumeNames::of)
                placed != null && roles[i] == Role.UNKNOWN && VolumeNames.isArchive(files[i].first16kb) &&
                    par2Name(i, hashes)?.let(VolumeNames::isVolume) != true &&
                    files[i].first16kb?.let(Rar5Parser::volumeNumber) == null &&
                    stemOf(placed).let { set ->
                        val others = kept.filter { it != i }
                        others.none { set in setsOf.getValue(it) } &&
                            others.any { isArchiveVolume(it) && setsOf.getValue(it).any { other -> other != set } }
                    }
            }
        }

        /**
         * The sets the release opens, by their names (not obfuscated ones), whose volumes skip one,
         * each by its first file with the cause. A set's volumes are its files, the volumes
         * of [found] (found among the others, by the name that places them) in it, and the files of
         * [kept] the PAR2 data names into it, posted under names that tell nothing or obfuscated by
         * themselves, as the archive is ordered. Their places are those their RAR5 headers give
         * ([Rar5Parser.volumeNumber]) when each of them does, else, when no PAR2 data names one of
         * them, those their names give ([VolumeNames.number]), a file of [present] whose name places
         * it in the set counting as the volume it names when it is an archive by its content, or a
         * 7z volume (one past the first has no signature to show it is one). A set whose places skip
         * one or don't start at its first lacks one; a missing last volume isn't seen this way.
         */
        private fun skipping(
            hashes: Map<Int, String>,
            found: Map<Int, String>,
            present: List<Int>,
        ): Map<Int, ArchiveVolumeNotInNzbException> = sets
            .filter { set -> set.all { it in kept } && !isObfuscatedSet(names[set.first()]!!) }
            .mapNotNull { set ->
                val stems = stemsOf(set)
                val namedInto = kept.filterNot { it in set }.mapNotNull { i ->
                    par2Name(i, hashes)?.takeIf { VolumeNames.isVolume(it) && stemOf(it) in stems }?.let { i to it }
                }
                val volumes = set.associateWith { names[it]!! } + namedInto + found.filterValues { stemOf(it) in stems }
                val headers = volumes.keys.map { files[it].first16kb?.let(Rar5Parser::volumeNumber) }
                val places = if (headers.all { it != null }) headers.filterNotNull() else {
                    val described = volumes.keys.any { par2Name(it, hashes) != null } ||
                        descriptions.any { stemOf(it.filename) in stems }
                    if (described) return@mapNotNull null
                    val claimed = present.mapNotNull { i ->
                        VolumeNames.of(files[i].yencHeaders?.name ?: names[i]!!)?.takeIf {
                            stemOf(it) in stems && (VolumeNames.isArchive(files[i].first16kb) || VolumeNames.isSplit7z(it))
                        }
                    }
                    (volumes.values + claimed).map { VolumeNames.number(it) ?: return@mapNotNull null }
                }
                val absent = (0..places.max()).firstOrNull { it !in places } ?: return@mapNotNull null
                set.first() to ArchiveVolumeNotInNzbException(
                    "Archive volume ${absent + 1} of the set of '${nameOf(set.first())}' isn't in the NZB",
                )
            }.toMap()

        // The MD5 of the first 16 KB of each file fetched so far.
        private fun hashes(): Map<Int, String> =
            files.indices.mapNotNull { i -> files[i].first16kb?.let { i to VolumeNames.md5Hex(it) } }.toMap()

        /** The name the PAR2 data gives file [i]: by its first 16 KB once fetched ([hashes]), else by its own. */
        private fun par2Name(i: Int, hashes: Map<Int, String>): String? =
            describedAs(i, hashes) ?: names[i]?.lowercase()?.let(byName::get)

        /** The name the PAR2 data gives file [i] by its first 16 KB ([hashes]) and its length, once fetched. */
        private fun describedAs(i: Int, hashes: Map<Int, String>): String? =
            hashes[i]?.let { hash -> files[i].yencHeaders?.size?.let { par2Names.nameOf(hash, it) } }

        /**
         * The sets whose volumes the release opens: those of [kept], and of [found], by every name
         * they go by: the PAR2 data's or their subjects', and their yEnc names', which the archive is
         * ordered by.
         */
        private fun opened(hashes: Map<Int, String>, found: Map<Int, String>): Set<String> =
            (kept.flatMap { listOfNotNull(par2Name(it, hashes) ?: names[it], files[it].yencHeaders?.name) } + found.values)
                .filter(VolumeNames::isVolume).map(::stemOf).toSet()

        /** Whether file [i] is an archive volume: by its name, or, where its name tells nothing, by its content. */
        private fun isArchiveVolume(i: Int): Boolean =
            roles[i] == Role.ARCHIVE || roles[i] == Role.UNKNOWN && VolumeNames.isArchive(files[i].first16kb)

        /**
         * Whether every archive volume of [kept] is named obfuscated by every name it goes by (or
         * goes by none that is a volume's): a volume of the release's may then go by a name of its own.
         */
        private fun obfuscatedOnly(hashes: Map<Int, String>): Boolean = kept
            .filter(::isArchiveVolume)
            .none { i ->
                listOfNotNull(names[i], files[i].yencHeaders?.name, par2Name(i, hashes))
                    .any { VolumeNames.isVolume(it) && !isObfuscatedSet(it) }
            }

        /**
         * The volumes the PAR2 data names of the sets [opened] that no file holds, by name or by
         * first 16 KB and length.
         */
        private fun unheld(opened: Set<String>, hashes: Map<Int, String>): List<String> {
            val named = names.mapNotNull { it?.lowercase() }.toSet()
            val fetched = hashes.mapNotNull { (i, hash) -> files[i].yencHeaders?.let { hash to it.size } }.toSet()
            return descriptions.filter { d ->
                VolumeNames.isVolume(d.filename) && stemOf(d.filename) in opened &&
                    d.filename.lowercase() !in named && (d.hash16kHex() to d.fileSize) !in fetched
            }.map { it.filename }.distinct()
        }
    }

    /** What [Volumes.found] finds. */
    sealed interface Found {
        data class Volumes(val volumes: Map<Int, String>, val incomplete: Map<Int, Exception>) : Found

        /**
         * The release can't open whole: [cause] is an [ArticleNotFoundException], or an
         * [ArchiveVolumeNotInNzbException] where no provider can supply what it lacks, or
         * where a provider its breaker passed over couldn't serve a first article the others lack,
         * that provider's failure.
         */
        data class Missing(val cause: Exception) : Found
        data class Unplaced(val name: String) : Found
    }

    private companion object {
        // A volume posted as big as another is so within a hundredth, by the sizes the NZB gives.
        const val SIZE_TOLERANCE = 100

        // More than a PAR2 index takes as posted for a release of up to about 3,000 files: it holds a
        // Main packet, a FileDesc and an IFSC packet for each file (about 400 bytes a file with its
        // name), and 20 bytes for each of at most 32,768 slices (640 KiB).
        const val INDEX_LIMIT = 2L * 1024 * 1024

        val CONTENT = setOf(Role.ARCHIVE, Role.VIDEO, Role.OTHER, Role.UNKNOWN)
        val SAMPLED = setOf(Role.ARCHIVE, Role.VIDEO)
        val RECOVERY_VOLUME = Regex("""(?i)\.vol\d+[+-]\d+\.par2$""")
        val INDEX = Regex("""(?i)\.par2$""")
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
         * Whether the set volume [name] belongs to is named obfuscated: its name but for the volume
         * suffix, which the heuristic judges without its extension (so it is given one).
         */
        fun isObfuscatedSet(name: String): Boolean =
            ObfuscationHeuristic.isProbablyObfuscated(name.replace(VOLUME_SUFFIX, ".rar"))

        /** The set volume [name] belongs to: its name but for the volume suffix, in lower case. */
        fun stemOf(name: String): String = name.replace(VOLUME_SUFFIX, "").lowercase()

        /** Whether [name] is a sample's ("Movie-sample.mkv", "Movie.2020-sample.part1.rar"), by its stem. */
        fun isSample(name: String): Boolean = SAMPLE.containsMatchIn(stem(name))

        /**
         * Each file's role. A sample counts as one only beside a file of its kind that isn't one: a
         * release whose only video (or archive) is named like a sample is that.
         */
        private fun rolesOf(names: List<String?>): List<Role> {
            val roles = names.map(::roleOf)
            val samples = names.indices.filter { i -> roles[i] in SAMPLED && names[i]?.let(::isSample) == true }
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
