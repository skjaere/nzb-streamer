package io.skjaere.nzbstreamer.play

import io.skjaere.compressionutils.ArchiveService
import io.skjaere.nntp.NntpDispatchers
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.nzb.NzbDocument
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.Locale
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * A release to try, in ranked order. [id] is opaque to the pipeline (an app's indexer and guid, say).
 * [indexer] says which indexer serves it; candidates without one count as one indexer.
 * [passworded]: its indexer says its archive needs a password. [wasPlaying]: the release the player
 * just left while it still played, which the play goes back to ([PlayPipeline.play]'s last).
 * [source]: where it plays from, which [PlaySource] tries it: [USENET] for an NZB. [routes]: what may
 * play it, each of which a failure can stop for the rest of the play ([AttemptFailure.route]); once
 * every one is stopped, it isn't tried.
 */
data class PlayCandidate(
    val id: String,
    val title: String,
    val indexer: String? = null,
    val passworded: Boolean = false,
    val wasPlaying: Boolean = false,
    val source: String = USENET,
    val routes: List<String> = listOf(USENET),
) {
    companion object {
        /**
         * [source] of a release that plays from Usenet, its NZB from its indexer and its articles from the providers; and
         * the [routes] entry of the providers, which a provider failure stops.
         */
        const val USENET = "usenet"
    }
}

/** Gets a candidate's NZB, usually an indexer grab. Throws [FetchException] on failure. */
fun interface NzbSource {
    suspend fun fetch(candidate: PlayCandidate): ByteArray

    /**
     * [fetch], with the archive password that came with the NZB from elsewhere than the NZB itself (its indexer sent it
     * beside it, or the app kept it with it), if one did; the pipeline asks this. By default none did.
     */
    suspend fun fetchWithPassword(candidate: PlayCandidate): FetchedNzb = FetchedNzb(fetch(candidate))
}

/** An NZB's [bytes] and the archive [password] that came with them, if any ([NzbSource.fetchWithPassword]). */
class FetchedNzb(val bytes: ByteArray, val password: String? = null)

/**
 * [blocksIndexer]: the candidate's indexer would refuse its other releases too (download limit
 * used up, API key refused). [stopsAll]: every later Usenet candidate would fail the same way: it
 * stops the Usenet route ([PlayCandidate.USENET]).
 */
class FetchException(
    val reason: String,
    val stopsAll: Boolean = false,
    val blocksIndexer: Boolean = false,
    cause: Throwable? = null,
) : Exception(reason, cause)

fun interface AvailabilityBackend {
    /** STATs articles of [nzb]; [progress] gets (checked, total) as answers arrive, from any thread. */
    suspend fun verify(nzb: NzbDocument, progress: (checked: Int, total: Int) -> Unit): VerificationResult
}

/**
 * Releases remembered as missing on the providers, as needing a password they don't come with, or as
 * copies of a disc (by indexer and guid, say). [knownMissing]: why [candidate] isn't worth its grab, as the
 * play's progress says it; null to try it. The pipeline asks
 * it of a candidate before trying it, so what [missing] needs of that moment (the providers in use)
 * can be noted then, and of those after it to count the ones left to try, as an attempt starts and
 * once the attempts run out. The play's progress waits on it: it shouldn't wait on what a use of the
 * streamer would (checking the providers again). [missing]: its articles were just found
 * missing [on] the providers. [found]: it just passed every step, so what was remembered of it for
 * the providers in use is wrong (a provider's bad day, a post still propagating) and the next play
 * tries it. [passwordNeeded]: its archive is encrypted, and no password came with it. [discCopy]: its
 * files are a copy of a disc ([AttemptFailure.DiscCopy]), which no play of it plays, whatever file it
 * picked. None has a default: a memory that wraps others (one per source) must pass each on.
 */
interface TriedReleases {
    suspend fun knownMissing(candidate: PlayCandidate): String?
    suspend fun missing(candidate: PlayCandidate, on: MissingOn)
    suspend fun found(candidate: PlayCandidate)
    suspend fun passwordNeeded(candidate: PlayCandidate)
    suspend fun discCopy(candidate: PlayCandidate)

    companion object {
        /** Remembers nothing, and skips nothing. */
        val None: TriedReleases = object : TriedReleases {
            override suspend fun knownMissing(candidate: PlayCandidate): String? = null
            override suspend fun missing(candidate: PlayCandidate, on: MissingOn) = Unit
            override suspend fun found(candidate: PlayCandidate) = Unit
            override suspend fun passwordNeeded(candidate: PlayCandidate) = Unit
            override suspend fun discCopy(candidate: PlayCandidate) = Unit
        }
    }
}

/** Where a release's articles were found missing. */
enum class MissingOn {
    /**
     * On every provider in use, as the availability check found: each answered that it lacks them. One left out
     * wasn't asked; one passed over for its open circuit breaker fails the check instead.
     */
    ProvidersInUse,

    /**
     * On the providers fetched from, as opening the release, probing it or playing it found: nzb-streamer passes over
     * a provider whose circuit breaker is open without asking it, and says an article the others lack is on no
     * provider. It doesn't say which it asked: every provider in use only while there is one.
     */
    ProvidersFetchedFrom,

    /**
     * On any provider: the NZB itself lacks a file. The pipeline doesn't say so while nzb-streamer can report a file the
     * NZB holds as one it lacks (when the providers fetched from lack its first article).
     */
    AnyProvider,
}

/**
 * Asks which of a release's videos is the one to play where their names don't tell ([FilePicker.choices]): the user. It
 * waits for the answer, one of [choices] in [listing], or null to go on without one, which fails the attempt as finding no
 * file. [None] asks no one.
 */
fun interface FileChooser {
    suspend fun choose(candidate: PlayCandidate, listing: FileListing<*>, choices: List<ListedFile>): ListedFile?

    companion object {
        val None = FileChooser { _, _, _ -> null }
    }
}

/** Opens the picked file far enough to read its track list. */
fun interface TrackProbe {
    suspend fun probe(target: PlaybackTarget): ProbeResult

    companion object {
        val None = TrackProbe { ProbeResult.Playable() }
    }
}

/**
 * What the probe read of the video that plays, for a player that matches the display's frame rate before the player
 * exists: its frame rate ([fps], null when the file doesn't state one), its length ([durationMs], null when unknown), and
 * whether it is Dolby Vision. Plain numbers, to keep this module free of the player's types.
 */
data class ProbedVideo(val fps: Double?, val durationMs: Long?, val dolbyVision: Boolean)

/**
 * A file's tracks as the probe read them, each kind in the file's order: what a verdict for a Cast device reads, as such a
 * device plays the first video track and the first audio track it can play, whatever the local decoders make of
 * them. Plain values, to keep this module free of the player's types; whoever starts the player keeps them.
 */
data class ProbedTracks(
    val video: List<ProbedTrack> = emptyList(),
    val audio: List<ProbedTrack> = emptyList(),
    val text: List<ProbedTrack> = emptyList(),
)

/**
 * One track of a file: its codec as its MIME type ([mime], null when the file names none) and in the release list's
 * words ([label]: "HEVC", "DTS-HD", "SRT"); its [language], Media3's normalised code ("en", "no-nob"), null when the file
 * doesn't say; whether the file flags it [default] and [forced]. Video has its [width] and [height], its [bitDepth], its
 * [hdr] and its Dolby Vision profile ([dolbyVisionProfile]), audio its [channels]: each null where the file doesn't say.
 */
data class ProbedTrack(
    val mime: String?,
    val label: String,
    val language: String? = null,
    val default: Boolean = false,
    val forced: Boolean = false,
    val width: Int? = null,
    val height: Int? = null,
    val bitDepth: Int? = null,
    val hdr: Hdr? = null,
    val dolbyVisionProfile: Int? = null,
    val channels: Int? = null,
)

/** A video track's high dynamic range format ([ProbedTrack.hdr]), with its [label] in the release list's words. */
enum class Hdr(val label: String) { Hdr10("HDR10"), Hdr10Plus("HDR10+"), DolbyVision("Dolby Vision"), Hlg("HLG") }

sealed interface ProbeResult {
    /**
     * [video]: what the probe read of the video that plays; null for audio alone, or a probe that reads nothing of it.
     * [tracks]: the file's tracks, null from a probe that reads none.
     */
    data class Playable(val video: ProbedVideo? = null, val tracks: ProbedTracks? = null) : ProbeResult
    /**
     * [found] describes the audio tracks, e.g. "E-AC-3 (en)"; [tracks] and [video] are what the probe read of the file, as
     * [Playable]'s: a Cast device may play what this device can't.
     */
    data class NoPlayableAudio(val found: List<String>, val tracks: ProbedTracks? = null, val video: ProbedVideo? = null) : ProbeResult
    /**
     * [found] describes the video tracks as the ranker names releases, e.g. "2160p Dolby Vision profile 7": the file has
     * video and no decoder of this device takes any of it. Checked before the audio, as without a picture that doesn't matter.
     * [tracks] and [video] are what the probe read of the file, as [NoPlayableAudio]'s.
     */
    data class NoPlayableVideo(val found: List<String>, val tracks: ProbedTracks? = null, val video: ProbedVideo? = null) : ProbeResult

    /**
     * A play to a Cast device: the file is of a type the device can't play, by its [extension] (lowercase, empty for
     * none), so it wasn't read. The device the play started on may play it.
     */
    data class NotForCast(val extension: String) : ProbeResult

    /**
     * A play to a Cast device whose file the device can't fully play, by the verdict on its tracks, found before anything
     * was sent to it: the attempt fails as [failure] says. The device the play started on may play it. None names the
     * Cast device.
     */
    sealed interface CastMiss : ProbeResult {
        val failure: AttemptFailure.CastMiss
    }

    /** No audio track of the file is one a Cast device plays: the picture would play silent. [found] names them ("DTS-HD"). */
    data class NoCastAudio(val found: List<String>) : CastMiss {
        override val failure get() = AttemptFailure.NoCastAudio(found)
    }

    /** The device shows no picture of the file's first video track, [found] by its codec and ten bits ("VC-1", "HEVC 10-bit"). */
    data class NoCastVideo(val found: String) : CastMiss {
        override val failure get() = AttemptFailure.NoCastVideo(found)
    }

    /** The file's first video track, of [height] lines (its resolution class: 2160), is over the largest the device shows of it, [maxHeight]. */
    data class TooBigForCast(val height: Int, val maxHeight: Int) : CastMiss {
        override val failure get() = AttemptFailure.TooBigForCast(height, maxHeight)
    }

    /**
     * [unreadable]: how the player couldn't read the file's bytes ([UnreadableFile]), null when it failed otherwise (a
     * missing article, a link no longer to be had); then [error] says why. Bytes of no format the player knows, or broken
     * ones, may be a damaged file, or one an archive holds encrypted though its headers aren't, which is read as it is
     * stored (kotlin-compression-utils reads no file's own encryption record, so prepare lists and opens it as if it were
     * plain). [cause]: what the probe's read failed with, for a source to read its own failures from (a debrid link no
     * longer to be had); not compared.
     */
    data class Failed(val error: UsenetError, val unreadable: UnreadableFile? = null) : ProbeResult {
        var cause: Throwable? = null
            private set

        constructor(error: UsenetError, unreadable: UnreadableFile?, cause: Throwable?) : this(error, unreadable) {
            this.cause = cause
        }
    }
}

/**
 * How the player couldn't read a file's bytes, as the probe tells it from what its read failed with, said in plain
 * words wherever it is shown ([AttemptFailure.Unreadable]); the player's own words go to the log alone.
 */
enum class UnreadableFile {
    /** No extractor took the file: a format the player doesn't know, or bytes that look random (data encrypted). */
    UnknownFormat,

    /** An extractor took the file, then failed on it: damaged, or using what the player doesn't support. */
    Broken,

    /** What read the file crashed, no read of its bytes having failed: it may be the app's own fault, not the release's. */
    ReaderCrashed,
}

/**
 * What an attempt opened to play ([PlaySource.attempt]): [candidate]'s [file] in its [listing], which the player and the
 * track probe read at [uri] once it is kept ([keep]).
 */
interface PlaybackTarget {
    val candidate: PlayCandidate
    val listing: FileListing<*>
    val file: ListedFile

    /** Where the player's data source reads [file]: the file's own address, which names no secret. */
    val uri: String

    /** Keeps what the data source needs to read [uri] where it finds it. Throws when it can't, as on a full disk. */
    suspend fun keep()

    /**
     * Lets go of what [keep] kept, for an attempt that failed, unless it was kept before: a play's failed attempts take
     * none of the places in a store that resuming a play needs. What another keeps meanwhile stays.
     */
    suspend fun drop() = Unit
}

/**
 * In the order they run, except that a release that may hold more than the file to play is checked after it is opened.
 * Each source runs its own of them ([PlaySource.steps]): Usenet's from [FetchNzb]; a debrid service's from [Prepare], one
 * step for whatever the service needs before a link, whichever service it is.
 */
enum class PipelineStep { Prepare, FetchNzb, CheckAvailability, Open, ProbeTracks }

/** What a release's files are a copy of ([AttemptFailure.DiscCopy]): a Blu-ray disc, a DVD, or a disc image (`.iso`). */
enum class DiscKind { BluRay, Dvd, Image }

/**
 * Why one candidate didn't play. [text] is what the user is shown of it, beside the step that failed, and what the log
 * says. [route]: the route it stops ([PlayCandidate.routes]), by which every other candidate would fail too; null when
 * it stops none. [blocksIndexer]: those from the same indexer would.
 */
sealed class AttemptFailure(val text: String, val route: String? = null, val blocksIndexer: Boolean = false) {
    class FetchFailed(reason: String, route: String? = null, blocksIndexer: Boolean = false) :
        AttemptFailure("Couldn't get the NZB: $reason", route, blocksIndexer)
    /**
     * [missing] of the [checked] articles the availability check had answers for are on no
     * provider; null when the gap was found another way (opening the release, playing it).
     * [providersLeftOut]: a provider was left out (its login rejected, or out of reach), which may
     * have them. [unanswered]: why a provider couldn't answer for them, which may have them too; the
     * others lack them. [notInNzb]: what is missing is a file the NZB doesn't hold, which no
     * provider has.
     */
    class MissingArticles(
        val detail: String,
        val missing: Int? = null,
        val checked: Int? = null,
        val providersLeftOut: Boolean = false,
        val unanswered: UsenetError? = null,
        val notInNzb: Boolean = false,
    ) : AttemptFailure(
        when {
            unanswered != null -> "Incomplete on the providers that answered · a provider failed: ${unanswered.detail ?: unanswered::class.simpleName}"
            providersLeftOut -> "Incomplete on the providers in use"
            else -> "Incomplete on your providers"
        } + if (missing != null && checked != null) " · $missing of $checked checked article${if (checked == 1) "" else "s"} missing" else "",
    ) {
        /** Whether a provider that wasn't asked or couldn't answer may have them: then the release isn't known to be incomplete. */
        val mayBeWhole: Boolean get() = providersLeftOut || unanswered != null

        /**
         * Where they are known to be missing: as the availability check found them ([checked]), on the providers in use,
         * every one of which answered that it lacks them (one left out wasn't asked); as fetching them found them, on
         * the providers fetched from; null while one couldn't answer. A file the NZB seems to lack ([notInNzb]) is
         * fetching's too, not on any provider: nzb-streamer also says so of a file the NZB holds whose first article the
         * providers fetched from lack, which a provider not asked may have.
         */
        val missingOn: MissingOn?
            get() = when {
                unanswered != null -> null
                checked != null -> MissingOn.ProvidersInUse
                else -> MissingOn.ProvidersFetchedFrom
            }
    }
    data object Passworded : AttemptFailure("Passworded")
    class Unsupported(val detail: String) : AttemptFailure("Can't be streamed: $detail") {
        companion object {
            /** A torrent whose video is only in RAR volumes, which its service streams nothing out of ([unstreamable]). */
            const val PACKED_IN_RAR = "its video is packed in RAR archives"
        }
    }
    data object NoVideo : AttemptFailure("No video file")
    data class NoMatchingEpisode(val season: Int, val episode: Int) : AttemptFailure("No file for S%02dE%02d".format(season, episode))
    /**
     * The file that played before isn't in the release any more, and nothing tells which other one
     * is the same: the one named [name], or, remembered by its position alone (null), the one
     * at a position that is now past the end of the release's files or holds no video.
     */
    data class NoRememberedFile(val name: String?) : AttemptFailure(name?.let { "No file '$it'" } ?: "Can't tell which file played before")
    class NoPlayableAudio(val found: List<String>) :
        AttemptFailure("No audio this device can play" + if (found.isEmpty()) "" else " (${found.joinToString()})")
    /**
     * No decoder of this device takes the file's video ([found], e.g. "2160p Dolby Vision profile 7"): the release's,
     * as another of the title may play, but this device's limit, and sometimes its screen's, so it isn't remembered against
     * the release (what is remembered of releases may be shared with other devices).
     */
    class NoPlayableVideo(val found: List<String>) :
        AttemptFailure("No video this device can play" + if (found.isEmpty()) "" else ": ${found.joinToString()}")
    /**
     * A play to a Cast device whose file is of a type the device can't play ([ProbeResult.NotForCast]), found before
     * anything was sent to it. The release plays on the device the play started on, so it isn't remembered against it; the
     * next release is tried as for any attempt.
     */
    class NotForCast(val extension: String) : AttemptFailure("${kindOf(extension)}. Cast devices play MKV, MP4, WebM and MOV.") {
        /** The file's type in words: "AVI file". */
        val kind: String get() = kindOf(extension)

        private companion object {
            fun kindOf(extension: String) = if (extension.isEmpty()) "Unknown file type" else "${extension.uppercase(Locale.ROOT)} file"
        }
    }
    /**
     * The player can't read the file the attempt opened, as the probe found it ([kind]), by every source alike: in
     * plain words, never the player's own, its type named by its [extension] in capitals ("M2TS", which points at a disc),
     * null where its name gives none fit to show ([unreadableFailure]). "May be damaged", as an extractor that took the
     * file also fails on what it doesn't support. A reader that crashed is said as the player says a read that failed.
     */
    data class Unreadable(val kind: UnreadableFile, val extension: String?) : AttemptFailure(
        when (kind) {
            UnreadableFile.UnknownFormat -> extension?.let { "The player can't read this $it file" } ?: "The player can't read this file's format"
            UnreadableFile.Broken -> (extension?.let { "The player can't read this $it file" } ?: "The player can't read this file") + ": it may be damaged"
            UnreadableFile.ReaderCrashed -> "Reading the release failed"
        },
    )
    /**
     * The release is a copy of a disc ([kind], told by its files: [DiscFiles]), which plays as no single video: it
     * fails as soon as its files are listed, before any is picked or probed, in plain words that name no app (whichever
     * app shows them), and is remembered so, so the next play skips it without a grab ([TriedReleases.discCopy]).
     */
    data class DiscCopy(val kind: DiscKind) : AttemptFailure(
        when (kind) {
            DiscKind.BluRay -> "A copy of a Blu-ray disc"
            DiscKind.Dvd -> "A copy of a DVD"
            DiscKind.Image -> "A disc image (ISO)"
        } + ": only single video files can be played",
    )
    /**
     * A play to a Cast device whose file the device can't fully play ([ProbeResult.CastMiss]), found before anything was
     * sent to it. The release plays on the device the play started on, and the Cast device's limits are no fault of the
     * release, so it isn't remembered against it; the next release is tried as for any attempt. [text] names no device:
     * the log has it.
     */
    sealed class CastMiss(text: String) : AttemptFailure(text)

    /** No audio track a Cast device plays ([ProbeResult.NoCastAudio]): "No sound for Cast devices (DTS-HD, TrueHD)". */
    class NoCastAudio(val found: List<String>) : CastMiss("No sound for Cast devices" + if (found.isEmpty()) "" else " (${found.joinToString()})")

    /** No picture of its video on the device ([ProbeResult.NoCastVideo]): "Video Cast devices can't show (VC-1)". */
    class NoCastVideo(val found: String) : CastMiss("Video Cast devices can't show ($found)")

    /** Its video over the largest the device shows ([ProbeResult.TooBigForCast]): "2160p over the device's 1080p". */
    class TooBigForCast(val height: Int, val maxHeight: Int) : CastMiss("${height}p over the device's ${maxHeight}p")

    /** Login, connection or configuration problems hit every Usenet release alike: they stop the Usenet route. */
    class Provider(val error: UsenetError) : AttemptFailure("Provider problem: ${error.detail ?: error::class.simpleName}", route = PlayCandidate.USENET)
    class Failed(val error: UsenetError) : AttemptFailure(error.detail ?: error::class.simpleName.orEmpty())
    /**
     * The step failed on another device, in [words] said of it there: a play another device runs and this one follows, in
     * that device's own words, or a Cast device's start of a play sent to it. Nothing here failed, so nothing here stops
     * or is remembered for it.
     */
    class Elsewhere(words: String) : AttemptFailure(words)

    /**
     * A debrid service's account no longer has the release cached: no fault of the release, which another account or
     * a later play may have, and the account's other releases still play.
     */
    data object NoLongerCached : AttemptFailure("No longer cached")

    /** The debrid service took the release down: the release's own fault. */
    data object RemovedByService : AttemptFailure("Removed by the debrid service")

    /**
     * The account reached a limit of its debrid service: its [route] stops, as each of its releases would fail the
     * same way. [wait]: how long the service said it holds, null where its answer said none.
     */
    class ServiceLimit(val wait: Duration?, route: String?) :
        AttemptFailure("The debrid service’s limit was reached · " + (wait?.let { "try again in about ${about(it)}" } ?: "try again later"), route)

    /**
     * The debrid service won't play this one release, [tooLarge] for the account's plan or for its own reasons: no
     * fault of the release, and the account's route stays open.
     */
    class ServiceRefused(val tooLarge: Boolean) :
        AttemptFailure(if (tooLarge) "Too large for your debrid plan" else "The debrid service won’t play this release")

    /**
     * The debrid service refused the account's key: its [route] stops. The one failure that names a service, by the
     * name of the account's ([serviceName]), as where to replace the key is the user's to find.
     */
    class KeyRefused(val serviceName: String, route: String?) : AttemptFailure("$serviceName refused your API key", route)

    /** The debrid service stopped answering: its [route] stops. */
    class ServiceDown(route: String?) : AttemptFailure("The debrid service stopped answering", route)

    /** No working link to the file could be had from the service again, its one fresh link failing too. */
    data object NoWorkingLink : AttemptFailure("No longer available from the service")

    override fun toString() = "${this::class.simpleName}($text)"

    private companion object {
        /** A service's wait as the words say it: "20 min", "2 h"; never under a minute. */
        fun about(wait: Duration): String {
            val minutes = maxOf(1L, (wait.inWholeSeconds + 59) / 60)
            return if (minutes < 60) "$minutes min" else "${(minutes + 30) / 60} h"
        }
    }
}

/**
 * What a probe that [failed] on [fileName] fails the attempt as when the player couldn't read the file
 * ([ProbeResult.Failed.unreadable]); null when it failed otherwise. The one mapping every source says it by. The file's
 * type is its extension where that is two to five letters or digits; a name with none, or with one that is no type, gets
 * the words without it.
 */
fun unreadableFailure(failed: ProbeResult.Failed, fileName: String): AttemptFailure.Unreadable? = failed.unreadable?.let { kind ->
    AttemptFailure.Unreadable(kind, ContainerTypes.extension(fileName).takeIf(SHOWN_TYPE::matches)?.uppercase(Locale.ROOT))
}

/** An extension the words may name: [ContainerTypes.extension]'s, lower case. */
private val SHOWN_TYPE = Regex("[a-z0-9]{2,5}")

sealed interface StepStatus {
    /** [done] of [total] when the step can tell (availability check), else null. */
    data class Running(val done: Int? = null, val total: Int? = null) : StepStatus
    data class Done(val millis: Long, val detail: String? = null) : StepStatus
    data class Failed(val millis: Long, val failure: AttemptFailure) : StepStatus
}

sealed interface PipelineEvent {
    /** [of] drops when an indexer refuses and its other candidates are skipped; it doesn't count those known to be missing. */
    data class AttemptStarted(val attempt: Int, val of: Int, val candidate: PlayCandidate) : PipelineEvent
    data class Step(val candidate: PlayCandidate, val step: PipelineStep, val status: StepStatus) : PipelineEvent
    /** [candidate] was skipped without a fetch, for [reason] ([TriedReleases.knownMissing]). */
    data class Skipped(val candidate: PlayCandidate, val reason: String) : PipelineEvent
    /**
     * [candidate]'s attempt goes on with another target of the same release after [failure], at one of its source's steps
     * or at the probe ([PlaySource.retarget], [AttemptSteps.retrying]): the source's steps run again, from its first.
     */
    data class Retrying(val candidate: PlayCandidate, val failure: AttemptFailure) : PipelineEvent
    /**
     * [video] and [tracks]: what the probe read of [target]'s video and tracks ([ProbeResult.Playable]), for the player and a
     * Cast device. [stopped]: the routes the play's failures stopped, those a source stopped inside the attempt that played
     * among them ([AttemptSteps.stop]), which a later try of the same play passes over too.
     */
    data class Succeeded(
        val target: PlaybackTarget,
        val failedAttempts: List<Pair<PlayCandidate, AttemptFailure>>,
        val video: ProbedVideo? = null,
        val tracks: ProbedTracks? = null,
        val stopped: Set<String> = emptySet(),
    ) : PipelineEvent
    /**
     * [notTried]: left over when the attempts ran out or failures stopped every route to them, but for
     * those known to be missing; [noRoute] of them for the second reason (no grab was saved by
     * leaving those). [skipped]: never fetched because their indexer refused an earlier grab.
     */
    data class Exhausted(val failedAttempts: List<Pair<PlayCandidate, AttemptFailure>>, val notTried: Int, val skipped: Int = 0, val noRoute: Int = 0) : PipelineEvent
}

/**
 * What a source of releases does to play one ([PlayPipeline]): Usenet's fetches an NZB, checks its articles and prepares
 * it; another's lists a release's files where it keeps them. [attempt] opens the release and picks the file to play,
 * through the pipeline's [AttemptSteps]; the pipeline then probes it. [T]: what an attempt of it opens.
 */
interface PlaySource<T : PlaybackTarget> {
    /** The steps an attempt of it runs, in their order, the probe's last: what the play's progress lists for its releases. */
    val steps: List<PipelineStep>

    /**
     * Opens [candidate]'s release and picks the file to play with [pickFile], each step run as [AttemptSteps.step]: the file
     * to probe. A step that fails ends the attempt there, unless [retarget] gives another target.
     */
    suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): T

    /** What the probe failing to read [target]'s file ([failed]) fails the attempt as. */
    suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: T): AttemptFailure

    /**
     * Another target of [candidate]'s release, after [failure] at one of its steps or at the probe: what plays it by
     * another way (a debrid service's next account with it cached), opened by its steps as [attempt] opens the first,
     * once [AttemptSteps.retrying] has said so. The pipeline probes it in its turn, and asks again should that fail too.
     * Null for none, which ends the attempt with [failure]: by default, as a Usenet release has one way alone.
     */
    suspend fun AttemptSteps.retarget(candidate: PlayCandidate, failure: AttemptFailure, pickFile: FilePicker): T? = null

    /** What an exception thrown in one of its attempt's steps, the probe's among them, fails the attempt as. */
    fun failed(error: Exception): AttemptFailure
}

/**
 * The pipeline's side of a source's attempt ([PlaySource.attempt]): its steps, each sent to the play's events as it starts
 * and ends, timed by [timeSource], logged, and watched while it runs ([StepWatch]).
 */
interface AttemptSteps {
    /**
     * Whether what fails now is what any play of the release would use, not the one file this play picked from several (an
     * episode of a season pack, whose others may be whole): true until the source says otherwise. A failure of the whole
     * release is remembered against it ([TriedReleases.missing]).
     */
    var wholeRelease: Boolean

    /** What times the steps. */
    val timeSource: TimeSource

    /**
     * The routes failures have stopped so far in this play ([AttemptFailure.route], [stop]): a source whose candidate may
     * play by several of them (a torrent several service accounts have cached) passes these over.
     */
    val stopped: Set<String>

    /**
     * Stops [route] for the rest of the play, as a failure that names it does ([AttemptFailure.route]): what a source finds
     * of one of a candidate's several routes before it tries the next. Candidates left with no open route aren't tried.
     */
    fun stop(route: String)

    /**
     * Runs [block] as [step] of [candidate]'s attempt: its value once it is done, with the detail it gives to show. Its
     * failure, or what an exception it throws fails as ([PlaySource.failed]), fails the attempt: it is sent, and the
     * attempt ends.
     */
    suspend fun <T> step(candidate: PlayCandidate, step: PipelineStep, block: suspend (StepWatch) -> StepResult<T>): T

    /** Sends [event], what a source says of a step besides [step]'s own: one it skips. */
    suspend fun send(event: PipelineEvent.Step)

    /** How a log line names [candidate]: by its title, unless the pipeline names its candidates otherwise (a download's). */
    fun named(candidate: PlayCandidate): String = candidate.title

    /**
     * Says the attempt goes on with another target of its release after [failure] ([PlaySource.retarget]), before the
     * source's steps run again for it ([PipelineEvent.Retrying]).
     */
    suspend fun retrying(failure: AttemptFailure)

    /** Sends [event] if the collector takes it at once, from any thread: a step's progress, which the next one supersedes. */
    fun trySend(event: PipelineEvent.Step)

    /**
     * The file [chooser] answers for, of the videos of [listing] that [pickFile] offers where their names don't tell
     * ([FilePicker.choices]); null when it offers none or nothing is chosen. The attempt waits for it, its step's [watch]
     * stopped meanwhile.
     */
    suspend fun <F : ListedFile> chosen(candidate: PlayCandidate, listing: FileListing<F>, pickFile: FilePicker, watch: StepWatch, chooser: FileChooser): F?

    /**
     * [block] off the caller's thread, which may be Android's main one: checking, opening and probing a release fetch and
     * decode.
     */
    suspend fun <T> guarded(block: suspend () -> T): T
}

/** What a step came to ([AttemptSteps.step]): [Ok] with its value and the detail to show for it ([Ok.detail]), or [Fail]. */
sealed interface StepResult<out T> {
    data class Ok<T>(val value: T, val detail: String? = null) : StepResult<T>
    data class Fail(val failure: AttemptFailure) : StepResult<Nothing>
}

/** The watch on a running step, which logs it while it runs ([PlayPipeline]'s stepWarnAfter). */
interface StepWatch {
    /**
     * [block], a question to the user (which file plays), with the watch stopped: the step waits for the answer, which
     * may take a while, and isn't stuck. Once the answer is in, the watch starts again.
     */
    suspend fun <R> whileAsking(block: suspend () -> R): R
}

/**
 * Turns a ranked list of releases into something playable: for each candidate, up to [maxAttempts], its source
 * ([sources], [PlaySource]) opens the release and picks the file in its own steps, and the file's tracks are probed
 * ([probe]). The first candidate that passes every step wins; a failure moves on to the next candidate, unless its source
 * has another way to play the same release ([PlaySource.retarget]: a debrid service's next account). One that stops a
 * route ([AttemptFailure.route]: the provider down stops Usenet's) passes over the candidates it leaves no route to, and
 * once an indexer refuses a grab (download limit, API key), its other candidates are passed over: neither uses up
 * attempts.
 *
 * A release [tried] remembers as missing on the providers is skipped before its NZB is fetched,
 * without using up an attempt; one found missing on every provider that answered is remembered
 * ([AttemptFailure.MissingArticles.missingOn]), when the gap is in what any play of it would use
 * ([AttemptSteps.wholeRelease]): not in one episode of a season pack, whose others may be whole. One that plays is
 * forgotten. One found encrypted, with no password to open it, is remembered as needing one
 * ([TriedReleases.passwordNeeded]), and one whose files are a copy of a disc as one ([TriedReleases.discCopy]), whatever
 * file the play picked: that is the release's shape, not a gap.
 *
 * Each fetch usually costs an indexer grab, which is why attempts are capped.
 *
 * A step still running after [stepWarnAfter] is logged as such, again each [stepWarnAfter] while it runs (at most
 * [STEP_LINES] times), but not while it waits for the user to choose the file: a play that stalls says where.
 */
class PlayPipeline(
    private val sources: (PlayCandidate) -> PlaySource<*>,
    private val probe: TrackProbe = TrackProbe.None,
    private val maxAttempts: Int = 3,
    private val dispatcher: CoroutineDispatcher = NntpDispatchers.IO,
    private val timeSource: TimeSource = TimeSource.Monotonic,
    private val tried: TriedReleases = TriedReleases.None,
    private val stepWarnAfter: Duration = 20.seconds,
    /**
     * How its log lines name a candidate: by its title, as a play's do; a download's by its own words, as its lines name
     * no release ([AttemptSteps.named]).
     */
    private val named: (PlayCandidate) -> String = PlayCandidate::title,
    /**
     * How its log lines say an attempt's failure: in its own words, as a play's do; a download's by its kind alone, as the
     * words can name a file (one left out, one no longer there).
     */
    private val failureWords: (AttemptFailure) -> String = AttemptFailure::text,
) {
    private val log = LoggerFactory.getLogger(PlayPipeline::class.java)

    /**
     * Tries [candidates] in turn, as the class says, then [last] if none plays: the release the player left while it still
     * played, which the play goes back to. The others share the attempts but one, which is its own (with one attempt
     * to make, they have it, and it has one more: it costs no grab, and the play was left to try another); it is never
     * skipped as known to be missing nor for its indexer refusing grabs (its NZB is the one the app keeps), and it isn't
     * checked for missing articles, as it was just playing ([SKIPPED_PLAYING]). Failures that stop every route it plays by
     * (the provider down, for a Usenet release) leave it untried.
     */
    fun play(candidates: List<PlayCandidate>, pickFile: FilePicker = FilePicker.Largest, last: PlayCandidate? = null): Flow<PipelineEvent> = channelFlow {
        val failures = mutableListOf<Pair<PlayCandidate, AttemptFailure>>()
        val pending = candidates.toMutableList()
        val refusing = mutableSetOf<String?>() // indexers that refused a grab
        // The way back has an attempt of its own; the others share the rest, at least one.
        val wayBack = if (last != null) 1 else 0
        val others = if (last != null) maxOf(1, maxAttempts - 1) else maxAttempts
        val stopped = mutableSetOf<String>() // routes a failure stopped
        /** Whether a failure stopped every route [candidate] plays by. */
        fun noRoute(candidate: PlayCandidate) = candidate.routes.isNotEmpty() && stopped.containsAll(candidate.routes)
        // What [tried] knows of each candidate, looked up once: when its turn comes, or before, to count those to try.
        val known = mutableMapOf<PlayCandidate, String?>()
        suspend fun knownMissing(candidate: PlayCandidate): String? =
            if (candidate in known) known[candidate] else remembered("look up") { tried.knownMissing(candidate) }.also { known[candidate] = it }

        /** How many of [rest], in order, would be tried, up to [limit]: those not known to be missing. */
        suspend fun toTry(rest: List<PlayCandidate>, limit: Int): Int {
            var count = 0
            for (candidate in rest) {
                if (count >= limit) break
                if (knownMissing(candidate) == null) count++
            }
            return count
        }

        /** Whether [candidate]'s attempt played; its failure kept, as the class says, when it didn't. */
        suspend fun tried(candidate: PlayCandidate, result: AttemptResult): Boolean = when (result) {
            is AttemptResult.Success -> {
                remembered("forget") { tried.found(candidate) }
                send(PipelineEvent.Succeeded(result.target, failures.toList(), result.video, result.tracks, stopped.toSet()))
                true
            }
            is AttemptResult.Failure -> {
                log.info("play_pipeline: {} failed: {}", named(candidate), failureWords(result.failure))
                val on = (result.failure as? AttemptFailure.MissingArticles)?.missingOn
                if (on != null && result.wholeRelease) remembered("remember") { tried.missing(candidate, on) }
                if (result.failure == AttemptFailure.Passworded) remembered("remember") { tried.passwordNeeded(candidate) }
                // The release's shape, not a gap in the file this play picked: remembered whatever wholeRelease says.
                if (result.failure is AttemptFailure.DiscCopy) remembered("remember") { tried.discCopy(candidate) }
                failures += candidate to result.failure
                if (result.failure.blocksIndexer) refusing += candidate.indexer
                result.failure.route?.let { stopped += it }
                false
            }
        }

        while (failures.size < others) {
            val open = pending.filter { it.indexer !in refusing && !noRoute(it) }
            val candidate = open.firstOrNull() ?: break
            pending.remove(candidate)
            val reason = knownMissing(candidate)
            if (reason != null) {
                log.info("play_pipeline: {} skipped without a fetch: {}", named(candidate), reason)
                send(PipelineEvent.Skipped(candidate, reason))
                continue
            }
            val of = failures.size + 1 + toTry(open.drop(1), others - failures.size - 1) + wayBack
            send(PipelineEvent.AttemptStarted(failures.size + 1, of, candidate))
            if (tried(candidate, attempt(candidate, pickFile, stopped))) return@channelFlow
        }
        if (last != null && !noRoute(last)) {
            log.info("play_pipeline: {}: no other release played, going back to it", named(last))
            send(PipelineEvent.AttemptStarted(failures.size + 1, failures.size + 1, last))
            if (tried(last, attempt(last, pickFile, stopped))) return@channelFlow
        }
        val skipped = pending.count { it.indexer in refusing }
        val notTried = pending.filter { it.indexer !in refusing }.filter { knownMissing(it) == null }
        send(PipelineEvent.Exhausted(failures.toList(), notTried = notTried.size, skipped = skipped, noRoute = notTried.count(::noRoute)))
    }
        // No buffer: each event waits for the collector, so the pipeline never runs ahead of
        // it (a cancelled play must not go on fetching NZBs, which cost grabs).
        .buffer(Channel.RENDEZVOUS)

    private sealed interface AttemptResult {
        /** [video] and [tracks]: what the probe read of the video that plays, and of the file's tracks. */
        data class Success(val target: PlaybackTarget, val video: ProbedVideo?, val tracks: ProbedTracks?) : AttemptResult

        /**
         * [wholeRelease]: what failed is what any play of the release would use, not the one file this play
         * picked from several (an episode of a season pack, whose others may be whole).
         */
        data class Failure(val failure: AttemptFailure, val wholeRelease: Boolean) : AttemptResult
    }

    /**
     * Runs [block], a read or write of what is remembered of releases ([tried]): a failure (a full disk) is logged as
     * failing to [what], and the play goes on as if nothing were remembered.
     */
    private suspend fun <T> remembered(what: String, block: suspend () -> T): T? = try {
        block()
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        log.warn("play_pipeline: couldn't {} the releases found missing", what, e)
        null
    }

    private class StepFailed(val failure: AttemptFailure) : Exception(failure.text)

    private suspend fun ProducerScope<PipelineEvent>.attempt(candidate: PlayCandidate, pickFile: FilePicker, stopped: MutableSet<String>): AttemptResult =
        attempt(sources(candidate), candidate, pickFile, stopped)

    /**
     * [candidate]'s attempt: [source]'s steps, which open the release and pick its file, then the probe of that file. A
     * failure at either asks [source] for another target of the release ([PlaySource.retarget]), which is probed in its
     * turn; with none, the attempt fails there. The routes the play has [stopped] are the source's to read and add to.
     */
    private suspend fun <T : PlaybackTarget> ProducerScope<PipelineEvent>.attempt(
        source: PlaySource<T>, candidate: PlayCandidate, pickFile: FilePicker, stopped: MutableSet<String>,
    ): AttemptResult {
        val steps = Steps(this, source, stopped, candidate)
        // The last failure of the release's targets: none before the first is opened.
        var failed: AttemptFailure? = null
        while (true) {
            try {
                val last = failed
                val target = if (last == null) {
                    with(source) { steps.attempt(candidate, pickFile) }
                } else {
                    with(source) { steps.retarget(candidate, last, pickFile) } ?: return AttemptResult.Failure(last, steps.wholeRelease)
                }
                val probed = try {
                    steps.step(candidate, PipelineStep.ProbeTracks) {
                        when (val result = steps.guarded { probe.probe(target) }) {
                            is ProbeResult.Playable -> StepResult.Ok(result)
                            is ProbeResult.NoPlayableAudio -> StepResult.Fail(AttemptFailure.NoPlayableAudio(result.found))
                            is ProbeResult.NoPlayableVideo -> StepResult.Fail(AttemptFailure.NoPlayableVideo(result.found))
                            is ProbeResult.NotForCast -> StepResult.Fail(AttemptFailure.NotForCast(result.extension))
                            is ProbeResult.CastMiss -> StepResult.Fail(result.failure)
                            is ProbeResult.Failed -> StepResult.Fail(with(source) { steps.probeFailed(result, target) })
                        }
                    }
                } catch (e: StepFailed) {
                    // The probe kept it to read it ([PlaybackTarget.keep]): a target that didn't play lets go of it.
                    target.drop()
                    throw e
                }
                return AttemptResult.Success(target, probed.video, probed.tracks)
            } catch (e: StepFailed) {
                failed = e.failure
            }
        }
    }

    /**
     * One attempt's [AttemptSteps], [candidate]'s, in [scope], the play's flow: what an exception in a step fails as is
     * [source]'s. The routes the play's failures [stop]ped are the play's, so what the source stops counts for the
     * candidates after it.
     */
    private inner class Steps(
        private val scope: ProducerScope<PipelineEvent>,
        private val source: PlaySource<*>,
        private val stoppedInPlay: MutableSet<String>,
        private val candidate: PlayCandidate,
    ) : AttemptSteps {
        // Until the source says otherwise, what fails is the release's.
        override var wholeRelease = true
        override val timeSource: TimeSource get() = this@PlayPipeline.timeSource
        override val stopped: Set<String> get() = stoppedInPlay.toSet()

        override fun stop(route: String) {
            log.info("play_pipeline: {} stopped for the rest of the play", route)
            stoppedInPlay += route
        }

        override fun named(candidate: PlayCandidate): String = this@PlayPipeline.named(candidate)

        override suspend fun <T> step(candidate: PlayCandidate, step: PipelineStep, block: suspend (StepWatch) -> StepResult<T>): T {
            scope.send(PipelineEvent.Step(candidate, step, StepStatus.Running()))
            val started = timeSource.markNow()
            val watch = Watch(scope, candidate, step)
            val result = try {
                block(watch)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                StepResult.Fail(source.failed(e))
            } finally {
                // The flow waits for its children: a watch left running would hold the play open.
                watch.stop()
            }
            val millis = started.elapsedNow().inWholeMilliseconds
            log.info("play_pipeline: {} {} {} ms", named(candidate), step, millis)
            return when (result) {
                is StepResult.Ok -> result.value.also { scope.send(PipelineEvent.Step(candidate, step, StepStatus.Done(millis, result.detail))) }
                is StepResult.Fail -> {
                    scope.send(PipelineEvent.Step(candidate, step, StepStatus.Failed(millis, result.failure)))
                    throw StepFailed(result.failure)
                }
            }
        }

        override suspend fun send(event: PipelineEvent.Step) = scope.send(event)

        override suspend fun retrying(failure: AttemptFailure) {
            log.info("play_pipeline: {} failed: {}; trying it another way", named(candidate), failureWords(failure))
            scope.send(PipelineEvent.Retrying(candidate, failure))
        }

        override fun trySend(event: PipelineEvent.Step) {
            scope.trySend(event)
        }

        override suspend fun <F : ListedFile> chosen(candidate: PlayCandidate, listing: FileListing<F>, pickFile: FilePicker, watch: StepWatch, chooser: FileChooser): F? {
            val choices = pickFile.choices(listing).ifEmpty { return null }
            log.info("play_pipeline: {}: no file named for what plays, asking which of {} videos it is", named(candidate), choices.size)
            val answer = watch.whileAsking { chooser.choose(candidate, listing, choices) }
            return choices.firstOrNull { it == answer }
                .also { log.info("play_pipeline: {}: {}", named(candidate), it?.let { file -> "${file.name} chosen" } ?: "nothing chosen") }
        }

        override suspend fun <T> guarded(block: suspend () -> T): T = withContext(dispatcher) { block() }
    }

    /**
     * Says every [stepWarnAfter], at most [STEP_LINES] times, that [step] of [candidate] is still running, until [stop],
     * in [scope], but not while the step asks the user ([whileAsking]). On Dispatchers.Default, so on real time: a test's
     * virtual time doesn't spin it. Its seconds are real time too, not [timeSource]'s, from the step's start.
     */
    private inner class Watch(private val scope: CoroutineScope, private val candidate: PlayCandidate, private val step: PipelineStep) : StepWatch {
        private val since = TimeSource.Monotonic.markNow()
        private var job = start()

        private fun start(): Job = scope.launch(Dispatchers.Default) {
            repeat(STEP_LINES) {
                delay(stepWarnAfter)
                log.warn("play_pipeline: {} {} still running after {} s", named(candidate), step, since.elapsedNow().inWholeSeconds)
            }
        }

        override suspend fun <R> whileAsking(block: suspend () -> R): R {
            job.cancel()
            return block().also { job = start() }
        }

        fun stop() = job.cancel()
    }

    companion object {
        /**
         * At most this many lines for one step still running, counted again after the user's answer: over three minutes'
         * worth at the default [stepWarnAfter].
         */
        private const val STEP_LINES = 10

        /** What the availability check's step says of the release that was playing, which isn't checked. */
        const val SKIPPED_PLAYING = "skipped, it was playing"
    }
}

/**
 * Reads the season and episode numbers a file's or a release's name states, for the episode picker ([FilePicker.Episode]):
 * "Show.S01E02.mkv" is season 1, episode 2. An app passes its release parser's reading of them.
 */
fun interface EpisodeNames {
    fun of(name: String): EpisodeNumbers
}

/** The [seasons] and [episodes] a name states ([EpisodeNames]), each empty where it states none. */
data class EpisodeNumbers(val seasons: List<Int>, val episodes: List<Int>)

/** Chooses the file to play from a release's files: a prepared NZB's, or another source's listing. */
interface FilePicker {
    fun <F : ListedFile> pick(listing: FileListing<F>): F?
    val noMatch: AttemptFailure

    /** Why [pick] found nothing in [listing]: [noMatch], unless the picker picks otherwise in some releases. */
    fun noMatchIn(listing: FileListing<*>): AttemptFailure = noMatch

    /** The videos of [listing] to ask which is the one to play ([FileChooser]) where [pick] found none; none by default. */
    fun <F : ListedFile> choices(listing: FileListing<F>): List<F> = emptyList()

    /**
     * Whether a release named [title] holds just the one video this picker wants (besides PAR2,
     * NFO and the like), so a gap anywhere in it is a gap in what would play.
     */
    fun takesWhole(title: String): Boolean

    /**
     * Whether a file the NZB lists as [name] (its subject's) holds what this picker looks for, by
     * the name alone: a release that lists one prepare left out is incomplete rather than without
     * it. Only an episode can be told so; by default nothing is.
     */
    fun wants(name: String): Boolean = false

    /** The largest video that isn't a sample. */
    object Largest : FilePicker {
        override fun <F : ListedFile> pick(listing: FileListing<F>) = listing.defaultPick
        override val noMatch: AttemptFailure = AttemptFailure.NoVideo

        /** What is played this way is a release of one title (a film). */
        override fun takesWhole(title: String) = true
    }

    /**
     * The video whose name says SxxEyy (largest if several), not a sample where the release holds
     * other videos. A release with one video is taken as is: single-episode releases often have
     * obfuscated file names. Episode numbers are read only in the forms [names] knows; where
     * a release of several videos names none of them so, the user is asked which it is ([choices]).
     */
    data class Episode(val season: Int, val episode: Int, val names: EpisodeNames) : FilePicker {
        override fun <F : ListedFile> pick(listing: FileListing<F>): F? {
            val videos = listing.files.filter { it.isVideo }
            // A sample named for the episode isn't it, nor says which of the others is; nor is an extra in a folder of them.
            val named = videos.filter { isThisEpisode(it.name) }
            val matches = if (videos.all { VideoFileFilter.isSample(it.path) }) named else named.filterNot { VideoFileFilter.isSample(it.path) }
            if (matches.isNotEmpty()) return matches.maxByOrNull { it.size }
            // An obfuscated name says nothing; a name for another episode says "wrong file".
            val lone = listing.defaultPick?.takeIf { videos.count { !it.name.contains("sample", ignoreCase = true) && !VideoFileFilter.inExtras(it.path) } == 1 }
            return lone?.takeIf { names.of(it.name).let { parsed -> parsed.episodes.isEmpty() && parsed.seasons.isEmpty() } }
        }

        override val noMatch: AttemptFailure get() = AttemptFailure.NoMatchingEpisode(season, episode)

        /**
         * The release's videos, samples among them, where it holds several, its name ([FileListing.title]) doesn't rule
         * this episode out (another season, other episodes) and none but samples is named for an episode ("Show.1_6.mp4"):
         * nothing tells which is this one. Where they are named for episodes, the release lacks this one, whatever extras
         * it holds besides.
         */
        override fun <F : ListedFile> choices(listing: FileListing<F>): List<F> {
            if (listing.oneVideo || !mayHold(listing.title)) return emptyList()
            val videos = listing.files.filter { it.isVideo }
            return videos.takeIf { all -> all.none { !VideoFileFilter.isSample(it.path) && names.of(it.name).episodes.isNotEmpty() } }.orEmpty()
        }

        private fun mayHold(title: String) =
            names.of(title).let { (it.seasons.isEmpty() || season in it.seasons) && (it.episodes.isEmpty() || episode in it.episodes) }

        /** A video of this episode (not a sample of it), or a volume of an archive named for it. */
        override fun wants(name: String) =
            (VideoFileFilter.isVideo(name) && !VideoFileFilter.isSample(name) || ArchiveService.fileHasKnownExtension(name)) && isThisEpisode(name)

        private fun isThisEpisode(name: String) = names.of(name).let { it.seasons.firstOrNull() == season && episode in it.episodes }

        /** A release named for this episode alone; a season pack or a multi-episode release holds others. */
        override fun takesWhole(title: String) =
            names.of(title).let { it.seasons == listOf(season) && it.episodes == listOf(episode) }
    }
}
