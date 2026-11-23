package io.skjaere.nzbstreamer.play

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.last
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The loop over a play's releases. Usenet's releases are tried through [FakeUsenet], a source that fails each fake release
 * at the step, and with the failure, the app's Usenet source does: how that source fetches, checks and opens an NZB is
 * the app's to test, with its engine.
 */
class PlayPipelineTest {

    /** How each fake release behaves; keyed by candidate id, which reads e.g. "Good:1" (release, position). */
    private enum class Release {
        Good, Missing, Passworded, Unfetchable, GrabLimit, BadKey, Silent, Pack, ProviderDown, CompressedOnly, OtherEpisode,
        TimesOut, GapOnOpen, ProviderDownOnOpen, VolumeNotInNzb, VolumeUnplaced,
        /** A season pack whose videos are named for no episode in a form the parser knows: "Show.1_6.mp4" is episode 1 of 6. */
        UnnamedPack,
        /** A season pack of E01 to E03, named for them, and an extra named for no episode. */
        PackWithExtra,
        /** A season pack whose videos are named for no episode the parser reads ("Show.1_6.mp4"), and a sample named for E01. */
        UnnamedPackNamedSample,
        /** One video under an obfuscated name, and a sample named for the episode. */
        ObfuscatedNamedSample,
        /** Its video is of a kind no decoder of the device takes: Dolby Vision profile 5, on a device without its decoder. */
        NoVideoDecoder,
        /** A UHD Blu-ray disc's files in a RAR set, their folders kept ([DISC]/BDMV/…), as prepare lists them. */
        BluRayDisc,
        /** A Blu-ray disc's files posted as they are, without folders: two streams beside the disc's navigation files. */
        FlatDisc,
        /** A DVD's files in a RAR set, in their VIDEO_TS folder. */
        DvdCopy,
        /** A disc image, posted as it is. */
        DiscImage,
        /** [BluRayDisc]'s files and, beside them, the film as one video, smaller than the disc's stream. */
        DiscWithMkv,
        /** A disc's stream and navigation files posted without folders, and a video of the release's own, smaller than the stream. */
        FlatDiscWithMkv,
    }

    /** Grabs an indexer refuses for every release it has: a used-up download limit, a revoked API key. */
    private val refusals = mapOf(Release.GrabLimit to "Download limit reached", Release.BadKey to "Incorrect API key")

    /** A file in a fake NZB of [segments] articles. */
    private data class FakeFile(val name: String, val segments: Int = 3)

    /** What a fake release's NZB holds. [rarEntries]: the files inside its RAR set, by the volumes they are stored in. */
    private class Content(val files: List<FakeFile>, val rarEntries: Map<String, IntRange> = emptyMap())

    private val single = listOf(FakeFile("a8f3c1.mkv", 10), FakeFile("a8f3c1.sample.mkv", 1))

    private fun contentOf(release: Release): Content = when (release) {
        Release.Pack -> Content((1..3).map { FakeFile("Show.S01E%02d.1080p.WEB.mkv".format(it)) } + FakeFile("Show.S01.nfo", 1))
        Release.PackWithExtra -> Content((1..3).map { FakeFile("Show.S01E%02d.1080p.WEB.mkv".format(it)) } + FakeFile("Show.S01.Behind.The.Harbour.1080p.WEB.mkv"))
        Release.UnnamedPack -> Content(
            listOf(FakeFile("Show.1_6.mp4", 4), FakeFile("Show.2-6.mp4", 5), FakeFile("Show.3_6.mp4", 3), FakeFile("Show.sample.mp4", 1), FakeFile("Show.S01.nfo", 1)),
        )
        Release.OtherEpisode -> Content(listOf(FakeFile("Show.S01E01.1080p.WEB.mkv")))
        Release.UnnamedPackNamedSample -> Content(
            listOf(FakeFile("Show.1_6.mp4", 4), FakeFile("Show.2-6.mp4", 5), FakeFile("Show.3_6.mp4", 3), FakeFile("Show.S01E01.Sample.mkv", 1), FakeFile("Show.S01.nfo", 1)),
        )
        Release.ObfuscatedNamedSample -> Content(listOf(FakeFile("a8f3c1.mkv", 10), FakeFile("Show.S01E01.1080p.WEB.sample.mkv", 1)))
        Release.BluRayDisc -> Content(rarVolumes, rarEntries = discEntries)
        Release.DiscWithMkv -> Content(rarVolumes, rarEntries = discEntries + ("$DISC/Copper.Crown.2019.2160p.BluRay.REMUX.mkv" to 1..1))
        Release.DvdCopy -> Content(rarVolumes, rarEntries = mapOf("Quiet.Orbit.1999.DVD/VIDEO_TS/VIDEO_TS.IFO" to 0..0, "Quiet.Orbit.1999.DVD/VIDEO_TS/VTS_01_1.VOB" to 0..1))
        Release.FlatDisc -> Content(listOf(FakeFile("00001.m2ts", 10), FakeFile("00002.m2ts", 4), FakeFile("00800.mpls", 1), FakeFile("00001.clpi", 1), FakeFile("index.bdmv", 1)))
        Release.FlatDiscWithMkv -> Content(listOf(FakeFile("00001.m2ts", 10), FakeFile("00800.mpls", 1), FakeFile("index.bdmv", 1), FakeFile("a8f3c1.mkv", 5)))
        Release.DiscImage -> Content(listOf(FakeFile("Quiet.Orbit.1999.iso", 10), FakeFile("Quiet.Orbit.1999.nfo", 1)))
        else -> Content(single)
    }

    /** A RAR set's two volumes and its repair volume, which hold its [Content.rarEntries]. */
    private val rarVolumes = listOf(FakeFile("a8f3c1.part01.rar"), FakeFile("a8f3c1.part02.rar"), FakeFile("a8f3c1.vol00+01.par2"))

    /** A UHD Blu-ray disc's files, the stream in both volumes, its navigation files and a backup of its index beside it. */
    private val discEntries = mapOf(
        "$DISC/BDMV/index.bdmv" to 0..0, "$DISC/BDMV/PLAYLIST/00800.mpls" to 0..0, "$DISC/BDMV/CLIPINF/00001.clpi" to 0..0,
        "$DISC/BDMV/STREAM/00001.m2ts" to 0..1, "$DISC/BDMV/BACKUP/index.bdmv" to 1..1,
    )

    /** Season packs go by a pack's name; the rest by "Release n". */
    private val packTitle = "Show.S01.1080p.WEB-GRP"
    private val packs = setOf(Release.Pack, Release.UnnamedPack, Release.PackWithExtra, Release.UnnamedPackNamedSample)

    private val fetched = mutableListOf<String>()

    private fun release(candidate: PlayCandidate) = Release.valueOf(candidate.id.substringBefore(':'))

    /** What [target] opened. */
    private fun release(target: PlaybackTarget): Release = release(target.candidate)

    private val source = object : NzbSource {
        override suspend fun fetch(candidate: PlayCandidate): ByteArray = fetchWithPassword(candidate).bytes

        override suspend fun fetchWithPassword(candidate: PlayCandidate): FetchedNzb {
            fetched += candidate.id
            return when (val release = release(candidate)) {
                Release.Unfetchable -> throw FetchException("HTTP 404")
                Release.GrabLimit, Release.BadKey -> throw FetchException(refusals.getValue(release), blocksIndexer = true)
                else -> FetchedNzb(candidate.id.encodeToByteArray())
            }
        }
    }

    /** The candidates whose articles were checked, in order. */
    private val checked = mutableListOf<String>()

    /** A file of a fake release's listing, or of a service's ([Cached]). */
    private data class Listed(override val index: Int, override val path: String, override val size: Long) : ListedFile

    private class FakeListing(override val id: String, override val title: String, override val files: List<Listed>) : FileListing<Listed>

    /** What an attempt of [FakeUsenet] opened: [dropped] counts the times the pipeline let go of it ([PlaybackTarget.drop]). */
    private class FakeTarget(override val candidate: PlayCandidate, override val listing: FakeListing, override val file: Listed) : PlaybackTarget {
        override val uri: String get() = "fake://${listing.id}/file/${file.index}"
        var dropped = 0

        override suspend fun keep() = Unit

        override suspend fun drop() {
            dropped++
        }
    }

    /**
     * [candidate]'s files as prepare lists them: every file of its NZB but the PAR2 ones, each sized by its articles,
     * unless the release is a RAR set, whose entries are listed instead, each as large as the volumes it is stored in.
     */
    private fun listingOf(candidate: PlayCandidate): FakeListing {
        val content = contentOf(release(candidate))
        val volumes = content.files.filterNot { it.name.endsWith(".par2") }
        val files = if (content.rarEntries.isEmpty()) {
            volumes.map { it.name to it.segments * SEGMENT_SIZE }
        } else {
            content.rarEntries.map { (name, range) -> name to range.sumOf { volumes[it].segments * SEGMENT_SIZE } }
        }
        return FakeListing(candidate.id, candidate.title, files.mapIndexed { i, (path, size) -> Listed(i, path, size) })
    }

    /**
     * Usenet's releases as the loop sees them: Usenet's steps (fetching the NZB from [source], checking its articles,
     * opening it), each fake [Release] failing at the step, and with the failure, the app's Usenet source does. A release
     * that was playing isn't checked, its check's step said to be skipped ([PlayPipeline.SKIPPED_PLAYING]). Opening lists
     * the release's files ([listingOf]) and picks the one to play as any source does: a disc copy fails at once, else
     * [FilePicker.pick], else [chooser]'s answer, else the picker's no match. [checking] and [opening] run as their steps
     * start. [providersLeftOut]: a provider is left out, which may have what the others lack; [providerFallback]: another
     * provider serves when one fails, so a provider failing fails the release, not the play.
     */
    private inner class FakeUsenet(
        private val chooser: FileChooser = FileChooser.None,
        private val providersLeftOut: Boolean = false,
        private val providerFallback: Boolean = false,
        private val checking: suspend () -> Unit = {},
        private val opening: suspend () -> Unit = {},
    ) : PlaySource<FakeTarget> {
        override val steps = listOf(PipelineStep.FetchNzb, PipelineStep.CheckAvailability, PipelineStep.Open, PipelineStep.ProbeTracks)

        override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): FakeTarget {
            step(candidate, PipelineStep.FetchNzb) {
                try {
                    StepResult.Ok(source.fetchWithPassword(candidate))
                } catch (e: FetchException) {
                    StepResult.Fail(AttemptFailure.FetchFailed(e.reason, if (e.stopsAll) PlayCandidate.USENET else null, e.blocksIndexer))
                }
            }
            val release = release(candidate)
            if (candidate.wasPlaying) {
                send(PipelineEvent.Step(candidate, PipelineStep.CheckAvailability, StepStatus.Done(0, PlayPipeline.SKIPPED_PLAYING)))
            } else {
                step(candidate, PipelineStep.CheckAvailability) {
                    guarded { checking() }
                    checked += candidate.id
                    when (release) {
                        Release.Missing -> StepResult.Fail(missing("Missing article <${candidate.id}/a8f3c1.mkv/7>", missing = 1, checked = 1))
                        Release.ProviderDown -> StepResult.Fail(classify(UsenetError.AuthFailed(AUTH_FAILED)))
                        else -> StepResult.Ok(Unit)
                    }
                }
            }
            val (listing, file) = step(candidate, PipelineStep.Open) { watch ->
                guarded { opening() }
                openingFailure(release)?.let { return@step StepResult.Fail(it) }
                val listing = listingOf(candidate)
                listing.discCopy()?.let { return@step StepResult.Fail(it) }
                val file = pickFile.pick(listing)
                    ?: chosen(candidate, listing, pickFile, watch, chooser)
                    ?: return@step StepResult.Fail(pickFile.noMatchIn(listing))
                StepResult.Ok(listing to file, file.name)
            }
            return FakeTarget(candidate, listing, file)
        }

        /** How opening [release] fails, as prepare finds it; null when it opens. */
        private fun openingFailure(release: Release): AttemptFailure? = when (release) {
            Release.Passworded -> AttemptFailure.Passworded
            Release.CompressedOnly -> AttemptFailure.Unsupported("No streamable files. Compressed archives cannot be streamed.")
            Release.GapOnOpen -> missing("Article <x> not found")
            Release.VolumeNotInNzb -> missing(NOT_IN_NZB, notInNzb = true)
            Release.ProviderDownOnOpen -> classify(UsenetError.AuthFailed(AUTH_FAILED))
            Release.TimesOut -> classify(UsenetError.Timeout(TIMED_OUT))
            Release.VolumeUnplaced -> classify(UsenetError.Corrupt(UNPLACED))
            else -> null
        }

        override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: FakeTarget): AttemptFailure =
            unreadableFailure(failed, target.file.name) ?: classify(failed.error)

        override fun failed(error: Exception): AttemptFailure = classify(error.toUsenetError())

        /** A login or connection problem hits every release alike, unless another provider serves: then it may have what the others lack. */
        private fun classify(error: UsenetError): AttemptFailure = when (error) {
            is UsenetError.AuthFailed, is UsenetError.ConnectionFailed ->
                if (providerFallback) AttemptFailure.MissingArticles(error.detail.orEmpty(), providersLeftOut = providersLeftOut, unanswered = error)
                else AttemptFailure.Provider(error)
            UsenetError.NotConfigured -> AttemptFailure.Provider(error)
            is UsenetError.MissingArticle -> missing(error.detail.orEmpty())
            else -> AttemptFailure.Failed(error)
        }

        /** A gap that is a file the NZB doesn't hold ([notInNzb]) is on no provider, left out or not. */
        private fun missing(detail: String, missing: Int? = null, checked: Int? = null, notInNzb: Boolean = false) =
            AttemptFailure.MissingArticles(detail, missing, checked, providersLeftOut = !notInNzb && providersLeftOut, notInNzb = notInNzb)
    }

    private val probe = TrackProbe { target ->
        // A disc's stream, its packets 192 bytes: no extractor reads it.
        if (target.file.name.endsWith(".m2ts")) return@TrackProbe ProbeResult.Failed(UsenetError.Unknown(null), UnreadableFile.UnknownFormat)
        when (release(target)) {
            Release.Silent -> ProbeResult.NoPlayableAudio(listOf("E-AC-3 (en)"))
            Release.NoVideoDecoder -> ProbeResult.NoPlayableVideo(listOf("2160p Dolby Vision profile 5"))
            else -> ProbeResult.Playable()
        }
    }

    private fun pipeline(
        maxAttempts: Int = 3,
        tried: TriedReleases = TriedReleases.None,
        probe: TrackProbe = this.probe,
        usenet: FakeUsenet = FakeUsenet(),
        stepWarnAfter: Duration = 20.seconds,
    ) = PlayPipeline({ usenet }, probe, maxAttempts, dispatcher = Dispatchers.Unconfined, tried = tried, stepWarnAfter = stepWarnAfter)

    /**
     * Skips the candidates [known] names (by id) for the reason given, and keeps where the pipeline says a release's
     * articles are missing, which releases it says played, needed a password or are disc copies.
     */
    private class FakeTried(private val known: Map<String, String> = emptyMap()) : TriedReleases {
        val recorded = mutableListOf<Pair<String, MissingOn>>()
        val found = mutableListOf<String>()
        val lookedUp = mutableListOf<String>()
        val passwordsNeeded = mutableListOf<String>()
        val discCopies = mutableListOf<String>()

        override suspend fun knownMissing(candidate: PlayCandidate): String? = known[candidate.id].also { lookedUp += candidate.id }

        override suspend fun missing(candidate: PlayCandidate, on: MissingOn) {
            // As TriedReleases promises: a candidate is looked up before it is tried.
            assertTrue(candidate.id in lookedUp, "${candidate.id} found missing before it was looked up")
            recorded += candidate.id to on
        }

        override suspend fun found(candidate: PlayCandidate) {
            found += candidate.id
        }

        override suspend fun passwordNeeded(candidate: PlayCandidate) {
            assertTrue(candidate.id in lookedUp, "${candidate.id} found to need a password before it was looked up")
            passwordsNeeded += candidate.id
        }

        override suspend fun discCopy(candidate: PlayCandidate) {
            assertTrue(candidate.id in lookedUp, "${candidate.id} found a disc copy before it was looked up")
            discCopies += candidate.id
        }
    }

    private fun candidates(vararg releases: Release) =
        releases.mapIndexed { i, r -> PlayCandidate("$r:$i", if (r in packs) packTitle else "$r $i") }

    /** Candidates served by the named indexers; ids read e.g. "Good:B1" (release, indexer, position). */
    private fun fromIndexers(vararg releases: Pair<String, Release>) =
        releases.mapIndexed { i, (indexer, r) -> PlayCandidate("$r:$indexer$i", "$r $i", indexer) }

    private fun episode(season: Int, episode: Int) = FilePicker.Episode(season, episode, testEpisodeNames)

    @Test
    fun `first good release plays after four steps`() = runTest {
        val events = pipeline().play(candidates(Release.Good, Release.Good)).toList()
        val success = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("a8f3c1.mkv", success.target.file.name)
        assertEquals(listOf("Good:0"), fetched)
        val steps = events.filterIsInstance<PipelineEvent.Step>().filter { it.status is StepStatus.Done }.map { it.step }
        assertEquals(listOf(PipelineStep.FetchNzb, PipelineStep.CheckAvailability, PipelineStep.Open, PipelineStep.ProbeTracks), steps)
    }

    /**
     * What the probe read of the video that plays (its frame rate, length and Dolby Vision) comes with the play's success,
     * for a player that matches the display's frame rate: the release that played's, not one that failed before it.
     */
    @Test
    fun `the probe's video facts come with the play's success`() = runTest {
        val film = ProbedVideo(fps = 24_000.0 / 1001, durationMs = 2_640_000, dolbyVision = true)
        val probed = TrackProbe { target ->
            if (release(target) == Release.Silent) ProbeResult.NoPlayableAudio(listOf("TrueHD (en)")) else ProbeResult.Playable(film)
        }
        val played = pipeline(probe = probed).play(candidates(Release.Silent, Release.Good)).last()
        val success = assertIs<PipelineEvent.Succeeded>(played)
        assertEquals("Good:1", success.target.candidate.id)
        assertEquals(film, success.video)
        // Audio alone, or a probe that read nothing of the video: none.
        val silent = pipeline(probe = TrackProbe { ProbeResult.Playable() })
        assertNull(assertIs<PipelineEvent.Succeeded>(silent.play(candidates(Release.Good)).last()).video)
        assertNull(assertIs<PipelineEvent.Succeeded>(pipeline().play(candidates(Release.Good)).last()).video)
    }

    /**
     * What the probe read of the file's tracks comes with the play's success, for a Cast device's verdict and whoever starts
     * the player: the release that played's, not one that failed before it, whose tracks came with its failure.
     */
    @Test
    fun `the probe's tracks come with the play's success`() = runTest {
        val film = ProbedTracks(video = listOf(ProbedTrack("video/avc", "H.264", width = 1920, height = 1080)), audio = listOf(ProbedTrack("audio/ac3", "AC-3", "en", channels = 6)))
        val silent = ProbedTracks(audio = listOf(ProbedTrack("audio/true-hd", "TrueHD", "en", channels = 8)))
        val probed = TrackProbe { target ->
            if (release(target) == Release.Silent) ProbeResult.NoPlayableAudio(listOf("TrueHD (en)"), silent) else ProbeResult.Playable(tracks = film)
        }
        val played = pipeline(probe = probed).play(candidates(Release.Silent, Release.Good)).last()
        val success = assertIs<PipelineEvent.Succeeded>(played)
        assertEquals("Good:1", success.target.candidate.id)
        assertEquals(film, success.tracks)
        // A probe that reads none: none.
        assertNull(assertIs<PipelineEvent.Succeeded>(pipeline().play(candidates(Release.Good)).last()).tracks)
    }

    @Test
    fun `incomplete, passworded, unfetchable, silent and undecodable releases fall through to the next one`() = runTest {
        val cases = listOf(Release.Missing, Release.Passworded, Release.Unfetchable, Release.Silent, Release.CompressedOnly, Release.NoVideoDecoder)
        cases.forEach { bad ->
            fetched.clear()
            val last = pipeline().play(candidates(bad, Release.Good)).last()
            val success = assertIs<PipelineEvent.Succeeded>(last, "$bad")
            assertEquals("Good:1", success.target.candidate.id)
            assertEquals(1, success.failedAttempts.size)
        }
    }

    @Test
    fun `failure reasons are specific`() = runTest {
        val last = pipeline(maxAttempts = 6).play(
            candidates(Release.Missing, Release.Passworded, Release.Unfetchable, Release.Silent, Release.CompressedOnly, Release.NoVideoDecoder),
        ).last()
        val failures = assertIs<PipelineEvent.Exhausted>(last).failedAttempts.map { it.second }
        assertIs<AttemptFailure.MissingArticles>(failures[0])
        assertIs<AttemptFailure.Passworded>(failures[1])
        assertEquals("Couldn't get the NZB: HTTP 404", failures[2].text)
        assertEquals("No audio this device can play (E-AC-3 (en))", failures[3].text)
        assertIs<AttemptFailure.Unsupported>(failures[4])
        assertEquals("No video this device can play: 2160p Dolby Vision profile 5", failures[5].text)
    }

    /**
     * Video no decoder of the device takes is this device's limit, and sometimes its screen's: the release isn't remembered
     * (what is remembered of releases may be shared with other devices), the same indexer's next release is tried, and the
     * attempt counts against the releases to try, as each costs a grab.
     */
    @Test
    fun `a release without playable video tries the next one, uses an attempt and isn't remembered`() = runTest {
        val tried = FakeTried()
        val next = pipeline(tried = tried).play(fromIndexers("A" to Release.NoVideoDecoder, "A" to Release.Good)).last()
        assertEquals("Good:A1", assertIs<PipelineEvent.Succeeded>(next).target.candidate.id)
        val failure = assertIs<AttemptFailure.NoPlayableVideo>(next.failedAttempts.single().second)
        assertFalse(failure.route != null || failure.blocksIndexer)
        assertEquals(emptyList(), tried.recorded)
        assertEquals(emptyList(), tried.passwordsNeeded)
        val exhausted = pipeline(maxAttempts = 2).play(candidates(Release.NoVideoDecoder, Release.NoVideoDecoder, Release.Good)).last()
        assertEquals(2, assertIs<PipelineEvent.Exhausted>(exhausted).failedAttempts.size)
        assertEquals("No video this device can play", AttemptFailure.NoPlayableVideo(emptyList()).text)
    }

    /**
     * A file of a type a Cast device can't play, found before anything is sent to it: the release's file plays on the
     * device the play started on, so it isn't remembered against the release, and the next release is tried, as for any
     * failed attempt.
     */
    @Test
    fun `a file a Cast device can't play fails its attempt, the next release is tried, and nothing is remembered`() = runTest {
        val tried = FakeTried()
        val castProbe = TrackProbe { target -> if (target.candidate.id == "Good:0") ProbeResult.NotForCast("avi") else ProbeResult.Playable() }
        val events = pipeline(probe = castProbe, tried = tried).play(candidates(Release.Good, Release.Good)).toList()
        val success = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("Good:1", success.target.candidate.id)
        val failure = assertIs<AttemptFailure.NotForCast>(success.failedAttempts.single().second)
        assertEquals("avi", failure.extension)
        assertEquals("AVI file. Cast devices play MKV, MP4, WebM and MOV.", failure.text)
        assertFalse(failure.route != null || failure.blocksIndexer)
        // The probe's step failed with it, as the events show.
        val probed = events.filterIsInstance<PipelineEvent.Step>().first { it.step == PipelineStep.ProbeTracks && it.status is StepStatus.Failed }
        assertEquals("Good:0", probed.candidate.id)
        assertEquals(emptyList(), tried.recorded)
        assertEquals(emptyList(), tried.passwordsNeeded)
        assertEquals(listOf("Good:1"), tried.found)
        // A file with no extension has no type to name.
        assertEquals("Unknown file type. Cast devices play MKV, MP4, WebM and MOV.", AttemptFailure.NotForCast("").text)
    }

    /**
     * A Cast device's file it can't fully play, found before anything is sent to it: the release plays on the device the
     * play started on and the Cast device's limits are no fault of it, so it isn't remembered against it, and the next
     * release is tried, as for any failed attempt. Its words name no device: the log has them.
     */
    @Test
    fun `a file its Cast device can't fully play fails its attempt, the next release is tried, and nothing is remembered`() = runTest {
        val misses = listOf(ProbeResult.NoCastAudio(listOf("DTS-HD", "TrueHD")), ProbeResult.NoCastVideo("VC-1"), ProbeResult.TooBigForCast(2160, 1080))
        misses.forEach { miss ->
            val tried = FakeTried()
            val castProbe = TrackProbe { target -> if (target.candidate.id == "Good:0") miss else ProbeResult.Playable() }
            val events = pipeline(probe = castProbe, tried = tried).play(candidates(Release.Good, Release.Good)).toList()
            val success = assertIs<PipelineEvent.Succeeded>(events.last(), "$miss")
            assertEquals("Good:1", success.target.candidate.id)
            val failure = success.failedAttempts.single().second
            assertIs<AttemptFailure.CastMiss>(failure)
            assertEquals(miss.failure.text, failure.text)
            assertFalse(failure.route != null || failure.blocksIndexer)
            // The probe's step failed with it, as the events show.
            val probed = events.filterIsInstance<PipelineEvent.Step>().first { it.step == PipelineStep.ProbeTracks && it.status is StepStatus.Failed }
            assertEquals("Good:0", probed.candidate.id)
            assertEquals(emptyList(), tried.recorded)
            assertEquals(emptyList(), tried.passwordsNeeded)
            assertEquals(listOf("Good:1"), tried.found)
        }
        assertEquals("No sound for Cast devices (DTS-HD, TrueHD)", AttemptFailure.NoCastAudio(listOf("DTS-HD", "TrueHD")).text)
        assertEquals("No sound for Cast devices", AttemptFailure.NoCastAudio(emptyList()).text)
        assertEquals("Video Cast devices can't show (VC-1)", AttemptFailure.NoCastVideo("VC-1").text)
        assertEquals("2160p over the device's 1080p", AttemptFailure.TooBigForCast(2160, 1080).text)
        // Each kind fails as its own failure.
        assertEquals(listOf(AttemptFailure.NoCastAudio::class, AttemptFailure.NoCastVideo::class, AttemptFailure.TooBigForCast::class), misses.map { it.failure::class })
    }

    /** Every release failing as one its Cast device can't fully play exhausts the play, each with its own words. */
    @Test
    fun `a play whose every file its Cast device can't fully play is exhausted with each one's words`() = runTest {
        val castProbe = TrackProbe { target -> if (target.candidate.id.endsWith(":0")) ProbeResult.NoCastAudio(listOf("DTS-HD")) else ProbeResult.NoCastVideo("VC-1") }
        val last = pipeline(probe = castProbe).play(candidates(Release.Good, Release.Good)).last()
        val exhausted = assertIs<PipelineEvent.Exhausted>(last)
        assertEquals(listOf("No sound for Cast devices (DTS-HD)", "Video Cast devices can't show (VC-1)"), exhausted.failedAttempts.map { it.second.text })
    }

    /**
     * A release whose files are a copy of a disc fails at Opening as soon as they are listed, in the words of its kind,
     * before any file is picked or probed, and the next release plays. Its grab counts as an attempt.
     */
    @Test
    fun `a disc copy fails as soon as it is opened, without a probe, and the next release plays`() = runTest {
        listOf(
            Release.BluRayDisc to DiscKind.BluRay, Release.FlatDisc to DiscKind.BluRay, Release.DvdCopy to DiscKind.Dvd, Release.DiscImage to DiscKind.Image,
        ).forEach { (disc, kind) ->
            val events = pipeline().play(candidates(disc, Release.Good)).toList()
            val success = assertIs<PipelineEvent.Succeeded>(events.last(), "$disc")
            assertEquals("Good:1", success.target.candidate.id)
            assertEquals(listOf("$disc:0" to AttemptFailure.DiscCopy(kind)), success.failedAttempts.map { (candidate, failure) -> candidate.id to failure })
            val steps = events.filterIsInstance<PipelineEvent.Step>().filter { it.candidate.id == "$disc:0" }
            assertEquals(listOf(PipelineStep.FetchNzb, PipelineStep.CheckAvailability, PipelineStep.Open), steps.map { it.step }.distinct(), "$disc")
            assertEquals(AttemptFailure.DiscCopy(kind), (steps.last().status as StepStatus.Failed).failure)
            assertEquals(listOf(1 to 2, 2 to 2), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        }
    }

    /** A disc's streams name no episode: a disc copy in a play of an episode fails before anyone is asked which is the episode. */
    @Test
    fun `a disc copy fails before anyone is asked which file is the episode`() = runTest {
        val asked = mutableListOf<String>()
        val pipeline = asking { candidate, _, choices -> asked += candidate.id; choices.first() }
        listOf(Release.FlatDisc, Release.BluRayDisc, Release.DvdCopy, Release.DiscImage).forEach { disc ->
            val success = assertIs<PipelineEvent.Succeeded>(pipeline.play(candidates(disc, Release.Good), episode(1, 1)).last(), "$disc")
            assertIs<AttemptFailure.DiscCopy>(success.failedAttempts.single().second, "$disc")
        }
        assertEquals(emptyList(), asked)
    }

    /** A disc beside a video of the release's own plays that video, however larger the disc's stream is. */
    @Test
    fun `a disc beside a video of the release's own plays the video`() = runTest {
        val success = assertIs<PipelineEvent.Succeeded>(pipeline().play(candidates(Release.DiscWithMkv)).last())
        assertEquals("Copper.Crown.2019.2160p.BluRay.REMUX.mkv", success.target.file.name)
        assertEquals(emptyList(), success.failedAttempts)
    }

    /**
     * A disc's files posted without their folders beside a video of the release's own are no disc copy. Its larger
     * stream may be picked, which no extractor reads: said as a file the player can't read, and the next release plays.
     */
    @Test
    fun `a flat disc beside a video of its own may pick the disc's stream, which the player can't read, and the next release plays`() = runTest {
        val success = assertIs<PipelineEvent.Succeeded>(pipeline().play(candidates(Release.FlatDiscWithMkv, Release.Good)).last())
        assertEquals("Good:1", success.target.candidate.id)
        assertEquals(listOf("FlatDiscWithMkv:0" to "The player can't read this M2TS file"), success.failedAttempts.map { (candidate, failure) -> candidate.id to failure.text })
    }

    /**
     * A disc copy is remembered, whatever play found it, a film's or an episode's, and nothing else is: the
     * next play skips it without a grab, and it isn't counted among the releases to try.
     */
    @Test
    fun `a disc copy is remembered, and a play that knows it skips it without a fetch`() = runTest {
        val tried = FakeTried()
        assertIs<PipelineEvent.Succeeded>(pipeline(tried = tried).play(candidates(Release.BluRayDisc, Release.Good)).last())
        assertIs<PipelineEvent.Succeeded>(pipeline(tried = tried).play(candidates(Release.DiscImage, Release.Good), episode(1, 1)).last())
        assertIs<PipelineEvent.Succeeded>(pipeline(tried = tried).play(candidates(Release.FlatDiscWithMkv, Release.DiscWithMkv)).last())
        assertEquals(listOf("BluRayDisc:0", "DiscImage:0"), tried.discCopies)
        assertEquals(emptyList(), tried.recorded)
        assertEquals(emptyList(), tried.passwordsNeeded)

        fetched.clear()
        val known = FakeTried(known = mapOf("BluRayDisc:0" to DISC_KNOWN))
        val events = pipeline(tried = known).play(candidates(Release.BluRayDisc, Release.Good)).toList()
        assertEquals("Good:1", assertIs<PipelineEvent.Succeeded>(events.last()).target.candidate.id)
        assertEquals(listOf("Good:1"), fetched)
        assertEquals(listOf("BluRayDisc:0" to DISC_KNOWN), events.filterIsInstance<PipelineEvent.Skipped>().map { it.candidate.id to it.reason })
        assertEquals(listOf(1 to 1), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        // The user's own choice is looked up as nothing known: it is grabbed again, and fails as one again.
        val chosen = FakeTried()
        assertIs<PipelineEvent.Exhausted>(pipeline(tried = chosen).play(candidates(Release.BluRayDisc)).last())
        assertEquals(listOf("BluRayDisc:0"), chosen.discCopies)
    }

    /**
     * A disc copy is the release's shape, not a gap in one file a play picked of several: remembered whatever the
     * source says of what failed ([AttemptSteps.wholeRelease]), as no source's file of a release can play it.
     */
    @Test
    fun `a disc copy is remembered though what failed was one file of the release's`() = runTest {
        val source = object : PlaySource<PlaybackTarget> {
            override val steps = listOf(PipelineStep.Open, PipelineStep.ProbeTracks)

            override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): PlaybackTarget {
                wholeRelease = false
                step<Nothing>(candidate, PipelineStep.Open) { StepResult.Fail(AttemptFailure.DiscCopy(DiscKind.Dvd)) }
            }

            override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: PlaybackTarget): AttemptFailure = error("nothing was opened")

            override fun failed(error: Exception): AttemptFailure = AttemptFailure.Failed(error.toUsenetError())
        }
        val tried = FakeTried()
        assertIs<PipelineEvent.Exhausted>(PlayPipeline({ source }, probe, dispatcher = Dispatchers.Unconfined, tried = tried).play(candidates(Release.DvdCopy)).last())
        assertEquals(listOf("DvdCopy:0"), tried.discCopies)
    }

    @Test
    fun `at most three releases are fetched`() = runTest {
        val last = pipeline().play(candidates(Release.Missing, Release.Missing, Release.Missing, Release.Good)).last()
        val exhausted = assertIs<PipelineEvent.Exhausted>(last)
        assertEquals(3, exhausted.failedAttempts.size)
        assertEquals(1, exhausted.notTried)
        assertEquals(3, fetched.size)
    }

    @Test
    fun `provider errors stop at once`() = runTest {
        val exhausted = assertIs<PipelineEvent.Exhausted>(pipeline().play(candidates(Release.ProviderDown, Release.Good)).last())
        assertEquals(1, fetched.size)
        assertEquals(PlayCandidate.USENET, exhausted.failedAttempts.single().second.route)
        assertEquals(1, exhausted.notTried)
    }

    /**
     * Fails every release it is asked for at its first step, as an outage of [route] would, stopping that route: what a
     * source other than Usenet says when, say, its account's limit is reached. Nothing of it is fetched.
     */
    private class RouteDown(private val route: String) : PlaySource<PlaybackTarget> {
        val tried = mutableListOf<String>()
        override val steps = listOf(PipelineStep.Open, PipelineStep.ProbeTracks)

        override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): PlaybackTarget {
            tried += candidate.id
            step<Nothing>(candidate, PipelineStep.Open) { StepResult.Fail(AttemptFailure.FetchFailed("out of reach", route = route)) }
        }

        override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: PlaybackTarget): AttemptFailure = error("nothing was opened")

        override fun failed(error: Exception): AttemptFailure = AttemptFailure.Failed(error.toUsenetError())
    }

    /** Usenet's releases through [FakeUsenet], and those of the source "down" through [down]. */
    private fun routed(down: RouteDown, maxAttempts: Int = 5): PlayPipeline {
        val usenet = FakeUsenet()
        return PlayPipeline({ if (it.source == "down") down else usenet }, probe, maxAttempts, dispatcher = Dispatchers.Unconfined)
    }

    /**
     * A provider failing stops the Usenet route alone: Usenet's releases after it aren't fetched, and a release that
     * plays by another route is tried as before (played here by the same fake source: the loop goes by the routes alone).
     */
    @Test
    fun `a provider failure stops the Usenet releases and leaves another route's to be tried`() = runTest {
        val elsewhere = PlayCandidate("Good:2", "Good 2", routes = listOf("service:a"))
        val events = pipeline().play(candidates(Release.ProviderDown, Release.Good) + elsewhere).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("Good:2", played.target.candidate.id)
        assertEquals(listOf("ProviderDown:0", "Good:2"), fetched)
        assertEquals(listOf("ProviderDown:0" to PlayCandidate.USENET), played.failedAttempts.map { (candidate, failure) -> candidate.id to failure.route })
        // Release 2 of 2: the Usenet release after the failure is no longer counted as one to try.
        assertEquals(listOf(1 to 3, 2 to 2), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
    }

    /** A release that plays by two routes is tried while either is open, and passed over once both are stopped. */
    @Test
    fun `a release with two routes is tried until both are stopped`() = runTest {
        val down = RouteDown("service:a")
        val both = listOf(PlayCandidate.USENET, "service:a")
        val releases = listOf(
            PlayCandidate("ProviderDown:0", "ProviderDown 0"),
            PlayCandidate("Down:1", "Down 1", routes = both, source = "down"),
            PlayCandidate("Good:2", "Good 2", routes = both),
            PlayCandidate("Good:3", "Good 3", routes = listOf("service:b")),
        )
        val played = assertIs<PipelineEvent.Succeeded>(routed(down).play(releases).last())
        assertEquals("Good:3", played.target.candidate.id)
        assertEquals(
            listOf("ProviderDown:0" to PlayCandidate.USENET, "Down:1" to "service:a"),
            played.failedAttempts.map { (candidate, failure) -> candidate.id to failure.route },
        )
        assertEquals(listOf("Down:1"), down.tried)
        assertEquals(listOf("ProviderDown:0", "Good:3"), fetched)

        // With no release left on an open route, the play ends: the release passed over counts as not tried, as Usenet's
        // releases after a provider failure always have, and isn't taken for one an indexer refused.
        fetched.clear()
        val exhausted = assertIs<PipelineEvent.Exhausted>(routed(RouteDown("service:a")).play(releases.take(3)).last())
        assertEquals(listOf("ProviderDown:0", "Down:1"), exhausted.failedAttempts.map { it.first.id })
        assertEquals(1 to 0, exhausted.notTried to exhausted.skipped)
        assertEquals(listOf("ProviderDown:0"), fetched)
    }

    private class OneFile(override val id: String, override val title: String) : FileListing<Listed> {
        override val files = listOf(Listed(0, "$title.mkv", 900))
    }

    private class CachedTarget(override val candidate: PlayCandidate, override val listing: OneFile) : PlaybackTarget {
        override val file: Listed get() = listing.files.single()
        override val uri: String get() = "debrid://${listing.id}/file/0"
        override suspend fun keep() = Unit
    }

    /**
     * A source of releases a service holds: Preparing first, whatever the service needs, then Opening and the probe.
     * A release may play from several accounts, each its own route, tried in turn within its one attempt: one of the
     * accounts [down] stops its route there ([AttemptSteps.stop]) and the next is tried; one it is [missing] from fails the
     * release alone. Routes stopped before aren't tried. A release [failing] fails as given there, its route stopped or not
     * as the failure says.
     */
    private class Cached(
        private val down: Set<String> = emptySet(),
        private val missing: Set<String> = emptySet(),
        private val failing: Map<String, AttemptFailure> = emptyMap(),
    ) : PlaySource<CachedTarget> {
        val tried = mutableListOf<String>()
        override val steps = listOf(PipelineStep.Prepare, PipelineStep.Open, PipelineStep.ProbeTracks)

        override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): CachedTarget {
            step(candidate, PipelineStep.Prepare) {
                var failure: AttemptFailure? = null
                for (route in candidate.routes.filterNot { it in stopped }) {
                    tried += "${candidate.id} on $route"
                    failing[candidate.id]?.let { return@step StepResult.Fail(it) }
                    when (route) {
                        in down -> stop(route).also { failure = AttemptFailure.ServiceDown(route) }
                        in missing -> failure = AttemptFailure.NoLongerCached
                        else -> return@step StepResult.Ok(route)
                    }
                }
                StepResult.Fail(failure ?: AttemptFailure.NoLongerCached)
            }
            val listing = OneFile(candidate.id, candidate.title)
            step(candidate, PipelineStep.Open) { StepResult.Ok(Unit, listing.files.single().name) }
            return CachedTarget(candidate, listing)
        }

        override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: CachedTarget): AttemptFailure = AttemptFailure.Failed(failed.error)

        override fun failed(error: Exception): AttemptFailure = AttemptFailure.Failed(error.toUsenetError())
    }

    /** Usenet's releases through [FakeUsenet], and torrents through [cached], each probed as playable once opened. */
    private fun withService(cached: Cached): PlayPipeline {
        val usenet = FakeUsenet()
        val probe = TrackProbe { target -> if (target is CachedTarget) ProbeResult.Playable() else this.probe.probe(target) }
        return PlayPipeline({ if (it.source == TORRENT) cached else usenet }, probe, maxAttempts = 5, dispatcher = Dispatchers.Unconfined)
    }

    private fun torrent(n: Int, vararg routes: String) = PlayCandidate("btih:$n", "Harbor.Lights.S01E02.Torrent$n", "Home Prowlarr", source = TORRENT, routes = routes.toList())

    /** In a mixed list a torrent tried after a Usenet release that failed has its source's steps, Preparing first. */
    @Test
    fun `in a mixed list a torrent after a Usenet release that failed starts at Preparing`() = runTest {
        val events = withService(Cached()).play(candidates(Release.Missing) + torrent(1, "service:a")).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("btih:1", played.target.candidate.id)
        assertEquals(listOf("Missing:0"), played.failedAttempts.map { it.first.id })
        val first = events.filterIsInstance<PipelineEvent.Step>().filter { it.status is StepStatus.Running }.groupBy { it.candidate.id }.mapValues { it.value.first().step }
        assertEquals(mapOf("Missing:0" to PipelineStep.FetchNzb, "btih:1" to PipelineStep.Prepare), first)
        val done = events.filterIsInstance<PipelineEvent.Step>().filter { it.candidate.id == "btih:1" && it.status is StepStatus.Done }.map { it.step }
        assertEquals(listOf(PipelineStep.Prepare, PipelineStep.Open, PipelineStep.ProbeTracks), done)
        assertEquals(listOf(1 to 2, 2 to 2), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
    }

    /**
     * A route a source stops inside an attempt ([AttemptSteps.stop]: an account that stopped answering) is passed over by
     * the rest of the play, as one a failure stops: the release goes on to its next account, and a release cached on that
     * account alone isn't tried.
     */
    @Test
    fun `a route stopped inside an attempt moves on to the next account, and its other releases are passed over`() = runTest {
        val cached = Cached(down = setOf("service:a"), missing = setOf("service:b"))
        val releases = listOf(torrent(1, "service:a", "service:b"), torrent(2, "service:a"), torrent(3, "service:c"))
        val events = withService(cached).play(releases).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("btih:3", played.target.candidate.id)
        assertEquals(listOf("btih:1 on service:a", "btih:1 on service:b", "btih:3 on service:c"), cached.tried)
        assertEquals(listOf("btih:1" to null), played.failedAttempts.map { (candidate, failure) -> candidate.id to failure.route })
        // Release 2 of 2: the one cached on the stopped account alone is no longer counted, as after a provider failure.
        assertEquals(listOf(1 to 3, 2 to 2), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
    }

    /**
     * The routes the play stopped come with its success, those stopped inside the attempt that played among them (an
     * account that stopped answering before the next played the release), so trying another release passes over that
     * account's releases too.
     */
    @Test
    fun `the routes a play stopped come with its success, those stopped inside the attempt that played among them`() = runTest {
        val played = assertIs<PipelineEvent.Succeeded>(withService(Cached(down = setOf("service:a"))).play(listOf(torrent(1, "service:a", "service:b"))).toList().last())
        assertEquals("btih:1", played.target.candidate.id)
        assertEquals(emptyList(), played.failedAttempts)
        assertEquals(setOf("service:a"), played.stopped)
        // A play that stopped nothing says so.
        assertEquals(emptySet(), assertIs<PipelineEvent.Succeeded>(withService(Cached()).play(listOf(torrent(2, "service:b"))).toList().last()).stopped)
    }

    /**
     * An account whose limit is reached, or whose key is refused, stops its route: its other releases are passed
     * over without an attempt, and a Usenet release plays.
     */
    @Test
    fun `a limit or a refused key on an account passes over its other releases, and a Usenet release plays`() = runTest {
        listOf(AttemptFailure.ServiceLimit(null, "service:a"), AttemptFailure.KeyRefused("TorBox", "service:a")).forEach { failure ->
            fetched.clear()
            val cached = Cached(failing = mapOf("btih:1" to failure))
            val events = withService(cached).play(listOf(torrent(1, "service:a"), torrent(2, "service:a")) + candidates(Release.Good)).toList()
            val played = assertIs<PipelineEvent.Succeeded>(events.last(), "$failure")
            assertEquals("Good:0", played.target.candidate.id)
            assertEquals(listOf("btih:1 on service:a"), cached.tried)
            assertEquals(listOf("btih:1" to "service:a"), played.failedAttempts.map { (candidate, f) -> candidate.id to f.route })
            assertEquals(listOf("Good:0"), fetched)
        }
    }

    /**
     * Of the releases left untried, those left as every route they play by stopped are counted apart, as no grab was
     * saved by leaving them: an account's limit leaves its other release with nothing to play from.
     */
    @Test
    fun `releases left with no route to play by are counted apart from those left to save grabs`() = runTest {
        val cached = Cached(failing = mapOf("btih:1" to AttemptFailure.ServiceLimit(null, "service:a")))
        val exhausted = assertIs<PipelineEvent.Exhausted>(withService(cached).play(listOf(torrent(1, "service:a"), torrent(2, "service:a"))).toList().last())
        assertEquals(1 to 1, exhausted.notTried to exhausted.noRoute)
    }

    /** A release the service won't take is that release's alone: its account's route stays open, and its next release is tried. */
    @Test
    fun `a release its account won't take is passed, and the same account's next release is tried`() = runTest {
        val cached = Cached(failing = mapOf("btih:1" to AttemptFailure.ServiceRefused(tooLarge = true)))
        val played = assertIs<PipelineEvent.Succeeded>(withService(cached).play(listOf(torrent(1, "service:a"), torrent(2, "service:a"))).last())
        assertEquals("btih:2", played.target.candidate.id)
        assertEquals(listOf("btih:1 on service:a", "btih:2 on service:a"), cached.tried)
    }

    /** A provider failing stops Usenet's route alone: the torrents after it are tried, as an outage of a service leaves Usenet's. */
    @Test
    fun `a provider failure leaves the torrents to be tried`() = runTest {
        val cached = Cached()
        val played = assertIs<PipelineEvent.Succeeded>(withService(cached).play(candidates(Release.ProviderDown, Release.Good) + torrent(1, "service:a")).last())
        assertEquals("btih:1", played.target.candidate.id)
        assertEquals(listOf("ProviderDown:0"), fetched)
        assertEquals(listOf("btih:1 on service:a"), cached.tried)
    }

    @Test
    fun `an indexer refusing a grab moves on to the other indexers`() = runTest {
        refusals.keys.forEach { refusal ->
            fetched.clear()
            val last = pipeline().play(fromIndexers("A" to refusal, "B" to Release.Good, "B" to Release.Good)).last()
            assertEquals("Good:B1", assertIs<PipelineEvent.Succeeded>(last, "$refusal").target.candidate.id)
            assertEquals(listOf("$refusal:A0", "Good:B1"), fetched)
        }
    }

    @Test
    fun `later releases from a refusing indexer are skipped without a fetch`() = runTest {
        refusals.keys.forEach { refusal ->
            fetched.clear()
            val events = pipeline().play(fromIndexers("A" to refusal, "A" to Release.Good, "B" to Release.Good)).toList()
            assertEquals("Good:B2", assertIs<PipelineEvent.Succeeded>(events.last(), "$refusal").target.candidate.id)
            assertEquals(listOf("$refusal:A0", "Good:B2"), fetched)
            // Once A refuses, B's release is the last one left to try.
            val starts = events.filterIsInstance<PipelineEvent.AttemptStarted>()
            assertEquals(listOf(1 to 3, 2 to 2), starts.map { it.attempt to it.of })
        }
    }

    @Test
    fun `skipped releases don't use up attempts`() = runTest {
        val events = pipeline().play(
            fromIndexers("A" to Release.GrabLimit, "A" to Release.Good, "A" to Release.Good, "B" to Release.Missing, "B" to Release.Good),
        ).toList()
        assertEquals("Good:B4", assertIs<PipelineEvent.Succeeded>(events.last()).target.candidate.id)
        val starts = events.filterIsInstance<PipelineEvent.AttemptStarted>()
        assertEquals(listOf("GrabLimit:A0", "Missing:B3", "Good:B4"), starts.map { it.candidate.id })
        assertEquals(listOf(1 to 3, 2 to 3, 3 to 3), starts.map { it.attempt to it.of })
    }

    @Test
    fun `skipped releases are counted apart from untried ones`() = runTest {
        val exhausted = assertIs<PipelineEvent.Exhausted>(
            pipeline().play(
                fromIndexers(
                    "A" to Release.GrabLimit, "A" to Release.Good, "B" to Release.Missing,
                    "B" to Release.Missing, "B" to Release.Good, "A" to Release.Good,
                ),
            ).last(),
        )
        assertEquals(listOf("GrabLimit:A0", "Missing:B2", "Missing:B3"), exhausted.failedAttempts.map { it.first.id })
        assertEquals(1, exhausted.notTried)
        assertEquals(2, exhausted.skipped)
    }

    @Test
    fun `releases all from a refusing indexer stop after the first`() = runTest {
        val runs = refusals.keys.flatMap { refusal ->
            listOf(
                refusal to fromIndexers("A" to refusal, "A" to Release.Good, "A" to Release.Good),
                // Without indexer ids every release counts as the same indexer's.
                refusal to candidates(refusal, Release.Good, Release.Good),
            )
        }
        runs.forEach { (refusal, candidates) ->
            fetched.clear()
            val exhausted = assertIs<PipelineEvent.Exhausted>(pipeline().play(candidates).last(), "$refusal")
            assertEquals(listOf(candidates.first().id), fetched)
            assertEquals("Couldn't get the NZB: ${refusals.getValue(refusal)}", exhausted.failedAttempts.single().second.text)
            assertEquals(0, exhausted.notTried)
            assertEquals(2, exhausted.skipped)
        }
    }

    @Test
    fun `episode picker finds the episode inside a season pack`() = runTest {
        val success = pipeline().play(candidates(Release.Pack), episode(1, 3)).last()
        assertEquals("Show.S01E03.1080p.WEB.mkv", assertIs<PipelineEvent.Succeeded>(success).target.file.name)
    }

    @Test
    fun `episode picker takes an obfuscated single video but not a pack without the episode`() = runTest {
        val single = pipeline().play(candidates(Release.Good), episode(1, 3)).last()
        assertEquals("a8f3c1.mkv", assertIs<PipelineEvent.Succeeded>(single).target.file.name)

        val missing = assertIs<PipelineEvent.Exhausted>(pipeline().play(candidates(Release.Pack), episode(1, 9)).last())
        assertEquals("No file for S01E09", missing.failedAttempts.single().second.text)
    }

    @Test
    fun `episode picker wants the episode's video or archive volumes by name`() {
        val picker = episode(1, 3)
        listOf("Show.S01E03.1080p.WEB.mkv", "Show.S01E03.1080p.WEB.part01.rar", "Show.S01E03.1080p.WEB.r00", "show.s01e03.720p.mp4")
            .forEach { assertTrue(picker.wants(it), it) }
        listOf("Show.S01E04.1080p.WEB.mkv", "Show.S01E03.1080p.WEB.nfo", "Show.S01E03.1080p.WEB.srt", "Show.S01E03.sample.mkv", "Show.S01E03.vol00+01.par2", "a8f3c1.mkv")
            .forEach { assertFalse(picker.wants(it), it) }
        assertFalse(FilePicker.Largest.wants("Show.S01E03.1080p.WEB.mkv"))
    }

    @Test
    fun `attempt events count the attempts`() = runTest {
        val starts = pipeline().play(candidates(Release.Missing, Release.Good, Release.Good, Release.Good))
            .filterIsInstance<PipelineEvent.AttemptStarted>().toList()
        assertEquals(listOf(1 to 3, 2 to 3), starts.map { it.attempt to it.of })
    }

    @Test
    fun `stopping collection stops fetching`() = runTest {
        pipeline().play(candidates(Release.Missing, Release.Good)).first()
        assertEquals(emptyList(), fetched)
    }

    @Test
    fun `episode picker rejects a single video named as another episode`() = runTest {
        val exhausted = assertIs<PipelineEvent.Exhausted>(pipeline().play(candidates(Release.OtherEpisode), episode(1, 3)).last())
        assertEquals("No file for S01E03", exhausted.failedAttempts.single().second.text)
    }

    private fun asking(chooser: FileChooser) = pipeline(usenet = FakeUsenet(chooser = chooser))

    /**
     * A sample is never the episode where the release holds other videos: in a pack whose videos name no episode, the one
     * whose sample is named for it is asked for as any other, and with no one to ask has no file; in a release of one
     * video under an obfuscated name, that video plays.
     */
    @Test
    fun `a sample named for the episode isn't played as it`() = runTest {
        val asked = mutableListOf<List<String>>()
        val pipeline = asking { _, _, choices -> asked += choices.map { it.name }; choices.single { it.name == "Show.1_6.mp4" } }
        val success = assertIs<PipelineEvent.Succeeded>(pipeline.play(candidates(Release.UnnamedPackNamedSample), episode(1, 1)).last())
        assertEquals("Show.1_6.mp4", success.target.file.name)
        assertEquals(listOf(listOf("Show.1_6.mp4", "Show.2-6.mp4", "Show.3_6.mp4", "Show.S01E01.Sample.mkv")), asked)
        val exhausted = assertIs<PipelineEvent.Exhausted>(pipeline().play(candidates(Release.UnnamedPackNamedSample), episode(1, 1)).last())
        assertEquals("No file for S01E01", exhausted.failedAttempts.single().second.text)
        assertEquals("a8f3c1.mkv", assertIs<PipelineEvent.Succeeded>(pipeline().play(candidates(Release.ObfuscatedNamedSample), episode(1, 1)).last()).target.file.name)
    }

    /** With no one to ask, or nothing chosen, such a pack has no file for the episode, as before. */
    @Test
    fun `with no one to ask or nothing chosen, a pack whose videos name no episode has no file for it`() = runTest {
        listOf(pipeline(), asking { _, _, _ -> null }).forEach { pipeline ->
            val exhausted = assertIs<PipelineEvent.Exhausted>(pipeline.play(candidates(Release.UnnamedPack), episode(1, 2)).last())
            assertEquals("No file for S01E02", exhausted.failedAttempts.single().second.text)
        }
    }

    /**
     * Nothing is asked where the names tell: videos named for other episodes (the pack lacks this one), beside an extra
     * named for none too, a release named for another season or for other episodes, a release of one video (taken as the
     * episode, or named for another), one with a file named for the episode, and a film.
     */
    @Test
    fun `nothing is asked where the names tell`() = runTest {
        val asked = mutableListOf<String>()
        val pipeline = asking { candidate, _, choices -> asked += candidate.id; choices.first() }
        suspend fun failure(candidates: List<PlayCandidate>, picker: FilePicker) =
            assertIs<PipelineEvent.Exhausted>(pipeline.play(candidates, picker).last()).failedAttempts.single().second.text
        assertEquals("No file for S01E09", failure(candidates(Release.Pack), episode(1, 9)))
        assertEquals("No file for S01E09", failure(candidates(Release.PackWithExtra), episode(1, 9)))
        assertEquals("No file for S02E01", failure(candidates(Release.UnnamedPack), episode(2, 1)))
        assertEquals("No file for S01E02", failure(listOf(PlayCandidate("UnnamedPack:0", "Show.S01E05E06.1080p.WEB-GRP")), episode(1, 2)))
        assertEquals("No file for S01E03", failure(candidates(Release.OtherEpisode), episode(1, 3)))
        assertEquals("a8f3c1.mkv", assertIs<PipelineEvent.Succeeded>(pipeline.play(candidates(Release.Good), episode(1, 3)).last()).target.file.name)
        assertEquals("Show.S01E03.1080p.WEB.mkv", assertIs<PipelineEvent.Succeeded>(pipeline.play(candidates(Release.Pack), episode(1, 3)).last()).target.file.name)
        assertEquals("Show.2-6.mp4", assertIs<PipelineEvent.Succeeded>(pipeline.play(candidates(Release.UnnamedPack)).last()).target.file.name)
        assertEquals(emptyList(), asked)
    }

    /**
     * A step still running is logged every so often (a play stalled, and nothing said where), as a stalled availability
     * check on a large release would be. Once the step ends it says no more, and the play goes on and ends: the line's
     * watch is a child of the pipeline's flow, which waits for its children, so one left running would hold the play open.
     */
    @Test
    fun `a step still running says so every so often, and no more once it ends`() = runBlocking<Unit> {
        val gate = CompletableDeferred<Unit>()
        val pipeline = pipeline(usenet = FakeUsenet(checking = { gate.await() }), stepWarnAfter = 150.milliseconds)
        val (last, log) = logOf {
            coroutineScope {
                val played = async { pipeline.play(candidates(Release.Good)).last() }
                // About three lines' worth.
                delay(500)
                gate.complete(Unit)
                val ended = withTimeout(5_000) { played.await() }
                // A watch left running would go on saying so.
                delay(600)
                ended
            }
        }
        assertIs<PipelineEvent.Succeeded>(last)
        val lines = log.lines().filter { "still running after" in it }
        assertTrue(lines.size in 2..4, "expected about three lines, got ${lines.size}:\n$log")
        lines.forEach { assertTrue(Regex("""play_pipeline: Good 0 CheckAvailability still running after \d+ s""").containsMatchIn(it), it) }
        // None once the step had ended.
        val stepDone = log.lines().indexOfFirst { Regex("""play_pipeline: Good 0 CheckAvailability \d+ ms""").containsMatchIn(it) }
        assertTrue(stepDone >= 0, log)
        assertTrue(log.lines().drop(stepDone).none { "still running after" in it }, "a line after the step ended:\n$log")
    }

    /**
     * Open waits for the user where it asks which file plays: nothing is stuck however long they take, so nothing says
     * it's still running meanwhile. Opening before the question is watched as any step is.
     */
    @Test
    fun `a step waiting for the user's choice of file doesn't say it's still running`() = runBlocking<Unit> {
        val opening = CompletableDeferred<Unit>()
        val asked = CompletableDeferred<Unit>()
        val answer = CompletableDeferred<Unit>()
        val chooser = FileChooser { _, _, choices ->
            asked.complete(Unit)
            answer.await()
            choices.single { it.name == "Show.2-6.mp4" }
        }
        val pipeline = pipeline(usenet = FakeUsenet(chooser = chooser, opening = { opening.await() }), stepWarnAfter = 150.milliseconds)
        val (last, log) = logOf {
            coroutineScope {
                val played = async { pipeline.play(candidates(Release.UnnamedPack), episode(1, 2)).last() }
                // About three lines' worth of opening, then as long again for the user's answer.
                delay(500)
                opening.complete(Unit)
                withTimeout(5_000) { asked.await() }
                delay(500)
                answer.complete(Unit)
                withTimeout(5_000) { played.await() }
            }
        }
        assertEquals("Show.2-6.mp4", assertIs<PipelineEvent.Succeeded>(last).target.file.name)
        val lines = log.lines()
        val question = lines.indexOfFirst { "asking which of" in it }
        val chosen = lines.indexOfFirst { "Show.2-6.mp4 chosen" in it }
        assertTrue(question in 0 until chosen, log)
        val opened = lines.take(question).filter { "Open still running after" in it }
        assertTrue(opened.size in 2..4, "expected about three lines while it opened, got ${opened.size}:\n$log")
        assertTrue(lines.subList(question, chosen).none { "still running after" in it }, "a line while the user chose:\n$log")
    }

    @Test
    fun `a gap found without the availability check keeps the plain line`() {
        // Opening the release or playing it finds a missing article without counting any.
        assertEquals("Incomplete on your providers", AttemptFailure.MissingArticles("430 No such article").text)
        assertEquals(
            "Incomplete on the providers that answered · a provider failed: AUTHINFO PASS failed: 481 Authentication failed",
            AttemptFailure.MissingArticles("No provider could answer", unanswered = UsenetError.AuthFailed("AUTHINFO PASS failed: 481 Authentication failed")).text,
        )
    }

    /**
     * A release remembered as missing on every provider in use is skipped before its NZB is fetched: no grab, and no
     * attempt used up, so the next release is tried in its place. The play's events say why, and so does the log.
     */
    @Test
    fun `a release known to be missing is skipped without a fetch and doesn't use up an attempt`() = runTest {
        val tried = FakeTried(known = mapOf("Missing:0" to KNOWN, "Missing:2" to KNOWN))
        val (events, log) = logOf {
            pipeline(maxAttempts = 2, tried = tried).play(candidates(Release.Missing, Release.Missing, Release.Missing, Release.Good)).toList()
        }
        val success = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("Good:3", success.target.candidate.id)
        assertEquals(listOf("Missing:1"), success.failedAttempts.map { it.first.id })
        assertEquals(listOf("Missing:1", "Good:3"), fetched)
        assertEquals(listOf("Missing:0" to KNOWN, "Missing:2" to KNOWN), events.filterIsInstance<PipelineEvent.Skipped>().map { it.candidate.id to it.reason })
        assertEquals(listOf(1 to 2, 2 to 2), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        assertTrue("play_pipeline: Missing 0 skipped without a fetch: $KNOWN" in log, log)

        // Every one known: nothing is fetched, nothing failed, nothing is left to try.
        fetched.clear()
        val exhausted = assertIs<PipelineEvent.Exhausted>(pipeline(tried = FakeTried(mapOf("Missing:0" to KNOWN, "Good:1" to KNOWN))).play(candidates(Release.Missing, Release.Good)).last())
        assertEquals(emptyList(), fetched)
        assertEquals(emptyList(), exhausted.failedAttempts)
        assertEquals(0 to 0, exhausted.notTried to exhausted.skipped)
    }

    /**
     * "Release 1 of 3" counts the releases that will be tried, not those known to be missing, which are skipped when their
     * turn comes; nor does "not tried" count them when the attempts run out, as no play would try them. Each is looked up
     * once, and only as far as the count needs.
     */
    @Test
    fun `releases known to be missing aren't counted as releases to try`() = runTest {
        val tried = FakeTried(known = mapOf("Missing:1" to KNOWN, "Missing:2" to KNOWN))
        val events = pipeline(maxAttempts = 3, tried = tried).play(candidates(Release.Missing, Release.Missing, Release.Missing)).toList()
        assertEquals(listOf(1 to 1), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        assertEquals(0, assertIs<PipelineEvent.Exhausted>(events.last()).notTried)
        assertEquals(listOf("Missing:0", "Missing:1", "Missing:2"), tried.lookedUp)

        val ranOut = FakeTried(known = mapOf("Missing:1" to KNOWN))
        val exhausted = assertIs<PipelineEvent.Exhausted>(
            pipeline(maxAttempts = 1, tried = ranOut).play(candidates(Release.Missing, Release.Missing, Release.Good, Release.Good)).last(),
        )
        assertEquals(2, exhausted.notTried)
        assertEquals(listOf("Missing:0", "Missing:1", "Good:2", "Good:3"), ranOut.lookedUp)

        // Two attempts: the count looks no further than the second release it would try.
        val ahead = FakeTried(known = mapOf("Missing:1" to KNOWN))
        val started = pipeline(maxAttempts = 2, tried = ahead).play(candidates(Release.Good, Release.Missing, Release.Good, Release.Good)).toList()
        assertEquals(listOf(1 to 2), started.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        assertEquals(listOf("Good:0", "Missing:1", "Good:2"), ahead.lookedUp)
    }

    /**
     * A gap the availability check finds is remembered as on the providers in use, also while one is left out (it wasn't
     * asked: it is tried when it is back). One found opening the release or by its probe is remembered as on the providers
     * fetched from, which may not be all of those in use: nzb-streamer passes over one whose circuit breaker is open without
     * asking it. So is a file the NZB seems to lack, not as on any provider: it may be one the NZB holds whose first article
     * the providers fetched from lack, which another provider may have.
     */
    @Test
    fun `a release found missing is remembered as on the providers in use when checked, on those fetched from when fetched`() = runTest {
        val gapInStream = TrackProbe { target -> if (target.candidate.id == "Good:3") ProbeResult.Failed(UsenetError.MissingArticle("430 No such article")) else ProbeResult.Playable() }
        listOf(false, true).forEach { leftOut ->
            val tried = FakeTried()
            val pipeline = pipeline(maxAttempts = 5, tried = tried, probe = gapInStream, usenet = FakeUsenet(providersLeftOut = leftOut))
            assertIs<PipelineEvent.Succeeded>(pipeline.play(candidates(Release.Missing, Release.GapOnOpen, Release.VolumeNotInNzb, Release.Good, Release.Good)).last())
            assertEquals(
                listOf(
                    "Missing:0" to MissingOn.ProvidersInUse, "GapOnOpen:1" to MissingOn.ProvidersFetchedFrom,
                    "VolumeNotInNzb:2" to MissingOn.ProvidersFetchedFrom, "Good:3" to MissingOn.ProvidersFetchedFrom,
                ),
                tried.recorded,
                "left out: $leftOut",
            )
        }
    }

    /**
     * Only a gap every provider that answered has is remembered: not a provider failing where the others lack an article
     * (it may have it), a timeout, a failed grab, a passworded release, one without playable audio or video or that
     * can't be streamed, or one broken otherwise.
     */
    @Test
    fun `what isn't the providers lacking a release's articles isn't remembered`() = runTest {
        val tried = FakeTried()
        val pipeline = pipeline(maxAttempts = 10, tried = tried, usenet = FakeUsenet(providerFallback = true))
        val releases = arrayOf(
            Release.ProviderDown, Release.ProviderDownOnOpen, Release.TimesOut, Release.Unfetchable, Release.Passworded,
            Release.Silent, Release.CompressedOnly, Release.VolumeUnplaced, Release.NoVideoDecoder, Release.Good,
        )
        assertEquals("Good:9", assertIs<PipelineEvent.Succeeded>(pipeline.play(candidates(*releases)).last()).target.candidate.id)
        assertEquals(emptyList(), tried.recorded)
    }

    /**
     * A release that passes every step is told to [TriedReleases.found]: whatever was remembered of it for the providers in
     * use is wrong (a provider's bad day, a post still propagating), as when the user chose a release marked missing and it
     * played. The releases that failed before it aren't, nor is anything when none plays.
     */
    @Test
    fun `a release that plays is told as found, so what was remembered of it is forgotten`() = runTest {
        val tried = FakeTried()
        assertIs<PipelineEvent.Succeeded>(pipeline(tried = tried).play(candidates(Release.Missing, Release.Good)).last())
        assertEquals(listOf("Good:1"), tried.found)
        assertEquals(listOf("Missing:0" to MissingOn.ProvidersInUse), tried.recorded)

        val none = FakeTried()
        assertIs<PipelineEvent.Exhausted>(pipeline(tried = none).play(candidates(Release.Missing, Release.Passworded)).last())
        assertEquals(emptyList(), none.found)
    }

    /** What is remembered is a help, never in the way: failing to read it tries the release, failing to write it goes on. */
    @Test
    fun `remembering failing doesn't fail the play`() = runTest {
        val failing = object : TriedReleases {
            override suspend fun knownMissing(candidate: PlayCandidate): String? = throw java.io.IOException("disk I/O error")
            override suspend fun missing(candidate: PlayCandidate, on: MissingOn): Unit = throw java.io.IOException("disk I/O error")
            override suspend fun found(candidate: PlayCandidate): Unit = throw java.io.IOException("disk I/O error")
            override suspend fun passwordNeeded(candidate: PlayCandidate): Unit = throw java.io.IOException("disk I/O error")
            override suspend fun discCopy(candidate: PlayCandidate): Unit = throw java.io.IOException("disk I/O error")
        }
        val success = assertIs<PipelineEvent.Succeeded>(
            pipeline(maxAttempts = 4, tried = failing).play(candidates(Release.Missing, Release.Passworded, Release.BluRayDisc, Release.Good)).last(),
        )
        assertEquals("Good:3", success.target.candidate.id)
        assertEquals(listOf("Missing:0", "Passworded:1", "BluRayDisc:2", "Good:3"), fetched)
    }

    /**
     * A release found encrypted, with no password to open it, is remembered as needing one ([TriedReleases.passwordNeeded]),
     * so later plays can skip it; no other failure is, and nor is the release that plays.
     */
    @Test
    fun `a release found encrypted with no password is remembered as needing one, and nothing else is`() = runTest {
        val tried = FakeTried()
        val releases = candidates(Release.Missing, Release.Passworded, Release.Unfetchable, Release.Silent, Release.CompressedOnly, Release.Good)
        val success = assertIs<PipelineEvent.Succeeded>(pipeline(maxAttempts = 6, tried = tried).play(releases).last())
        assertEquals("Good:5", success.target.candidate.id)
        assertEquals("Passworded:1" to AttemptFailure.Passworded, success.failedAttempts[1].let { (candidate, failure) -> candidate.id to failure })
        assertEquals(listOf("Passworded:1"), tried.passwordsNeeded)
    }

    /**
     * A gap in the one file a play picked of several (an episode of a season pack, whose others may be whole) isn't
     * remembered against the release: its source says so ([AttemptSteps.wholeRelease] false). The same gap where the
     * source says it is in what any play of the release would use is remembered, as on the providers in use.
     */
    @Test
    fun `a gap in the file a play picked of several isn't remembered against the release, one in the whole release is`() = runTest {
        /** Opens a pack, picks the episode, saying whether a failure now is the [whole] release's, and finds a gap in it. */
        fun gapInPick(whole: Boolean) = object : PlaySource<FakeTarget> {
            override val steps = listOf(PipelineStep.Open, PipelineStep.CheckAvailability, PipelineStep.ProbeTracks)

            override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): FakeTarget {
                val listing = listingOf(candidate)
                val file = step(candidate, PipelineStep.Open) {
                    wholeRelease = whole
                    pickFile.pick(listing)?.let { StepResult.Ok(it, it.name) } ?: StepResult.Fail(pickFile.noMatchIn(listing))
                }
                step<Unit>(candidate, PipelineStep.CheckAvailability) {
                    StepResult.Fail(AttemptFailure.MissingArticles("Missing article <x>", missing = 1, checked = 3))
                }
                return FakeTarget(candidate, listing, file)
            }

            override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: FakeTarget): AttemptFailure = error("nothing was probed")

            override fun failed(error: Exception): AttemptFailure = AttemptFailure.Failed(error.toUsenetError())
        }
        listOf(false to emptyList(), true to listOf("Pack:0" to MissingOn.ProvidersInUse)).forEach { (whole, remembered) ->
            val tried = FakeTried()
            val pipeline = PlayPipeline({ gapInPick(whole) }, probe, dispatcher = Dispatchers.Unconfined, tried = tried)
            val exhausted = assertIs<PipelineEvent.Exhausted>(pipeline.play(candidates(Release.Pack), episode(1, 3)).last(), "whole release: $whole")
            val failure = assertIs<AttemptFailure.MissingArticles>(exhausted.failedAttempts.single().second, "whole release: $whole")
            assertEquals(MissingOn.ProvidersInUse, failure.missingOn)
            assertEquals(remembered, tried.recorded, "whole release: $whole")
        }
    }

    /**
     * A target whose probe failed lets go of what the probe kept to read it ([PlaybackTarget.drop]), whether the probe
     * found nothing it can play or couldn't read the file: a play's failed attempts keep nothing. The target that plays
     * isn't let go of: whoever plays it does that.
     */
    @Test
    fun `a target whose probe failed is let go of, and the one that plays isn't`() = runTest {
        val probed = mutableListOf<FakeTarget>()
        val keeping = TrackProbe { target ->
            probed += target as FakeTarget
            target.keep()
            probe.probe(target)
        }
        val success = assertIs<PipelineEvent.Succeeded>(pipeline(probe = keeping).play(candidates(Release.Silent, Release.FlatDiscWithMkv, Release.Good)).last())
        assertEquals("Good:2", success.target.candidate.id)
        assertEquals(
            listOf("Silent:0" to "No audio this device can play (E-AC-3 (en))", "FlatDiscWithMkv:1" to "The player can't read this M2TS file"),
            success.failedAttempts.map { (candidate, failure) -> candidate.id to failure.text },
        )
        assertEquals(listOf("Silent:0" to 1, "FlatDiscWithMkv:1" to 1, "Good:2" to 0), probed.map { it.candidate.id to it.dropped })
    }

    /** The release that was playing as the player left it for its missing parts: the way back if nothing else plays. */
    private val wasPlaying = PlayCandidate("Missing:9", "Playing 9", wasPlaying = true)

    /**
     * The way back: tried after every other release has failed, from where it was, without the availability check,
     * which would find the parts the player was skipping; the check's step says so.
     */
    @Test
    fun `the release that was playing is tried last, without the availability check`() = runTest {
        val events = pipeline().play(candidates(Release.Silent, Release.Unfetchable), last = wasPlaying).toList()
        val success = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("Missing:9", success.target.candidate.id)
        assertEquals(listOf("Silent:0", "Unfetchable:1"), success.failedAttempts.map { it.first.id })
        assertEquals(listOf(Triple(1, 3, "Silent:0"), Triple(2, 3, "Unfetchable:1"), Triple(3, 3, "Missing:9")), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { Triple(it.attempt, it.of, it.candidate.id) })
        val check = events.filterIsInstance<PipelineEvent.Step>().single { it.candidate == wasPlaying && it.step == PipelineStep.CheckAvailability }
        assertEquals(StepStatus.Done(0, PlayPipeline.SKIPPED_PLAYING), check.status)
        assertTrue(checked.none { it.contains("Missing:9") }, "the way back was checked: $checked")
    }

    /** The others share the attempts but one: the way back always has its own. */
    @Test
    fun `the way back keeps its attempt when the others would use them all`() = runTest {
        val events = pipeline(maxAttempts = 3).play(candidates(Release.Silent, Release.Unfetchable, Release.Good), last = wasPlaying).toList()
        assertEquals("Missing:9", assertIs<PipelineEvent.Succeeded>(events.last()).target.candidate.id)
        assertEquals(listOf("Silent:0", "Unfetchable:1", "Missing:9"), fetched)
        // A release that plays before it leaves it untried.
        fetched.clear()
        val first = pipeline().play(candidates(Release.Good), last = wasPlaying).toList()
        assertEquals("Good:0", assertIs<PipelineEvent.Succeeded>(first.last()).target.candidate.id)
        assertEquals(listOf(1 to 2), first.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        assertEquals(listOf("Good:0"), fetched)
    }

    /**
     * Releases to try set to 1 still tries one other before the way back, which costs no grab: else trying another release,
     * or a smaller one, would go straight back to the release that was playing.
     */
    @Test
    fun `with one attempt to make one other release is tried before the way back`() = runTest {
        val events = pipeline(maxAttempts = 1).play(candidates(Release.Unfetchable, Release.Good), last = wasPlaying).toList()
        assertEquals(listOf("Unfetchable:0", "Missing:9"), fetched)
        assertEquals(listOf(1 to 2, 2 to 2), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        assertEquals("Missing:9", assertIs<PipelineEvent.Succeeded>(events.last()).target.candidate.id)
        // Without a way back, one attempt is one release, as before.
        fetched.clear()
        val alone = pipeline(maxAttempts = 1).play(candidates(Release.Unfetchable, Release.Good)).toList()
        assertEquals(listOf("Unfetchable:0"), fetched)
        assertIs<PipelineEvent.Exhausted>(alone.last())
    }

    /** It isn't skipped as known to be missing, nor for its indexer refusing grabs: it was just playing, from the NZB the app keeps. */
    @Test
    fun `the way back is tried whatever is remembered of it or its indexer`() = runTest {
        val tried = FakeTried(known = mapOf(wasPlaying.id to KNOWN))
        val events = pipeline(tried = tried).play(fromIndexers("A" to Release.GrabLimit, "A" to Release.Good), last = wasPlaying.copy(indexer = "A")).toList()
        assertEquals("Missing:9", assertIs<PipelineEvent.Succeeded>(events.last()).target.candidate.id)
    }

    /** A failure that would fail every release (the provider down) fails the way back too: it isn't tried. */
    @Test
    fun `the way back isn't tried after a failure that would fail it too`() = runTest {
        val events = pipeline().play(candidates(Release.ProviderDown), last = wasPlaying).toList()
        assertIs<PipelineEvent.Exhausted>(events.last())
        assertEquals(listOf("ProviderDown:0"), fetched)
    }

    /** What the pipeline logs while [block] runs, a line for each message. */
    private suspend fun <T> logOf(block: suspend () -> T): Pair<T, String> {
        val logger = LoggerFactory.getLogger(PlayPipeline::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        try {
            val result = block()
            // The appender appends while holding its own lock.
            val lines = synchronized(appender) { appender.list.toList() }.joinToString("\n") { it.formattedMessage }
            return result to lines
        } finally {
            logger.detachAppender(appender)
        }
    }

    private companion object {
        /** The source of a torrent's releases, which [Cached] plays. */
        const val TORRENT = "torrent"
        const val SEGMENT_SIZE = 1_000L
        const val AUTH_FAILED = "481 Authentication failed"
        const val TIMED_OUT = "Segment <seg> timed out after 5000ms on pool[0] (news.example.net:563)"
        const val NOT_IN_NZB = "Archive volume 'Movie.2020.part2.rar', which the PAR2 index names, isn't in the NZB"
        const val KNOWN = "Skipped: missing on your providers (checked 3 days ago)"
        const val DISC = "Harbor.Lights.2024.2160p.UHD.BluRay.HEVC-GRP"
        const val DISC_KNOWN = "Skipped: a disc copy (checked 3 days ago)"
        const val UNPLACED = "Archive volume 'x7f3k9q2.avi' was posted under a name that doesn't give its place in its archive, and no PAR2 index names it"
    }

    /** Another device's failed step is told in that device's own words: those words, and nothing stopped by them here. */
    @Test
    fun `a failure on another device is told in its words, and stops nothing`() {
        val failure = AttemptFailure.Elsewhere("Incomplete on the TV's providers · 41 of 256 checked articles missing")
        assertEquals("Incomplete on the TV's providers · 41 of 256 checked articles missing", failure.text)
        assertFalse(failure.route != null || failure.blocksIndexer)
    }
}
