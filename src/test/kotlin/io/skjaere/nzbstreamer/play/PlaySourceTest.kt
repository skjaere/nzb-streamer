package io.skjaere.nzbstreamer.play

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The pipeline's loop runs any source's attempt ([PlaySource]), not Usenet's alone: a second source, with steps of its own
 * in its own order, plays through it beside Usenet's releases, its failures moving on to the next release as theirs do,
 * and what it opens is probed as any release's file is.
 */
class PlaySourceTest {

    private data class Listed(override val index: Int, override val path: String, override val size: Long) : ListedFile

    private class Listing(override val id: String, override val title: String, override val files: List<Listed>) : FileListing<Listed>

    /** A file the second source opened: kept by its own store, played from its own kind of address. */
    private class OtherTarget(override val candidate: PlayCandidate, override val listing: Listing, override val file: Listed) : PlaybackTarget {
        override val uri: String get() = "other://${listing.id}/file/${file.index}"
        var kept = 0

        override suspend fun keep() {
            kept++
        }
    }

    /**
     * Opens a release first and checks it after, unlike Usenet's: a release named "Broken …" fails its check, one named
     * "Thrown …" throws as it opens, and one named "Unreadable …" opens to a file the probe can't read.
     */
    private class OtherSource : PlaySource<OtherTarget> {
        override val steps = listOf(PipelineStep.Open, PipelineStep.CheckAvailability, PipelineStep.ProbeTracks)
        val probeFailures = mutableListOf<String>()

        override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): OtherTarget {
            val listing = Listing(
                candidate.id, candidate.title,
                listOf(Listed(0, "Harbor.Lights.S01E02/sample.mkv", 10), Listed(1, "Harbor.Lights.S01E02/Harbor.Lights.S01E02.mkv", 900)),
            )
            val file = step(candidate, PipelineStep.Open) {
                if (candidate.title.startsWith("Thrown")) throw IllegalStateException("listing lost")
                pickFile.pick(listing)?.let { StepResult.Ok(it, it.name) } ?: StepResult.Fail(pickFile.noMatchIn(listing))
            }
            step(candidate, PipelineStep.CheckAvailability) {
                if (candidate.title.startsWith("Broken")) StepResult.Fail(AttemptFailure.Unsupported("not available here")) else StepResult.Ok(Unit)
            }
            return OtherTarget(candidate, listing, file)
        }

        override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: OtherTarget): AttemptFailure {
            probeFailures += target.candidate.id
            return AttemptFailure.Unsupported("unreadable from here")
        }

        override fun failed(error: Exception): AttemptFailure = AttemptFailure.Unsupported("failed here: ${error.message}")
    }

    private val other = OtherSource()

    /**
     * Usenet's releases here can't be fetched: each fails its first step, as Usenet's source fails a release whose indexer
     * answers its grab with an HTTP 404, and the play moves on. Its steps are Usenet's four, in their order.
     */
    private val usenet = object : PlaySource<PlaybackTarget> {
        override val steps = listOf(PipelineStep.FetchNzb, PipelineStep.CheckAvailability, PipelineStep.Open, PipelineStep.ProbeTracks)

        override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): PlaybackTarget =
            step<Nothing>(candidate, PipelineStep.FetchNzb) { StepResult.Fail(AttemptFailure.FetchFailed("HTTP 404")) }

        override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: PlaybackTarget): AttemptFailure = error("nothing was opened")

        override fun failed(error: Exception): AttemptFailure = AttemptFailure.Failed(error.toUsenetError())
    }

    private val probed = mutableListOf<PlaybackTarget>()

    private val probe = TrackProbe { target ->
        probed += target
        if (target.candidate.title.startsWith("Unreadable")) ProbeResult.Failed(UsenetError.Network("unrecognised"), unreadable = UnreadableFile.UnknownFormat) else ProbeResult.Playable()
    }

    private fun pipeline(maxAttempts: Int = 3) =
        PlayPipeline({ candidate -> if (candidate.source == OTHER) other else usenet }, probe, maxAttempts, dispatcher = Dispatchers.Unconfined)

    private fun otherRelease(title: String) = PlayCandidate("other/$title", title, "Home", source = OTHER)

    @Test
    fun `a second source's steps reach the events in its own order and what it opens is probed`() = runTest {
        val events = pipeline().play(listOf(otherRelease("Harbor.Lights.S01E02.1080p.WEB-GRP"))).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        val target = assertIs<OtherTarget>(played.target)
        assertEquals("Harbor.Lights.S01E02.mkv", target.file.name)
        assertEquals(listOf<PlaybackTarget>(target), probed)
        val done = events.filterIsInstance<PipelineEvent.Step>().filter { it.status is StepStatus.Done }
        assertEquals(other.steps, done.map { it.step })
        assertEquals("Harbor.Lights.S01E02.mkv", (done.first().status as StepStatus.Done).detail)
        // The pipeline doesn't keep what plays: whoever plays it does, once it has played.
        assertEquals(0, target.kept)
    }

    @Test
    fun `a second source's failures move on to the next release, as Usenet's do`() = runTest {
        val releases = listOf(
            PlayCandidate("usenet/1", "Harbor.Lights.S01E02.720p.WEB-OTH", "Indexer A"),
            otherRelease("Broken.Harbor.Lights.S01E02"),
            otherRelease("Thrown.Harbor.Lights.S01E02"),
            otherRelease("Unreadable.Harbor.Lights.S01E02"),
            otherRelease("Harbor.Lights.S01E02.1080p.WEB-GRP"),
        )
        val events = pipeline(maxAttempts = 5).play(releases).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("other/Harbor.Lights.S01E02.1080p.WEB-GRP", played.target.candidate.id)
        assertEquals(
            listOf(
                "usenet/1" to "Couldn't get the NZB: HTTP 404",
                "other/Broken.Harbor.Lights.S01E02" to "Can't be streamed: not available here",
                "other/Thrown.Harbor.Lights.S01E02" to "Can't be streamed: failed here: listing lost",
                "other/Unreadable.Harbor.Lights.S01E02" to "Can't be streamed: unreadable from here",
            ),
            played.failedAttempts.map { (candidate, failure) -> candidate.id to failure.text },
        )
        // Each failed in its own source's step: Usenet's at its fetch, the others' where they failed.
        val failedSteps = events.filterIsInstance<PipelineEvent.Step>().filter { it.status is StepStatus.Failed }.map { it.candidate.id to it.step }
        assertEquals(
            listOf(
                "usenet/1" to PipelineStep.FetchNzb,
                "other/Broken.Harbor.Lights.S01E02" to PipelineStep.CheckAvailability,
                "other/Thrown.Harbor.Lights.S01E02" to PipelineStep.Open,
                "other/Unreadable.Harbor.Lights.S01E02" to PipelineStep.ProbeTracks,
            ),
            failedSteps,
        )
        assertEquals(listOf("other/Unreadable.Harbor.Lights.S01E02"), other.probeFailures)
        // "Release n of m" counts across both sources.
        assertEquals(listOf(1 to 5, 2 to 5, 3 to 5, 4 to 5, 5 to 5), events.filterIsInstance<PipelineEvent.AttemptStarted>().map { it.attempt to it.of })
        assertTrue(probed.none { it.candidate.id == "usenet/1" })
    }

    /** A file the routes' source opened, from [route]. */
    private class RouteTarget(override val candidate: PlayCandidate, override val listing: Listing, override val file: Listed, val route: String) : PlaybackTarget {
        override val uri: String get() = "routes://${listing.id}/$route/file/${file.index}"

        override suspend fun keep() = Unit
    }

    /**
     * Plays a release from the first of its routes left, as a debrid service's accounts do: one in [openFails] fails
     * Opening as its service stopping, one in [unreadable] opens to a file the probe can't read, and one in [noVideo] to a
     * file with no video this device plays, the release's own failure. After the service's failures, [retarget] tries the
     * release on the next route left, saying so first; after the release's own, or with no route left, none.
     */
    private class RoutesSource(
        private val openFails: Set<String> = emptySet(),
        val unreadable: Set<String> = emptySet(),
        val noVideo: Set<String> = emptySet(),
    ) : PlaySource<RouteTarget> {
        override val steps = listOf(PipelineStep.Prepare, PipelineStep.Open, PipelineStep.ProbeTracks)

        /** The routes each release was prepared on, by its id. */
        val tried = mutableMapOf<String, MutableList<String>>()

        override suspend fun AttemptSteps.attempt(candidate: PlayCandidate, pickFile: FilePicker): RouteTarget {
            tried[candidate.id] = mutableListOf()
            return open(candidate, pickFile)
        }

        override suspend fun AttemptSteps.retarget(candidate: PlayCandidate, failure: AttemptFailure, pickFile: FilePicker): RouteTarget? {
            if (failure !is AttemptFailure.ServiceDown && failure != AttemptFailure.NoWorkingLink) return null
            if (left(candidate) == null) return null
            retrying(failure)
            return open(candidate, pickFile)
        }

        private fun left(candidate: PlayCandidate) = candidate.routes.firstOrNull { it !in tried.getValue(candidate.id) }

        private suspend fun AttemptSteps.open(candidate: PlayCandidate, pickFile: FilePicker): RouteTarget {
            val listing = Listing(candidate.id, candidate.title, listOf(Listed(0, "Copper.Crown.2019/sample.mkv", 10), Listed(1, "Copper.Crown.2019/Copper.Crown.2019.mkv", 900)))
            val route = step(candidate, PipelineStep.Prepare) {
                left(candidate)?.let { route -> StepResult.Ok(route.also { tried.getValue(candidate.id) += it }) } ?: StepResult.Fail(AttemptFailure.NoLongerCached)
            }
            val file = step(candidate, PipelineStep.Open) {
                if (route in openFails) StepResult.Fail(AttemptFailure.ServiceDown(route)) else pickFile.pick(listing)?.let { StepResult.Ok(it, it.name) } ?: StepResult.Fail(pickFile.noMatchIn(listing))
            }
            return RouteTarget(candidate, listing, file, route)
        }

        override suspend fun AttemptSteps.probeFailed(failed: ProbeResult.Failed, target: RouteTarget): AttemptFailure = AttemptFailure.NoWorkingLink

        override fun failed(error: Exception): AttemptFailure = AttemptFailure.Unsupported("failed here: ${error.message}")
    }

    /** The routes' releases through the pipeline, each probed as the source says its route reads ([probedRoutes] in order). */
    private val probedRoutes = mutableListOf<String>()

    private fun routesPipeline(source: RoutesSource) = PlayPipeline(
        { source },
        TrackProbe { target ->
            val route = (target as RouteTarget).route
            probedRoutes += route
            when (route) {
                in source.unreadable -> ProbeResult.Failed(UsenetError.Network("unexpected end of stream"))
                in source.noVideo -> ProbeResult.NoPlayableVideo(listOf("2160p Dolby Vision profile 7"))
                else -> ProbeResult.Playable()
            }
        },
        maxAttempts = 3, dispatcher = Dispatchers.Unconfined,
    )

    private fun routesRelease(title: String, vararg routes: String) = PlayCandidate("routes/$title", title, "Home", source = ROUTES, routes = routes.toList())

    /** [events] of one attempt, each step as "Open Failed", and a retry as "Retrying: <its failure's words>". */
    private fun said(events: List<PipelineEvent>): List<String> = events.mapNotNull { event ->
        when (event) {
            is PipelineEvent.Step -> "${event.step} ${event.status::class.simpleName}"
            is PipelineEvent.Retrying -> "Retrying: ${event.failure.text}"
            else -> null
        }
    }

    /**
     * A step of the source's failing, Opening here, asks the source for another target of the same release before the
     * play moves on; the one it gives is probed, and plays. The events say the step failed, the retry, and the source's
     * steps again, in that order, all in the one attempt.
     */
    @Test
    fun `a source's retarget after its Opening failed gives a target that is probed, in the same attempt`() = runTest {
        val source = RoutesSource(openFails = setOf("a"))
        val events = routesPipeline(source).play(listOf(routesRelease("Copper.Crown.2019.1080p.WEB-DL", "a", "b"))).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("b", assertIs<RouteTarget>(played.target).route)
        assertEquals(emptyList(), played.failedAttempts)
        assertEquals(listOf("b"), probedRoutes)
        assertEquals(1, events.filterIsInstance<PipelineEvent.AttemptStarted>().size)
        assertEquals(
            listOf(
                "Prepare Running", "Prepare Done", "Open Running", "Open Failed", "Retrying: The debrid service stopped answering",
                "Prepare Running", "Prepare Done", "Open Running", "Open Done", "ProbeTracks Running", "ProbeTracks Done",
            ),
            said(events),
        )
        val retrying = events.filterIsInstance<PipelineEvent.Retrying>().single()
        assertEquals("routes/Copper.Crown.2019.1080p.WEB-DL", retrying.candidate.id)
        assertEquals("a", retrying.failure.route)
    }

    /** The probe failing to read the file asks for another target too, and that one is probed in its turn. */
    @Test
    fun `a source's retarget after the probe failed probes its new target`() = runTest {
        val source = RoutesSource(unreadable = setOf("a"))
        val events = routesPipeline(source).play(listOf(routesRelease("Copper.Crown.2019.1080p.WEB-DL", "a", "b"))).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("b", assertIs<RouteTarget>(played.target).route)
        assertEquals(listOf("a", "b"), probedRoutes)
        assertEquals(
            listOf(
                "Prepare Running", "Prepare Done", "Open Running", "Open Done", "ProbeTracks Running", "ProbeTracks Failed",
                "Retrying: No longer available from the service", "Prepare Running", "Prepare Done", "Open Running", "Open Done",
                "ProbeTracks Running", "ProbeTracks Done",
            ),
            said(events),
        )
    }

    /**
     * No other target (none left, or a failure that is the release's own) fails the attempt with the failure it had, as
     * before the hook: the play moves on to the next release, with no retry said.
     */
    @Test
    fun `a retarget giving none fails the attempt with its failure, and the play moves on`() = runTest {
        val source = RoutesSource(openFails = setOf("a"), noVideo = setOf("c"))
        val releases = listOf(
            routesRelease("Copper.Crown.2019.2160p.WEB-DL", "a"),
            routesRelease("Copper.Crown.2019.1080p.BluRay", "c", "b"),
            routesRelease("Copper.Crown.2019.1080p.WEB-DL", "b"),
        )
        val events = routesPipeline(source).play(releases).toList()
        val played = assertIs<PipelineEvent.Succeeded>(events.last())
        assertEquals("routes/Copper.Crown.2019.1080p.WEB-DL", played.target.candidate.id)
        assertEquals(
            listOf("routes/Copper.Crown.2019.2160p.WEB-DL" to "The debrid service stopped answering", "routes/Copper.Crown.2019.1080p.BluRay" to "No video this device can play: 2160p Dolby Vision profile 7"),
            played.failedAttempts.map { (candidate, failure) -> candidate.id to failure.text },
        )
        assertEquals(emptyList(), events.filterIsInstance<PipelineEvent.Retrying>())
        // The release's own failure isn't tried on its next route: c alone was probed of it.
        assertEquals(listOf("c", "b"), probedRoutes)
        assertEquals(listOf("a"), source.tried.getValue("routes/Copper.Crown.2019.2160p.WEB-DL"))
        assertEquals(listOf("c"), source.tried.getValue("routes/Copper.Crown.2019.1080p.BluRay"))
    }

    private companion object {
        const val OTHER = "other"
        const val ROUTES = "routes"
    }
}
