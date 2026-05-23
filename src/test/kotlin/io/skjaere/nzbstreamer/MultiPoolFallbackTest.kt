package io.skjaere.nzbstreamer

import io.ktor.utils.io.toByteArray
import io.skjaere.mocknntp.testcontainer.MockNntpServerContainer
import io.skjaere.nntp.StatResult
import io.skjaere.nntp.YencHeaders
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.enrichment.VerificationResult
import io.skjaere.nzbstreamer.enrichment.VerificationService
import io.skjaere.nzbstreamer.nzb.NzbDocument
import io.skjaere.nzbstreamer.nzb.NzbFile
import io.skjaere.nzbstreamer.nzb.NzbSegment
import io.skjaere.nzbstreamer.queue.SegmentQueueService
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.assertEquals

class MultiPoolFallbackTest {

    companion object {
        private val primaryContainer = MockNntpServerContainer()
        private val fillContainer = MockNntpServerContainer()

        @BeforeAll
        @JvmStatic
        fun startContainers() {
            primaryContainer.start()
            fillContainer.start()
        }

        @AfterAll
        @JvmStatic
        fun stopContainers() {
            primaryContainer.stop()
            fillContainer.stop()
        }
    }

    @AfterEach
    fun tearDown() {
        runBlocking {
            primaryContainer.client.clearYencBodyExpectations()
            primaryContainer.client.clearStatExpectations()
            primaryContainer.client.clearStats()
            fillContainer.client.clearYencBodyExpectations()
            fillContainer.client.clearStatExpectations()
            fillContainer.client.clearStats()
        }
    }

    private fun createConfigs(): List<NntpConfig> = listOf(
        NntpConfig(
            host = primaryContainer.nntpHost,
            port = primaryContainer.nntpPort,
            username = "",
            password = "",
            useTls = false,
            maxConnections = 4
        ),
        NntpConfig(
            host = fillContainer.nntpHost,
            port = fillContainer.nntpPort,
            username = "",
            password = "",
            useTls = false,
            maxConnections = 4
        )
    )

    private fun createStreamingService(configs: List<NntpConfig>): NntpStreamingService {
        val service = NntpStreamingService(configs)
        runBlocking { service.connect() }
        return service
    }

    /**
     * Creates an enriched NzbFile with yencHeaders set, suitable for SegmentQueueService.
     */
    private fun enrichedNzbFile(
        segments: List<Pair<String, Int>>,
        filename: String = "data.bin"
    ): NzbFile {
        val totalSize = segments.sumOf { it.second }.toLong()
        val partSize = segments.first().second.toLong()
        return NzbFile(
            poster = "test",
            date = 0,
            subject = filename,
            groups = listOf("alt.binaries.test"),
            segments = segments.mapIndexed { index, (articleId, bytes) ->
                NzbSegment(bytes = bytes.toLong(), number = index + 1, articleId = articleId)
            },
            yencHeaders = YencHeaders(
                line = 128,
                size = totalSize,
                name = filename,
                partEnd = if (segments.size > 1) partSize else null
            )
        )
    }

    @Test
    fun `downloadSegment falls back to fill pool when primary returns 430`() = runBlocking {
        val seg1Data = ByteArray(16 * 1024) { (it % 256).toByte() }
        val seg2Data = ByteArray(16 * 1024) { ((it + 128) % 256).toByte() }
        val seg1Id = "seg1@primary"
        val seg2Id = "seg2@fill"

        // seg1 on primary, seg2 only on fill
        primaryContainer.client.addYencBodyExpectation("<$seg1Id>", seg1Data, "data.bin")
        fillContainer.client.addYencBodyExpectation("<$seg2Id>", seg2Data, "data.bin")

        primaryContainer.client.clearStats()
        fillContainer.client.clearStats()

        val file = enrichedNzbFile(
            listOf(seg1Id to seg1Data.size, seg2Id to seg2Data.size)
        )
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)

        val service = createStreamingService(createConfigs())
        service.use {
            service.streamSegments(queue, name = "test") { channel ->
                val result = channel.toByteArray()
                assertEquals(seg1Data.size + seg2Data.size, result.size)
            }
        }

        // Verify fill server handled at least one BODY request
        val fillStats = fillContainer.client.getStats()
        assertTrue(
            (fillStats["BODY"] ?: 0) >= 1,
            "Fill server should have received at least 1 BODY request, got: $fillStats"
        )
    }

    @Test
    fun `streamSegments succeeds when all segments on primary pool`() = runBlocking {
        val segData = ByteArray(32 * 1024) { (it % 256).toByte() }
        val segId = "seg-all-primary@test"

        primaryContainer.client.addYencBodyExpectation("<$segId>", segData, "data.bin")

        primaryContainer.client.clearStats()
        fillContainer.client.clearStats()

        val file = enrichedNzbFile(listOf(segId to segData.size))
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)

        val service = createStreamingService(createConfigs())
        service.use {
            service.streamSegments(queue, name = "test") { channel ->
                val result = channel.toByteArray()
                assertEquals(segData.size, result.size)
                assertTrue(result.contentEquals(segData))
            }
        }

        // Fill server should not have been called
        val fillStats = fillContainer.client.getStats()
        assertEquals(0, fillStats.getOrDefault("BODY", 0), "Fill server should not have received any BODY requests")
    }

    @Test
    fun `statAcrossPools finds article on fill when primary returns NotFound`(): Unit = runBlocking {
        val articleId = "<test-article@mock>"

        primaryContainer.client.addStatExpectation(articleId, false)
        fillContainer.client.addStatExpectation(articleId, true)

        val service = createStreamingService(createConfigs())
        service.use {
            val result = service.statAcrossPools(articleId)
            assertIs<StatResult.Found>(result)
        }
    }

    @Test
    fun `statAcrossPools returns NotFound when missing on all pools`(): Unit = runBlocking {
        val articleId = "<missing-everywhere@mock>"

        primaryContainer.client.addStatExpectation(articleId, false)
        fillContainer.client.addStatExpectation(articleId, false)

        val service = createStreamingService(createConfigs())
        service.use {
            val result = service.statAcrossPools(articleId)
            assertIs<StatResult.NotFound>(result)
        }
    }

    @Test
    fun `statAcrossPools early-exits on the first Found`() = runBlocking {
        val articleId = "<on-both@mock>"

        // Article exists on both pools. statAcrossPools picks its starting pool by
        // weighted random (proportional to maxConnections — used to spread STAT load),
        // so we can't assert which pool got the request — only that exactly one did.
        primaryContainer.client.addStatExpectation(articleId, true)
        fillContainer.client.addStatExpectation(articleId, true)

        primaryContainer.client.clearStats()
        fillContainer.client.clearStats()

        val service = createStreamingService(createConfigs())
        service.use {
            val result = service.statAcrossPools(articleId)
            assertIs<StatResult.Found>(result)
        }

        val totalStats = primaryContainer.client.getStats().getOrDefault("STAT", 0) +
            fillContainer.client.getStats().getOrDefault("STAT", 0)
        assertEquals(1, totalStats, "Expected exactly one pool to receive the STAT; got $totalStats")
    }

    @Test
    fun `verifySegments succeeds when article found on fill pool`(): Unit = runBlocking {
        val articleOnPrimary = "<seg1@primary>"
        val articleOnFill = "<seg2@fill>"

        // Set expectations on BOTH pools for BOTH articles — statAcrossPools picks its
        // starting pool by weighted random, so a missing expectation on whichever pool
        // happens to be tried first would throw and fail verification.
        primaryContainer.client.addStatExpectation(articleOnPrimary, true)
        primaryContainer.client.addStatExpectation(articleOnFill, false)
        fillContainer.client.addStatExpectation(articleOnPrimary, false)
        fillContainer.client.addStatExpectation(articleOnFill, true)

        val nzb = NzbDocument(
            listOf(
                NzbFile(
                    poster = "test",
                    date = 0,
                    subject = "test",
                    groups = listOf("alt.binaries.test"),
                    segments = listOf(
                        NzbSegment(bytes = 1000, number = 1, articleId = "seg1@primary"),
                        NzbSegment(bytes = 1000, number = 2, articleId = "seg2@fill")
                    )
                )
            )
        )

        val service = createStreamingService(createConfigs())
        service.use {
            val verificationService = VerificationService(service, concurrency = 1)
            val result = verificationService.verifySegments(nzb)
            assertIs<VerificationResult.Success>(result)
        }
    }

    /**
     * Reproduces the prod failure mode where every NZB import threw
     * `IllegalStateException: every pool refused the call but none threw …`.
     *
     * The circuit-breaker config uses `permittedNumberOfCallsInHalfOpenState(1)`
     * — only one probe call per breaker is allowed while HALF_OPEN. The preflight
     * check inside `withFallback` only looks for `state == OPEN`, so HALF_OPEN
     * breakers count as "active" and the per-pool `tryAcquirePermission` call is
     * required. With both pools HALF_OPEN and their single permits already taken,
     * every `tryAcquirePermission` returns false, the iteration produces no result,
     * and the code throws.
     *
     * The fix: when every pool refuses permission at iteration time, fall through
     * with breakers disabled (the same path already used when every pool is OPEN at
     * preflight). The call goes through against whatever's actually live.
     */
    @Test
    fun `withFallback falls through when every pool refuses HALF_OPEN permit`(): Unit = runBlocking {
        val configs = createConfigs()
        val service = createStreamingService(configs)

        // Drive both breakers into HALF_OPEN and consume their lone permits via
        // reflection so the test doesn't have to wait the 60s default cooldown
        // for an organic transition.
        val registryField = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        registryField.isAccessible = true
        val registry = registryField.get(service)
            as io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry

        val breakers = configs.map { registry.circuitBreaker("${it.host}:${it.port}") }
        breakers.forEach { breaker ->
            breaker.transitionToOpenState()
            breaker.transitionToHalfOpenState()
            check(breaker.tryAcquirePermission()) {
                "test setup: expected to acquire the single HALF_OPEN permit"
            }
            // Next tryAcquirePermission on this breaker now returns false — exactly
            // the prod state.
        }

        service.use {
            // Without the fix: throws IllegalStateException("every pool refused …").
            // With the fix: the block runs and returns its value.
            val result = service.withClient { _ -> "ok" }
            assertEquals("ok", result)
        }
    }

    /**
     * Reproduces the upstream cause of the HALF_OPEN-stuck breakers: when `block`
     * throws `CancellationException` (most commonly `TimeoutCancellationException`
     * from a segment-fetch timeout), none of `withFallback`'s typed catch blocks
     * fire, so the breaker permit is never released. Resilience4j tracks acquired
     * permits separately from observed results — an un-released HALF_OPEN permit
     * stays consumed even after the breaker auto-transitions back into HALF_OPEN
     * later, so every subsequent call refuses.
     *
     * The fix: catch CancellationException in `withFallback`, release the permit
     * (or record it as a failure for TimeoutCancellationException, which is in
     * `recordExceptions`), then rethrow so structured concurrency keeps working.
     */
    @Test
    fun `withFallback releases breaker permit when block throws TimeoutCancellationException`(): Unit = runBlocking {
        val configs = createConfigs()
        val service = createStreamingService(configs)

        val registryField = NntpStreamingService::class.java.getDeclaredField("circuitBreakerRegistry")
        registryField.isAccessible = true
        val registry = registryField.get(service)
            as io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry

        val breaker = registry.circuitBreaker("${configs[0].host}:${configs[0].port}")
        breaker.transitionToOpenState()
        breaker.transitionToHalfOpenState()

        // Run a withClient call that gets cancelled (timeout). Without the fix,
        // the breaker permit is leaked.
        val ex = runCatching {
            kotlinx.coroutines.withTimeout(50) {
                service.withClient { _ ->
                    kotlinx.coroutines.delay(10_000)
                    "should never reach"
                }
            }
        }.exceptionOrNull()
        assertIs<kotlinx.coroutines.TimeoutCancellationException>(ex)

        // Permit must have been released. The breaker should NOT be HALF_OPEN with
        // its single permit consumed — that's the stuck state we're trying to
        // avoid. Either it stayed HALF_OPEN with a permit available (release-without-
        // record path) or transitioned to OPEN (recorded as failure). Both are fine;
        // what we don't accept is `tryAcquirePermission()` returning false while
        // we're nominally HALF_OPEN.
        service.use {
            val permitAvailable = breaker.tryAcquirePermission()
                || breaker.state == io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN
                || breaker.state == io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED
            assertTrue(
                permitAvailable,
                "Breaker is stuck in ${breaker.state} with no permits available — " +
                    "leak detected from the CancellationException path."
            )
        }
    }

    @Test
    fun `single-config constructor backward compatibility`() = runBlocking {
        val segData = ByteArray(16 * 1024) { (it % 256).toByte() }
        val segId = "seg-single@test"

        primaryContainer.client.addYencBodyExpectation("<$segId>", segData, "data.bin")

        val singleConfig = NntpConfig(
            host = primaryContainer.nntpHost,
            port = primaryContainer.nntpPort,
            username = "",
            password = "",
            useTls = false,
            maxConnections = 4
        )
        val service = NntpStreamingService(singleConfig)
        runBlocking { service.connect() }

        val file = enrichedNzbFile(listOf(segId to segData.size))
        val queue = SegmentQueueService.createFileQueue(file, 0, 0L)

        service.use {
            service.streamSegments(queue, name = "test") { channel ->
                val result = channel.toByteArray()
                assertEquals(segData.size, result.size)
            }
        }
    }
}
