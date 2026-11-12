package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.nzb.FilenameResolver
import io.skjaere.nzbstreamer.nzb.ObfuscationHeuristic
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * A name that is one random mixed-case token of letters reads as obfuscated: its case breaks the
 * shape of a word in several places. Camel-case words, and a word with an acronym or a short word
 * in it, still read as names.
 */
class RandomCaseNameTest {
    @ParameterizedTest
    @ValueSource(strings = ["KTpqWfbxRNcZ.mkv", "PLmwRtqHNbzA.mkv", "bnRTqwZkfMLe.mkv", "HwqZRtbnKLpcvXdm.mkv"])
    fun `a random mixed-case name reads as obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "HarborLights.mkv", "BluRayRemux.mkv", "iPhoneXsMax.mkv", "WEBRip.mkv", "CopperCrown.S01E02.mkv",
            "Harbor.Lights.KTpqWfbxRNcZ.mkv", "Harbor.Lights.2019.1080p.BluRay.x264-GROUP.mkv",
        ],
    )
    fun `a name of camel-case words still reads as one`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @Test
    fun `a readable yEnc name wins over a random mixed-case subject`() {
        val name = FilenameResolver.bestFilename("Harbor.Lights.2019.mkv", "\"KTpqWfbxRNcZ.mkv\" yEnc (1/50)", par2Filename = null)
        assertEquals("Harbor.Lights.2019.mkv", name)
    }
}
