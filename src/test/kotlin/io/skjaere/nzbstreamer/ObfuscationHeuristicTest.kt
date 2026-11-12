package io.skjaere.nzbstreamer

import io.skjaere.nzbstreamer.nzb.ObfuscationHeuristic
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ObfuscationHeuristicTest {

    // --- random names ------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
        strings = [
            "3f9c2a7e1b5d4c8f0a6e2d9b7c1f4a3e.mkv",
            "5c1d8e3a9f2b7046e1d3c5a8b9f0e2d4c6a7b183.rar",
            "7a2f9c4e1d6b3a8f5c0e2d7b9a4f1c6e3d8b5a2f7c9e4d1b6a3f8c5e0d2b7a94.mkv",
            "0f3a9c71",
            "4F1AD2B5.mkv",
            "abcdef0123456789.mkv",
        ]
    )
    fun `a hash is obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "5d2c7e4a-91b3-4f6e-a8d0-2c7b9e1f3a56.mkv",
            "{5d2c7e4a-91b3-4f6e-a8d0-2c7b9e1f3a56}.mkv",
            "dead0001-cafe-face-bead-beefcafe0001.mkv",
        ]
    )
    fun `a UUID is obfuscated, even when some of its groups are letters only`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["Tg4LmW9qZr2K.mkv", "Vb3NqK8tRx.mkv", "k3j9x2m7q4.avi", "a8f3c1.mkv", "Harbor2019Lights.mkv"])
    fun `letters and digits scattered through a token are obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["ZqXwPlMnKbVr.mkv", "zXcVbNmAsDfG.mkv", "ItIsAnOkName.mkv"])
    fun `case that changes nearly every letter is obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["20231105143022.mkv", "8271934.mkv"])
    fun `a number of seven digits or more is obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "3f9c2a7e1b5d4c8f.part01.rar",
            "3f9c2a7e1b5d4c8f.mkv.001",
            "m4p8t2w6.r01.bin",
            "Tg4LmW9qZr2K.vol03+04.par2",
            "Tg4LmW9qZr2K.1080p.mkv",
            "Tg4LmW9qZr2K.2019.mkv",
        ]
    )
    fun `a random name is judged without its volume and type suffixes`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Kx7Qm2Vb9Lp4.Rt8Wn3Zs",
            "Lw9a.Tz8W.Qp3x.mkv",
            "[0f3a9c71].mkv",
            "0f3a9c71-sample.mkv",
        ]
    )
    fun `a random token with nothing readable beside it is obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @Test
    fun `the file's own name is judged, not its folder`() {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated("Show.S01/0f3a9c71.mkv"))
    }

    @ParameterizedTest
    @ValueSource(strings = ["", "   ", ".mkv"])
    fun `a name with nothing in it is obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), "'$name'")
    }

    @Test
    fun `a name longer than 255 characters is obfuscated`() {
        val words = "Harbor.Lights.".repeat(20)
        val longest = words.take(251) + ".mkv"
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(longest), "255 characters")
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(words.take(252) + ".mkv"), "256 characters")
    }

    @ParameterizedTest
    @ValueSource(strings = ["\u0000", "\r\n.mkv", "\uD800.mkv", "\uDC00\uD800", "/", "\\"])
    fun `odd input is answered without throwing`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name))
    }

    // --- readable names ----------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Harbor.Lights.2019.1080p.BluRay.x264-GROUP.mkv",
            "Copper.Crown.S01E02.720p.WEB.mkv",
            "Copper Crown - S01E02 - Harbor.mkv",
            "harbor_lights_2019.mkv",
            "Movie.mkv",
            "Movie.2020",
            "VIDEO_TS.IFO",
            "VTS_01_1.VOB",
            "Ταινία.2019.mkv",
        ]
    )
    fun `a release-style name of words is readable`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["S01E01.mkv", "S01E01E02.mkv", "1x02.mkv"])
    fun `an episode code alone is readable`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["a.mkv", "x264.mkv", "01.flac", "00001.m2ts", "2019.mkv", "123456.mkv", "sample.mkv"])
    fun `a short name with nothing random in it is readable`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Harbor.Lights.2019.part01.rar",
            "Harbor.Lights.2019.r00",
            "Harbor.Lights.2019.7z.001",
            "Harbor.Lights.2019.mkv.002",
            "Harbor.Lights.2019.vol07+08.par2",
        ]
    )
    fun `volume and recovery suffixes leave a readable name readable`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Harbor.Lights.2019-sample.mkv",
            "Harbor.Lights.2019.Proof.jpg",
            "Copper.Crown.S01E01.[0F3A9C71].mkv",
        ]
    )
    fun `words keep a name readable beside a checksum or a sample marker`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "HarborLights.mkv",
            "HarborLights2019.mkv",
            "BluRayRemux.mkv",
            "IMG1234.jpg",
            "abc123.rar",
            "deadbeef.mkv",
        ]
    )
    fun `glued words and a word then a number are readable`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    // --- names the release tests rely on ------------------------------------------------

    @ParameterizedTest
    @ValueSource(
        strings = [
            "a8f7d6e5.rar", "b91c2d3e.7z", "c07e4f1a.mkv", "d2b5a9c4.iso",
            "e6f3c8b1", "f4a1d7e9.rar", "a3e9c2f7.mkv", "b8d4e1a6.7z",
            "b082fa0beaa644d3aa01045d5b8d0b36.r00", "b082fa0beaa644d3aa01045d5b8d0b36.mkv",
            "Lr2KpWu6Ns9c", "Hm6Rb1Xs9Vd.mkv",
        ]
    )
    fun `the release tests' random names are obfuscated`(name: String) {
        assertTrue(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Show.S01E01.mkv", "Show.S01E02.mkv", "Show.S01E03.mkv",
            "Release.Name.mkv", "Release.Name.r00",
            "Movie.2020.part4.bin", "Extras.part1.bin",
            "Show.S01E01.rar",
        ]
    )
    fun `the release tests' readable names are readable`(name: String) {
        assertFalse(ObfuscationHeuristic.isProbablyObfuscated(name), name)
    }
}
