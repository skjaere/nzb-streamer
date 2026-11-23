package io.skjaere.nzbstreamer.play

/**
 * The season and episode numbers a name states, as an app's release parser reads them for the episode picker
 * ([FilePicker.Episode]), in the forms these tests' names use: "S01E02", "S01E05E06", "S01E01-E02" or "S01E01-02", "1x02",
 * and a season alone, "S01". Each a token of its own: no letter or digit right before or after it. Names in no such form
 * ("Show.1_6.mp4", "a8f3c1.mkv") state none.
 */
val testEpisodeNames = EpisodeNames { name ->
    SEASON_EPISODE.find(name)?.let { m ->
        val first = m.groupValues[2].toInt()
        val more = Regex("\\d+").findAll(m.groupValues[3]).map { it.value.toInt() }.toList()
        val rangeEnd = m.groupValues[4].toIntOrNull()
        val episodes = when {
            rangeEnd != null && rangeEnd > first -> (first..rangeEnd).toList()
            more.size == 1 && '-' in m.groupValues[3] && more[0] > first -> (first..more[0]).toList()
            else -> listOf(first) + more
        }
        return@EpisodeNames EpisodeNumbers(listOf(m.groupValues[1].toInt()), episodes)
    }
    CROSS_EPISODE.find(name)?.let { m -> return@EpisodeNames EpisodeNumbers(listOf(m.groupValues[1].toInt()), listOf(m.groupValues[2].toInt())) }
    SEASON.find(name)?.let { m -> return@EpisodeNames EpisodeNumbers(listOf(m.groupValues[1].toInt()), emptyList()) }
    EpisodeNumbers(emptyList(), emptyList())
}

private fun token(pattern: String) = Regex("(?<![A-Za-z0-9])(?:$pattern)(?![A-Za-z0-9])", RegexOption.IGNORE_CASE)

private val SEASON_EPISODE = token("S(\\d{1,3})[ ._]?E(\\d{1,4})((?:[ ._-]*E\\d{1,4})*)(?:-(\\d{1,4}))?")
private val CROSS_EPISODE = token("(\\d{1,2})x(\\d{2,3})")
private val SEASON = token("S(\\d{1,2})")
