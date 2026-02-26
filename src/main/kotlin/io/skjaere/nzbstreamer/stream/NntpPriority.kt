package io.skjaere.nzbstreamer.stream

enum class NntpPriority(val value: Int) {
    HEALTH_CHECK(0),
    PREPARE(1),
    STREAMING(3)
}
