package io.skjaere.nzbstreamer.config

data class NntpConfig(
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val useTls: Boolean,
    val concurrency: Int,
    val readAheadSegments: Int = concurrency
)

data class SeekConfig(
    val forwardThresholdBytes: Long
)
