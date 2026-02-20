package io.skjaere.nzbstreamer.config

data class NntpConfig(
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val useTls: Boolean,
    val concurrency: Int,
    val maxConnections: Int = concurrency,
    val readAheadSegments: Int = concurrency * 3
)

data class SeekConfig(
    val forwardThresholdBytes: Long
)
