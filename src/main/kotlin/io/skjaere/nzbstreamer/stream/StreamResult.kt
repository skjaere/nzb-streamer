package io.skjaere.nzbstreamer.stream

import io.ktor.utils.io.*
import io.skjaere.nntp.YencHeaders

data class StreamResult(
    val yencHeaders: YencHeaders,
    val data: ByteReadChannel
)
