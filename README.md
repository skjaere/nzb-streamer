# nzb-streamer

[![CI](https://github.com/skjaere/nzb-streamer/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/skjaere/nzb-streamer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/skjaere/nzb-streamer/branch/main/graph/badge.svg)](https://codecov.io/gh/skjaere/nzb-streamer)
[![](https://jitpack.io/v/skjaere/nzb-streamer.svg)](https://jitpack.io/#skjaere/nzb-streamer)

A Kotlin library for streaming binary content from Usenet via NNTP, driven by NZB files. Parses NZB XML, enriches files with yEnc headers, streams decoded segments concurrently, and extracts archive metadata from RAR and 7-Zip volumes — all without downloading full files to disk.

## Features

- **NZB parsing** — XML to Kotlin data classes
- **Concurrent NNTP streaming** — configurable connection pool with ordered output
- **Transparent yEnc decoding** — via [ktor-nntp-client](https://github.com/skjaere/ktor-usenet-client)
- **Archive metadata extraction** — RAR4/5 and 7-Zip file listings via [kotlin-compression-utils](https://github.com/skjaere/kotlin-compression-utils)
- **Seekable streams over NNTP** — random-access interface for archive parsers with smart forward-seek optimization
- **PAR2 support** — volume ordering from recovery set metadata
- **Range requests** — stream partial byte ranges from individual files within archives

## Installation

**Gradle (JitPack):**

```kotlin
repositories {
    maven("https://jitpack.io")
}

dependencies {
    implementation("io.skjaere:nzb-streamer:<version>")
}
```

Requires **Java 25** and **Kotlin 2.3+**.

## Usage

### Create a client

```kotlin
val streamer = NzbStreamer {
    nntp {
        host = "nntp.example.com"
        port = 563
        username = "user"
        password = "pass"
        useTls = true
        concurrency = 4
    }
    seek {
        forwardThresholdBytes = 102_400L
    }
}
```

### Prepare an NZB

`prepare` parses the NZB, connects to NNTP to fetch yEnc headers and archive signatures, then extracts the full file listing:

```kotlin
val metadata = streamer.prepare(File("download.nzb").readBytes())

println("Volumes: ${metadata.response.volumes}")
println("Obfuscated: ${metadata.response.obfuscated}")
for (entry in metadata.response.entries) {
    println("  ${entry.path}  (${entry.size} bytes)")
}
```

### Stream a file from the archive

```kotlin
val result = streamer.resolveFile(metadata, "movie.mkv")
when (result) {
    is FileResolveResult.Ok -> {
        streamer.streamFile(metadata, result.splits) { channel ->
            // channel is a ByteReadChannel — write to disk, pipe to HTTP response, etc.
        }
    }
    is FileResolveResult.NotFound -> println("File not found")
    is FileResolveResult.IsDirectory -> println("Path is a directory")
    is FileResolveResult.Compressed -> println("Cannot stream: ${result.description}")
}
```

### Stream with a byte range

```kotlin
val range = 0L..1_048_575L  // first 1 MB
streamer.streamFile(metadata, splits, range) { channel ->
    // partial content
}
```

### Stream a raw archive volume

```kotlin
streamer.streamVolume(metadata, volumeIndex = 0) { channel ->
    // raw volume bytes
}
```

### Clean up

```kotlin
streamer.close()
```

## API overview

| Class | Description |
|---|---|
| `NzbStreamer` | Main entry point — prepare, resolve, stream |
| `NzbParser` | Parse NZB XML into `NzbDocument` |
| `NzbEnrichmentService` | Fetch yEnc headers and archive signatures from NNTP |
| `ArchiveMetadataService` | Extract file listings from RAR/7-Zip volumes |
| `ArchiveStreamingService` | Resolve files and stream decoded data |
| `NntpStreamingService` | Manage NNTP connection pool and segment downloads |
| `NntpSeekableInputStream` | Random-access `SeekableInputStream` over NNTP streams |
| `SegmentQueueService` | Build segment download queues with byte-range support |

### Key data types

| Type | Description |
|---|---|
| `NzbDocument` / `NzbFile` / `NzbSegment` | Parsed NZB structure |
| `ExtractedMetadata` | Enriched NZB + archive file entries + JSON-serializable response |
| `NzbMetadataResponse` | Serializable archive metadata (volumes, entries, obfuscation status) |
| `FileResolveResult` | Sealed type: `Ok`, `NotFound`, `IsDirectory`, `Compressed` |
| `NntpConfig` / `SeekConfig` | Configuration data classes |

## Dependencies

| Dependency | Role |
|---|---|
| [ktor-nntp-client](https://github.com/skjaere/ktor-usenet-client) | NNTP protocol client with yEnc decoding |
| [kotlin-compression-utils](https://github.com/skjaere/kotlin-compression-utils) | RAR/7-Zip archive parsing |
| kotlinx-coroutines | Concurrent streaming |
| kotlinx-serialization-json | JSON metadata responses |

## License

See [LICENSE](LICENSE).
