# nzb-streamer

[![CI](https://github.com/skjaere/nzb-streamer/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/skjaere/nzb-streamer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/skjaere/nzb-streamer/branch/main/graph/badge.svg)](https://codecov.io/gh/skjaere/nzb-streamer)
[![](https://jitpack.io/v/skjaere/nzb-streamer.svg)](https://jitpack.io/#skjaere/nzb-streamer)

A Kotlin library for streaming binary content from Usenet via NNTP, driven by NZB files. Parses NZB XML, enriches files with yEnc headers, streams decoded segments concurrently, and extracts archive metadata from RAR and 7-Zip volumes — all without downloading full files to disk.

## Features

- **NZB parsing** — XML to Kotlin data classes
- **Concurrent NNTP streaming** — configurable connection pool with ordered output
- **Transparent yEnc decoding** — via [ktor-nntp-client](https://github.com/skjaere/ktor-nntp-client)
- **Archive metadata extraction** — RAR4/5 and 7-Zip file listings via [kotlin-compression-utils](https://github.com/skjaere/kotlin-compression-utils)
- **Seekable streams over NNTP** — random-access interface for archive parsers with smart forward-seek optimization
- **PAR2 support** — volume ordering from recovery set metadata
- **Range requests** — stream partial byte ranges from individual files within archives

## Installation

**Gradle (JitPack):**

```kotlin
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.skjaere:nzb-streamer:<version>")
}
```

Requires **Java 17+** and **Kotlin 2.3+**, from the first release made from the current code. Releases up to v0.10.0 need Java 25+.

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
        maxConnections = 8
    }
    concurrency = 4
}
```

Each `nntp {}` block adds a server: the first is the primary, the others fill in what it lacks. `NzbStreamer.fromConfig(...)` builds a client from `NntpConfig`, `StreamingConfig` and `PrepareConfig` instead.

### Prepare an NZB

`prepare` parses the NZB, connects to NNTP to fetch yEnc headers and archive signatures, then extracts the full file listing. It returns a `PrepareResult`:

```kotlin
val metadata = when (val result = streamer.prepare(File("download.nzb").readBytes())) {
    is PrepareResult.Success -> result.metadata
    is PrepareResult.MissingArticles -> error("Articles missing: ${result.message}")
    is PrepareResult.Encrypted -> error("A password is needed: ${result.message}")
    is PrepareResult.UnsupportedArchive -> error("Unsupported archive: ${result.message}")
    is PrepareResult.Failure -> error("Prepare failed: ${result.message}")
}

println("Volumes: ${metadata.response.volumes}")
println("Obfuscated: ${metadata.response.obfuscated}")
for (file in streamer.resolveStreamableFiles(metadata)) {
    println("  ${file.path}  (${file.totalSize} bytes)")
}
```

`prepare(bytes, passwordOverride = "...")` opens an encrypted release whose NZB doesn't hold its password.

### Stream a file from the archive

```kotlin
when (val result = streamer.resolveFile(metadata, "Movie.2020.mkv")) {
    is FileResolveResult.Streamable -> {
        streamer.streamFile(metadata, result.namedSplits) { channel ->
            // channel is a ByteReadChannel: write it to disk, pipe it to an HTTP response, etc.
        }
    }
    is FileResolveResult.NotFound -> println("File not found")
    is FileResolveResult.IsDirectory -> println("Path is a directory")
    is FileResolveResult.Compressed -> println("Cannot stream: ${result.description}")
}
```

### Stream with a byte range

```kotlin
val file = streamer.resolveStreamableFiles(metadata).first()
val range = 0L..1_048_575L  // the first MiB
streamer.streamFile(metadata, file, range) { channel ->
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
| `NzbStreamer` | Main entry point: prepare, resolve, stream |
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
| `PrepareResult` | Sealed type: `Success` (with the metadata), `MissingArticles`, `Encrypted`, `UnsupportedArchive`, `Failure` |
| `ExtractedMetadata` | Enriched NZB + archive file entries + JSON-serializable response |
| `NzbMetadataResponse` | Serializable archive metadata (volumes, entries, obfuscation status) |
| `StreamableFile` | A file the release lists, with its path and size, to stream |
| `FileResolveResult` | Sealed type: `Streamable` (with the file's `NamedSplits`), `NotFound`, `IsDirectory`, `Compressed` |
| `NntpConfig` / `StreamingConfig` / `PrepareConfig` | Configuration data classes |

## Dependencies

| Dependency | Role |
|---|---|
| [ktor-nntp-client](https://github.com/skjaere/ktor-nntp-client) | NNTP protocol client with yEnc decoding |
| [kotlin-compression-utils](https://github.com/skjaere/kotlin-compression-utils) | RAR/7-Zip archive parsing |
| kotlinx-coroutines | Concurrent streaming |
| kotlinx-serialization-json | JSON metadata responses |

## Building

```bash
./gradlew build                # Build and run the tests
./gradlew publishToMavenLocal  # Install to the local Maven repository
```

- The main code is compiled for Java 17 with a JDK 17 toolchain, which Gradle downloads when none is installed. The Gradle wrapper (Gradle 9.3.1) runs on JDK 17 or newer; CI runs it on JDK 25.
- The tests compile and run on a JDK 25 toolchain, as the `mock-nntp-server` test dependency is published for Java 25 only. `MultiPoolFallbackTest` starts the mock server's Docker image through Testcontainers, so it needs Docker; the other tests run their NNTP servers in process.
- yEnc decoding loads the `librapidyenc.so` bundled in [rapidyenc-kotlin-wrapper](https://github.com/skjaere/rapidyenc-kotlin-wrapper). To test with another build of rapidyenc, set `RAPIDYENC_LIB_PATH` to the folder that holds its `librapidyenc.so`.
- `NestedArchiveTest` reads a real double archive (split 7z volumes holding a RAR set) from the folder named by the environment variable `NZB_STREAMER_DOUBLE_ARCHIVE_DIR`, which the build passes to the tests as the system property `nzbstreamer.doubleArchiveDir` (an IDE's run configuration can set either). It is skipped when neither is set.
- `build.gradle.kts` pins [ktor-nntp-client](https://github.com/skjaere/ktor-nntp-client) and [kotlin-compression-utils](https://github.com/skjaere/kotlin-compression-utils) to their releases built for Java 17 (ktor-nntp-client v0.8.0 and kotlin-compression-utils v0.4.1 or later); their earlier releases are built for Java 25 and lack APIs this code uses. When checkouts of them sit beside this one, as `../ktor-nntp-client` and `../kotlin-compression-utils`, the build uses them in place of the pinned versions, and ktor-nntp-client's build takes `../rapidyenc-kotlin-wrapper` (or `../yenc_kotlin_wrapper`) the same way.

## License

This repository's own code is under the [PolyForm Noncommercial License 1.0.0](LICENSE.md) (SPDX `PolyForm-Noncommercial-1.0.0`): it is free for any non-commercial use, and commercial use needs the author's permission.

These files keep their own licence:

- The Gradle wrapper (`gradlew`, `gradlew.bat` and `gradle/wrapper/`): Apache License 2.0 (SPDX `Apache-2.0`).
- Parts of `src/main/kotlin/io/skjaere/nzbstreamer/nzb/FilenameResolver.kt`, its filename priority weights and its list of important file types, come from [nzbdav](https://github.com/nzbdav-dev/nzbdav) and keep its MIT licence (SPDX `MIT`).

Third-party notices are in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
