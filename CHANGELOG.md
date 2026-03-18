# Changelog

## [0.7.3](https://github.com/skjaere/nzb-streamer/compare/v0.7.2...v0.7.3) (2026-03-18)


### Bug Fixes

* disable external DTD loading in NzbParser to prevent UnknownHostException

## [0.7.2](https://github.com/skjaere/nzb-streamer/compare/v0.7.1...v0.7.2) (2026-03-18)


### Bug Fixes

* allow NzbStreamer to be created with zero pools
* bump ktor-nntp-client to 0.4.2

## [0.7.1](https://github.com/skjaere/nzb-streamer/compare/v0.7.0...v0.7.1) (2026-03-13)


### Bug Fixes

* bump ktor-nntp-client to v0.4.1

## [0.7.0](https://github.com/skjaere/nzb-streamer/compare/v0.6.0...v0.7.0) (2026-03-04)


### Features

* change removePool signature to take NntpConfig and guard against duplicate host:port
* multi pool support
* runtime NNTP pool add/remove


### Bug Fixes

* bump kotlin-compression-utils to 0.3.2

## [0.6.0](https://github.com/skjaere/nzb-streamer/compare/v0.5.0...v0.6.0) (2026-02-27)


### Features

* feat: 
* adding a segment read ahead buffer
* adding support for nested archives
* initial commit
* streamlining streaming path
* supporting checking if all segments are present before import
* supporting pre-computed splits for nested archives
* supporting streaming of archive volumes


### Bug Fixes

* fix: 
* add missing VerificationResult and VerificationService source files
* adding metrics
* adding missing file
* adding separate configuration for segment verification concurrency
* adding streams metric
* bumping kotlin-compression-utils to 0.3.0
* bumping kotlin-compression-utils to 0.3.1
* bumping kotlin-compression-utils to v0.2.0 and adding release please config
* bumping ktor-nntp-client to 0.2.0
* bumping ktor-nntp-client to 0.3.1
* bumping ktor-nntp-client to 0.4.0
* fixing bug rar-in-rar streaming
* fixing package name of kotlin-compression-utils
* fixing package name of ktor-nntp-client
* handling unsupported archives
* refactoring
* refactoring ArchiveStreamingService.kt
* refactoring segment verification
* refactoring structured concurrency
* removing excessive logging
* returning proper response object from enrishment service
* separating concurrency and max connection settings
* separating read ahead and concurrency setting
* supporting nested archives within 7zip archives
* supporting non-archive nzbs
* using a data class ( NamedSplits ) rather than a list of SplitInfos

## [0.5.0](https://github.com/skjaere/nzb-streamer/compare/v0.4.0...v0.5.0) (2026-02-27)


### Features

* feat: 
* adding a segment read ahead buffer
* adding support for nested archives
* initial commit
* streamlining streaming path
* supporting checking if all segments are present before import
* supporting pre-computed splits for nested archives
* supporting streaming of archive volumes


### Bug Fixes

* fix: 
* add missing VerificationResult and VerificationService source files
* adding metrics
* adding missing file
* adding separate configuration for segment verification concurrency
* adding streams metric
* bumping kotlin-compression-utils to 0.3.0
* bumping kotlin-compression-utils to 0.3.1
* bumping kotlin-compression-utils to v0.2.0 and adding release please config
* bumping ktor-nntp-client to 0.2.0
* bumping ktor-nntp-client to 0.3.1
* bumping ktor-nntp-client to 0.4.0
* fixing bug rar-in-rar streaming
* fixing package name of kotlin-compression-utils
* fixing package name of ktor-nntp-client
* handling unsupported archives
* refactoring
* refactoring ArchiveStreamingService.kt
* refactoring segment verification
* refactoring structured concurrency
* removing excessive logging
* returning proper response object from enrishment service
* separating concurrency and max connection settings
* separating read ahead and concurrency setting
* supporting nested archives within 7zip archives
* supporting non-archive nzbs
* using a data class ( NamedSplits ) rather than a list of SplitInfos

## [0.4.0](https://github.com/skjaere/nzb-streamer/compare/v0.3.0...v0.4.0) (2026-02-25)


### Features

* adding support for nested archives
* supporting checking if all segments are present before import
* supporting pre-computed splits for nested archives


### Bug Fixes

* fix: 
* add missing VerificationResult and VerificationService source files
* bumping kotlin-compression-utils to 0.3.0
* bumping kotlin-compression-utils to 0.3.1
* bumping ktor-nntp-client to 0.2.0
* fixing bug rar-in-rar streaming
* handling unsupported archives
* refactoring segment verification
* returning proper response object from enrishment service
* separating concurrency and max connection settings
* supporting nested archives within 7zip archives
* supporting non-archive nzbs

## [0.3.0](https://github.com/skjaere/nzb-streamer/compare/v0.2.0...v0.3.0) (2026-02-18)


### Features

* adding a segment read ahead buffer
* initial commit
* streamlining streaming path
* supporting streaming of archive volumes


### Bug Fixes

* adding missing file
* bumping kotlin-compression-utils to v0.2.0 and adding release please config
* fixing package name of kotlin-compression-utils
* fixing package name of ktor-nntp-client
* refactoring
* refactoring ArchiveStreamingService.kt
* refactoring structured concurrency
* separating read ahead and concurrency setting

## [0.2.0](https://github.com/skjaere/nzb-streamer/compare/v0.1.0...v0.2.0) (2026-02-15)


### Features

* adding a segment read ahead buffer
* initial commit
* streamlining streaming path
* supporting streaming of archive volumes


### Bug Fixes

* adding missing file
* bumping kotlin-compression-utils to v0.2.0 and adding release please config
* fixing package name of kotlin-compression-utils
* fixing package name of ktor-nntp-client
* refactoring
* refactoring structured concurrency
