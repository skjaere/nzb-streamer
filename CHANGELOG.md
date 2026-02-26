# Changelog

## [0.5.0](https://github.com/skjaere/nzb-streamer/compare/v0.4.0...v0.5.0) (2026-02-26)


### Features

* feat: 


### Bug Fixes

* bumping ktor-nntp-client to 0.3.1

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
