# Changelog

## [0.4.0](https://github.com/skjaere/nzb-streamer/compare/v0.3.0...v0.4.0) (2026-02-25)


### Features

* adding support for nested archives ([89b711a](https://github.com/skjaere/nzb-streamer/commit/89b711af6d1ff38d5a38b00834387eb68a611f49))
* supporting checking if all segments are present before import ([0778191](https://github.com/skjaere/nzb-streamer/commit/077819134e3cbb71bf3d73ace651b643555d536b))
* supporting pre-computed splits for nested archives ([98f76a4](https://github.com/skjaere/nzb-streamer/commit/98f76a405d736fe3c9715298a1d31bdc04eccb26))


### Bug Fixes

* fix:  ([436895e](https://github.com/skjaere/nzb-streamer/commit/436895e740441c7e272374e5e73d25552e73ec8d))
* add missing VerificationResult and VerificationService source files ([7bada9b](https://github.com/skjaere/nzb-streamer/commit/7bada9b08c21dbd6c0812f60ae4273a15c56e7c0))
* bumping kotlin-compression-utils to 0.3.0 ([1453d54](https://github.com/skjaere/nzb-streamer/commit/1453d54cbfeb1b5adebf75f5a43f227398740668))
* bumping kotlin-compression-utils to 0.3.1 ([25ec896](https://github.com/skjaere/nzb-streamer/commit/25ec896b8d58fda183281bce118dfb17f6512ac3))
* bumping ktor-nntp-client to 0.2.0 ([b7266b4](https://github.com/skjaere/nzb-streamer/commit/b7266b461126febd927f6e825e5e1afb0664590d))
* fixing bug rar-in-rar streaming ([c75825b](https://github.com/skjaere/nzb-streamer/commit/c75825bbfb8a1d8d2a6c0ffd3f68023bce00748d))
* handling unsupported archives ([8826801](https://github.com/skjaere/nzb-streamer/commit/8826801e7562d0a3f026996e3655339165665771))
* refactoring segment verification ([b87b90a](https://github.com/skjaere/nzb-streamer/commit/b87b90adc45601bf0f477a1a7ee83725fb65bd68))
* returning proper response object from enrishment service ([5615d47](https://github.com/skjaere/nzb-streamer/commit/5615d47df7ab620c9437365a3b0cf408a6701a27))
* separating concurrency and max connection settings ([140e98c](https://github.com/skjaere/nzb-streamer/commit/140e98cd2366d171d669e4c4623e00c7704b7cb5))
* supporting nested archives within 7zip archives ([8e79476](https://github.com/skjaere/nzb-streamer/commit/8e79476c9ba53445235ba9f347a70fa6364135f8))
* supporting non-archive nzbs ([24a265f](https://github.com/skjaere/nzb-streamer/commit/24a265fa0a7ef105795ce790ee34220130c780dd))

## [0.3.0](https://github.com/skjaere/nzb-streamer/compare/v0.2.0...v0.3.0) (2026-02-18)


### Features

* adding a segment read ahead buffer ([125003f](https://github.com/skjaere/nzb-streamer/commit/125003fbf21d43e23c79128d5dfbb0fe58b3e305))
* initial commit ([3f2ff35](https://github.com/skjaere/nzb-streamer/commit/3f2ff35a1843e2f1e88fa0ad9bbd5cfec73f3864))
* streamlining streaming path ([d711c5a](https://github.com/skjaere/nzb-streamer/commit/d711c5aed4e15e6d516fe01fda63412863e56de5))
* supporting streaming of archive volumes ([c29a618](https://github.com/skjaere/nzb-streamer/commit/c29a61830163d9157f960927fd990e102a08c576))


### Bug Fixes

* adding missing file ([2984bff](https://github.com/skjaere/nzb-streamer/commit/2984bff920feb6cfdae8b7d267ca74c6005484fa))
* bumping kotlin-compression-utils to v0.2.0 and adding release please config ([6cbaf5e](https://github.com/skjaere/nzb-streamer/commit/6cbaf5ec4802916c8fcb3dd2bb430da03acb62da))
* fixing package name of kotlin-compression-utils ([57a27f1](https://github.com/skjaere/nzb-streamer/commit/57a27f10ab0f4bf7765b192acc3ad5edbd84ee85))
* fixing package name of ktor-nntp-client ([b7bb572](https://github.com/skjaere/nzb-streamer/commit/b7bb5723ae31a9e94154957bad82c4f04e8f37cd))
* refactoring ([70c18cc](https://github.com/skjaere/nzb-streamer/commit/70c18cc389b0c969efe4d4ec855c288d2f2de857))
* refactoring ArchiveStreamingService.kt ([d904ae0](https://github.com/skjaere/nzb-streamer/commit/d904ae06945a7255ecb895c5ef8713d138bb092f))
* refactoring structured concurrency ([358a40c](https://github.com/skjaere/nzb-streamer/commit/358a40c401ec34d043b5cf55fc30c9d492f706f7))
* separating read ahead and concurrency setting ([a0b8a76](https://github.com/skjaere/nzb-streamer/commit/a0b8a762c39c5bf18e0458a3023ca8a16cdb065c))

## [0.2.0](https://github.com/skjaere/nzb-streamer/compare/v0.1.0...v0.2.0) (2026-02-15)


### Features

* adding a segment read ahead buffer ([494fde5](https://github.com/skjaere/nzb-streamer/commit/494fde506aa6dad6ced4a1848875299beed77d54))
* initial commit ([b6dd3ea](https://github.com/skjaere/nzb-streamer/commit/b6dd3ea6d63d4bfde3d9111e315fa0e5eab3e099))
* streamlining streaming path ([dcaa016](https://github.com/skjaere/nzb-streamer/commit/dcaa016b508055bc3cc0e6f4d789b8f1611191a4))
* supporting streaming of archive volumes ([0d89f2e](https://github.com/skjaere/nzb-streamer/commit/0d89f2e5313621c476f1ba19937755a0c9906524))


### Bug Fixes

* adding missing file ([8c06c8b](https://github.com/skjaere/nzb-streamer/commit/8c06c8b01115d7adcc000885c5521cb3d8045445))
* bumping kotlin-compression-utils to v0.2.0 and adding release please config ([c34ce25](https://github.com/skjaere/nzb-streamer/commit/c34ce25755394fb0346817d137727e18b912ce03))
* fixing package name of kotlin-compression-utils ([4eaf4f7](https://github.com/skjaere/nzb-streamer/commit/4eaf4f7b7de1793283e55dd871decb5afe9095d9))
* fixing package name of ktor-nntp-client ([9ace378](https://github.com/skjaere/nzb-streamer/commit/9ace378564beab01def896778dae78d0854b28e7))
* refactoring ([a8f8410](https://github.com/skjaere/nzb-streamer/commit/a8f84104f68e92acd7c4a0ffed935e45836dc3ad))
* refactoring structured concurrency ([c94445d](https://github.com/skjaere/nzb-streamer/commit/c94445d21ce3be5c6d2cf7ab677587d45c00ea93))
