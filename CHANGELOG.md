# Changelog

## [0.11.0](https://github.com/skjaere/nzb-streamer/compare/v0.10.0...v0.11.0) (2026-11-12)


### Features

* **config:** verifyHostname setting for TLS providers, on by default ([094b6d1](https://github.com/skjaere/nzb-streamer/commit/094b6d11f0bf8cab4a19a453c0a090c469f7ed6d))
* **enrichment:** bound per-file fan-out + expose gate metrics ([ba64001](https://github.com/skjaere/nzb-streamer/commit/ba64001bc43a3ac7ddead41dd4700ac7c260cb29))
* **stream:** apply StreamingConfig's pool tuning to every pool ([692e04b](https://github.com/skjaere/nzb-streamer/commit/692e04b06601079f5673512b3f95ead299c078b6))
* **stream:** bound a stream's read-ahead in bytes ([9230736](https://github.com/skjaere/nzb-streamer/commit/9230736764642609d825ef941adc5f2d330a7103))
* **streaming:** stream zeros in place of an article no provider has, when asked ([6907c9b](https://github.com/skjaere/nzb-streamer/commit/6907c9bb32fb06ed6019529bef2883a6a0820077))
* **stream:** retry a timed-out segment once on another connection ([ebd8ad3](https://github.com/skjaere/nzb-streamer/commit/ebd8ad37c52e8eaff5100352372224b4b443c864))
* **stream:** use a damaged article copy when no provider has a good one ([f8d9e5f](https://github.com/skjaere/nzb-streamer/commit/f8d9e5f249fa90a0d8ddb7ee16c6f536ce0d8f87))
* **verification:** add verifySample for quick availability checks ([e7ad983](https://github.com/skjaere/nzb-streamer/commit/e7ad9835b1e2e89b892aabc057b07a813dc1a2ae))
* **verification:** pipelined, multi-pool article checks with progress ([901e2c2](https://github.com/skjaere/nzb-streamer/commit/901e2c227b7c4a8c5716fd64f58d89e15901766d))
* **verification:** priority and progress for parsed-NZB verifySegments ([e0736a0](https://github.com/skjaere/nzb-streamer/commit/e0736a0fb15a491a66cc8788971c3c4587fb51d8))
* **verification:** verifySegments overload for raw NZB bytes ([2d78931](https://github.com/skjaere/nzb-streamer/commit/2d789316fe5e162147cdbca26832e9237de1a862))


### Bug Fixes

* **build:** rerun NestedArchiveTest when its folder variable changes ([f0d2a34](https://github.com/skjaere/nzb-streamer/commit/f0d2a348b0d0a55d8d4d3f9f6e41615cad018e2d))
* **build:** target JVM 17 for Android, keep tests on JDK 25 ([8cded19](https://github.com/skjaere/nzb-streamer/commit/8cded197256b863aa355f687a346343eff3a51aa))
* **config:** leave the password out of NntpConfig.toString() ([9a02bdd](https://github.com/skjaere/nzb-streamer/commit/9a02bdd190a69877d09f10646c22e83913842681))
* **config:** make fromConfig apply every setting it is given ([dc01f48](https://github.com/skjaere/nzb-streamer/commit/dc01f484375a56714e66301a3aa68081e9b5d4db))
* **config:** refuse a read-ahead of no bytes in StreamingConfig ([24bd9b6](https://github.com/skjaere/nzb-streamer/commit/24bd9b6cf204f4e80863c9fdf1652ab57a058606))
* **enrichment:** check a set under another extension that the PAR2 data names by other files ([18e31a1](https://github.com/skjaere/nzb-streamer/commit/18e31a1cb594f1f862a01255f73791229b3a3192))
* **enrichment:** count a lacking last volume by its name (follow-up) ([bd8505d](https://github.com/skjaere/nzb-streamer/commit/bd8505da5d62dd516205b619ba0fca698260d8d5))
* **enrichment:** count a PAR2 index against a set under another extension only when it is the set's ([62a3fc5](https://github.com/skjaere/nzb-streamer/commit/62a3fc5b1253a3c3cacc5cfc7cffd8570e5e5789))
* **enrichment:** count PAR2-named volumes in the gap check (follow-up) ([706468c](https://github.com/skjaere/nzb-streamer/commit/706468ca17cd2f3bc4b96309d9d0c461b07b52a0))
* **enrichment:** don't fail a release over a lacking proof or subtitle ([17409b1](https://github.com/skjaere/nzb-streamer/commit/17409b1426fedfd229bae69a86b3d1de4535130b))
* **enrichment:** don't join an extra's archive to the release's volumes ([6008ced](https://github.com/skjaere/nzb-streamer/commit/6008ced6cccda4353c138475b9cc5eb8d4db2266))
* **enrichment:** don't let volumes that share a random name place themselves beside lone ones ([d8959aa](https://github.com/skjaere/nzb-streamer/commit/d8959aae8fabf3a51b64e070ee354eed914bb185))
* **enrichment:** fail a release whose volume only a random name places ([a074854](https://github.com/skjaere/nzb-streamer/commit/a0748545844c810e634b4dc35c63288601ef3164))
* **enrichment:** fail a set under another extension that lacks a last volume its PAR2 index names ([16ed267](https://github.com/skjaere/nzb-streamer/commit/16ed267c196cabf9f9b55a322a26dc8d9b291328))
* **enrichment:** fail a set under another extension whose names skip a volume ([af6fe1a](https://github.com/skjaere/nzb-streamer/commit/af6fe1a60d8b55ebca14b493c9dc7ae8abe7a7ab))
* **enrichment:** fail prepare when a stand-in can't be read (follow-up) ([83aaf11](https://github.com/skjaere/nzb-streamer/commit/83aaf11e004d8ff92519ae70683fc7224b616a97))
* **enrichment:** fetch a volume that a PAR2 index over the posted names describes ([62817c0](https://github.com/skjaere/nzb-streamer/commit/62817c018b2065c950e668ab5daa0b45ee7ef900))
* **enrichment:** fetch first articles through the segment cache ([5c7aadc](https://github.com/skjaere/nzb-streamer/commit/5c7aadc668786b762116c1fb403ad8ca1c00a6e4))
* **enrichment:** find archive volumes posted under another extension ([0f9af96](https://github.com/skjaere/nzb-streamer/commit/0f9af9636ce152b9e578c2373096dda0ba5d3935))
* **enrichment:** gate recovery volumes posted as &lt;hash&gt;.par2 (follow-up) ([1cc7d44](https://github.com/skjaere/nzb-streamer/commit/1cc7d44e768c422bf9f146d9f9563752507348e6))
* **enrichment:** keep PAR2 descriptions whatever fails past them ([f4ef904](https://github.com/skjaere/nzb-streamer/commit/f4ef9048b5d34b9bdd360120e752bfa2b389cc19))
* **enrichment:** match folder-prefixed PAR2 names by their file name ([7f5e41e](https://github.com/skjaere/nzb-streamer/commit/7f5e41e94223a12562549d685d80eb9b8eaa7125))
* **enrichment:** match hidden volumes by yEnc names too (follow-up) ([87b7369](https://github.com/skjaere/nzb-streamer/commit/87b73698e25bab53f12e6e8f3625b5ca58b1e1f2))
* **enrichment:** open a release without the files it doesn't need ([f65602a](https://github.com/skjaere/nzb-streamer/commit/f65602aa7650fea8bc8508be36522f7838add0e6))
* **enrichment:** open a set posted wholly under another extension by its names ([f0ba6b4](https://github.com/skjaere/nzb-streamer/commit/f0ba6b4c46893a4648d6973c798f48073c91e13b))
* **enrichment:** read a PAR2 index that names no file in its first 16 KB ([b80cea2](https://github.com/skjaere/nzb-streamer/commit/b80cea2e2f7f4c7b985b241355e40276c435926a))
* **enrichment:** read a recovery volume until it names every file ([fb07f29](https://github.com/skjaere/nzb-streamer/commit/fb07f295af7bb5fb66cfa6c9a8a707cdf1971cd5))
* **enrichment:** read a recovery volume's descriptions past its block ([15ead4e](https://github.com/skjaere/nzb-streamer/commit/15ead4e7c6ce96ea911f764cad292fc31846063e))
* **enrichment:** read a stand-in only when names are needed ([40c4057](https://github.com/skjaere/nzb-streamer/commit/40c405771360069d743c5ac464f5e36738d977c2))
* **enrichment:** read PAR2 descriptions as they stream, recovery volumes too ([fde74fb](https://github.com/skjaere/nzb-streamer/commit/fde74fbbec66f009eee6f6c1dee212af5c46b64b))
* **enrichment:** report a volume a set's names skip as missing ([99ed8fc](https://github.com/skjaere/nzb-streamer/commit/99ed8fce91f1d16c534d9cb50337d5cefa73a66b))
* **enrichment:** rethrow the caller's cancellation from enrich ([96fb2ca](https://github.com/skjaere/nzb-streamer/commit/96fb2ca1feb115a94b85a93b0ee7c2a3ad75333b))
* **enrichment:** stand a recovery volume in for its own set's index ([f736e1b](https://github.com/skjaere/nzb-streamer/commit/f736e1b5ec7babad94170456c88928cbf898de82))
* **enrichment:** treat obfuscated volume names as one archive again ([aae56f9](https://github.com/skjaere/nzb-streamer/commit/aae56f98d29950f271ae19cb9c9a7478ea9c151d))
* **enrichment:** type the causes of unplaced and absent archive volumes ([8931a44](https://github.com/skjaere/nzb-streamer/commit/8931a44ba99185b8b5c46caf1e4817b830ba05d1))
* **enrichment:** walk cause chain when classifying NNTP failures ([68c5326](https://github.com/skjaere/nzb-streamer/commit/68c5326b56aea9418fc3616a89dc49ad2af3318d))
* **fallback:** rank a provider's failure above another's bad copy, whatever the order ([cb61e85](https://github.com/skjaere/nzb-streamer/commit/cb61e854ca1c26735edd9217bf7e45bb1d3aa910))
* **log:** warn of no connection and of the last pool's failure ([f4d55c8](https://github.com/skjaere/nzb-streamer/commit/f4d55c8668071ff8d37515b53fb9363a9d0fcc73))
* **metadata:** order obfuscated .rar volumes by their PAR2 names ([404b69a](https://github.com/skjaere/nzb-streamer/commit/404b69a7918b2c5b3e0e3bfa40fd48a444130d23))
* **metadata:** prepare returns header-walk failures instead of throwing ([bd9b2c7](https://github.com/skjaere/nzb-streamer/commit/bd9b2c71e9ad58161ae1bf4a41ca951788a1002c))
* **metadata:** report a volume RAR5 headers show absent as missing ([7f4c9ce](https://github.com/skjaere/nzb-streamer/commit/7f4c9ce40e4ab8ca67c61cb1f30be1701f97f210))
* **nzb:** apply Xerces-only parser hardening best-effort for Android ([7a4a31e](https://github.com/skjaere/nzb-streamer/commit/7a4a31e740adade6230d6919d8906a045f1756c7))
* **nzb:** count .bin, .img and .cue as file types a release holds ([21297c9](https://github.com/skjaere/nzb-streamer/commit/21297c9cf3bb0af5c7bb037bd5375c7a7bf6a391))
* **nzb:** don't read a number as a file name in a subject ([58865cb](https://github.com/skjaere/nzb-streamer/commit/58865cb5a518fa5dc9cb1b8362b6fdf386bb834b))
* **nzb:** escape CR LF from NZB ids and names in messages and logs ([90f37b7](https://github.com/skjaere/nzb-streamer/commit/90f37b7bae15a8f849debb83d0452dab664c9bec))
* **nzb:** name nothing from an unquoted subject whose name has spaces ([80acad7](https://github.com/skjaere/nzb-streamer/commit/80acad7b336ad29acef8ea8daf079af58ee1d326))
* **nzb:** prefer known file types and skip codec tags in unquoted subjects ([36cff7a](https://github.com/skjaere/nzb-streamer/commit/36cff7ae13ff89f0494f9ff0f3c3d64c05ab13ed))
* **nzb:** read random mixed-case names as obfuscated ([7a4a69a](https://github.com/skjaere/nzb-streamer/commit/7a4a69a867d526c7729e891355f86380fbeb9402))
* **nzb:** rewrite the obfuscated-name check and the unquoted subject rule ([95ce15e](https://github.com/skjaere/nzb-streamer/commit/95ce15e56c23e0de16543c159717e164ca9eee5b))
* **prepare:** bound the ask of a pool passed over, and leave out an optional file it fails ([e58d549](https://github.com/skjaere/nzb-streamer/commit/e58d549c59984ed9ec30f0ee87c34243dcd3e4d3))
* **prepare:** order volumes by the volume names their PAR2 names give ([6dd8844](https://github.com/skjaere/nzb-streamer/commit/6dd8844c09c6680942b9cd33f64777e4aebdac0c))
* **queue:** fail at parts the NZB doesn't list, not end early ([df102e4](https://github.com/skjaere/nzb-streamer/commit/df102e40b92ac27859e658fd21627b3f9c3b5bf5))
* **seekable:** compare offsets as Long before narrowing to Int ([f786452](https://github.com/skjaere/nzb-streamer/commit/f7864524d7285ab4126fb752664daba01aee8b78))
* **seekable:** don't block the reader's thread in close() ([09e1c3e](https://github.com/skjaere/nzb-streamer/commit/09e1c3eb1ce58faea31b3483138df048c373c208))
* **seekable:** refuse a negative seek with an IOException ([52c5897](https://github.com/skjaere/nzb-streamer/commit/52c589745aba039a00dd3376f72e65578ccb5ffe))
* **seekable:** take the download's failure from the channel (follow-up) ([eeabba1](https://github.com/skjaere/nzb-streamer/commit/eeabba1d3faf2bcd434bc7805dc3f2d9f35c14da))
* **seekable:** throw a download's failure itself, not Ktor's wrapper ([ba6a163](https://github.com/skjaere/nzb-streamer/commit/ba6a163887127885ba9a49b7c058e58632ba2ccc))
* **segment-cache:** add reconcile() for files deleted from under it ([3db3d4b](https://github.com/skjaere/nzb-streamer/commit/3db3d4b199e19cb9e543ac1fc20fd412f83ee6bb))
* **segment-cache:** clear and resize a cache that is in use ([8ef3653](https://github.com/skjaere/nzb-streamer/commit/8ef36532f406e1f993ac77dd50a22ce7ca739ca2))
* **segment-cache:** count the bytes on disk after a load ([31ef187](https://github.com/skjaere/nzb-streamer/commit/31ef1875381c9de13790b4f1fc3009d97460bc48))
* **segment-cache:** delete the files a failed load can't count ([f4117bf](https://github.com/skjaere/nzb-streamer/commit/f4117bf96ed17256081a1060eb4afbf4063f2b7f))
* **segment-cache:** free followers when the fetch they wait on stops ([0c1fd48](https://github.com/skjaere/nzb-streamer/commit/0c1fd4883d1486be6683ecf3bdda23eb9a894179))
* **segment-cache:** keep LRU order across a reboot ([bed9ac3](https://github.com/skjaere/nzb-streamer/commit/bed9ac3c308c9ae8472d470b5a6fdb262ad2444d))
* **segment-cache:** make close() cheap and final, without runBlocking ([40085b4](https://github.com/skjaere/nzb-streamer/commit/40085b410cc427eef5afb3081681f3bab82130d7))
* **segment-cache:** reconcile with the disk on load; add save() ([c3942ae](https://github.com/skjaere/nzb-streamer/commit/c3942ae31cf361ab5ba50901682d4b8616d0f96b))
* **segment-cache:** skip a file deleted during the load's walk ([8b755c8](https://github.com/skjaere/nzb-streamer/commit/8b755c83298eacc45463a8375b2c10961ff7f7f5))
* **segment-cache:** survive a cancelled leader or store ([6a80aba](https://github.com/skjaere/nzb-streamer/commit/6a80abaf584f71d817334383946a4735cbdf6369))
* **stream:** a missing article ends the stream after the bytes in front of it, not the caller's job ([c0db3d9](https://github.com/skjaere/nzb-streamer/commit/c0db3d9b4360046870ed48eb322e4c4d124464d4))
* **stream:** clamp a segment's read range as Long before narrowing ([3b4ef70](https://github.com/skjaere/nzb-streamer/commit/3b4ef705cf52032322eecbe663edabb4427a844f))
* **stream:** connect and select on Dispatchers.IO, not Default ([cc04b3e](https://github.com/skjaere/nzb-streamer/commit/cc04b3ea6311cd61e30c5e2552a45989cb19b447))
* **stream:** count a stream's read-ahead in bytes, not in segments of the largest ([a772fe5](https://github.com/skjaere/nzb-streamer/commit/a772fe5fbf2f30521598d2219dfede182f523433))
* **stream:** don't throw a failure that comes after consume returns ([3d0095f](https://github.com/skjaere/nzb-streamer/commit/3d0095f498236b2b27df331d7d3672d6d4e79f9a))
* **stream:** don't throw a reader's early close as the stream's failure (follow-up) ([e67de5d](https://github.com/skjaere/nzb-streamer/commit/e67de5d36f47535a251a807ab3cee39eaa5905f5))
* **streamer:** keep raw files that start alike under their own names ([12c64b0](https://github.com/skjaere/nzb-streamer/commit/12c64b04572d0c587617a0da7cf500a08fe5a9a1))
* **streamer:** let a caller keep the segment cache it lends ([0fbe6fc](https://github.com/skjaere/nzb-streamer/commit/0fbe6fcd0c551f3b5bf12e9bebf1c7f47b4d9b12))
* **streamer:** name raw files by their PAR2 names again ([61ff995](https://github.com/skjaere/nzb-streamer/commit/61ff99571a7f926ecd87e9ba8026493b71ee530f))
* **stream:** fall back to the next pool when a pool rejects the login ([4e87485](https://github.com/skjaere/nzb-streamer/commit/4e874851dc499ee3baf9fe4ad959a4bb89d3f3e5))
* **stream:** fetch a segment again when its connection drops ([1769c56](https://github.com/skjaere/nzb-streamer/commit/1769c565aab15ba6abb431f93328a09318f7f8bf))
* **stream:** give a file's stream opened past its start the whole read-ahead ([273b202](https://github.com/skjaere/nzb-streamer/commit/273b20281b7354edff286f39437ac87217a0db90))
* **stream:** give each account on a server its own breaker and metrics ([98537bb](https://github.com/skjaere/nzb-streamer/commit/98537bbd2d007854c4a4eae8cd040b06f6e082a5))
* **stream:** hand each decrypted chunk to the reader at once ([87d6770](https://github.com/skjaere/nzb-streamer/commit/87d67708c574d56de954876917a74b2c32809eea))
* **streaming:** try the next pool on an unreadable copy, and ask a skipped pool before calling an article missing (K55, N93) ([b577751](https://github.com/skjaere/nzb-streamer/commit/b57775154374f160989ead3e893f7e8eb6f036ae))
* **stream:** log no breaker state change once the service is closed ([28798f2](https://github.com/skjaere/nzb-streamer/commit/28798f29f1f7adb2da0e9328287f43a358c4352e))
* **stream:** other pools keep serving while a removed pool closes ([a2c0226](https://github.com/skjaere/nzb-streamer/commit/a2c022631b7c1f4cd14ed957e721e0d1dc05987b))
* **stream:** report a pool's real failure instead of "not found on any pool" ([bc63b93](https://github.com/skjaere/nzb-streamer/commit/bc63b93973f6196bffb56d798691f98bc05f94bb))
* **stream:** return the half-open breaker permit when a fetch ends without an outcome ([9dcc24c](https://github.com/skjaere/nzb-streamer/commit/9dcc24c34b8f6132fb6313a4e2e96bce08750e28))
* **stream:** run the pools' connects and drains on the NNTP client's dispatcher ([e97597b](https://github.com/skjaere/nzb-streamer/commit/e97597b73dda9d33d561e67742b1b725b676a72d))
* **stream:** say when a fetch lost its connection and got no other in time ([6381536](https://github.com/skjaere/nzb-streamer/commit/63815362f03f275ad946c1732cae69e80fc9302c))
* **stream:** start a segment's timeout once it has a connection ([2f2456a](https://github.com/skjaere/nzb-streamer/commit/2f2456ab77bc4edf7777671e6bf6d6bc34b4576d))
* **stream:** stream 7z entries that start past the first volume ([4499f39](https://github.com/skjaere/nzb-streamer/commit/4499f39b8edc12b52c0f4ecde5106903845771d7))
* **stream:** throw SegmentTimeoutException instead of a cancellation ([7494ac6](https://github.com/skjaere/nzb-streamer/commit/7494ac643f7691e6f9f143bed6f3f3d8696bd91d))
* **verification:** a progress callback that throws fails the check ([f863c4e](https://github.com/skjaere/nzb-streamer/commit/f863c4ecb90a851bd2b0cd36df03d363c97141fd))
* **verification:** leave no article without an answer (follow-up) ([13c14f7](https://github.com/skjaere/nzb-streamer/commit/13c14f747567b37b2262ef2b18dd6fbb067e49b0))
* **verification:** pass a failing provider's articles to the next one ([38fc7aa](https://github.com/skjaere/nzb-streamer/commit/38fc7aac04d02e996b8e923081c03eb9305595a8))
* **verification:** report parts an NZB doesn't list as missing ([6cda9ee](https://github.com/skjaere/nzb-streamer/commit/6cda9ee4d14ef39b75e9a5a7479d9c7cd372e9c5))
* **verification:** send no more STATs after the 430 that ends a check ([04180bf](https://github.com/skjaere/nzb-streamer/commit/04180bfdc74867884969af4d97deb0727eeb2206))
* **verification:** stop the availability check at the first missing article ([1e89edf](https://github.com/skjaere/nzb-streamer/commit/1e89edfdee77a9a673b8504768feb014230d973b))


### Performance Improvements

* **enrichment:** fetch an obfuscated post's recovery volumes only when the PAR2 index can't name its files ([eb9bfff](https://github.com/skjaere/nzb-streamer/commit/eb9bffff43784623919fe8d6544225459a1dba91))
* **metadata:** open nested RAR releases without downloading them ([901e2c2](https://github.com/skjaere/nzb-streamer/commit/901e2c227b7c4a8c5716fd64f58d89e15901766d))
* **segment-cache:** read cached segments without the JDK's temporary direct buffer ([4c1af1d](https://github.com/skjaere/nzb-streamer/commit/4c1af1deaf37deaf6f90b1adb900dae0d05f2d0b))
* **stream:** fetch a segment whole with bodyYencBytes ([8cb3ea3](https://github.com/skjaere/nzb-streamer/commit/8cb3ea37b7d65d5e7ee84f93e4602d5e5cf85c34))
* **stream:** start a stream with two segments and widen once its reader reads on ([8926604](https://github.com/skjaere/nzb-streamer/commit/8926604f68da8685a7981b8e5753d5f633fc4258))

## [0.10.0](https://github.com/skjaere/nzb-streamer/compare/v0.9.1...v0.10.0) (2026-05-25)


### Features

* **metrics:** record body-fetch timeouts into nntp.body.duration
* **metrics:** record skipped bytes and time-to-first-article-resolved
* **nzb:** multi-source filename resolution + obfuscation heuristic
* **rar5:** support encrypted (HEAD_CRYPT) RAR5 archives
* **stat:** weight starting-pool pick by maxConnections
* **stream:** expose circuit breaker config as runtime setters
* **stream:** on-disk segment cache + bounded segment fetch timeout
* **stream:** optional streamName override for streamFile callers
* **stream:** per-pool resilience4j circuit breaker
* **stream:** per-startup phase trace + cache get/getorfetch timers
* **stream:** runtime-tunable segmentFetchTimeoutMs
* **verification:** log position+filename of first missing segment
* **verification:** make concurrency mutable for runtime override


### Bug Fixes

* **segment-cache:** lock-free singleflight via ConcurrentHashMap.putIfAbsent
* **stream:** don't deny every call when all pool breakers are OPEN
* **stream:** unstick HALF_OPEN circuit breakers in withFallback

## [0.9.1](https://github.com/skjaere/nzb-streamer/compare/v0.9.0...v0.9.1) (2026-04-25)


### Bug Fixes

* **deps:** bump ktor-nntp-client to v0.5.1

## [0.9.0](https://github.com/skjaere/nzb-streamer/compare/v0.8.0...v0.9.0) (2026-04-23)


### ⚠ BREAKING CHANGES

* SeekConfig and the seekConfig parameter on NzbStreamer.fromConfig are removed.

### Code Refactoring

* hard-code forward-threshold-bytes, drop SeekConfig

## [0.8.0](https://github.com/skjaere/nzb-streamer/compare/v0.7.4...v0.8.0) (2026-04-19)


### Features

* expose per-stream bitrate gauge


### Bug Fixes

* pass pre-computed splits through for multi-volume RARs

## [0.7.4](https://github.com/skjaere/nzb-streamer/compare/v0.7.3...v0.7.4) (2026-03-19)


### Bug Fixes

* short-circuit segment verification on first missing article

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
