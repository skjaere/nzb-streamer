plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "nzb-streamer"

includeBuild("../kotlin-compression-utils")
includeBuild("../ktor-usenet-client")
includeBuild("../mock-nntp-server/mock-nntp-server")
includeBuild("../nzb-streamer-utils")
