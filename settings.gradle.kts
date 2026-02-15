plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "nzb-streamer"

includeBuild("../kotlin-compression-utils") {
    dependencySubstitution {
        substitute(module("com.github.skjaere:kotlin-compression-utils")).using(project(":"))
    }
}
includeBuild("../ktor-usenet-client") {
    dependencySubstitution {
        substitute(module("com.github.skjaere:ktor-nntp-client")).using(project(":"))
    }
}
includeBuild("../mock-nntp-server/mock-nntp-server")
includeBuild("../nzb-streamer-utils")
