plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "nzb-streamer"

if (file("../ktor-nntp-client").exists()) {
    includeBuild("../ktor-nntp-client") {
        dependencySubstitution {
            substitute(module("com.github.skjaere:ktor-nntp-client")).using(project(":"))
        }
    }
}

