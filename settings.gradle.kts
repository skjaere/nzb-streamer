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

if (file("../kotlin-compression-utils").exists()) {
    includeBuild("../kotlin-compression-utils") {
        dependencySubstitution {
            substitute(module("com.github.skjaere:kotlin-compression-utils")).using(project(":"))
        }
    }
}

if (file("../mock-nntp-server/mock-nntp-server").exists()) {
    includeBuild("../mock-nntp-server/mock-nntp-server") {
        dependencySubstitution {
            substitute(module("io.skjaere.mocknntp:mock-nntp-server")).using(project(":"))
        }
    }
}

