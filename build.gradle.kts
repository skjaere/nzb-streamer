plugins {
    `java-library`
    `maven-publish`
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.kotlinx.kover)
}

group = "io.skjaere"

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
}

dependencies {
    api("io.skjaere:ktor-nntp-client:0.1.2")
    api("io.skjaere:kotlin-compression-utils:0.0.2")
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.slf4j.api)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
}

tasks.test {
    useJUnitPlatform()
    val libPath = System.getenv("RAPIDYENC_LIB_PATH")
        ?: "/home/william/IdeaProjects/rapidyenc/build"
    systemProperty("jna.library.path", libPath)
    systemProperty("kotlinx.coroutines.test.default.timeout", "PT5M")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
