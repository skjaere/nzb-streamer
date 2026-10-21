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

kotlin {
    jvmToolchain(17)
}

java {
    withSourcesJar()
}

dependencies {
    api("com.github.skjaere:ktor-nntp-client:v0.7.0")
    api("com.github.skjaere:kotlin-compression-utils:v0.4.0")
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.slf4j.api)
    // Per-pool circuit breaker for the multi-upstream fallback path. Each pool gets
    // its own named CircuitBreaker in a shared registry, keyed by host:port.
    implementation(libs.resilience4j.circuitbreaker)
    implementation(libs.resilience4j.micrometer)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers)
    testImplementation("com.github.skjaere:mock-nntp-server:v0.2.0")
    // The mock's release brings Ktor 3.4.0's client and server: lifted to ktor-nntp-client's Ktor, so the tests don't
    // run them beside a newer ktor-io. Until the mock moves to that Ktor itself.
    testImplementation(platform(libs.ktor.bom))
}

tasks.test {
    useJUnitPlatform()
    val libPath = System.getenv("RAPIDYENC_LIB_PATH")
        ?: "${System.getProperty("user.home")}/rapidyenc/build"
    systemProperty("jna.library.path", libPath)
    systemProperty("kotlinx.coroutines.test.default.timeout", "PT5M")
    // SegmentCacheTest looks at the temporary direct buffers the JDK keeps per thread (sun.nio.ch.Util).
    jvmArgs("--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}

// Main targets JVM 17 so Android consumers can dex the classes. The mock-nntp-server
// test dependency is published for JVM 25 only, so test configurations resolve and
// run against a JDK 25 toolchain instead.
listOf("testCompileClasspath", "testRuntimeClasspath").forEach { name -> configurations.named(name) {
    attributes {
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 25)
    }
} }

tasks.withType<Test>().configureEach {
    javaLauncher = javaToolchains.launcherFor {
        languageVersion = JavaLanguageVersion.of(25)
    }
}
