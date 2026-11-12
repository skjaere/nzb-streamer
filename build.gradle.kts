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
    testImplementation(libs.logback.classic)
    testImplementation(libs.testcontainers)
    testImplementation("com.github.skjaere:mock-nntp-server:v0.2.0")
    // The mock's release brings Ktor 3.4.0's client and server: lifted to ktor-nntp-client's Ktor, so the tests don't
    // run them beside a newer ktor-io. Until the mock moves to that Ktor itself.
    testImplementation(platform(libs.ktor.bom))
}

tasks.test {
    useJUnitPlatform()
    // The yEnc decoder loads the librapidyenc.so its wrapper bundles. RAPIDYENC_LIB_PATH points the tests at a folder
    // holding another build of it.
    System.getenv("RAPIDYENC_LIB_PATH")?.let { systemProperty("jna.library.path", it) }
    // NestedArchiveTest's fixture folder, passed as a system property so that setting or changing the variable
    // reruns the tests (Gradle doesn't track a test task's environment).
    System.getenv("NZB_STREAMER_DOUBLE_ARCHIVE_DIR")?.takeIf { it.isNotBlank() }?.let {
        systemProperty("nzbstreamer.doubleArchiveDir", it)
    }
    systemProperty("kotlinx.coroutines.test.default.timeout", "PT5M")
    // SegmentCacheTest looks at the temporary direct buffers the JDK keeps per thread (sun.nio.ch.Util).
    jvmArgs("--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                licenses {
                    license {
                        name = "PolyForm Noncommercial License 1.0.0"
                        url = "https://polyformproject.org/licenses/noncommercial/1.0.0"
                    }
                }
            }
        }
    }
}

// The jars carry the licence and the third-party notices in a folder of their own under META-INF, so they
// don't clash with another library's in an app that packs several.
tasks.withType<Jar>().configureEach {
    into("META-INF/${project.name}") {
        from("LICENSE.md", "THIRD_PARTY_NOTICES.md")
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
