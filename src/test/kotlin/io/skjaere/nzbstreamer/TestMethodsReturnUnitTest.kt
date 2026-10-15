package io.skjaere.nzbstreamer

import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.invariantSeparatorsPathString
import kotlin.io.path.relativeTo
import kotlin.streams.asSequence
import kotlin.test.assertEquals

/**
 * JUnit 5 skips a test method that returns a value, without a word. In Kotlin that is
 * `fun x() = runBlocking { ...; assertIs<T>(...) }`, which returns the asserted value: give such
 * a test a block body or `runBlocking<Unit>`.
 */
class TestMethodsReturnUnitTest {

    private val testAnnotations = listOf(Test::class.java, ParameterizedTest::class.java, RepeatedTest::class.java)

    @Test
    fun `no test method returns a value`() {
        val root = Path.of(javaClass.protectionDomain.codeSource.location.toURI())
        val skipped = Files.walk(root).use { paths ->
            paths.asSequence()
                .filter { it.extension == "class" }
                .map { it.relativeTo(root).invariantSeparatorsPathString.removeSuffix(".class").replace('/', '.') }
                .map { Class.forName(it, false, javaClass.classLoader) }
                .flatMap { it.declaredMethods.asSequence() }
                .filter { method -> testAnnotations.any { method.isAnnotationPresent(it) } && method.returnType != Void.TYPE }
                .map { "${it.declaringClass.simpleName}.${it.name}" }
                .sorted()
                .toList()
        }
        assertEquals(emptyList(), skipped, "JUnit never runs these tests")
    }
}
