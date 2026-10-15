package io.skjaere.nzbstreamer

import io.skjaere.nntp.NntpConnectionException
import io.skjaere.nntp.StatResult
import io.skjaere.nzbstreamer.config.NntpConfig
import io.skjaere.nzbstreamer.stream.NntpStreamingService
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.CertificateException
import java.util.concurrent.TimeUnit
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLServerSocket
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * The pools check that a TLS server's certificate names the host connected to, unless the
 * pool's config turns hostname verification off (for a server reached by an address its
 * certificate doesn't list). The server's certificate is self-signed for another host and
 * trusted through the JVM's default trust store, the one ktor-nntp-client checks against.
 */
class TlsHostnameVerificationTest {
    private lateinit var dir: Path
    private lateinit var server: SSLServerSocket
    private val savedProperties = mutableMapOf<String, String?>()

    @BeforeEach
    fun setUp() {
        dir = Files.createTempDirectory("nzb-streamer-tls-test")
        val keyStore = dir.resolve("server.p12")
        val trustStore = dir.resolve("trust.p12")
        val certificate = dir.resolve("server.cer")
        keytool(
            "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-validity", "3650",
            "-dname", "CN=other.example", "-ext", "SAN=dns:other.example",
            "-keystore", keyStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD, "-keypass", PASSWORD
        )
        keytool("-exportcert", "-alias", "server", "-keystore", keyStore.toString(), "-storepass", PASSWORD, "-file", certificate.toString())
        keytool(
            "-importcert", "-noprompt", "-alias", "server", "-file", certificate.toString(),
            "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD
        )
        mapOf(
            "javax.net.ssl.trustStore" to trustStore.toString(),
            "javax.net.ssl.trustStoreType" to "PKCS12",
            "javax.net.ssl.trustStorePassword" to PASSWORD
        ).forEach { (key, value) ->
            savedProperties[key] = System.getProperty(key)
            System.setProperty(key, value)
        }
        server = startServer(keyStore)
    }

    @AfterEach
    fun tearDown() {
        server.close()
        savedProperties.forEach { (key, value) -> if (value == null) System.clearProperty(key) else System.setProperty(key, value) }
        dir.toFile().deleteRecursively()
    }

    private fun keytool(vararg args: String) {
        val keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString()
        val process = ProcessBuilder(listOf(keytool) + args).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) fail("keytool ${args.first()} failed: $output")
    }

    /** A TLS 1.2 NNTP server (Ktor's TLS client speaks 1.2 only) that has every article. */
    private fun startServer(keyStore: Path): SSLServerSocket {
        val keys = KeyStore.getInstance("PKCS12").apply { Files.newInputStream(keyStore).use { load(it, PASSWORD.toCharArray()) } }
        val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
            .apply { init(keys, PASSWORD.toCharArray()) }.keyManagers
        val context = SSLContext.getInstance("TLS").apply { init(keyManagers, null, null) }
        val serverSocket = (context.serverSocketFactory.createServerSocket(0) as SSLServerSocket)
            .apply { enabledProtocols = arrayOf("TLSv1.2") }
        Thread {
            while (true) {
                val socket = try {
                    serverSocket.accept()
                } catch (_: IOException) {
                    break
                }
                Thread {
                    socket.use {
                        try {
                            val out = it.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            val reader = it.getInputStream().bufferedReader()
                            while (true) {
                                val cmd = reader.readLine() ?: break
                                val reply = if (cmd.startsWith("STAT")) "223 0 <x@test> article exists" else "205 bye"
                                out.write("$reply\r\n".toByteArray())
                                out.flush()
                                if (cmd == "QUIT") break
                            }
                        } catch (_: IOException) {
                            // The client refused the certificate, or went away.
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        return serverSocket
    }

    private fun config(verifyHostname: Boolean = true) =
        NntpConfig("localhost", server.localPort, "", "", useTls = true, maxConnections = 1, verifyHostname = verifyHostname)

    @Test
    fun `a pool refuses a certificate that names another host`() = runBlocking<Unit> {
        NntpStreamingService(config()).use { service ->
            service.connect()
            val failure = assertFailsWith<NntpConnectionException> {
                withTimeout(10_000) { service.statAcrossPools("<x@test>") }
            }
            assertTrue(
                generateSequence<Throwable>(failure) { it.cause }.any { it is CertificateException },
                "the STAT should fail on the certificate: $failure"
            )
        }
    }

    @Test
    fun `a pool whose config turns hostname verification off accepts a certificate that names another host`() =
        runBlocking<Unit> {
            NntpStreamingService(config(verifyHostname = false)).use { service ->
                service.connect()
                assertIs<StatResult.Found>(withTimeout(10_000) { service.statAcrossPools("<x@test>") })
            }
        }

    @Test
    fun `the builder keeps a config's hostname verification`() {
        NzbStreamer.fromConfig(config(verifyHostname = false)).use { streamer ->
            assertEquals(listOf(false), streamer.getPoolConfigs().map { it.verifyHostname })
        }
        NzbStreamer { nntp { host = "localhost"; verifyHostname = false } }.use { streamer ->
            assertEquals(listOf(false), streamer.getPoolConfigs().map { it.verifyHostname })
        }
    }

    private companion object {
        const val PASSWORD = "changeit"
    }
}
