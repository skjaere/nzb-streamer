package io.skjaere.nzbstreamer

import java.io.Closeable
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * An NNTP server for tests that need to control when answers come. Each connection is served by
 * its own thread, which reads one command, answers it, then reads the next: pipelined commands
 * wait in the socket meanwhile, as on a server that answers them one by one.
 *
 * [stat] answers `STAT <id>` (true: 223, false: 430) and [body] answers `BODY <id>` (an encoded
 * yEnc article, or null for 430). Both get the id with its angle brackets and may block, e.g. on a
 * latch, which holds back that connection's later answers too. When [dropAfter] gives a number for
 * an article, the connection drops after that many bytes of its body, as a provider's can; when
 * [dropOnStat] is true for an id, the connection drops instead of answering its STAT, and when
 * [statError] gives a status line for an id, that line answers its STAT. [login] takes the user and
 * password of an AUTHINFO login (true: 281, false: 481); when [loginError] gives a status line for
 * a user, that line answers its AUTHINFO PASS instead (e.g. 502 for too many connections). [quit]
 * runs before a QUIT is answered and may block.
 *
 * Its connections can die after sitting idle, as a NAT, a VPN or a provider ends a flow nobody used
 * for a while. Each connection notes when its last answer ended (the greeting's, at first). A
 * command that comes once the connection has been idle [idleAfterMs] or longer finds it dead for
 * good, in the way [onIdle] says. Any answer keeps a connection alive, a DATE's as any other, as a
 * keepalive refreshes a NAT's entry. Null [idleAfterMs]: none die. Only [affected] of every [outOf]
 * connections can die, by accept order from 0 (K of M): connection i when `i % outOf < affected`.
 * On loopback, as through a NAT that dropped the flow, a silent connection's writes are still taken:
 * the client sees no answer, not an error.
 */
class FakeNntpServer(
    private val stat: (id: String) -> Boolean = { false },
    private val body: (id: String) -> ByteArray? = { null },
    private val dropAfter: (id: String) -> Int? = { null },
    private val dropOnStat: (id: String) -> Boolean = { false },
    private val statError: (id: String) -> String? = { null },
    private val login: (user: String, password: String) -> Boolean = { _, _ -> true },
    private val loginError: (user: String) -> String? = { null },
    private val quit: () -> Unit = {},
    private val idleAfterMs: Long? = null,
    private val onIdle: OnIdle = OnIdle.SILENT,
    private val affected: Int = 1,
    private val outOf: Int = 1,
) : Closeable {

    /** How a connection dies once idle ([idleAfterMs]). */
    enum class OnIdle {
        /** Reads and drops whatever comes, never writes, never closes. */
        SILENT,

        /** Closes with RST (SO_LINGER 0): the client's next read fails with a reset. */
        RST,

        /** Closes with FIN: the client's next read finds the stream's end. */
        FIN,

        /** Writes "400 idle timeout", then closes with FIN: the client reads the 400 as its command's answer. */
        KICK,
    }

    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val sockets = CopyOnWriteArrayList<Socket>()

    val port: Int get() = server.localPort

    /** Connections accepted so far. */
    val connections = AtomicInteger()

    /** Connections the idle rule ended ([idleAfterMs]): gone silent, or closed. */
    val silenced = AtomicInteger()

    /** Commands read and dropped on connections gone silent. */
    val dropped = AtomicInteger()

    /** STAT ids in the order this server read them (over all connections; not those a dead connection dropped). */
    val statsRead = CopyOnWriteArrayList<String>()

    /** BODY ids in the order this server read them (over all connections; not those a dead connection dropped). */
    val bodiesRead = CopyOnWriteArrayList<String>()

    init {
        thread(isDaemon = true, name = "fake-nntp-accept") {
            while (!server.isClosed) {
                val socket = runCatching { server.accept() }.getOrNull() ?: break
                val index = connections.getAndIncrement()
                sockets += socket
                thread(isDaemon = true, name = "fake-nntp-conn") { runCatching { serve(socket, index) } }
            }
        }
    }

    private fun serve(socket: Socket, index: Int) = socket.use {
        val reader = socket.getInputStream().bufferedReader(Charsets.ISO_8859_1)
        val out = socket.getOutputStream()
        out.line("200 fake server ready")
        var lastAnswerEnd = System.nanoTime()
        var silent = false
        var user = ""
        while (true) {
            val command = reader.readLine() ?: break
            if (silent) {
                dropped.incrementAndGet()
                continue
            }
            if (diesAt(index, lastAnswerEnd)) {
                silenced.incrementAndGet()
                when (onIdle) {
                    OnIdle.SILENT -> {
                        silent = true
                        dropped.incrementAndGet()
                        continue
                    }
                    OnIdle.RST -> socket.setSoLinger(true, 0)
                    OnIdle.FIN -> socket.shutdownOutput()
                    OnIdle.KICK -> {
                        out.line("400 idle timeout")
                        socket.shutdownOutput()
                    }
                }
                break
            }
            val id = command.substringAfter(' ', "").trim()
            when (command.substringBefore(' ').uppercase()) {
                "STAT" -> {
                    statsRead += id
                    if (dropOnStat(id)) break
                    out.line(statError(id) ?: if (stat(id)) "223 0 $id" else "430 No such article")
                }
                "AUTHINFO" -> {
                    val value = id.substringAfter(' ', "")
                    when (id.substringBefore(' ').uppercase()) {
                        "USER" -> {
                            user = value
                            out.line("381 Password required")
                        }
                        "PASS" -> out.line(
                            loginError(user)
                                ?: if (login(user, value)) "281 Authentication accepted" else "481 Authentication failed"
                        )
                        else -> out.line("501 Syntax error")
                    }
                }
                "BODY" -> {
                    bodiesRead += id
                    val article = body(id)
                    val drop = article?.let { dropAfter(id) }
                    if (article == null) {
                        out.line("430 No such article")
                    } else if (drop != null) {
                        out.write("222 0 $id\r\n".toByteArray(Charsets.ISO_8859_1))
                        out.write(article, 0, minOf(drop, article.size))
                        out.flush()
                        break
                    } else {
                        out.write("222 0 $id\r\n".toByteArray(Charsets.ISO_8859_1))
                        out.write(article)
                        out.line(".")
                    }
                }
                "DATE" -> out.line("111 20261015000000")
                "QUIT" -> {
                    quit()
                    out.line("205 bye")
                    break
                }
                else -> out.line("500 What?")
            }
            lastAnswerEnd = System.nanoTime()
        }
    }

    /** Whether connection [index], whose last answer ended at [lastAnswerEnd], is dead for a command now. */
    private fun diesAt(index: Int, lastAnswerEnd: Long): Boolean {
        val after = idleAfterMs ?: return false
        return index % outOf < affected && System.nanoTime() - lastAnswerEnd >= after * NANOS_PER_MS
    }

    private fun OutputStream.line(text: String) {
        write("$text\r\n".toByteArray(Charsets.ISO_8859_1))
        flush()
    }

    override fun close() {
        server.close()
        sockets.forEach { runCatching { it.close() } }
    }

    private companion object {
        const val NANOS_PER_MS = 1_000_000L
    }
}
