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
 * password of an AUTHINFO login (true: 281, false: 481). [quit] runs before a QUIT is answered and
 * may block.
 */
class FakeNntpServer(
    private val stat: (id: String) -> Boolean = { false },
    private val body: (id: String) -> ByteArray? = { null },
    private val dropAfter: (id: String) -> Int? = { null },
    private val dropOnStat: (id: String) -> Boolean = { false },
    private val statError: (id: String) -> String? = { null },
    private val login: (user: String, password: String) -> Boolean = { _, _ -> true },
    private val quit: () -> Unit = {},
) : Closeable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val sockets = CopyOnWriteArrayList<Socket>()

    val port: Int get() = server.localPort

    /** Connections accepted so far. */
    val connections = AtomicInteger()

    /** STAT ids in the order this server read them (over all connections). */
    val statsRead = CopyOnWriteArrayList<String>()

    /** BODY ids in the order this server read them (over all connections). */
    val bodiesRead = CopyOnWriteArrayList<String>()

    init {
        thread(isDaemon = true, name = "fake-nntp-accept") {
            while (!server.isClosed) {
                val socket = runCatching { server.accept() }.getOrNull() ?: break
                connections.incrementAndGet()
                sockets += socket
                thread(isDaemon = true, name = "fake-nntp-conn") { runCatching { serve(socket) } }
            }
        }
    }

    private fun serve(socket: Socket) = socket.use {
        val reader = socket.getInputStream().bufferedReader(Charsets.ISO_8859_1)
        val out = socket.getOutputStream()
        out.line("200 fake server ready")
        var user = ""
        while (true) {
            val command = reader.readLine() ?: break
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
                        "PASS" -> out.line(if (login(user, value)) "281 Authentication accepted" else "481 Authentication failed")
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
        }
    }

    private fun OutputStream.line(text: String) {
        write("$text\r\n".toByteArray(Charsets.ISO_8859_1))
        flush()
    }

    override fun close() {
        server.close()
        sockets.forEach { runCatching { it.close() } }
    }
}
